"""Sync insider.filings.raw.v1 observations into form4.trades.

The missing half of the insider cutover. The dataplane ingests Form 4 filings
into signal_observations; the entire product — /feed, /explore, the leaderboard,
every score — reads form4.trades, which today is written directly by
fetch_latest.py under the insider-fetch launchd job. Nothing connects the two.
Retiring insider-fetch without this would leave ingestion working perfectly
into a table nothing reads, which is the exact failure mode this migration
exists to remove (congress sat frozen for 4.5 months for that reason).

Reuses insert_trades() from backfill_live rather than writing its own INSERT.
That function owns insider identity resolution (get_or_create_insider), the
entity flag, normalized titles, and a guard against future-dated trades from
issuer year typos. Reimplementing any of that would be a second definition of
"what a trade row is", and the two would drift.

The reuse goes further than convenience: fetch_latest.py and
insider.filings.raw.v1 already call the SAME fetch + parse functions
(fetch_all_form4_filings, fetch_form4_xml, parse_form4_xml). The parsed dicts
are identical by construction, which is why ingestion parity measured 100.000%
rather than merely high. The only thing that ever differed was the sink. This
asset makes the sinks agree too.

Idempotent: insert_trades relies on idx_trades_dedup_v2
(insider_id, ticker, trade_date, trade_type, value, trans_code) and skips rows
that already exist, so re-running a partition adds nothing and — critically —
cannot clobber the ~60 enrichment columns (signal_grade, pit_grade,
is_recurring, is_tax_sale, pit_cluster_size, is_largest_ever, …) that the
form4_pipeline computes downstream.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
from dagster import AssetKey, MetadataValue, Output, asset

from dagster_project.assets.signals import daily_partitions

# backfill_live lives in the trading-framework repo, outside this package.
# Guarded at call time as well as import time — a Dagster op runs in a
# subprocess, and module-scope sys.path setup alone was not enough for the
# congress signal (it failed the nightly on 2026-08-12 and 08-13).
_REPO_ROOT = Path(__file__).resolve().parents[3].parent


def _ensure_repo_on_path() -> None:
    for p in (str(_REPO_ROOT), str(_REPO_ROOT / "strategies" / "insider_catalog")):
        if p not in sys.path:
            sys.path.insert(0, p)


_ensure_repo_on_path()

INSIDER_SIGNAL_KEY = AssetKey(["insider", "filings", "raw", "v1.0.0"])
SIGNAL_PREFIX = "insider.filings.raw"

# Fields insert_trades() reads off each parsed trade dict. Rebuilt from the
# observation payload, which stored exactly what the parser produced.
_TRADE_FIELDS = (
    "ticker", "insider_name", "title", "trade_type", "trans_code",
    "trade_date", "filing_date", "price", "qty", "value", "cik", "company",
    "is_csuite", "is_10b5_1", "security_title", "direct_indirect",
    "trans_acquired_disp",
)


def _observations_for(day: str, dsn: str) -> dict:
    """Parsed trades for one partition, grouped by accession.

    Grouped because insert_trades takes an accession per call — it is the unit
    a filing is marked processed by.
    """
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT value
                     FROM signal_observations
                    WHERE signal_id LIKE %s
                      AND value->>'filing_date' = %s""",
                (f"{SIGNAL_PREFIX}%", day),
            )
            rows = [r["value"] for r in cur.fetchall()]
    finally:
        conn.close()

    by_accession: dict = {}
    for v in rows:
        acc = v.get("accession")
        if not acc:
            continue
        by_accession.setdefault(acc, {"filed_at": v.get("accepted_at"), "trades": []})
        by_accession[acc]["trades"].append({k: v.get(k) for k in _TRADE_FIELDS})
    return by_accession


@asset(
    name="insider_trades_form4_sync",
    deps=[INSIDER_SIGNAL_KEY],
    partitions_def=daily_partitions,
    group_name="bridges",
    description="Upsert dataplane-ingested Form 4 trades into form4.trades, "
                "the read model the whole product queries.",
)
def insider_trades_form4_sync(context) -> Output:
    """Sync one partition's filings into form4.trades.

    Set PYRRHO_INSIDER_SYNC=off to make this a no-op. It ships disabled: while
    insider-fetch is still running, both writers would target the same rows,
    and although the dedup index makes that safe it makes the cutover
    unmeasurable — you could not tell which writer produced a row. The flip in
    B4 is: stop insider-fetch, set this to on.
    """
    _ensure_repo_on_path()
    from strategies.insider_catalog.backfill_live import insert_trades  # noqa: E402
    from config.database import get_connection  # noqa: E402

    day = context.partition_key
    enabled = os.environ.get("PYRRHO_INSIDER_SYNC", "off").lower() == "on"
    dp_dsn = os.environ.get("PYRRHO_DATAPLANE_DSN",
                            "dbname=pyrrho_data_dev host=localhost")

    by_accession = _observations_for(day, dp_dsn)
    n_filings = len(by_accession)
    n_trades = sum(len(v["trades"]) for v in by_accession.values())

    if not enabled:
        context.log.info(
            "insider sync DISABLED (PYRRHO_INSIDER_SYNC != on): "
            "%d filing(s), %d trade(s) available for %s", n_filings, n_trades, day
        )
        return Output(
            None,
            metadata={
                "enabled": False,
                "filings_available": n_filings,
                "trades_available": n_trades,
                "partition": day,
            },
        )

    conn = get_connection()
    inserted = 0
    try:
        for acc, payload in by_accession.items():
            inserted += insert_trades(
                conn, payload["trades"], acc, filed_at=payload["filed_at"]
            ) or 0
        conn.commit()
    finally:
        conn.close()

    context.log.info(
        "insider sync %s: %d filing(s), %d trade(s) seen, %d row(s) inserted",
        day, n_filings, n_trades, inserted,
    )
    return Output(
        None,
        metadata={
            "enabled": True,
            "filings": n_filings,
            "trades_seen": n_trades,
            "rows_inserted": MetadataValue.int(inserted),
            "partition": day,
        },
    )

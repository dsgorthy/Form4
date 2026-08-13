"""Sync congress.trades.raw.v1 observations into form4.congress_trades.

The dataplane owns ingestion; form4.congress_trades is a read model that the
existing API (/congress, /congress/by-ticker) and the company + screener
pages already query. Syncing keeps the product unchanged while the dataplane
takes over the feed.

This exists as a Dagster asset rather than a script someone remembers to run.
A manual sync step would reintroduce exactly the failure this whole migration
is removing: an ingestion that works but never reaches the thing that reads
it. Congress was frozen for 4.5 months for that precise reason — the scraper
wrote to a database nothing queried.

Depends on the congress signal asset, so within one partition the ordering is
enforced by the graph rather than by wall-clock scheduling.
"""
from __future__ import annotations

import os
import re

import psycopg2
from dagster import AssetKey, MetadataValue, Output, asset

from dagster_project.assets.signals import daily_partitions

CONGRESS_SIGNAL_KEY = AssetKey(["congress", "trades", "raw", "v1.0.0"])
SIGNAL_PREFIX = "congress.trades.raw"


def _normalize_name(name: str) -> str:
    """Match the legacy scraper's normalization so existing politicians reuse."""
    return re.sub(r"[^a-z ]", "", (name or "").lower()).strip()


@asset(
    name="congress_trades_form4_sync",
    deps=[CONGRESS_SIGNAL_KEY],
    partitions_def=daily_partitions,
    group_name="bridges",
    description="Upsert congress disclosures into form4.congress_trades "
                "(read model for /congress and the company pages).",
)
def congress_trades_form4_sync(context) -> Output:
    """Upsert one partition's disclosures into the form4 read model."""
    day = context.partition_key
    dp_dsn = os.environ.get("PYRRHO_DATAPLANE_DSN",
                            "dbname=pyrrho_data_dev host=localhost")
    f4_dsn = os.environ.get("FORM4_DSN", "dbname=form4 host=localhost")

    dp = psycopg2.connect(dp_dsn)
    cur = dp.cursor()
    cur.execute(
        """SELECT ticker, value
             FROM signal_observations
            WHERE signal_id LIKE %s
              AND value->>'filing_date' = %s""",
        (f"{SIGNAL_PREFIX}%", day),
    )
    rows = cur.fetchall()
    cur.close()
    dp.close()

    if not rows:
        return Output(
            0,
            metadata={"partition": day, "observations": 0,
                      "note": MetadataValue.text("no disclosures published this day")},
        )

    f4 = psycopg2.connect(f4_dsn)
    fcur = f4.cursor()
    fcur.execute("SELECT politician_id, name_normalized FROM politicians")
    politicians = {n: pid for pid, n in fcur.fetchall() if n}

    inserted = already = skipped = new_pols = 0
    for ticker, value in rows:
        v = value or {}
        name = v.get("politician")
        if not name or not ticker:
            skipped += 1
            continue

        norm = _normalize_name(name)
        pid = politicians.get(norm)
        if pid is None:
            fcur.execute(
                """INSERT INTO politicians (name, name_normalized, chamber, state, party)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING politician_id""",
                (name, norm, v.get("chamber"), v.get("state"), v.get("party")),
            )
            pid = fcur.fetchone()[0]
            politicians[norm] = pid
            new_pols += 1

        # Idempotent via congress_trades_natural_key
        # (migrations/2026-08-12_congress_trades_natural_key.sql).
        fcur.execute(
            """INSERT INTO congress_trades
                   (politician_id, ticker, company, trade_type, trade_date,
                    filing_date, value_low, value_high, value_estimate,
                    owner, report_url, source)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                pid, ticker, v.get("company"), v.get("trade_type"),
                v.get("trade_date"), v.get("filing_date"),
                v.get("value_low"), v.get("value_high"), v.get("value_estimate"),
                v.get("owner"), v.get("report_url"),
                "dataplane:congress.trades.raw.v1",
            ),
        )
        if fcur.rowcount:
            inserted += 1
        else:
            already += 1

    f4.commit()
    fcur.close()
    f4.close()

    context.log.info("congress sync %s: inserted=%d already=%d skipped=%d",
                     day, inserted, already, skipped)
    return Output(
        inserted,
        metadata={
            "partition": day,
            "observations": len(rows),
            "inserted": inserted,
            "already_present": already,
            "skipped_no_ticker_or_name": skipped,
            "new_politicians": new_pols,
        },
    )

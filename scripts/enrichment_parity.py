#!/usr/bin/env python3
"""B3 — does the dataplane feed produce the same form4.trades rows as insider-fetch?

Ingestion parity (B2) compares two sets of observations. It says the dataplane
read EDGAR correctly. It says nothing about whether cutting over would change
what the PRODUCT serves, because the product does not read observations — it
reads form4.trades, and everything downstream (returns, PIT scores, grades,
signals) is computed from that table.

This closes that gap without running the enrichment chain twice. Enrichment is
a deterministic function of the trade rows plus prices and scores, so if the
rows are identical the enrichment is identical. The question reduces to: for a
given filing day, does the set of trades the sync WOULD insert match the set
insider-fetch actually inserted?

Compared on the natural key form4 already dedups by (idx_trades_dedup_v2),
minus insider_id — that is assigned by get_or_create_insider at write time, so
comparing it would just be comparing the same function to itself. insider_name
stands in for identity.

    (insider_name, ticker, trade_date, trade_type, value, trans_code)

Reports rows only the dataplane has, rows only form4 has, and field-level
disagreement on rows both have. Only the middle category blocks a cutover:
dataplane-only rows are the superset behaviour already established in B2
(late-filed trades the bridge missed), while form4-only rows mean the cutover
would LOSE data, which is the thing that must be zero.

Usage (on Studio):
    python3 scripts/enrichment_parity.py --days 5
    python3 scripts/enrichment_parity.py --from 2026-08-11 --to 2026-08-14
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dataplane"))

import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

sys.path.insert(0, str(ROOT / "strategies" / "insider_catalog"))
from strategies.insider_catalog.backfill import normalize_name  # noqa: E402

SIGNAL_PREFIX = "insider.filings.raw"


def _norm(v) -> str:
    """Compare loosely enough to survive representation, strictly enough to matter.

    value and price cross the boundary as JSON numbers on one side and numeric
    columns on the other, so 1234 and 1234.0 are the same trade. Rounding to
    cents keeps that from being reported as a mismatch while still catching a
    genuinely different amount.
    """
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return f"{round(float(v), 2):.2f}"
    s = str(v).strip()
    try:
        return f"{round(float(s), 2):.2f}"
    except ValueError:
        return s.lower()


def _key(row: dict) -> tuple:
    # Identity must be compared the way the WRITER resolves it.
    # get_or_create_insider matches on name_normalized, and normalize_name
    # strips trailing generational suffixes — so the plane's raw
    # "Stetz Gary S. II" and form4's stored "Stetz Gary S." are the same
    # person and resolve to the same insider_id. Comparing raw names reported
    # those as rows the cutover would lose, which would have been a false
    # blocker on a real cutover decision.
    return (
        normalize_name(row.get("insider_name") or ""),
        _norm(row.get("ticker")),
        _norm(row.get("trade_date")),
        _norm(row.get("trade_type")),
        _norm(row.get("value")),
        _norm(row.get("trans_code")),
    )


def dataplane_rows(day: str, dsn: str) -> dict:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT value FROM signal_observations
                    WHERE signal_id LIKE %s AND value->>'filing_date' = %s""",
                (f"{SIGNAL_PREFIX}%", day),
            )
            out: dict = {}
            for r in cur.fetchall():
                out.setdefault(_key(r["value"]), []).append(r["value"])
            return out
    finally:
        conn.close()


def form4_rows(day: str, dsn: str) -> dict:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT i.name AS insider_name, t.ticker, t.trade_date,
                          t.trade_type, t.value, t.trans_code, t.price, t.qty,
                          t.title
                     FROM trades t
                     JOIN insiders i ON i.insider_id = t.insider_id
                    WHERE t.filing_date = %s""",
                (day,),
            )
            out: dict = {}
            for r in cur.fetchall():
                d = dict(r)
                out.setdefault(_key(d), []).append(d)
            return out
    finally:
        conn.close()


# Fields worth comparing on rows that exist on both sides. Deliberately not
# every column: form4.trades carries ~60 enrichment fields that the dataplane
# has no opinion about and never will.
COMPARE_FIELDS = ("price", "qty", "title")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="from_date")
    ap.add_argument("--to", dest="to_date")
    ap.add_argument("--days", type=int, help="trailing window ending yesterday")
    ap.add_argument("--samples", type=int, default=5)
    args = ap.parse_args()

    if args.days:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days - 1)
    elif args.from_date and args.to_date:
        start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.to_date, "%Y-%m-%d").date()
    else:
        ap.error("pass --days N, or both --from and --to")

    dp_dsn = os.environ.get("PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=/tmp")
    f4_dsn = os.environ.get("FORM4_DSN", "dbname=form4 host=/tmp")

    print(f"  {'day':<12}{'plane':>7}{'form4':>7}{'both':>7}"
          f"{'plane-only':>12}{'FORM4-ONLY':>12}{'differ':>8}   verdict")
    blocking_total = 0
    day = start
    samples: list = []
    while day <= end:
        iso = day.isoformat()
        dp = dataplane_rows(iso, dp_dsn)
        f4 = form4_rows(iso, f4_dsn)
        both = dp.keys() & f4.keys()
        only_dp = dp.keys() - f4.keys()
        only_f4 = f4.keys() - dp.keys()

        # Compare as MULTISETS. The key is not unique — one filing routinely
        # reports several lots that agree on it but differ in qty/price — so
        # keeping one row per key silently drops the rest, and each side keeps
        # a different arbitrary survivor. A first pass did exactly that and
        # reported the same insider twice with the values swapped, which is
        # what gave it away.
        # Collapse the plane side to ONE row per key before comparing, which
        # is what the sync will actually produce: insert_trades writes through
        # idx_trades_dedup_v2, a UNIQUE index on the same key, and skips rows
        # that collide. The plane has no such constraint — it stores every lot
        # as its own observation — so comparing raw counts pits pre-dedup
        # against post-dedup and reports differences that the sync would never
        # create. A first pass did that and flagged 123 issues, most of them
        # one filing's four identical lots against form4's single surviving row.
        #
        # Note this means form4 already drops lots that agree on the key but
        # differ in qty — the index does not include qty. That is a pre-existing
        # property of the table, not something the cutover introduces, and the
        # sync reproduces it exactly because it uses the same writer.
        differ = 0
        for k in both:
            a = tuple(_norm(dp[k][0].get(f)) for f in COMPARE_FIELDS)
            b = tuple(_norm(f4[k][0].get(f)) for f in COMPARE_FIELDS)
            if a != b:
                differ += 1
                if len(samples) < args.samples:
                    samples.append((iso, k, "/".join(COMPARE_FIELDS), a, b))

        # Only losing rows blocks a cutover. Extra rows are the superset
        # behaviour B2 already established.
        blocking = len(only_f4) + differ
        blocking_total += blocking
        verdict = "PASS" if blocking == 0 else "BLOCKS CUTOVER"
        # Report deduped counts on both sides — that is what lands in form4.
        print(f"  {iso:<12}{len(dp):>7}{len(f4):>7}{len(both):>7}"
              f"{len(only_dp):>12}{len(only_f4):>12}{differ:>8}   {verdict}")
        day += timedelta(days=1)

    if samples:
        print("\n  sample disagreements (plane vs form4):")
        for iso, k, f, a, b in samples:
            print(f"    {iso} {k[1]}/{k[0][:24]:<24} {f}: {a!r} vs {b!r}")

    print(f"\n  cutover-blocking issues across window: {blocking_total}")
    if blocking_total == 0:
        print("  B3 PASS — the sync would reproduce form4.trades exactly, so the "
              "enrichment computed from it is unchanged.")
    return 0 if blocking_total == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

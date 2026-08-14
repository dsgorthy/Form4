#!/usr/bin/env python3
"""Measure candidate-vs-baseline signal parity and record it per day.

The cutover gate for retiring the form4 bridge is "recall >= 99.5% held for N
consecutive days". `python3 -m dataplane parity` answers what parity is right
now over an arbitrary range; it cannot answer whether the number has been
stable, and one good day is indistinguishable from a lucky run.

This walks a date range one day at a time, records each day into
parity_history, and prints the streak against the gate.

Deliberately runs per-day rather than over the whole range at once: a range
comparison averages a bad day away, and the gate is about consecutive days.

Re-running a day overwrites it — a late-arriving filing that improves an
earlier day should be reflected, not double-counted.

Usage (on Studio):
    python3 scripts/record_parity.py --from 2026-08-10 --to 2026-08-12
    python3 scripts/record_parity.py --days 5            # trailing window
    python3 scripts/record_parity.py --days 5 --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_DATAPLANE = Path(__file__).resolve().parents[1] / "dataplane"
sys.path.insert(0, str(_DATAPLANE))

import psycopg2  # noqa: E402

from dataplane.parity import compare as parity_compare  # noqa: E402

CANDIDATE = "insider.filings.raw.v1"
BASELINE = "insider.trades.raw.v1"
GATE = 99.5


def _dsn() -> str:
    return os.environ.get("PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=/tmp")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--candidate", default=CANDIDATE)
    ap.add_argument("--baseline", default=BASELINE)
    ap.add_argument("--from", dest="from_date")
    ap.add_argument("--to", dest="to_date")
    ap.add_argument("--days", type=int, help="trailing window ending yesterday")
    ap.add_argument("--gate", type=float, default=GATE)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.days:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days - 1)
    elif args.from_date and args.to_date:
        start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.to_date, "%Y-%m-%d").date()
    else:
        ap.error("pass --days N, or both --from and --to")

    rows = []
    day = start
    while day <= end:
        iso = day.isoformat()
        r = parity_compare(args.candidate, args.baseline, iso, iso)
        # Distinct denominators: `matched` counts distinct fingerprints, so
        # dividing it by a row count understates agreement wherever a signal
        # emits the same fingerprint twice (the bridge does).
        cov_b = (r.matched / r.distinct_b * 100) if r.distinct_b else None
        cov_a = (r.matched / r.distinct_a * 100) if r.distinct_a else None
        rows.append((day, r, cov_b, cov_a))
        day += timedelta(days=1)

    print(f"  {args.candidate} vs {args.baseline}   gate: recall >= {args.gate}%")
    print(f"  {'day':<12}{'A':>8}{'B':>8}{'match':>8}{'recall':>9}   status")
    for day, r, cov_b, cov_a in rows:
        if cov_b is None:
            status, shown = "no baseline rows", "  n/a"
        else:
            status = "PASS" if cov_b >= args.gate else "below gate"
            shown = f"{cov_b:6.2f}%"
        print(f"  {day.isoformat():<12}{r.distinct_a:>8}{r.distinct_b:>8}"
              f"{r.matched:>8}{shown:>9}   {status}")

    # Consecutive passing days ending at the most recent measured day — the
    # thing the gate actually asks about.
    streak = 0
    for _, _, cov_b, _ in reversed(rows):
        if cov_b is not None and cov_b >= args.gate:
            streak += 1
        else:
            break
    print(f"  current passing streak: {streak} day(s)")

    if args.dry_run:
        print("  DRY RUN — nothing recorded")
        return 0

    conn = psycopg2.connect(_dsn())
    with conn, conn.cursor() as cur:
        for day, r, cov_b, cov_a in rows:
            cur.execute(
                """INSERT INTO parity_history
                     (signal_a, signal_b, trade_day, count_a, count_b, matched,
                      only_in_a, only_in_b, coverage_b, coverage_a, measured_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                   ON CONFLICT (signal_a, signal_b, trade_day) DO UPDATE SET
                     count_a=EXCLUDED.count_a, count_b=EXCLUDED.count_b,
                     matched=EXCLUDED.matched, only_in_a=EXCLUDED.only_in_a,
                     only_in_b=EXCLUDED.only_in_b,
                     coverage_b=EXCLUDED.coverage_b, coverage_a=EXCLUDED.coverage_a,
                     measured_at=NOW()""",
                (args.candidate, args.baseline, day, r.count_a, r.count_b,
                 r.matched, r.only_in_a, r.only_in_b,
                 None if cov_b is None else round(cov_b, 3),
                 None if cov_a is None else round(cov_a, 3)),
            )
    conn.close()
    print(f"  recorded {len(rows)} day(s) into parity_history")
    return 0


if __name__ == "__main__":
    sys.exit(main())

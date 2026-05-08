#!/usr/bin/env python3
"""V2-vs-V3 distribution comparison report.

Reads insider_ticker_scores (V2) and insider_ticker_scores_v3, joins on
(insider_id, ticker, as_of_date), and reports:
  - Grade distribution shift (how many insiders changed tier)
  - Top movers (D->A class, A->D class)
  - Per-grade aggregate stats
  - Sanity check on Baker

Usage (on Studio):
    python3 -m strategies.insider_catalog.compare_v2_v3
    python3 -m strategies.insider_catalog.compare_v2_v3 --latest-only
"""
from __future__ import annotations

import argparse
from collections import Counter

from config.database import get_connection
from strategies.insider_catalog.pit_scoring import pit_score_to_grade


GRADE_ORDER = ["A+", "A", "B", "C", "D", None]


def grade_index(g):
    try:
        return GRADE_ORDER.index(g)
    except ValueError:
        return len(GRADE_ORDER)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-only", action="store_true",
                        help="Only compare the latest as_of_date per (insider, ticker)")
    args = parser.parse_args()

    if args.latest_only:
        join = """
            JOIN (
                SELECT insider_id, ticker, MAX(as_of_date) AS as_of_date
                FROM insider_ticker_scores
                GROUP BY insider_id, ticker
            ) latest USING (insider_id, ticker, as_of_date)
        """
    else:
        join = ""

    sql = f"""
        SELECT v2.insider_id, v2.ticker, v2.as_of_date,
               v2.blended_score AS v2_score,
               v3.blended_score AS v3_score,
               v2.ticker_trade_count AS v2_n,
               v3.ticker_trade_count AS v3_n
        FROM insider_ticker_scores v2
        JOIN insider_ticker_scores_v3 v3 USING (insider_id, ticker, as_of_date)
        {join}
    """

    with get_connection() as conn:
        rows = conn.execute(sql).fetchall()

    n = len(rows)
    if n == 0:
        print("No paired rows. Run backfill first.")
        return

    print(f"Comparing {n:,} rows (mode={'latest-only' if args.latest_only else 'all'})")
    print()

    # Grade distribution shift
    v2_grades = Counter()
    v3_grades = Counter()
    transitions = Counter()
    for r in rows:
        g2 = pit_score_to_grade(r["v2_score"])
        g3 = pit_score_to_grade(r["v3_score"])
        v2_grades[g2] += 1
        v3_grades[g3] += 1
        transitions[(g2, g3)] += 1

    print("Grade distribution:")
    print(f"  {'grade':<6} {'V2':<10} {'V3':<10}  diff")
    for g in GRADE_ORDER:
        v2c = v2_grades.get(g, 0)
        v3c = v3_grades.get(g, 0)
        diff = v3c - v2c
        sign = "+" if diff > 0 else ""
        print(f"  {str(g):<6} {v2c:>9,} {v3c:>9,}  {sign}{diff:,}")
    print()

    # Top transitions
    print("Top grade transitions (V2 -> V3):")
    for (g2, g3), count in sorted(transitions.items(), key=lambda x: -x[1])[:10]:
        marker = "  " if g2 == g3 else "->"
        print(f"  {str(g2):<4} {marker} {str(g3):<4}  {count:>9,}")
    print()

    # Movers — biggest score increases and decreases
    movers = []
    for r in rows:
        v2s = r["v2_score"]
        v3s = r["v3_score"]
        if v2s is None or v3s is None:
            continue
        delta = v3s - v2s
        movers.append((delta, r["insider_id"], r["ticker"], v2s, v3s, r["v2_n"], r["v3_n"]))

    movers.sort(key=lambda x: -x[0])  # by delta desc

    print("Top 10 score INCREASES (V2 -> V3):")
    for d, iid, t, v2s, v3s, v2n, v3n in movers[:10]:
        g2 = pit_score_to_grade(v2s)
        g3 = pit_score_to_grade(v3s)
        print(f"  +{d:6.3f}  {iid}/{t:<6}  {v2s:.2f}({g2})->{v3s:.2f}({g3})  n={v2n}->{v3n}")
    print()

    print("Top 10 score DECREASES (V2 -> V3):")
    for d, iid, t, v2s, v3s, v2n, v3n in movers[-10:][::-1]:
        g2 = pit_score_to_grade(v2s)
        g3 = pit_score_to_grade(v3s)
        print(f"  {d:7.3f}  {iid}/{t:<6}  {v2s:.2f}({g2})->{v3s:.2f}({g3})  n={v2n}->{v3n}")
    print()

    # Counts
    counts_v2 = sum(r["v2_n"] for r in rows if r["v2_n"])
    counts_v3 = sum(r["v3_n"] for r in rows if r["v3_n"])
    print(f"Sum of trade_counts: V2={counts_v2:,}  V3={counts_v3:,}  diff={counts_v3-counts_v2:+,}")

    zero_v2 = sum(1 for r in rows if (r["v2_n"] or 0) == 0)
    zero_v3 = sum(1 for r in rows if (r["v3_n"] or 0) == 0)
    print(f"Rows with 0 trade_count: V2={zero_v2:,}  V3={zero_v3:,}  diff={zero_v3-zero_v2:+,}")

    # Baker sanity check
    print()
    print("Baker (insider 3329) sanity check:")
    baker = [r for r in rows if r["insider_id"] == 3329]
    for r in baker[:3]:
        g2 = pit_score_to_grade(r["v2_score"])
        g3 = pit_score_to_grade(r["v3_score"])
        print(f"  {r['ticker']}/{r['as_of_date']}: V2 {r['v2_score']:.2f}({g2}) -> V3 {r['v3_score']:.2f}({g3})")


if __name__ == "__main__":
    main()

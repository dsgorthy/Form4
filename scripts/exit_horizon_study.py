#!/usr/bin/env python3
"""Which holding horizon actually pays, by insider grade. Read-only.

WHY NOW

The filing-anchored labels (abnormal_{3,5,7,10,21,42}td_from_filing) went from
28.9% populated in 2024 to 84-88% on 2026-08-27. They are the ONLY tradeable
labels -- every other abnormal_* column is anchored to trade_date, which nobody
can act on, and a model fitted on one of those scored a +6.85pp walk-forward
decile spread while having no ranking power on returns anyone could capture.

Until tonight there was not enough of them in the years the books trade to ask
this question. Now there is.

WHAT IS MEASURED

For each career_grade band and each horizon: mean and median abnormal return,
win rate, and the count. Abnormal is already SPY-relative, so a positive mean
is excess over holding the index for the same window.

THE UNIT IS THE EPISODE, NOT THE FILING

One insider buying RCG fourteen times in three weeks is ONE bet, not fourteen.
Counting filings inflates n by roughly the tranche count, shrinks every
confidence interval, and manufactures significance -- it produced two false
alarms on 2026-08-25 before it was caught. So filings by the same
(insider, ticker) within EPISODE_GAP_DAYS collapse to one observation, and the
episode takes its FIRST filing's entry and return.

WHAT THIS CANNOT ANSWER

Horizons stop at 42 trading days (~2 months) because that is the longest label
that exists. Price targets, trailing stops and stop losses are path-dependent
and need daily bars rather than endpoint returns; they are a separate study.

Usage:
    python3 scripts/exit_horizon_study.py
    python3 scripts/exit_horizon_study.py --since 2023-01-01
"""
from __future__ import annotations

import argparse
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

HORIZONS = (3, 5, 7, 10, 21, 42)
EPISODE_GAP_DAYS = 30

GRADE_BANDS = [
    ("A+/A", ("A+", "A")),
    ("B",    ("B",)),
    ("C/D",  ("C", "D")),
]


def fetch(conn, since: str):
    cols = ", ".join(f"r.abnormal_{h}td_from_filing AS h{h}" for h in HORIZONS)
    sql = f"""
        SELECT t.insider_id, t.ticker, t.filing_date, t.career_grade, {cols}
          FROM trades t JOIN trade_returns r USING (trade_id)
         WHERE t.signal_class = 'discretionary_buy'
           AND NOT COALESCE(t.value_suspect, FALSE)
           AND t.filing_date >= ?
           AND t.career_grade IS NOT NULL
           AND r.abnormal_21td_from_filing IS NOT NULL
         ORDER BY t.insider_id, t.ticker, t.filing_date
    """
    return conn.execute(sql, (since,)).fetchall()


def to_episodes(rows):
    """Collapse (insider, ticker) filings inside EPISODE_GAP_DAYS to one bet."""
    from datetime import date

    def d(s):
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))

    episodes, cur_key, last = [], None, None
    for r in rows:
        key = (r["insider_id"], r["ticker"])
        fd = d(r["filing_date"])
        if key != cur_key or last is None or (fd - last).days > EPISODE_GAP_DAYS:
            episodes.append(r)          # first filing of a new episode
            cur_key = key
        last = fd
    return episodes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    args = ap.parse_args()

    conn = get_connection(readonly=True)
    rows = fetch(conn, args.since)
    eps = to_episodes(rows)
    print(f"Exit-horizon study, discretionary buys filed {args.since} onward")
    print(f"{len(rows):,} filings collapse to {len(eps):,} episodes "
          f"({len(rows)/max(len(eps),1):.2f} filings per bet)\n")
    print("Abnormal return vs SPY, by career grade at filing. "
          "Episode-level; mean / median / win%\n")

    buckets = defaultdict(list)
    for r in eps:
        g = (r["career_grade"] or "").strip()
        for label, members in GRADE_BANDS:
            if g in members:
                buckets[label].append(r)
                break

    hdr = f"{'grade':<7}{'n':>7}  " + "".join(f"{h:>2}td{'':>13}" for h in HORIZONS)
    print(hdr)
    print("-" * len(hdr))
    for label, _ in GRADE_BANDS:
        rs = buckets.get(label, [])
        if not rs:
            continue
        line = f"{label:<7}{len(rs):>7}  "
        for h in HORIZONS:
            vals = [r[f"h{h}"] for r in rs if r[f"h{h}"] is not None]
            if not vals:
                line += f"{'--':>16}"
                continue
            mean = statistics.mean(vals) * 100
            med = statistics.median(vals) * 100
            win = 100.0 * sum(1 for v in vals if v > 0) / len(vals)
            line += f"{mean:>6.2f}/{med:>5.2f}/{win:>3.0f}%"
        print(line)

    # The spread is the signal: does the grade order returns at each horizon?
    print("\nA+/A minus C/D (percentage points of abnormal return):")
    for h in HORIZONS:
        a = [r[f"h{h}"] for r in buckets.get("A+/A", []) if r[f"h{h}"] is not None]
        c = [r[f"h{h}"] for r in buckets.get("C/D", []) if r[f"h{h}"] is not None]
        if a and c:
            print(f"  {h:>2}td: {100*(statistics.mean(a)-statistics.mean(c)):+.2f} pp "
                  f"(n={len(a):,} vs {len(c):,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

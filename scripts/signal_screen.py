#!/usr/bin/env python3
"""Which available signals actually order forward returns? Episode-level.

WHY NOW

Two signals reached usable coverage for the first time on 2026-08-27:
industry_buy_pct_90d went from 21.8% to 90-95% populated, and
net_buyer_flow_90d to its ceiling. Both are marked "Observational; no strategy
depends on it yet" in freshness_contracts -- not because they were rejected,
but because there was never enough of them to test. The same is true of the
filing-anchored labels this measures against, which were 28.9% populated in
2024 and are now 84-88%.

So this is the first time these questions can be asked at all.

METHOD

For each signal, split episodes at its median (or on the flag, for booleans)
and compare mean abnormal return at 21 trading days from filing. Reported as a
spread in percentage points with a t-statistic, so a signal that merely sorts
noise is visible as one.

THE UNIT IS THE EPISODE. One insider buying RCG fourteen times in three weeks
is one bet. Counting filings inflates n by ~3.4x here, shrinks every interval,
and manufactures significance -- it produced two false alarms on 2026-08-25.

WHAT A HIT HERE IS AND IS NOT

A spread with |t| > 2 says the signal orders returns on the full corpus. It
does NOT say it will help a book that already filters on grade, conviction and
concentration -- the candidates that survive those filters are not this
population. Anything promising here has to be re-tested inside the simulator.
"""
from __future__ import annotations

import argparse
import math
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.database import get_connection  # noqa: E402

HORIZON = 21
EPISODE_GAP_DAYS = 30

# (column, kind) — 'num' splits at the median, 'flag' splits on 0/1.
SIGNALS = [
    ("industry_buy_pct_90d",     "num"),
    ("net_buyer_flow_90d",       "num"),
    ("pit_cluster_size",         "num"),
    ("week52_proximity",         "num"),
    ("dip_3mo",                  "num"),
    ("dip_1mo",                  "num"),
    ("consecutive_sells_before", "num"),
    ("value",                    "num"),
    ("is_largest_ever",          "flag"),
    ("above_sma50",              "flag"),
    ("above_sma200",             "flag"),
    ("is_rare_reversal",         "flag"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--grade", default=None,
                    help="restrict to a grade band, e.g. 'A+,A,B'")
    args = ap.parse_args()

    cols = ", ".join(f"t.{c}" for c, _ in SIGNALS)
    where_grade = ""
    params = [args.since]
    if args.grade:
        marks = ",".join("?" for _ in args.grade.split(","))
        where_grade = f" AND t.career_grade IN ({marks})"
        params += args.grade.split(",")

    conn = get_connection(readonly=True)
    rows = conn.execute(f"""
        SELECT t.insider_id, t.ticker, t.filing_date, {cols},
               r.abnormal_{HORIZON}td_from_filing AS y
          FROM trades t JOIN trade_returns r USING (trade_id)
         WHERE t.signal_class = 'discretionary_buy'
           AND NOT COALESCE(t.value_suspect, FALSE)
           AND t.filing_date >= ?{where_grade}
           AND r.abnormal_{HORIZON}td_from_filing IS NOT NULL
         ORDER BY t.insider_id, t.ticker, t.filing_date
    """, tuple(params)).fetchall()

    # collapse to episodes
    from datetime import date
    def d(s): return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    eps, key, last = [], None, None
    for r in rows:
        k = (r["insider_id"], r["ticker"]); fd = d(r["filing_date"])
        if k != key or last is None or (fd - last).days > EPISODE_GAP_DAYS:
            eps.append(r); key = k
        last = fd

    band = args.grade or "all grades"
    print(f"Signal screen — {len(rows):,} filings -> {len(eps):,} episodes, "
          f"{band}, {HORIZON}td abnormal return\n")
    print(f"{'signal':<26}{'n_hi':>7}{'n_lo':>7}{'hi %':>8}{'lo %':>8}"
          f"{'spread':>9}{'t':>7}")
    print("-" * 72)

    out = []
    for col, kind in SIGNALS:
        vals = [(r[col], r["y"]) for r in eps if r[col] is not None]
        if len(vals) < 200:
            continue
        if kind == "flag":
            hi = [y for v, y in vals if float(v) == 1]
            lo = [y for v, y in vals if float(v) == 0]
        else:
            med = statistics.median([float(v) for v, _ in vals])
            hi = [y for v, y in vals if float(v) > med]
            lo = [y for v, y in vals if float(v) <= med]
        if len(hi) < 100 or len(lo) < 100:
            continue
        mh, ml = statistics.mean(hi) * 100, statistics.mean(lo) * 100
        se = math.sqrt(statistics.pvariance(hi) / len(hi)
                       + statistics.pvariance(lo) / len(lo)) * 100
        t = (mh - ml) / se if se else 0.0
        out.append((abs(t), col, len(hi), len(lo), mh, ml, mh - ml, t))

    for _, col, nh, nl, mh, ml, sp, t in sorted(out, reverse=True):
        star = " <<<" if abs(t) >= 2 else ""
        print(f"{col:<26}{nh:>7}{nl:>7}{mh:>8.2f}{ml:>8.2f}{sp:>+9.2f}{t:>+7.2f}{star}")
    print("\n<<< = |t| >= 2 on the full corpus. Must still be re-tested inside "
          "the simulator:\nthe candidates surviving grade/conviction filters "
          "are not this population.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

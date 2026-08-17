#!/usr/bin/env python3
"""The whole system, end to end, measured the way a subscriber would live it.

Everything upstream of this measured one piece in isolation. This joins them:

  1. Walk-forward — refit the trade model each year on every prior year only.
  2. Score that year's filings with coefficients that never saw them.
  3. Take the top decile.
  4. Measure what those filings actually returned from the first tradeable
     close AFTER the filing, held k trading days, against SPY.

Every number below is out of sample and actionable. No column from
trade_returns is used: those are anchored to trade_date, which nobody can
trade on.

WHAT IT HAS TO BEAT

Unconditionally, buying every discretionary purchase is not a strategy. From
the first tradeable close, winsorized, 2018-2026 (n~80k):

    hold  1td  2td  3td  5td  7td  10td 14td 21td 30td 45td  60td
    mean  .08  .10  .22  .25  .23  .19  .07  -.35 -.55 -1.50 -1.78
    med  -.03 -.15 -.09 -.18 -.25 -.33 -.53 -1.09 -1.18 -2.51 -3.39

The median is negative at every horizon and the win rate never reaches 50%.
So the entire product rests on selection, and this file is the test of whether
the selection is real.

Usage:
    python3 pipelines/insider_study/validate_system.py
    python3 pipelines/insider_study/validate_system.py --decile 1   # bottom
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402
from pipelines.insider_study.fit_trade_model import (  # noqa: E402
    build_design, ridge,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HOLDS = (1, 2, 3, 5, 7, 10, 14, 21)
WINSOR = 1.0

# Features + the forward price path, in one pass. The lateral joins walk the
# SPY trading calendar so every hold is in trading days.
SQL = """
WITH cal AS (
    SELECT date, close AS spy_close, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
), ev AS (
    SELECT t.trade_id, t.ticker, t.filing_date, t.normalized_title, t.value,
           t.pit_cluster_size, t.dip_1mo, t.is_largest_ever, t.is_csuite,
           tm.sector,
           (SELECT MIN(c.d) FROM cal c WHERE c.date > t.filing_date) AS entry_d
      FROM trades t
      LEFT JOIN ticker_metadata tm ON tm.ticker = t.ticker
     WHERE t.signal_class = 'discretionary_buy'
       AND NOT COALESCE(t.value_suspect, FALSE)
       AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
       AND t.superseded_by IS NULL
       AND t.filing_date >= %s
)
SELECT ev.trade_id, ev.filing_date, ev.normalized_title, ev.value,
       ev.pit_cluster_size, ev.dip_1mo, ev.is_largest_ever, ev.is_csuite,
       ev.sector,
       e.close AS entry_px, ec.spy_close AS entry_spy,
       {cols}
  FROM ev
  JOIN cal ec ON ec.d = ev.entry_d
  JOIN prices.daily_prices e ON e.ticker = ev.ticker AND e.date = ec.date
  {joins}
 WHERE e.close > 0
"""


def build_sql() -> str:
    cols, joins = [], []
    for k in HOLDS:
        cols.append(f"x{k}.close AS px{k}, c{k}.spy_close AS spy{k}")
        joins.append(
            f"LEFT JOIN cal c{k} ON c{k}.d = ev.entry_d + {k} "
            f"LEFT JOIN prices.daily_prices x{k} "
            f"ON x{k}.ticker = ev.ticker AND x{k}.date = c{k}.date AND x{k}.close > 0"
        )
    return SQL.format(cols=", ".join(cols), joins="\n  ".join(joins))


def abn(row, k: int) -> float | None:
    px, spy = row[f"px{k}"], row[f"spy{k}"]
    if not px or not spy or not row["entry_px"] or not row["entry_spy"]:
        return None
    r = px / row["entry_px"] - 1.0
    b = spy / row["entry_spy"] - 1.0
    return max(min(r - b, WINSOR), -WINSOR)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2017-01-01")
    ap.add_argument("--lam", type=float, default=50.0)
    ap.add_argument("--decile", type=int, default=10,
                    help="1 = worst-scored decile, 10 = best")
    ap.add_argument("--fit-horizon", default="7d",
                    help="trade_returns column used to FIT (labels only)")
    args = ap.parse_args()

    conn = get_connection()
    logger.info("Loading filings + forward price paths...")
    rows = [dict(r) for r in conn.execute(build_sql(), (args.since,)).fetchall()]
    logger.info("  %d filings with a tradeable entry price", len(rows))

    # Fit labels come from trade_returns (trade_date anchored) purely because
    # that is what exists for every row; evaluation never touches them.
    lab = {r[0]: r[1] for r in conn.execute(
        f"""SELECT trade_id, GREATEST(LEAST(abnormal_{args.fit_horizon},1.0),-1.0)
              FROM trade_returns WHERE abnormal_{args.fit_horizon} IS NOT NULL"""
    ).fetchall()}
    for r in rows:
        r["y"] = lab.get(r["trade_id"])

    years = sorted({r["filing_date"][:4] for r in rows})
    print("\n" + "=" * 78)
    print(f"  SYSTEM VALIDATION — decile {args.decile}/10, out-of-sample,")
    print("  entry = first tradeable close after filing, vs SPY, winsorized")
    print("=" * 78)
    hdr = "  ".join(f"{k:>4}td" for k in HOLDS)
    print(f"    {'year':>6} {'n':>6}  {hdr}")

    per_hold = {k: [] for k in HOLDS}
    for yr in years:
        tr = [r for r in rows if r["filing_date"][:4] < yr and r["y"] is not None]
        te = [r for r in rows if r["filing_date"][:4] == yr]
        if len(tr) < 3000 or len(te) < 400:
            continue
        X, lv = build_design(tr)
        y = np.array([r["y"] for r in tr], dtype=np.float64)
        w = ridge(X, y, args.lam)
        Xt, _ = build_design(te, lv)
        sc = Xt @ w

        order = np.argsort(sc)
        bins = np.array_split(order, 10)
        pick = [te[i] for i in bins[args.decile - 1]]

        cells = []
        for k in HOLDS:
            vals = [v for v in (abn(r, k) for r in pick) if v is not None]
            if vals:
                m = float(np.mean(vals)) * 100
                per_hold[k].append(m)
                cells.append(f"{m:>5.2f}")
            else:
                cells.append("    -")
        print(f"    {yr:>6} {len(pick):>6}  " + "  ".join(f"{c:>6}" for c in cells))

    print()
    means = [f"{np.mean(per_hold[k]):>5.2f}" if per_hold[k] else "    -" for k in HOLDS]
    pos = [f"{sum(1 for v in per_hold[k] if v > 0)}/{len(per_hold[k])}"
           if per_hold[k] else "-" for k in HOLDS]
    print(f"    {'mean':>6} {'':>6}  " + "  ".join(f"{m:>6}" for m in means))
    print(f"    {'yrs +':>6} {'':>6}  " + "  ".join(f"{p:>6}" for p in pos))

    ok = [k for k in HOLDS if per_hold[k]
          and sum(1 for v in per_hold[k] if v > 0) == len(per_hold[k])]
    if ok:
        best = max(ok, key=lambda k: np.mean(per_hold[k]))
        print(f"\n    positive in EVERY year at: {', '.join(f'{k}td' for k in ok)}")
        print(f"    best of those            : {best}td "
              f"({np.mean(per_hold[best]):+.2f}% mean)")
    else:
        print("\n    no holding period is positive in every year")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

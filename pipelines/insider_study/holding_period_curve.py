#!/usr/bin/env python3
"""Where does the insider edge actually live, measured from when you could act?

WHY THIS EXISTS

Every abnormal_* column in trade_returns is measured from trade_date. Nobody
can trade on trade_date. The filing appears on EDGAR at filing_date — typically
two days later — and that is the first moment the information is public and
actionable. So a "+2.90pp at 7d" edge measured from trade_date is really some
smaller number over a shorter window from where a subscriber stands, and the
difference is not cosmetic: the walk-forward says the whole effect is gone by
day 14, so losing two days to filing lag is losing a sixth of the signal's
entire life.

This measures the curve the product actually sells: enter at the close of the
first trading day AFTER the filing (you cannot assume a fill at a price printed
before you knew), exit k trading days later, benchmark against SPY over the
identical window.

The output is the answer to "how long do I hold this".

Usage:
    python3 pipelines/insider_study/holding_period_curve.py
    python3 pipelines/insider_study/holding_period_curve.py --since 2020-01-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HOLDS = (1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60)

SQL = """
WITH cal AS (
    -- SPY defines the trading calendar; every horizon below is in trading
    -- days, not calendar days, so a long weekend does not silently shorten
    -- a hold.
    SELECT date, close AS spy_close,
           row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
), ev AS (
    SELECT t.trade_id, t.ticker, t.filing_date,
           -- First trading day STRICTLY AFTER the filing date. Entering at
           -- the filing day's own close assumes you saw a filing that may
           -- have been posted after the bell.
           (SELECT MIN(c.d) FROM cal c WHERE c.date > t.filing_date) AS entry_d
      FROM trades t
     WHERE t.signal_class = %s
       AND NOT COALESCE(t.value_suspect, FALSE)
       AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
       AND t.superseded_by IS NULL
       AND t.filing_date >= %s
), px AS (
    SELECT ev.trade_id, ev.entry_d,
           e.close AS entry_px, ec.spy_close AS entry_spy
      FROM ev
      JOIN cal ec ON ec.d = ev.entry_d
      JOIN prices.daily_prices e ON e.ticker = ev.ticker AND e.date = ec.date
     WHERE e.close > 0
)
SELECT %s AS hold,
       count(*) AS n,
       avg((x.close / px.entry_px - 1) - (xc.spy_close / px.entry_spy - 1)) AS abn,
       avg(x.close / px.entry_px - 1) AS raw,
       100.0 * count(*) FILTER (
           WHERE (x.close / px.entry_px - 1) - (xc.spy_close / px.entry_spy - 1) > 0
       ) / count(*) AS win_pct
  FROM px
  JOIN cal xc ON xc.d = px.entry_d + %s
  JOIN ev ON ev.trade_id = px.trade_id
  JOIN prices.daily_prices x ON x.ticker = ev.ticker AND x.date = xc.date
 WHERE x.close > 0
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2018-01-01")
    ap.add_argument("--klass", default="discretionary_buy",
                    choices=["discretionary_buy", "discretionary_sell"])
    ap.add_argument("--by-year", type=int, metavar="HOLD",
                    help="Break one holding period out by filing year")
    args = ap.parse_args()

    if args.by_year:
        conn = get_connection()
        k = args.by_year
        sql = SQL.replace("SELECT %s AS hold,",
                          "SELECT left(ev.filing_date,4) AS yr,") + \
            " GROUP BY 1 ORDER BY 1"
        print("\n" + "=" * 62)
        print(f"  {args.klass} — {k} trading-day hold, by filing year")
        print("=" * 62)
        print(f"    {'year':>6} {'n':>8} {'vs SPY':>9} {'win%':>7}")
        pos = tot = 0
        for r in conn.execute(sql, (args.klass, args.since, k, k)).fetchall():
            yr, n, abn, raw, win = r
            print(f"    {yr:>6} {n:>8} {abn*100:>8.2f}% {win:>6.1f}%")
            tot += 1
            pos += 1 if abn > 0 else 0
        print(f"\n    years positive: {pos}/{tot}")
        return 0

    conn = get_connection()
    print("\n" + "=" * 74)
    print(f"  HOLDING-PERIOD CURVE — {args.klass}, entry = close of first")
    print(f"  trading day after filing_date, since {args.since}")
    print("=" * 74)
    print(f"    {'hold(td)':>9} {'n':>8} {'raw':>9} {'vs SPY':>9} {'win%':>7}")

    best = None
    for k in HOLDS:
        row = conn.execute(SQL, (args.klass, args.since, k, k)).fetchone()
        if not row or not row[1]:
            continue
        _, n, abn, raw, win = row
        print(f"    {k:>9} {n:>8} {raw*100:>8.2f}% {abn*100:>8.2f}% {win:>6.1f}%")
        if best is None or abn > best[1]:
            best = (k, abn)

    if best:
        print(f"\n    peak abnormal return at {best[0]} trading days "
              f"({best[1]*100:+.2f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

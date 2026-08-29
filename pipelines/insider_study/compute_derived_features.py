#!/usr/bin/env python3
"""Derived trade features, all anchored to filing_date. PIT by construction.

WHAT AND WHY

  pct_of_prior_holding   qty / (shares_owned_after - qty). How much the insider
                         grew their OWN stake -- the cleanest conviction proxy
                         available, and it needs no external source because
                         shares_owned_after is 95.8% populated (99%+ since
                         2020). value_owned_after is the empty column (194 rows)
                         and is merely shares x price, so it is not used.

  filing_lag_days        filing_date - trade_date. trans_timeliness is populated
                         on 427 of 317,901 rows and unusable; this is directly
                         computable. Values outside [0, 365] are left NULL --
                         48 rows have a filing BEFORE the trade and one is
                         730,485 days, which are errors, not slow filers.

  ret_20d_pre_filing     Price change over the 20 and 60 TRADING days ending at
  ret_60d_pre_filing     the filing. The strongest signal in our own screen is
                         above_sma50 (t=+9.53) and the microcap study puts
                         distance-from-52-week-high at 36% of feature
                         importance, with purchases disclosed INTO STRENGTH
                         outperforming those into weakness.

  pct_off_52w_high       (close / max close over the prior 252 trading days) - 1.
                         Continuous version of week52_proximity.

  ret_trade_to_filing    The move between transaction and disclosure. The
                         insider saw it and we currently ignore it; a large
                         run-up before we can act changes what the signal is
                         worth.

  value_pct_of_adv       Trade value / 20-day average DOLLAR volume. The
                         absolute-dollar decile curve is hump-shaped -- the
                         bottom two deciles (under $3,024) return a third of the
                         middle while the TOP decile is worse than the eighth --
                         so raw dollars is the wrong variable. This is the
                         standard liquidity normalisation.

THE PIT RULE

Every window ENDS AT filing_date and looks only backwards. Not trade_date:
filing_date is when we learn of the trade and the earliest anyone could act.
A window centred on or extending past the filing would be a look-ahead of the
same class as reading filed_at as UTC, which put 37 entries a session early.

The trading-day arithmetic walks the SPY calendar, so a long weekend cannot
shorten a window.

Usage:
    python3 pipelines/insider_study/compute_derived_features.py --dry-run
    python3 pipelines/insider_study/compute_derived_features.py --since 2016-01-01
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MAX_FILING_LAG_DAYS = 365

# Cheap, no price data needed. One statement for the whole range.
SQL_SIMPLE = """
UPDATE trades t SET
    pct_of_prior_holding = CASE
        WHEN t.qty > 0 AND t.shares_owned_after IS NOT NULL
             AND (t.shares_owned_after - t.qty) > 0
        THEN t.qty::float8 / (t.shares_owned_after - t.qty)
        ELSE NULL END,
    filing_lag_days = CASE
        WHEN (t.filing_date::date - t.trade_date::date) BETWEEN 0 AND %s
        THEN (t.filing_date::date - t.trade_date::date)
        ELSE NULL END
 WHERE t.filing_date >= %s AND t.filing_date < %s
"""

# Price-dependent. Every window ends at the filing and looks backwards.
SQL_PRICES = """
WITH cal AS (
    SELECT date, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
), ev AS (
    SELECT t.trade_id, t.ticker, t.value, t.trade_date,
           -- First session at or after the filing: the earliest observable point.
           (SELECT MIN(c.d) FROM cal c WHERE c.date >= t.filing_date) AS fd,
           (SELECT MIN(c.d) FROM cal c WHERE c.date >= t.trade_date)  AS td
      FROM trades t
     WHERE t.filing_date >= %s AND t.filing_date < %s
       AND t.ticker IS NOT NULL AND t.ticker <> 'NONE'
), px AS (
    SELECT ev.*,
           (SELECT p.close FROM prices.daily_prices p JOIN cal c ON c.date = p.date
             WHERE p.ticker = ev.ticker AND c.d = ev.fd AND p.close > 0) AS p_now,
           (SELECT p.close FROM prices.daily_prices p JOIN cal c ON c.date = p.date
             WHERE p.ticker = ev.ticker AND c.d = ev.fd - 20 AND p.close > 0) AS p_20,
           (SELECT p.close FROM prices.daily_prices p JOIN cal c ON c.date = p.date
             WHERE p.ticker = ev.ticker AND c.d = ev.fd - 60 AND p.close > 0) AS p_60,
           (SELECT p.close FROM prices.daily_prices p JOIN cal c ON c.date = p.date
             WHERE p.ticker = ev.ticker AND c.d = ev.td AND p.close > 0) AS p_trade,
           -- 52-week high STRICTLY at or before the filing session.
           (SELECT MAX(p.close) FROM prices.daily_prices p JOIN cal c ON c.date = p.date
             WHERE p.ticker = ev.ticker AND c.d BETWEEN ev.fd - 252 AND ev.fd
               AND p.close > 0) AS hi_52w,
           -- 20-session average DOLLAR volume ending at the filing session.
           (SELECT AVG(p.close * p.volume) FROM prices.daily_prices p
              JOIN cal c ON c.date = p.date
             WHERE p.ticker = ev.ticker AND c.d BETWEEN ev.fd - 20 AND ev.fd
               AND p.close > 0 AND p.volume > 0) AS adv
      FROM ev WHERE ev.fd IS NOT NULL
)
UPDATE trades t SET
    ret_20d_pre_filing  = CASE WHEN px.p_20   > 0 THEN px.p_now / px.p_20 - 1 END,
    ret_60d_pre_filing  = CASE WHEN px.p_60   > 0 THEN px.p_now / px.p_60 - 1 END,
    ret_trade_to_filing = CASE WHEN px.p_trade> 0 THEN px.p_now / px.p_trade - 1 END,
    pct_off_52w_high    = CASE WHEN px.hi_52w > 0 THEN px.p_now / px.hi_52w - 1 END,
    value_pct_of_adv    = CASE WHEN px.adv    > 0 THEN t.value / px.adv END
  FROM px
 WHERE px.trade_id = t.trade_id AND px.p_now IS NOT NULL
"""


def quarters(since: str, until: str):
    lo_y, hi_y = int(since[:4]), int(until[:4])
    for y in range(lo_y, hi_y + 1):
        for a, b in (("01-01", "04-01"), ("04-01", "07-01"),
                     ("07-01", "10-01"), ("10-01", "12-31")):
            lo = f"{y}-{a}"
            hi = f"{y}-{b}" if b != "12-31" else f"{y+1}-01-01"
            if hi <= since or lo >= until:
                continue
            yield max(lo, since), min(hi, until)


def main() -> int:
    from datetime import date
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--until", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    until = args.until or date.today().isoformat()

    conn = get_connection()
    # Never queue at the head of a lock queue on a table the API reads.
    conn.execute("SET lock_timeout = '5s'")

    wins = list(quarters(args.since, until))
    logger.info("%d quarterly window(s)", len(wins))
    if args.dry_run:
        for lo, hi in wins[:4]:
            logger.info("  would process %s .. %s", lo, hi)
        return 0

    t0, n_simple, n_px = time.time(), 0, 0
    for lo, hi in wins:
        cur = conn.execute(SQL_SIMPLE, (MAX_FILING_LAG_DAYS, lo, hi))
        n_simple += cur.rowcount or 0
        conn.commit()
        cur = conn.execute(SQL_PRICES, (lo, hi))
        n_px += cur.rowcount or 0
        conn.commit()
        logger.info("  %s .. %s : simple=%d price=%d (%.0fs)",
                    lo, hi, n_simple, n_px, time.time() - t0)

    logger.info("Done in %.0fs. simple=%d rows, price-derived=%d rows",
                time.time() - t0, n_simple, n_px)

    cov = conn.execute("""
        SELECT count(*) AS n,
               count(pct_of_prior_holding) AS holding,
               count(filing_lag_days)      AS lag,
               count(ret_20d_pre_filing)   AS r20,
               count(pct_off_52w_high)     AS hi52,
               count(value_pct_of_adv)     AS adv
          FROM trades
         WHERE signal_class = 'discretionary_buy' AND filing_date >= %s
    """, (args.since,)).fetchone()
    logger.info("Coverage on discretionary buys (n=%d): holding %.1f%%, lag %.1f%%, "
                "ret20 %.1f%%, off52w %.1f%%, adv %.1f%%",
                cov[0], *[100.0 * cov[i] / max(cov[0], 1) for i in range(1, 6)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

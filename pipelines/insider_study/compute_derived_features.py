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
  ret_60d_pre_filing     the last session CLOSED when the filing appeared. The strongest signal in our own screen is
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

Every window ends at the last session that had CLOSED when the filing was
ACCEPTED, and looks only backwards.

Not trade_date: the filing is when we learn of the trade. And not filing_DATE
either -- a filing accepted at 10:00 ET is public before that day's close, so
using the close reads six hours into the future. 27.3% of buys are accepted
intraday, so the date-only anchor leaked on a quarter of the corpus.
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

# ── Denominator floors and caps ────────────────────────────────────────────
#
# Both ratio features have a denominator that can approach zero, and the raw
# output is arithmetically correct and useless:
#
#   pct_of_prior_holding   p50 0.014   p99 8.3   p99.9 75,672   max 67,333,011
#   value_pct_of_adv       p50 0.018   p99 6.5   p99.9   113.4  max    533,491
#
# The pct_of_prior_holding tail is 1,199 filings where the insider held FEWER
# THAN TEN SHARES beforehand. Going from 3 shares to 50,000 is not a 16,000x
# increase in conviction, it is a first purchase with a rounding error in the
# denominator. A feature whose 99.9th percentile is 9,000x its 99th will
# dominate any model it enters, on 0.1% of the rows.
#
# So: require a denominator large enough to mean something, then cap. The cap
# is a real value, not a sentinel -- "increased the stake tenfold or more" and
# "traded a hundred days of volume or more" are both meaningful states, and
# both are rare.
MIN_PRIOR_SHARES = 100          # below this the ratio is denominator noise
MAX_HOLDING_RATIO = 10.0        # >10x reads as a new position, not an increase
MIN_ADV_DOLLARS = 10_000.0      # a name trading less than this has no usable ADV
MAX_ADV_MULTIPLE = 100.0        # 100 days of volume is already "cannot exit"
MAX_PRE_RETURN = 5.0            # +500% over 20-60 sessions; above is a split artefact

# Cheap, no price data needed. One statement for the whole range.
SQL_SIMPLE = """
UPDATE trades t SET
    -- BUYS ONLY, and the direction matters arithmetically. qty is unsigned
    -- in this DB, so for a buy prior = shares_owned_after - qty, but for a
    -- SALE prior = shares_owned_after + qty. Using the buy formula on a
    -- disposal computes prior - 2*qty, a quantity that means nothing and goes
    -- negative or near-zero, giving an unbounded ratio.
    --
    -- Gated on signal_class rather than trade_type: 184k comp grants and 221k
    -- option exercises carry trade_type='buy'.
    pct_of_prior_holding = CASE
        WHEN t.signal_class = 'discretionary_buy'
             AND t.qty > 0 AND t.shares_owned_after IS NOT NULL
             AND (t.shares_owned_after - t.qty) >= %s
        THEN LEAST(t.qty::float8 / (t.shares_owned_after - t.qty), %s)
        ELSE NULL END,
    filing_lag_days = CASE
        WHEN (t.filing_date::date - t.trade_date::date) BETWEEN 0 AND %s
        THEN (t.filing_date::date - t.trade_date::date)
        ELSE NULL END
 WHERE t.filing_date >= %s AND t.filing_date < %s
"""

# Price-dependent. Every window ends at the filing and looks backwards.
#
# PERFORMANCE. The first version issued six CORRELATED SUBQUERIES per event --
# spot price, t-20, t-60, trade-date, a 252-session MAX and a 20-session AVG --
# so the 52-week high was rescanned once per filing rather than once per ticker.
# It ran at ~10 minutes a quarter, ~7 hours for 2016-2026.
#
# Instead the price series is decorated ONCE with window aggregates into a temp
# table, and each event then does four indexed lookups against it. Same
# semantics, same PIT boundaries, one pass over prices instead of ~318,000.
PX_FEATURES = """
CREATE TEMP TABLE px_feat AS
WITH cal AS (
    SELECT date, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
)
SELECT p.ticker, c.d, p.close,
       -- 52-week high INCLUSIVE of the current session and looking only back.
       MAX(p.close) OVER (PARTITION BY p.ticker ORDER BY c.d
                          ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS hi_52w,
       -- 20-session average DOLLAR volume, same boundary.
       AVG(p.close * NULLIF(p.volume, 0)) OVER (
           PARTITION BY p.ticker ORDER BY c.d
           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS adv_20
  FROM prices.daily_prices p
  JOIN cal c ON c.date = p.date
 WHERE p.close > 0
"""

PX_INDEX = "CREATE INDEX ON px_feat (ticker, d)"

SQL_PRICES = """
WITH cal AS (
    SELECT date, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
), ev AS (
    SELECT t.trade_id, t.ticker, t.value,
           -- ANCHOR ON THE ACCEPTANCE TIMESTAMP, NOT THE DATE.
           --
           -- The first version used the first session at or after filing_date.
           -- For a filing accepted INTRADAY that reads the same day's CLOSE --
           -- a price up to six hours in the future relative to the moment the
           -- filing became public. 27.3% of discretionary buys are accepted
           -- between 09:30 and 16:00 ET (86,701 of 317,901), so this was a
           -- look-ahead on roughly a quarter of the corpus.
           --
           -- The correct anchor is the last session that had CLOSED when the
           -- filing appeared:
           --     accepted at/after 16:00 ET  ->  that session's close
           --     accepted before 16:00 ET    ->  the PREVIOUS session's close
           --
           -- That is the mirror of the entry rule in entry_timing.py, which
           -- fills an after-bell filing at the next session's open. Features
           -- describe the state the market was in when the filing landed;
           -- entry happens at the first price you could actually pay.
           --
           -- No timezone conversion: filed_at is TEXT holding naive EASTERN
           -- wall time and entry_timing.py:42 forbids casting it. A missing or
           -- short filed_at resolves to the conservative branch.
           CASE WHEN COALESCE(substring(t.filed_at from 12 for 5) >= '16:00',
                              TRUE)
                THEN (SELECT MAX(c.d) FROM cal c WHERE c.date <= t.filing_date)
                ELSE (SELECT MAX(c.d) FROM cal c WHERE c.date <  t.filing_date)
           END AS fd,
           (SELECT MIN(c.d) FROM cal c WHERE c.date >= t.trade_date)  AS td
      FROM trades t
     WHERE t.filing_date >= %s AND t.filing_date < %s
       AND t.ticker IS NOT NULL AND t.ticker <> 'NONE'
)
UPDATE trades t SET
    ret_20d_pre_filing  = CASE WHEN p20.close > 0
        THEN LEAST(now_.close / p20.close - 1, %s) END,
    ret_60d_pre_filing  = CASE WHEN p60.close > 0
        THEN LEAST(now_.close / p60.close - 1, %s) END,
    ret_trade_to_filing = CASE WHEN ptr.close > 0
        THEN LEAST(now_.close / ptr.close - 1, %s) END,
    -- No cap: bounded in [-1, 0] by construction, and the invariant is tested.
    pct_off_52w_high    = CASE WHEN now_.hi_52w > 0
        THEN now_.close / now_.hi_52w - 1 END,
    value_pct_of_adv    = CASE WHEN now_.adv_20 >= %s
        THEN LEAST(t.value / now_.adv_20, %s) END
  FROM ev
  JOIN px_feat now_ ON now_.ticker = ev.ticker AND now_.d = ev.fd
  LEFT JOIN px_feat p20 ON p20.ticker = ev.ticker AND p20.d = ev.fd - 20
  LEFT JOIN px_feat p60 ON p60.ticker = ev.ticker AND p60.d = ev.fd - 60
  LEFT JOIN px_feat ptr ON ptr.ticker = ev.ticker AND ptr.d = ev.td
 WHERE ev.trade_id = t.trade_id
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

    # A SESSION-LIFETIME temp table, NOT ON COMMIT DROP.
    #
    # ON COMMIT DROP forces every quarter into one transaction with the build,
    # which means ~90 minutes holding RowExclusive on `trades` and no visible
    # progress until the very end. That is the long-transaction pattern behind
    # the 2026-08-27 outage, and it also made the run impossible to check --
    # a separate connection reads pre-update values throughout, which briefly
    # looked like the caps were not being applied.
    #
    # A plain TEMP table lives for the SESSION, so each quarter can commit on
    # its own and the lock is released between them.
    logger.info("decorating the price series (one pass, all tickers)...")
    tb = time.time()
    conn.execute(PX_FEATURES)
    conn.execute(PX_INDEX)
    logger.info("  px_feat built in %.0fs", time.time() - tb)

    t0, n_simple, n_px = time.time(), 0, 0
    for lo, hi in wins:
        # ORDER MATTERS: psycopg2 substitutes %s positionally in the order
        # they appear in the SQL TEXT. lo/hi are in the ev CTE, which precedes
        # the SET clause, so the dates come FIRST.
        cur = conn.execute(SQL_PRICES,
                           (lo, hi,
                            MAX_PRE_RETURN, MAX_PRE_RETURN, MAX_PRE_RETURN,
                            MIN_ADV_DOLLARS, MAX_ADV_MULTIPLE))
        n_px += cur.rowcount or 0
        conn.commit()      # release the row locks between quarters
        logger.info("  price %s .. %s : %d rows (%.0fs)",
                    lo, hi, n_px, time.time() - t0)
    conn.execute("DROP TABLE IF EXISTS px_feat")
    conn.commit()

    for lo, hi in wins:
        cur = conn.execute(SQL_SIMPLE, (MIN_PRIOR_SHARES, MAX_HOLDING_RATIO,
                                MAX_FILING_LAG_DAYS, lo, hi))
        n_simple += cur.rowcount or 0
        conn.commit()
    logger.info("  simple features: %d rows", n_simple)

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

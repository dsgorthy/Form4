#!/usr/bin/env python3
"""Populate trade_returns.abnormal_*_from_filing — the tradeable labels.

See migrations/2026-08-17_returns_from_filing.sql for why these exist. Short
version: every other abnormal_* column is anchored to trade_date, which nobody
can trade on, and a model fitted on one of them scored a +6.85pp walk-forward
decile spread while having no ranking power on returns anyone could capture.

Runs in batches by filing_date so it is resumable and can be re-run daily to
pick up newly matured windows. Idempotent: a batch recomputes and overwrites,
so re-running after a price backfill corrects rows that had no price before.

Usage:
    python3 pipelines/insider_study/backfill_returns_from_filing.py
    python3 pipelines/insider_study/backfill_returns_from_filing.py --since 2026-01-01
    python3 pipelines/insider_study/backfill_returns_from_filing.py --only-missing
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

# 3wk .. 12mo. 63/126/189/252 trading days are roughly 3/6/9/12 months.
#
# EXTENDED 2026-08-28. It stopped at 42td (~2 months), which made the cluster
# question unanswerable: we measure cluster HARM at 21d (+3.25% for a solo
# buyer against -0.08% at six or more) while the literature ties 3+ insider
# clusters to above-market TWELVE-MONTH returns. Both can be true -- a crowd
# may signal slow-burn value while a lone buyer signals near-term news -- and
# without a 252td label there is no way to tell.
#
# The most recent ~12 months of filings hold NULL at 252td BY CONSTRUCTION.
# That is immaturity, not missing data; check_attribute_coverage exempts the
# current year for windows that cannot have matured.
HORIZONS = (3, 5, 7, 10, 21, 42, 63, 126, 189, 252)
#: Winsorisation is per-horizon. A flat +/-1.0 was calibrated where it binds on
#: 2% of rows, but returns fan out with time: at 12 months 6.72% of raw 365d
#: returns exceed +100% while ZERO fall below -100% (a stock can lose 100% and
#: gain 500%). Clamping symmetrically at 1.0 cuts the 12-month mean from +14.97%
#: to +4.58% -- a 69% haircut applied entirely to the right tail.
#:
#: That matters because the whole point of the 252td label is to test whether
#: 3+ insider clusters predict above-market TWELVE-MONTH returns, a hypothesis
#: about the right tail. The old constant would have flattened the thing being
#: measured and the test would have come back null for arithmetic reasons.
#:
#: The floor stays near -1.0 because abnormal return is stock minus SPY and a
#: stock cannot fall below -100%; the ceiling scales with the horizon.
WINSOR_BY_HORIZON = {3: 1.0, 5: 1.0, 7: 1.0, 10: 1.0, 21: 1.0, 42: 1.0,
                     63: 1.5, 126: 2.5, 189: 3.0, 252: 4.0}
WINSOR = 1.0          # retained for the floor and for horizons not listed

# One statement per batch. The lateral joins walk the SPY calendar, so every
# horizon is in trading days; a long weekend cannot shorten a hold.
#
# tradeable_same_day mirrors form4_signal_class's sibling rule and
# framework.decision.entry_timing: same-session entry only when the filing beat
# the 16:00 ET bell. A missing filed_at resolves to the next session, which is
# the conservative direction.
SQL = """
WITH cal AS (
    SELECT date, close AS spy, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
), ev AS (
    SELECT t.trade_id, t.ticker,
           (SELECT MIN(c.d) FROM cal c
             WHERE c.date >= CASE
                 -- NO TIMEZONE CONVERSION. filed_at is TEXT holding naive
                 -- EASTERN wall time and entry_timing.py:42 says in terms:
                 -- "DO NOT apply a timezone conversion when reading it".
                 --
                 -- This previously did
                 --   (filed_at::timestamptz AT TIME ZONE 'America/New_York')::time
                 -- which reinterprets the naive string in the SESSION timezone
                 -- and then converts. The result depends on who is connected:
                 --   Studio (PT):  +3h, pushing 11.9% of filings a session LATE
                 --   UTC (Docker,  -5h, giving 71.3% of filings a same-session
                 --   Dagster)      close they did not exist for -- look-ahead,
                 --                 the identical class to the filed_at-as-UTC
                 --                 bug that put 37 entries a session early.
                 -- insider-fetch is next in the launchd->Dagster migration, so
                 -- the dormant half was about to become the live half.
                 --
                 -- Lexicographic compare on the HH:MM substring, matching
                 -- entry_timing._parse exactly. A missing or short filed_at
                 -- yields NULL -> COALESCE FALSE -> next session, which is the
                 -- conservative direction.
                 WHEN COALESCE(substring(t.filed_at from 12 for 5) < '16:00',
                               FALSE)
                     THEN t.filing_date
                 ELSE to_char(t.filing_date::date + 1, 'YYYY-MM-DD')
             END) AS ed
      FROM trades t
     WHERE t.signal_class IN ('discretionary_buy', 'discretionary_sell')
       AND NOT COALESCE(t.value_suspect, FALSE)
       AND t.filing_date >= ? AND t.filing_date < ?
), b AS (
    SELECT ev.trade_id, ev.ticker, ev.ed, ce.date AS entry_date,
           e.close AS ep, ce.spy AS es
      FROM ev
      JOIN cal ce ON ce.d = ev.ed
      JOIN prices.daily_prices e
        ON e.ticker = ev.ticker AND e.date = ce.date AND e.close > 0
)
UPDATE trade_returns tr
   SET entry_date_from_filing = b.entry_date,
       {sets}
  FROM b {joins}
 WHERE tr.trade_id = b.trade_id
"""


def build_sql() -> str:
    sets, joins = [], []
    for k in HORIZONS:
        sets.append(
            f"abnormal_{k}td_from_filing = GREATEST(LEAST("
            f"(x{k}.close / b.ep - 1) - (c{k}.spy / b.es - 1), "
            f"{WINSOR_BY_HORIZON.get(k, WINSOR)}), -{WINSOR})"
        )
        joins.append(
            f"LEFT JOIN cal c{k} ON c{k}.d = b.ed + {k} "
            f"LEFT JOIN prices.daily_prices x{k} "
            f"ON x{k}.ticker = b.ticker AND x{k}.date = c{k}.date AND x{k}.close > 0"
        )
    return SQL.format(sets=",\n       ".join(sets), joins="\n  ".join(joins))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--until", default=None)
    ap.add_argument("--only-missing", action="store_true",
                    help="Skip quarters already fully populated (daily re-runs)")
    args = ap.parse_args()

    from datetime import date
    until = args.until or date.today().isoformat()
    conn = get_connection()
    sql = build_sql()

    # Quarterly batches: small enough that a failure loses little, large enough
    # that the SPY calendar CTE is not rebuilt thousands of times.
    lo_year = int(args.since[:4])
    hi_year = int(until[:4])
    windows = []
    for y in range(lo_year, hi_year + 1):
        for q, (a, b_) in enumerate([("01-01", "04-01"), ("04-01", "07-01"),
                                     ("07-01", "10-01"), ("10-01", "12-31")], 1):
            lo, hi = f"{y}-{a}", f"{y}-{b_}" if q < 4 else f"{y + 1}-01-01"
            if hi <= args.since or lo >= until:
                continue
            windows.append((max(lo, args.since), min(hi, until)))

    t0 = time.time()
    total = 0
    for lo, hi in windows:
        if args.only_missing:
            n = conn.execute("""
                SELECT count(*) FROM trades t
                  JOIN trade_returns tr ON tr.trade_id = t.trade_id
                 WHERE t.signal_class IN ('discretionary_buy','discretionary_sell')
                   AND t.filing_date >= ? AND t.filing_date < ?
                   AND tr.abnormal_7td_from_filing IS NULL
            """, (lo, hi)).fetchone()[0]
            if not n:
                continue
        cur = conn.execute(sql, (lo, hi))
        n = cur.rowcount or 0
        conn.commit()
        total += n
        logger.info("  %s .. %s : %d rows (%.0fs elapsed)", lo, hi, n, time.time() - t0)

    logger.info("Done. %d rows updated in %.0fs", total, time.time() - t0)

    cov = conn.execute("""
        SELECT count(*) AS eligible,
               count(tr.abnormal_7td_from_filing) AS have_7td,
               count(tr.abnormal_42td_from_filing) AS have_42td,
               count(tr.abnormal_252td_from_filing) AS have_252td
          FROM trades t JOIN trade_returns tr ON tr.trade_id = t.trade_id
         WHERE t.signal_class IN ('discretionary_buy','discretionary_sell')
           AND NOT COALESCE(t.value_suspect, FALSE)
    """).fetchone()
    logger.info("Coverage: %d eligible, %d with 7td (%.1f%%), %d with 42td "
                "(%.1f%%), %d with 252td (%.1f%%, immature for the last year "
                "by construction)",
                cov[0], cov[1], 100.0 * cov[1] / max(cov[0], 1),
                cov[2], 100.0 * cov[2] / max(cov[0], 1),
                cov[3], 100.0 * cov[3] / max(cov[0], 1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

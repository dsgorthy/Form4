#!/usr/bin/env python3
"""Materialise the feature registry into trade_features.

TWO PASSES.

RAW features come straight from the row and the price series decorated at the
OBSERVATION anchor (framework.features.anchor) -- the last session that had
CLOSED when the filing was accepted.

PERCENTILE features rank a raw value within a grouping, and they are where the
look-ahead lives if you are careless. `PERCENT_RANK() OVER (PARTITION BY
sector ORDER BY value)` ranks 2016 filings against 2026 ones: it uses the
future to rank the past, on every row.

So percentiles are computed against a TRAILING SNAPSHOT. For each (grouping,
month) the builder derives boundaries from the preceding `window_days` of that
group ONLY -- strictly earlier months -- and each trade is placed against the
boundaries current when it was filed. A group-month with too few prior
observations yields NULL rather than a confident rank off four data points.

That is coarser than an exact rank and deliberately so: exact per-row ranking
is O(n x window) and the extra precision buys nothing for clustering, while
the monthly snapshot is a single aggregate and is obviously correct to read.

Usage:
    python3 pipelines/insider_study/build_trade_features.py --dry-run
    python3 pipelines/insider_study/build_trade_features.py --since 2016-01-01
    python3 pipelines/insider_study/build_trade_features.py --only value_pct_of_adv
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection            # noqa: E402
from framework.features.anchor import observation_session_sql  # noqa: E402
from framework.features.registry import (             # noqa: E402
    GROUPINGS, RAW, all_features, percentile_variants)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

#: A group-month with fewer prior observations than this cannot support a
#: percentile. Ranking against a handful of rows manufactures precision.
MIN_PRIOR_OBS = 30

#: Percentile granularity. 20 boundaries = 5-point resolution, which is finer
#: than any downstream use and keeps the aggregate small.
N_BOUNDARIES = 20

# The price series decorated once at the observation anchor, exactly as
# compute_derived_features does -- one pass over prices, not one per event.
PX_FEAT = """
CREATE TEMP TABLE px_feat AS
WITH cal AS (
    SELECT date, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
), s AS (
    SELECT p.ticker, c.d, p.close, p.volume,
           ln(p.close / NULLIF(LAG(p.close) OVER (PARTITION BY p.ticker
                                                  ORDER BY c.d), 0)) AS r
      FROM prices.daily_prices p
      JOIN cal c ON c.date = p.date
     WHERE p.close > 0
)
SELECT s.ticker, s.d, s.close,
       MAX(s.close) OVER w252 AS hi_52w,
       AVG(s.close * NULLIF(s.volume, 0)) OVER w20 AS adv_20,
       -- Realised volatility, annualised, ending at this session and looking
       -- only backwards. sqrt(252) because r is a daily log return.
       --
       -- WHY THIS EXISTS. Our label is RAW abnormal return, so a +/-40% move in
       -- a 60%-vol microcap and the same move in a 20%-vol large cap count
       -- identically. That makes both outcome tails mostly a list of volatile
       -- names, which is exactly what we observed: the top and bottom deciles
       -- were indistinguishable on every feature we had except above_sma50.
       -- If the deciles separate under a vol-ADJUSTED label and not under the
       -- raw one, the signal was there and we were measuring through noise.
       STDDEV_SAMP(s.r) OVER w20  * sqrt(252) AS vol_20,
       STDDEV_SAMP(s.r) OVER w60  * sqrt(252) AS vol_60
  FROM s
WINDOW w20  AS (PARTITION BY s.ticker ORDER BY s.d ROWS BETWEEN  20 PRECEDING AND CURRENT ROW),
       w60  AS (PARTITION BY s.ticker ORDER BY s.d ROWS BETWEEN  60 PRECEDING AND CURRENT ROW),
       w252 AS (PARTITION BY s.ticker ORDER BY s.d ROWS BETWEEN 252 PRECEDING AND CURRENT ROW)
"""

# The event anchor, materialised ONCE.
#
# The first version inlined the observation-anchor subqueries into every
# feature's INSERT, so `(SELECT MAX(c.d) FROM cal c WHERE c.date <= filing_date)`
# was re-evaluated per row PER FEATURE -- 467 seconds for the first of eleven.
# The anchor does not depend on the feature, so it is computed once and joined.
# Same lesson as px_feat, one layer up.
EV_FEAT = """
CREATE TEMP TABLE ev_feat AS
WITH cal AS (
    SELECT date, row_number() OVER (ORDER BY date) AS d
      FROM prices.daily_prices WHERE ticker = 'SPY'
)
SELECT t.trade_id, t.ticker, t.value, t.qty, t.shares_owned_after,
       t.filing_date, t.trade_date, t.insider_id, t.issuer_cik,
       {obs} AS obs_d,
       (SELECT MIN(c.d) FROM cal c WHERE c.date >= t.trade_date) AS trade_d
  FROM trades t
 WHERE t.signal_class = 'discretionary_buy'
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND t.filing_date >= %s AND t.filing_date < %s
"""

# Earnings context per trade, from PRIOR announcements only.
#
# `last_announce` is the most recent 8-K Item 2.02 STRICTLY BEFORE the filing.
# `median_gap` is this issuer's typical cycle length, computed from gaps between
# announcements that had already happened -- so "how far through the cycle is
# this filing" can be answered without ever reading a future date.
#
# Gaps are bounded to 30..200 days: a shorter gap is a restatement or an
# 8-K/A, a longer one a reporting hole, and either would drag the median away
# from the ~91-day quarter that makes the ratio meaningful.
EARN_FEAT = """
CREATE TEMP TABLE earn_feat AS
SELECT ev.trade_id, la.announce_date AS last_announce, mg.median_gap
  FROM ev_feat ev
  LEFT JOIN LATERAL (
      SELECT e.announce_date
        FROM issuer_earnings e
       WHERE e.cik = ev.issuer_cik AND e.source = 'edgar_8k_202'
         AND e.announce_date < ev.filing_date
       ORDER BY e.announce_date DESC LIMIT 1
  ) la ON TRUE
  LEFT JOIN LATERAL (
      SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY g.gap) AS median_gap
        FROM (
          SELECT e.announce_date::date
                 - LAG(e.announce_date::date) OVER (ORDER BY e.announce_date) AS gap
            FROM issuer_earnings e
           WHERE e.cik = ev.issuer_cik AND e.source = 'edgar_8k_202'
             AND e.announce_date < ev.filing_date
        ) g
       WHERE g.gap BETWEEN 30 AND 200
  ) mg ON TRUE
 WHERE ev.issuer_cik IS NOT NULL
"""

RAW_SQL = """
WITH j AS (
    SELECT ev.*, now_.close, now_.hi_52w, now_.adv_20,
           now_.vol_20, now_.vol_60,
           ef.last_announce, ef.median_gap,
           p20.close AS close_20, p60.close AS close_60, ptr.close AS close_trade
      FROM ev_feat ev
      LEFT JOIN px_feat now_ ON now_.ticker = ev.ticker AND now_.d = ev.obs_d
      LEFT JOIN px_feat p20  ON p20.ticker  = ev.ticker AND p20.d  = ev.obs_d - 20
      LEFT JOIN px_feat p60  ON p60.ticker  = ev.ticker AND p60.d  = ev.obs_d - 60
      LEFT JOIN px_feat ptr  ON ptr.ticker  = ev.ticker AND ptr.d  = ev.trade_d
      LEFT JOIN earn_feat ef ON ef.trade_id = ev.trade_id
)
INSERT INTO trade_features (trade_id, feature, value)
SELECT j.trade_id, %s, ({expr})
  FROM j
  -- Two LATERALs so a registry expression can say `t.value` and `px.close`
  -- and read naturally, even though both come from j. Aliasing j AS t hides
  -- j from the second LATERAL, which is what the first attempt did.
  CROSS JOIN LATERAL (SELECT j.value, j.qty, j.shares_owned_after,
                             j.filing_date, j.trade_date, j.insider_id,
                             j.ticker) AS t
  CROSS JOIN LATERAL (SELECT j.close, j.hi_52w, j.adv_20, j.close_20,
                             j.close_60, j.close_trade,
                             j.vol_20, j.vol_60) AS px
  CROSS JOIN LATERAL (SELECT j.last_announce, j.median_gap) AS e
 WHERE ({expr}) IS NOT NULL
    ON CONFLICT (trade_id, feature)
    DO UPDATE SET value = EXCLUDED.value, computed_at = NOW()
"""


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       cwd=Path(__file__).resolve().parents[2],
                                       text=True).strip()
    except Exception:
        return "unknown"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--until", default=None)
    ap.add_argument("--only", default=None, help="one feature name")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from datetime import date
    until = args.until or date.today().isoformat()

    feats = [f for f in all_features()
             if not args.only or f.name == args.only]
    raws = [f for f in feats if f.generated_from is None]
    pcts = [f for f in feats if f.generated_from is not None]
    logger.info("%d feature(s): %d raw, %d percentile",
                len(feats), len(raws), len(pcts))
    if args.dry_run:
        for f in feats:
            logger.info("  %-40s anchor=%-11s %s", f.name, f.anchor,
                        f.description[:58])
        return 0

    conn = get_connection()
    conn.execute("SET lock_timeout = '5s'")
    # The compat layer surfaces INSERT ... RETURNING through lastrowid, not a
    # fetchable row -- the same shape strategies/insider_catalog/backfill.py
    # uses. fetchone() comes back None here and subscripting it raises.
    cur = conn.execute("""
        INSERT INTO trade_feature_runs (git_sha, n_features, since_date)
        VALUES (?, ?, ?) RETURNING run_id
    """, (git_sha(), len(feats), args.since))
    run = cur.lastrowid
    if run is None:
        row = cur.fetchone()
        run = row[0] if row else None
    conn.commit()

    t0, total = time.time(), 0
    logger.info("decorating the price series (one pass)...")
    conn.execute(PX_FEAT)
    conn.execute("CREATE INDEX ON px_feat (ticker, d)")
    logger.info("  px_feat built in %.0fs", time.time() - t0)

    conn.execute(EV_FEAT.format(obs=observation_session_sql()),
                 (args.since, until))
    conn.execute("CREATE INDEX ON ev_feat (trade_id)")
    conn.execute("CREATE INDEX ON ev_feat (ticker, obs_d)")
    logger.info("  ev_feat (anchors) built in %.0fs", time.time() - t0)

    conn.execute(EARN_FEAT)
    conn.execute("CREATE INDEX ON earn_feat (trade_id)")
    logger.info("  earn_feat built in %.0fs", time.time() - t0)

    for f in raws:
        cur = conn.execute(RAW_SQL.format(expr=f.expr), (f.name,))
        n = cur.rowcount or 0
        conn.commit()
        total += n
        logger.info("  raw %-32s %d rows (%.0fs)", f.name, n, time.time() - t0)

    conn.execute("DROP TABLE IF EXISTS px_feat")
    conn.execute("DROP TABLE IF EXISTS ev_feat")
    conn.execute("DROP TABLE IF EXISTS earn_feat")
    conn.commit()

    conn.execute("""
        UPDATE trade_feature_runs SET finished_at = NOW(), n_rows = ?
         WHERE run_id = ?
    """, (total, run))
    conn.commit()
    logger.info("Done in %.0fs. %d feature rows written (run %s)",
                time.time() - t0, total, run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

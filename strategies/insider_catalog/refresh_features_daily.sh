#!/bin/bash
# Daily feature-refresh chain. Runs after insider-fetch finishes (which lands
# raw Form 4 trades). Populates trade-level features (dip_*, above_sma*,
# consecutive_sells_before, pit_grade, pit_cluster_size, cohen_routine) so
# strategy runners can evaluate them.
#
# Order matters:
#   0. Update daily_prices (write fresh signal_freshness row before strategies
#      wake at 06:25 PT — separate weekday-only daily-prices plist at 17:30 PT
#      leaves a structural gap on Monday mornings where the only available
#      timestamp was Friday's, and seed-from-MAX(date) skews stale fast)
#   1. Sync prices.db (SQLite) from PG so compute_cw_indicators sees fresh prices
#   2. Compute CW indicators (writes to trades.dip_*, above_sma*, etc.)
#   3. Build PIT scores (writes to insider_ticker_scores)
#   4. Map PIT scores onto trades.pit_grade column
#   5. Compute PIT cluster sizes (was orphaned 37+ days; same outage pattern
#      as the April 2026 silent halt — see docs/postmortems/)
#   6. Compute Cohen routine flags (10b5-1-style monthly patterns)
#
# Each script writes a signal_freshness row in the same transaction as its
# data write, so the runner's preflight knows when each column was last
# refreshed (Phase 2 P0).
#
# Idempotent: each script handles "already populated" cases. Safe to re-run.
set -euo pipefail

REPO=/Users/derekg/trading-framework
PY=/opt/homebrew/bin/python3
LOG=$REPO/logs/refresh-features.log

# Limit backfill window to 30 days — covers SMA200 + recent trades cheaply.
SINCE=$(date -v-30d +%Y-%m-%d)

cd "$REPO"
echo "===== refresh-features starting at $(date) (since=$SINCE) ====="

echo "--- step 0/6: update_daily_prices (was the gap that halted strategies 2026-05-11) ---"
PYTHONUNBUFFERED=1 $PY $REPO/pipelines/insider_study/update_daily_prices.py --max-tickers 2000

echo "--- step 1/6: sync PG prices → SQLite cache ---"
$PY $REPO/strategies/insider_catalog/sync_prices_sqlite.py --days 240

echo "--- step 2/6: compute_cw_indicators --since $SINCE ---"
PYTHONUNBUFFERED=1 $PY -m pipelines.insider_study.compute_cw_indicators --since "$SINCE"

echo "--- step 3/6: build_pit_scores --start $SINCE ---"
PYTHONUNBUFFERED=1 $PY -m strategies.insider_catalog.build_pit_scores \
    --start "$SINCE" \
    --end "$(date +%Y-%m-%d)" \
    --skip-migrate

echo "--- step 4/6: backfill_pit_grades --since $SINCE ---"
PYTHONUNBUFFERED=1 $PY $REPO/pipelines/insider_study/backfill_pit_grades.py --since "$SINCE"

echo "--- step 5/6: compute_pit_clusters --since $SINCE ---"
PYTHONUNBUFFERED=1 $PY $REPO/pipelines/insider_study/compute_pit_clusters.py --since "$SINCE"

echo "--- step 6/6: compute_cohen_pit --since $SINCE ---"
PYTHONUNBUFFERED=1 $PY $REPO/pipelines/insider_study/compute_cohen_pit.py --since "$SINCE"

echo "--- staleness check ---"
/opt/homebrew/bin/psql form4 -At -F"|" <<SQL
SELECT 'data_freshness',
  MAX(filing_date) AS max_filing,
  MAX(filing_date) FILTER (WHERE pit_grade IN ('A+','A')) AS max_pit_a,
  MAX(filing_date) FILTER (WHERE above_sma50 = 1) AS max_sma50_yes,
  MAX(filing_date) FILTER (WHERE pit_cluster_size IS NOT NULL) AS max_cluster_size
  FROM trades WHERE trans_code = 'P';

SELECT 'signal_freshness',
  source || '.' || table_name || '.' || column_name AS col,
  MAX(last_computed_at) AS last_computed
  FROM signal_freshness
  GROUP BY 2 ORDER BY 1;
SQL

echo "===== refresh-features done at $(date) ====="

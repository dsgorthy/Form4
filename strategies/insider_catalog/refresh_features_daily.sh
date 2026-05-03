#!/bin/bash
# Daily feature-refresh chain. Runs after insider-fetch finishes (which lands
# raw Form 4 trades). Populates trade-level features (dip_*, above_sma*,
# consecutive_sells_before, pit_grade) so strategy runners can evaluate them.
#
# Order matters:
#   1. Sync prices.db (SQLite) from PG so compute_cw_indicators sees fresh prices
#   2. Compute CW indicators (writes to trades.dip_*, above_sma*, etc.)
#   3. Build PIT scores (writes to insider_ticker_scores)
#   4. Map PIT scores onto trades.pit_grade column
#
# Idempotent: each script handles "already populated" cases. Safe to re-run.
set -euo pipefail

REPO=/Users/derekg/trading-framework
PY=/usr/bin/python3
LOG=$REPO/logs/refresh-features.log

# Limit backfill window to 30 days — covers SMA200 + recent trades cheaply.
SINCE=$(date -v-30d +%Y-%m-%d)

cd "$REPO"
echo "===== refresh-features starting at $(date) (since=$SINCE) ====="

echo "--- step 1/4: sync PG prices → SQLite cache ---"
$PY $REPO/strategies/insider_catalog/sync_prices_sqlite.py --days 240

echo "--- step 2/4: compute_cw_indicators --since $SINCE ---"
PYTHONUNBUFFERED=1 $PY -m pipelines.insider_study.compute_cw_indicators --since "$SINCE"

echo "--- step 3/4: build_pit_scores --start $SINCE ---"
PYTHONUNBUFFERED=1 $PY -m strategies.insider_catalog.build_pit_scores \
    --start "$SINCE" \
    --end "$(date +%Y-%m-%d)" \
    --skip-migrate

echo "--- step 4/4: backfill_pit_grades --since $SINCE ---"
PYTHONUNBUFFERED=1 $PY $REPO/pipelines/insider_study/backfill_pit_grades.py --since "$SINCE"

echo "--- staleness check ---"
/opt/homebrew/bin/psql form4 -At -F"|" <<SQL
SELECT 'data_freshness',
  MAX(filing_date) AS max_filing,
  MAX(filing_date) FILTER (WHERE pit_grade IN ('A+','A')) AS max_pit_a,
  MAX(filing_date) FILTER (WHERE above_sma50 = 1) AS max_sma50_yes
  FROM trades WHERE trans_code = 'P';
SQL

echo "===== refresh-features done at $(date) ====="

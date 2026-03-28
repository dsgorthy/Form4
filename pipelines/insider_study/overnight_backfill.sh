#!/bin/bash
# Overnight EDGAR + data transformation backfill
# Pulls Form 4 filings from 2016-2019 via EDGAR EFTS API,
# applies all data transformations, and recomputes track records.
#
# Alpaca SIP data goes back to 2016-01-04, so we align EDGAR pull to that.
# The bulk import already covers 2020+ solidly.
#
# Run from: /Users/openclaw/trading-framework
# Expected runtime: 6-10 hours (EDGAR rate-limited to ~10 req/sec)

set -e

cd /Users/openclaw/trading-framework
LOG_DIR="pipelines/insider_study/data"
LOG_FILE="${LOG_DIR}/overnight_backfill_$(date +%Y%m%d_%H%M%S).log"

echo "=== Overnight Backfill Started: $(date) ===" | tee "$LOG_FILE"
echo "Log: $LOG_FILE" | tee -a "$LOG_FILE"

# ── Step 1: EDGAR live pull (2016-2019, one year at a time) ──────────────
# Process year by year to checkpoint progress and avoid memory issues.
# backfill_live.py handles dedup via INSERT OR IGNORE.

for YEAR in 2016 2017 2018 2019; do
    echo "" | tee -a "$LOG_FILE"
    echo "=== EDGAR Pull: ${YEAR} ===" | tee -a "$LOG_FILE"
    echo "Started: $(date)" | tee -a "$LOG_FILE"

    cd /Users/openclaw/trading-framework/strategies/insider_catalog
    python3 backfill_live.py \
        --start "${YEAR}-01-01" \
        --end "${YEAR}-12-31" \
        2>&1 | tee -a "/Users/openclaw/trading-framework/$LOG_FILE"

    echo "Completed ${YEAR}: $(date)" | tee -a "/Users/openclaw/trading-framework/$LOG_FILE"
done

cd /Users/openclaw/trading-framework

# ── Step 2: Normalize titles for all new trades ──────────────────────────
echo "" | tee -a "$LOG_FILE"
echo "=== Title Normalization ===" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"

python3 -m strategies.insider_catalog.normalize_titles 2>&1 | tee -a "$LOG_FILE"

echo "Completed: $(date)" | tee -a "$LOG_FILE"

# ── Step 3: Entity resolution for new insiders ───────────────────────────
echo "" | tee -a "$LOG_FILE"
echo "=== Entity Resolution ===" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"

cd /Users/openclaw/trading-framework/strategies/insider_catalog
INSIDER_DEDUP=1 python3 entity_resolution.py 2>&1 | tee -a "/Users/openclaw/trading-framework/$LOG_FILE"

cd /Users/openclaw/trading-framework

# ── Step 4: Recompute track records ──────────────────────────────────────
echo "" | tee -a "$LOG_FILE"
echo "=== Track Record Recomputation ===" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"

python3 -c "
import sqlite3
from strategies.insider_catalog.backfill import compute_track_records, print_summary, DB_PATH

conn = sqlite3.connect(str(DB_PATH))
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')
compute_track_records(conn)
print_summary(conn)
conn.close()
" 2>&1 | tee -a "$LOG_FILE"

echo "Completed: $(date)" | tee -a "$LOG_FILE"

# ── Step 5: Data quality report ──────────────────────────────────────────
echo "" | tee -a "$LOG_FILE"
echo "=== Data Quality Report ===" | tee -a "$LOG_FILE"

python3 -m strategies.insider_catalog.data_quality 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "=== Overnight Backfill Complete: $(date) ===" | tee -a "$LOG_FILE"

# Final summary
python3 -c "
import sqlite3
conn = sqlite3.connect('strategies/insider_catalog/insiders.db')
conn.row_factory = sqlite3.Row
src = conn.execute('SELECT source, COUNT(*) as n, MIN(trade_date) as earliest, MAX(trade_date) as latest FROM trades GROUP BY source').fetchall()
print()
print('=== Final Trade Coverage ===')
for r in src:
    print(f'  {r[\"source\"]}: {r[\"n\"]:,} trades ({r[\"earliest\"]} to {r[\"latest\"]})')
total = conn.execute('SELECT COUNT(*) FROM trades').fetchone()[0]
print(f'  TOTAL: {total:,} trades')
conn.close()
" 2>&1 | tee -a "$LOG_FILE"

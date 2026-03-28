#!/bin/bash
# Chains after EDGAR backfill: price extension + return computation
# Waits for PID 19819 (backfill shell) to finish, then runs sequentially.

set -e
cd /Users/openclaw/trading-framework
LOG="/Users/openclaw/trading-framework/pipelines/insider_study/data/post_backfill_chain.log"

echo "=== Waiting for EDGAR backfill (PID 19819) to complete ===" | tee "$LOG"
while kill -0 19819 2>/dev/null; do sleep 60; done
echo "EDGAR backfill finished: $(date)" | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "=== Step 1: Extend prices to 2016 ===" | tee -a "$LOG"
echo "Started: $(date)" | tee -a "$LOG"
python3 pipelines/insider_study/extend_prices_2016.py 2>&1 | tee -a "$LOG"
echo "Completed: $(date)" | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "=== Step 2: Compute trade returns (7d/30d/90d) ===" | tee -a "$LOG"
echo "Started: $(date)" | tee -a "$LOG"
cd /Users/openclaw/trading-framework/strategies/insider_catalog
python3 compute_returns.py --trade-type both 2>&1 | tee -a "$LOG"
echo "Completed: $(date)" | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "=== Step 3: Backfill 14d/60d returns ===" | tee -a "$LOG"
echo "Started: $(date)" | tee -a "$LOG"
cd /Users/openclaw/trading-framework/pipelines/insider_study
python3 backfill_14d_60d.py 2>&1 | tee -a "$LOG"
echo "Completed: $(date)" | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "=== Post-backfill chain complete: $(date) ===" | tee -a "$LOG"

#!/bin/bash
# Full cleanup pipeline: wait for active backfill, clean names, rebuild aggregates, re-run 2018
set -e
cd /Users/openclaw/trading-framework/strategies/insider_catalog

echo "$(date '+%H:%M:%S') === CLEANUP PIPELINE START ==="

# Step 1: Wait for the 2019 backfill to finish
PID_2019=$(pgrep -f "backfill_live.*2019" 2>/dev/null || true)
if [ -n "$PID_2019" ]; then
    echo "$(date '+%H:%M:%S') Waiting for 2019 backfill (PID $PID_2019) to finish..."
    while kill -0 "$PID_2019" 2>/dev/null; do
        sleep 10
    done
    echo "$(date '+%H:%M:%S') 2019 backfill finished"
else
    echo "$(date '+%H:%M:%S') No 2019 backfill running"
fi

# Step 2: Clean display names for all insiders missing them
echo "$(date '+%H:%M:%S') Running name cleaner..."
python3 name_cleaner.py
python3 name_cleaner.py --stats

# Step 3: Run price validator (catches anything the 2019 backfill may have added)
echo "$(date '+%H:%M:%S') Running price validator..."
python3 price_validator.py

# Step 4: Run 2018 backfill (has name cleaning + price validation integrated)
echo "$(date '+%H:%M:%S') Starting 2018 backfill..."
python3 backfill_live.py --start 2018-01-01 --end 2018-12-31

# Step 5: Final name clean pass (catch anything 2018 added)
echo "$(date '+%H:%M:%S') Final name cleaning pass..."
python3 name_cleaner.py
python3 name_cleaner.py --stats

# Step 6: Verify key names
echo ""
echo "$(date '+%H:%M:%S') === KEY INSIDER VERIFICATION ==="
sqlite3 insiders.db "
SELECT name, display_name
FROM insiders
WHERE name_normalized IN (
    'nadella satya', 'musk elon', 'dell michael s',
    'icahn carl c', 'ackman william a', 'smith bradford l',
    'baker julian', 'buffett warren e'
)
ORDER BY name_normalized
"

echo ""
echo "$(date '+%H:%M:%S') === NULL DISPLAY NAMES REMAINING ==="
sqlite3 insiders.db "SELECT COUNT(*) FROM insiders WHERE display_name IS NULL"

echo ""
echo "$(date '+%H:%M:%S') === CLEANUP PIPELINE COMPLETE ==="

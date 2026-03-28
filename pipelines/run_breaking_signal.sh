#!/usr/bin/env bash
# Breaking signal detector — runs every 30 min during market hours
# Detects high-profile insider trades and generates content + assets
# Copies output to Google Drive for immediate posting
cd /Users/openclaw/trading-framework
set -a
source .env 2>/dev/null
set +a

DATE=$(date +%Y-%m-%d)
HOUR=$(date +%H)

# Only run during extended market hours (8 AM - 7 PM ET)
# SEC filings can drop outside market hours
if [ "$HOUR" -lt 8 ] || [ "$HOUR" -gt 19 ]; then
    exit 0
fi

# Skip weekends
DOW=$(date +%u)
if [ "$DOW" -gt 5 ]; then
    exit 0
fi

echo "$(date): Checking for breaking signals..."

# Run detector (--limit 2 = max 2 signals per run, dedup prevents repeats)
OUTPUT=$(/opt/homebrew/bin/python3 pipelines/generate_breaking_signal.py --date "$DATE" --limit 2 2>&1)
echo "$OUTPUT"

# Copy any new breaking signal folders to Google Drive
CONTENT_DIR="pipelines/data/content"
DRIVE_BASE="$HOME/Library/CloudStorage/GoogleDrive-derek@sidequestgroup.com/My Drive/personal-share/form4-content"
DATE_SLUG=$(date +%Y%m%d)

for sig_dir in "$CONTENT_DIR/${DATE_SLUG}_breaking_"*; do
    if [ -d "$sig_dir" ]; then
        dir_name=$(basename "$sig_dir")
        dest="$DRIVE_BASE/$DATE/$dir_name"
        if [ ! -d "$dest" ]; then
            mkdir -p "$dest"
            cp -r "$sig_dir/"* "$dest/" 2>/dev/null
            echo "$(date): Copied $dir_name to Drive"
        fi
    fi
done

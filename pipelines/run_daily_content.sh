#!/usr/bin/env bash
# Daily content generation — runs at 5 PM ET after market close
#
# Generates:
#   1. Storyboard + SRT + captions + audio (video content)
#   2. Carousel assets: charts, logos, trades.json per group (for Canva)
#   3. slides.txt cheat sheet + carousel_caption.txt per group
#   4. Weekly aggregation on Fridays
#   5. Copies everything to Google Drive
#   6. Backs up old content from GDrive to local (>7 days)
#
cd /Users/openclaw/trading-framework
set -a
source .env 2>/dev/null
set +a

DATE=$(date +%Y-%m-%d)
DATE_SLUG=$(date +%Y%m%d)
DOW=$(date +%u)  # 1=Mon, 5=Fri, 6=Sat, 7=Sun
PYTHON=/opt/homebrew/bin/python3
echo "$(date): === Daily content pipeline for $DATE (day $DOW) ==="

CONTENT_DIR="pipelines/data/content"
DRIVE_BASE="$HOME/Library/CloudStorage/GoogleDrive-derek@sidequestgroup.com/My Drive/personal-share/form4-content"
DRIVE_DIR="$DRIVE_BASE/$DATE"
mkdir -p "$DRIVE_DIR"

# Step 1: Generate storyboard + captions + SRT + visual assets + audio
echo "$(date): Step 1 — Generating storyboard + assets + audio..."
$PYTHON pipelines/generate_daily_content.py --date "$DATE" --audio 2>&1

# Step 2: Generate carousel assets for Canva (charts, logos, trades.json)
echo "$(date): Step 2 — Generating carousel assets..."
$PYTHON pipelines/render_ig_carousel.py --date "$DATE" 2>&1

# Step 3: Generate slides.txt cheat sheet + carousel_caption.txt per group
echo "$(date): Step 3 — Generating slides.txt + captions..."
$PYTHON pipelines/generate_slides_txt.py --date "$DATE" 2>&1

# Step 4: Weekly aggregation on Fridays (for weekend posting)
if [ "$DOW" = "5" ]; then
    echo "$(date): Step 4 — Friday: generating weekly aggregation..."
    $PYTHON pipelines/generate_slides_txt.py --date "$DATE" --weekly 2>&1
fi

# Step 5: Copy to Google Drive
echo "$(date): Step 5 — Copying to Google Drive..."

# Copy storyboard folder (new nested format)
if [ -d "$CONTENT_DIR/$DATE_SLUG" ]; then
    cp -r "$CONTENT_DIR/$DATE_SLUG" "$DRIVE_DIR/storyboard/" 2>/dev/null
fi

# Copy carousel asset folders (one per group, including weekly on Fridays)
for carousel_dir in "$CONTENT_DIR/${DATE_SLUG}_carousel_"*; do
    if [ -d "$carousel_dir" ]; then
        group_name=$(basename "$carousel_dir" | sed "s/${DATE_SLUG}_carousel_//")
        mkdir -p "$DRIVE_DIR/carousels/$group_name"
        cp -r "$carousel_dir/"* "$DRIVE_DIR/carousels/$group_name/" 2>/dev/null
        echo "  Carousel: $group_name"
    fi
done

# Copy flat files
cp "$CONTENT_DIR/${DATE_SLUG}_video_script.txt" "$DRIVE_DIR/storyboard.txt" 2>/dev/null
cp "$CONTENT_DIR/${DATE_SLUG}_x_post.txt" "$DRIVE_DIR/x_post.txt" 2>/dev/null
cp "$CONTENT_DIR/${DATE_SLUG}_captions.txt" "$DRIVE_DIR/captions.txt" 2>/dev/null

# Step 6: Backup old content from GDrive to local (>7 days)
echo "$(date): Step 6 — Backing up old content..."
$PYTHON pipelines/content_sync.py --backup 2>&1

echo "$(date): === Done ==="

# Step 7: Send Telegram notification
echo "$(date): Step 7 — Sending Telegram notification..."

# Build summary of what's ready
CAROUSEL_SUMMARY=""
for d in "$DRIVE_DIR/carousels/"*/; do
    if [ -d "$d" ] && [ -f "$d/slides.txt" ]; then
        group=$(basename "$d")
        line2=$(head -2 "$d/slides.txt" | tail -1)
        CAROUSEL_SUMMARY="${CAROUSEL_SUMMARY}
📊 ${group}: ${line2}"
    fi
done

if [ -z "$CAROUSEL_SUMMARY" ]; then
    CAROUSEL_SUMMARY="
⚠️ No carousels today (not enough Q7+ signals)"
fi

# Check if it's Friday — mention weekly
WEEKLY_NOTE=""
if [ "$DOW" = "5" ]; then
    for d in "$DRIVE_DIR/carousels/weekly_"*/; do
        if [ -d "$d" ]; then
            group=$(basename "$d")
            line2=$(head -2 "$d/slides.txt" | tail -1)
            WEEKLY_NOTE="${WEEKLY_NOTE}
📅 ${group}: ${line2}"
        fi
    done
    if [ -n "$WEEKLY_NOTE" ]; then
        WEEKLY_NOTE="

Weekend carousels ready:${WEEKLY_NOTE}"
    fi
fi

MSG="📢 *Form4 Content Ready — ${DATE}*
${CAROUSEL_SUMMARY}${WEEKLY_NOTE}

✅ Assets in Google Drive: form4-content/${DATE}/
📝 Open slides.txt for copy + asset paths
📱 Post to: IG, X, FB"

curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${TELEGRAM_CHAT_ID}" \
    -d "parse_mode=Markdown" \
    --data-urlencode "text=${MSG}" > /dev/null 2>&1

echo "$(date): Telegram notification sent"

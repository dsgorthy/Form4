#!/bin/bash
# Options pull monitor — sends status to Trading Telegram bot every 5 minutes
# Kill with: kill $(cat /tmp/pull_monitor.pid)

BOT_TOKEN="8676824600:AAHcTkRFmRL25HwW1OC-l1jPyoDmYiu69u0"
CHAT_ID="8585305446"
LOG_DIR="/Users/openclaw/trading-framework/pipelines/insider_study/data"
PULL_DIR="/Users/openclaw/trading-framework/pipelines/insider_study"
LOG_FILE="${1:-full_pull_2016_2019.log}"
TOTAL_BUYS="${2:-19457}"

echo $$ > /tmp/pull_monitor.pid

send_tg() {
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        -d chat_id="${CHAT_ID}" \
        -d text="$1" \
        -d parse_mode="Markdown" > /dev/null 2>&1
}

send_tg "📡 Pull monitor started (PID $$). Reporting every 5 minutes."

while true; do
    sleep 300

    # Check if buy-side is running
    BUY_PID=$(pgrep -f 'options_pull.py.*buys-only' | head -1)
    SELL_PID=$(pgrep -f 'options_pull.py.*sells-only' | head -1)

    if [ -n "$BUY_PID" ]; then
        # Buy-side running — parse progress
        LAST_LINE=$(grep '\[' "$LOG_DIR/$LOG_FILE" | tail -1)
        CURRENT=$(echo "$LAST_LINE" | sed -n 's/.*\[\([0-9]*\)-[0-9]*\/[0-9]*\].*/\1/p')
        TICKER=$(echo "$LAST_LINE" | sed -n 's/.*\] \([A-Z]*\) .*/\1/p')
        if [ -n "$CURRENT" ]; then
            PCT=$(echo "scale=1; $CURRENT * 100 / $TOTAL_BUYS" | bc)
            FORMATTED=$(printf "%'d" "$CURRENT")
            MSG="Buy-side: ${FORMATTED}/${TOTAL_BUYS} events (${PCT}%) — currently pulling ${TICKER}. Process healthy (PID ${BUY_PID})."
        else
            MSG="Buy-side running (PID ${BUY_PID}) but couldn't parse progress."
        fi
        send_tg "$MSG"

    elif [ -n "$SELL_PID" ]; then
        # Sell-side running
        LAST_LINE=$(grep '\[' "$LOG_DIR/full_pull_sells.log" 2>/dev/null | tail -1)
        CURRENT=$(echo "$LAST_LINE" | sed -n 's/.*\[\([0-9]*\)\/[0-9]*\].*/\1/p')
        TOTAL_S=$(echo "$LAST_LINE" | sed -n 's/.*\[[0-9]*\/\([0-9]*\)\].*/\1/p')
        TICKER=$(echo "$LAST_LINE" | sed -n 's/.*\] \([A-Z]*\) .*/\1/p')
        if [ -n "$CURRENT" ] && [ -n "$TOTAL_S" ]; then
            PCT=$(echo "scale=1; $CURRENT * 100 / $TOTAL_S" | bc)
            FORMATTED=$(printf "%'d" "$CURRENT")
            TOTAL_F=$(printf "%'d" "$TOTAL_S")
            MSG="Sell-side: ${FORMATTED}/${TOTAL_F} events (${PCT}%) — currently pulling ${TICKER}. Process healthy (PID ${SELL_PID})."
        else
            MSG="Sell-side running (PID ${SELL_PID}) but couldn't parse progress."
        fi
        send_tg "$MSG"

    else
        # Neither running — check if buys finished
        LAST_BUY=$(grep '\[' "$LOG_DIR/$LOG_FILE" | tail -1)
        LAST_NUM=$(echo "$LAST_BUY" | sed -n 's/.*\[\([0-9]*\)-[0-9]*\/[0-9]*\].*/\1/p')

        if [ "$LAST_NUM" -ge "$TOTAL_BUYS" ] 2>/dev/null; then
            send_tg "✅ Buy-side pull COMPLETE (${TOTAL_BUYS}/${TOTAL_BUYS}). Monitor exiting."
            exit 0
        else
            # Buys crashed — restart
            TAIL=$(tail -3 "$LOG_DIR/$LOG_FILE")
            send_tg "⚠️ Buy-side process died at event ${LAST_NUM}/${TOTAL_BUYS}. Restarting...

Last log:
\`\`\`
${TAIL}
\`\`\`"
            cd "$PULL_DIR"
            nohup python3 options_pull.py --full --all --buys-only --batch-size 4 --buy-csv data/results_bulk_2016_2019_7d.csv >> "$LOG_DIR/$LOG_FILE" 2>&1 &
            NEW_PID=$!
            send_tg "Buy-side restarted (PID ${NEW_PID})."
        fi
    fi
done

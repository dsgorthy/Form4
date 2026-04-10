#!/usr/bin/env bash
# Form4 uptime monitor — pings critical endpoints, alerts on sustained failure.
# Designed to run every 60s via launchd. Tracks consecutive failures in a state
# file so a single transient blip doesn't page (3+ consecutive = real outage).

set -uo pipefail

BASE="https://form4.app"
TIMEOUT=5
STATE_FILE="/tmp/form4-uptime-state.json"
LOG_FILE="/Users/openclaw/trading-framework/logs/uptime.log"
TG_BOT="8676824600:AAHcTkRFmRL25HwW1OC-l1jPyoDmYiu69u0"
TG_CHAT="${TELEGRAM_CHAT_ID:-}"
ALERT_THRESHOLD=3   # consecutive failures before alerting

# Critical endpoints — keep this list small (one ping every 60s × this many)
ENDPOINTS=(
    "/api/v1/health"
    "/api/v1/dashboard/stats"
    "/api/v1/filings?limit=1"
    "/api/v1/clusters?days=14&limit=1"
    "/"
)

mkdir -p "$(dirname "$LOG_FILE")"

ts() { date "+%Y-%m-%d %H:%M:%S"; }

log() { echo "[$(ts)] $*" >> "$LOG_FILE"; }

send_telegram() {
    local msg="$1"
    if [ -z "$TG_CHAT" ]; then
        log "TELEGRAM_CHAT_ID not set, skipping alert: $msg"
        return
    fi
    curl -s -X POST "https://api.telegram.org/bot${TG_BOT}/sendMessage" \
        -d chat_id="${TG_CHAT}" \
        -d text="$msg" \
        -d parse_mode="Markdown" > /dev/null 2>&1 || true
}

# Read previous state (consecutive_failures, last_alert_state)
prev_failures=0
prev_state="ok"
if [ -f "$STATE_FILE" ]; then
    prev_failures=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('consecutive_failures', 0))" 2>/dev/null || echo 0)
    prev_state=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('alert_state', 'ok'))" 2>/dev/null || echo "ok")
fi

# Hit each endpoint, count failures
failed_endpoints=()
for path in "${ENDPOINTS[@]}"; do
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" "$BASE$path" 2>/dev/null || echo "000")
    if [ "$code" != "200" ]; then
        failed_endpoints+=("$path → $code")
    fi
done

if [ ${#failed_endpoints[@]} -gt 0 ]; then
    new_failures=$((prev_failures + 1))
    log "FAIL [$new_failures] ${failed_endpoints[*]}"

    # Alert when we cross the threshold (only once per outage)
    if [ "$new_failures" -ge "$ALERT_THRESHOLD" ] && [ "$prev_state" != "down" ]; then
        msg="🚨 *form4.app DOWN*

After $new_failures consecutive failures (60s apart):
$(printf '• %s\n' "${failed_endpoints[@]}")

Check: \`docker logs trading-framework-api-1 --tail 50\`"
        send_telegram "$msg"
        prev_state="down"
        log "ALERT SENT (state: down)"
    fi

    # Update state
    python3 -c "import json; json.dump({'consecutive_failures': $new_failures, 'alert_state': '$prev_state', 'last_check': '$(ts)'}, open('$STATE_FILE', 'w'))"
else
    # All endpoints OK
    if [ "$prev_state" = "down" ]; then
        msg="✅ *form4.app RECOVERED*

After being down for ~$((prev_failures * 60))s, all endpoints responding 200."
        send_telegram "$msg"
        log "RECOVERY ALERT SENT"
    fi
    log "OK"
    python3 -c "import json; json.dump({'consecutive_failures': 0, 'alert_state': 'ok', 'last_check': '$(ts)'}, open('$STATE_FILE', 'w'))"
fi

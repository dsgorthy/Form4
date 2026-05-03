#!/usr/bin/env bash
# Stream API container logs, filter for 500s and exceptions, alert via Telegram.
# Buffers + dedupes — same error within 5 minutes only sends one alert.
# Designed to run as a long-lived launchd KeepAlive process.

set -uo pipefail

CONTAINER="trading-framework-api-1"
LOG_FILE="/Users/openclaw/trading-framework/logs/api-errors.log"
ALERT_LOG="/Users/openclaw/trading-framework/logs/alerts.ndjson"
DEDUPE_DB="/tmp/form4-error-dedupe.txt"
DEDUPE_WINDOW=300   # 5 minutes

mkdir -p "$(dirname "$LOG_FILE")"
touch "$DEDUPE_DB"

ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" >> "$LOG_FILE"; }

emit_alert() {
    local severity="$1"
    local message="$2"
    local utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local esc_msg
    esc_msg=$(printf '%s' "$message" | python3 -c 'import sys, json; print(json.dumps(sys.stdin.read()))' 2>/dev/null) \
        || esc_msg="\"(message escape failed)\""
    mkdir -p "$(dirname "$ALERT_LOG")"
    printf '{"ts":"%s","severity":"%s","component":"api_error_tail","message":%s}\n' \
        "$utc" "$severity" "$esc_msg" >> "$ALERT_LOG"
}

# Returns 0 if we should alert (not seen recently), 1 if deduped
should_alert() {
    local key="$1"
    local now=$(date +%s)
    # Clean entries older than DEDUPE_WINDOW
    if [ -s "$DEDUPE_DB" ]; then
        awk -v cutoff=$((now - DEDUPE_WINDOW)) '$1 >= cutoff' "$DEDUPE_DB" > "$DEDUPE_DB.new"
        mv "$DEDUPE_DB.new" "$DEDUPE_DB"
    fi
    # Check if key seen in window
    if grep -qF "|$key" "$DEDUPE_DB" 2>/dev/null; then
        return 1
    fi
    echo "$now|$key" >> "$DEDUPE_DB"
    return 0
}

log "Started error tail for container $CONTAINER"

# Stream new logs (--follow), only the most recent
docker logs --follow --tail 0 "$CONTAINER" 2>&1 | while IFS= read -r line; do
    # Match 500 status lines
    if echo "$line" | grep -qE "500 Internal Server Error"; then
        # Extract endpoint from uvicorn access log: GET /api/v1/foo HTTP/1.1
        endpoint=$(echo "$line" | grep -oE '"[A-Z]+ [^"]+"' | head -1 | tr -d '"')
        if [ -z "$endpoint" ]; then endpoint="(unknown endpoint)"; fi

        log "500: $endpoint"

        # Dedupe by endpoint (so 100 hits to same broken route = 1 alert per 5min)
        dedupe_key=$(echo "$endpoint" | sed 's/[?].*//')   # strip query string
        if should_alert "$dedupe_key"; then
            emit_alert "error" "form4 API 500 — $endpoint. Check: docker logs $CONTAINER --tail 50 | grep -B2 -A20 'Traceback'"
            log "ALERTED: $endpoint"
        fi
    fi

    # Match exception traces — first line of psycopg2.* or other exceptions
    if echo "$line" | grep -qE "^psycopg2\.|^sqlite3\.|^OperationalError|^InterfaceError"; then
        log "EXC: $line"
        dedupe_key=$(echo "$line" | head -c 80)
        if should_alert "$dedupe_key"; then
            local short_line=$(echo "$line" | head -c 200)
            emit_alert "error" "form4 API Exception — $short_line. Check: docker logs $CONTAINER --tail 100 | grep -B 1 -A 15"
            log "ALERTED EXC: $line"
        fi
    fi
done

log "Tail loop exited (container restarted or docker error). Will be restarted by KeepAlive."

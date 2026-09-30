#!/usr/bin/env bash
# Stream API container logs, filter for 500s and exceptions, and PUSH.
# Buffers + dedupes — same error within 5 minutes only sends one alert.
# Designed to run as a long-lived launchd KeepAlive process.

set -uo pipefail

CONTAINER="trading-framework-api-1"
LOG_FILE="/Users/derekg/trading-framework/logs/api-errors.log"
ALERT_LOG="/Users/derekg/trading-framework/logs/alerts.ndjson"
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

    # PUSH, do not only log.
    #
    # This wrote to alerts.ndjson and nothing else, and the header claimed
    # Telegram it had not used in months. On 2026-08-24 the Stripe webhook
    # returned 500 to every event for weeks — the exact thing this filter
    # catches — and a paying customer had to email support about it. A monitor
    # whose output nobody reads is indistinguishable from no monitor. Dedupe
    # above caps this at one push per distinct error per 5 minutes.
    if [ "$severity" = "error" ] || [ "$severity" = "critical" ]; then
        ( \
          /opt/homebrew/bin/python3 -c '
import sys
sys.path.insert(0, "/Users/derekg/trading-framework")
try:
    from dotenv import load_dotenv; load_dotenv("/Users/derekg/trading-framework/.env")
except Exception: pass
from framework.alerts.ntfy import send_ntfy
send_ntfy(sys.argv[1], title="form4 API error", priority=4, tags=["rotating_light"])
' "$message" >/dev/null 2>&1 ) &
    fi
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

        # Dedupe by ROUTE TEMPLATE, not the concrete path.
        #
        # This stripped only the query string, so /api/v1/filings/xdfh9d and
        # /api/v1/filings/vsvzxd were different keys — and every insider and
        # filing id is a different key. The stated intent, "100 hits to the same
        # broken route = 1 alert per 5min", was therefore never achieved on any
        # parameterised route: ONE broken endpoint sent one push per distinct id,
        # unbounded.
        #
        # Measured 2026-09-30 during the port-exhaustion outage: 94 pushes in six
        # hours and 21 in five minutes, all "form4 API 500" on the same two or
        # three routes with different ids, while a crawler walked thousands of
        # insider pages. That is the flood, and a pager that does that is one
        # people mute.
        #
        # Collapse the id-shaped segments so the key is the route: sqids
        # (6+ chars of base58-ish), numeric ids, and slugs that carry a trailing
        # sqid. Tickers stay — /companies/GME and /companies/AAPL failing are
        # genuinely different facts.
        # Order matters: drop the query string AND the " HTTP/1.1" suffix
        # before collapsing, or a greedy [^/]+ eats the protocol too and
        # /insiders/allan-bombard-gkpgbv becomes /insiders/{id}/1.1.
        dedupe_key=$(echo "$endpoint" \
          | sed 's/[?].*//' \
          | sed -E 's# HTTP/[0-9.]+$##' \
          | sed -E 's#/(filings|trades)/[A-Za-z0-9]{5,}#/\1/{id}#g' \
          | sed -E 's#/insiders/[^/ ]+#/insiders/{id}#g' \
          | sed -E 's#/[0-9]{3,}#/{n}#g')
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

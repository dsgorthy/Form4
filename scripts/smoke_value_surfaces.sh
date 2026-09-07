#!/usr/bin/env bash
# Exercise every surface that publishes a dollar figure.
#
# `studio deploy form4` checks /api/v1/health and nothing else, so a SQL change
# to a router query ships unverified — the unit suite cannot catch it either,
# because the Mini has no form4 DB and never parses the SQL. This sweep is the
# missing gate: it asks each affected endpoint for a real answer.
#
# Usage: scripts/smoke_value_surfaces.sh [base_url]
set -uo pipefail
BASE="${1:-https://form4.app}"
fail=0; ok=0

hit() {  # hit <path> [jq-expr to echo]
  local path="$1" show="${2:-}"
  local body code
  body=$(curl -sS -m 25 -w $'\n%{http_code}' "$BASE$path" 2>/dev/null)
  code=$(printf '%s' "$body" | tail -1)
  body=$(printf '%s' "$body" | sed '$d')
  if [ "$code" != "200" ]; then
    printf '  \033[31mFAIL\033[0m %-52s HTTP %s\n' "$path" "$code"
    printf '%s' "$body" | head -c 220 | sed 's/^/        /'; echo
    fail=$((fail+1)); return
  fi
  ok=$((ok+1))
  if [ -n "$show" ]; then
    printf '  \033[32m ok \033[0m %-52s %s\n' "$path" \
      "$(printf '%s' "$body" | jq -r "$show" 2>/dev/null | head -1)"
  else
    printf '  \033[32m ok \033[0m %-52s\n' "$path"
  fi
}

echo "== value surfaces on $BASE =="
hit /api/v1/dashboard/stats
hit /api/v1/dashboard/highlights
hit /api/v1/dashboard/sentiment
hit /api/v1/clusters                        '"clusters=\(.clusters|length? // 0)"'
hit /api/v1/signals/sell-cessation
hit /api/v1/signals/tagged
hit /api/v1/sectors                         '"sectors=\(.sectors|length? // 0)"'
hit /api/v1/search?q=AAPL
hit /api/v1/companies/AAPL                  '"AAPL total=\(.total_value // 0) trades=\(.total_trades // 0)"'
hit /api/v1/companies/IHT                   '"IHT  total=\(.total_value // 0) trades=\(.total_trades // 0)"'
hit /api/v1/companies/CNTM                  '"CNTM total=\(.total_value // 0) trades=\(.total_trades // 0)"'
hit /api/v1/companies/AMMA                  '"AMMA total=\(.total_value // 0) trades=\(.total_trades // 0)"'
hit /api/v1/filings
hit /api/v1/private-companies

echo
printf 'passed %d, failed %d\n' "$ok" "$fail"
[ "$fail" -eq 0 ] || exit 1

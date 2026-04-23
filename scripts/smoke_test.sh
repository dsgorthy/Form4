#!/usr/bin/env bash
# Form4 API smoke test — hits critical endpoints, exits non-zero on any failure.
# Run after deploys and from uptime monitor.
#
# Usage:
#   ./scripts/smoke_test.sh                  # default: hit prod (https://form4.app)
#   ./scripts/smoke_test.sh http://localhost # hit a specific base URL
#
# Output: prints PASS/FAIL per endpoint, summary at end. Exit 0 = all pass.

set -uo pipefail

BASE="${1:-https://form4.app}"
TIMEOUT=10
FAILED=0
PASSED=0
FAILURES=()

# Get a real recent filing ID for the dynamic /filings/{id} test
RECENT_ID=$(curl -sf --max-time "$TIMEOUT" "$BASE/api/v1/filings?limit=1&trade_type=buy" 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['items'][0]['trade_id'])" 2>/dev/null \
  || echo "")

# Endpoints to check. Format: "label|path"
ENDPOINTS=(
  "health|/api/v1/health"
  "health-deep|/api/v1/health/deep"
  "dashboard-stats|/api/v1/dashboard/stats"
  "dashboard-sentiment|/api/v1/dashboard/sentiment?days=30"
  "dashboard-heatmap|/api/v1/dashboard/heatmap?days=365"
  "dashboard-filing-delays|/api/v1/dashboard/filing-delays"
  "filings-list|/api/v1/filings?limit=10&min_grade=B"
  "clusters|/api/v1/clusters?days=14&limit=10&offset=0"
  "portfolio-quality|/api/v1/portfolio?strategy=quality_momentum&page=1&per_page=10"
  "company-detail|/api/v1/companies/AAPL"
  "company-trades|/api/v1/companies/AAPL/trades?limit=10"
  "congress-by-ticker|/api/v1/congress/by-ticker/AAPL?limit=10"
)

if [ -n "$RECENT_ID" ]; then
  ENDPOINTS+=("filing-detail|/api/v1/filings/$RECENT_ID")
  ENDPOINTS+=("filing-related|/api/v1/filings/$RECENT_ID/related")
fi

echo "=== Smoke test: $BASE ==="
for entry in "${ENDPOINTS[@]}"; do
  label="${entry%%|*}"
  path="${entry#*|}"
  url="$BASE$path"

  # -w writes status, time. -o discards body. -s silent. --max-time hard limit.
  result=$(curl -s -o /tmp/smoke_body.$$ -w "%{http_code}|%{time_total}" \
    --max-time "$TIMEOUT" "$url" 2>&1 || echo "000|timeout")
  http_code="${result%%|*}"
  duration="${result#*|}"

  if [ "$http_code" = "200" ]; then
    # Validate it's JSON (not an HTML error page)
    if python3 -c "import json,sys; json.load(open('/tmp/smoke_body.$$'))" 2>/dev/null; then
      printf "  PASS  %-25s %s (%ss)\n" "$label" "$http_code" "$duration"
      PASSED=$((PASSED + 1))
    else
      printf "  FAIL  %-25s %s (%ss) — body is not JSON\n" "$label" "$http_code" "$duration"
      FAILURES+=("$label: 200 but invalid JSON")
      FAILED=$((FAILED + 1))
    fi
  else
    body_snip=$(head -c 200 /tmp/smoke_body.$$ 2>/dev/null | tr -d '\n')
    printf "  FAIL  %-25s %s (%ss)\n        body: %s\n" "$label" "$http_code" "$duration" "$body_snip"
    FAILURES+=("$label: HTTP $http_code")
    FAILED=$((FAILED + 1))
  fi
  rm -f /tmp/smoke_body.$$
done


# Aggregate-ceiling sanity check — catches the class of bug where a
# derivative row with notional pricing (e.g. $11M/share for a Call option
# notional, $10M for a Convertible Note face value) leaks past the
# is_derivative=0 filter into a SUM-of-value display. Realistic top
# cluster values today are sub-$1B; $10B is a 10x sanity ceiling. If this
# fires the deploy fails and the issue is caught before users see it.
clusters_body=$(curl -sf --max-time "$TIMEOUT" "$BASE/api/v1/clusters?days=14&limit=20" 2>/dev/null || echo "")
if [ -n "$clusters_body" ]; then
  max_cluster=$(echo "$clusters_body" | python3 -c "import json,sys; d=json.load(sys.stdin); print(max((c.get('total_value', 0) or 0) for c in d.get('items', [])) if d.get('items') else 0)" 2>/dev/null || echo "0")
  # 10_000_000_000 = $10B
  if python3 -c "import sys; sys.exit(0 if float('$max_cluster') < 10_000_000_000 else 1)"; then
    printf "  PASS  %-25s max cluster total_value=\$%.0f\n" "aggregate-ceiling" "$max_cluster"
    PASSED=$((PASSED + 1))
  else
    printf "  FAIL  %-25s max cluster total_value=\$%.0f exceeds \$10B sanity ceiling — likely is_derivative leak\n" "aggregate-ceiling" "$max_cluster"
    FAILURES+=("aggregate-ceiling: max cluster total_value=\$$max_cluster > \$10B")
    FAILED=$((FAILED + 1))
  fi
fi

echo ""
echo "=== Summary: $PASSED passed, $FAILED failed ==="

if [ "$FAILED" -gt 0 ]; then
  echo ""
  echo "Failures:"
  for f in "${FAILURES[@]}"; do
    echo "  - $f"
  done
  exit 1
fi

exit 0

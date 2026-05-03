#!/bin/bash
# Pre-deploy secret scanner. Refuses to let credentials enter the repo.
#
# Scans all tracked files for credential-shaped strings. Returns:
#   0 — clean
#   1 — at least one suspicious pattern matched
#
# Designed to be invoked by `studio deploy form4 --check` and as a git
# pre-commit hook. The Engineering persona's #1 rule: "if Derek is tired
# and just wants to ship, will the gate still catch the bad change?"
#
# Patterns intentionally pessimistic: false positives are cheap (one extra
# review), false negatives are catastrophic.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
cd "$REPO_ROOT"

# Pattern set. Each line is an extended-grep regex.
# Each pattern aims for "looks like a secret AND is in source code", not
# "literally any 32-character string."
PATTERNS=(
    # Alpaca paper + live keys (PK*, AK*)
    'AP[CK]A-API-KEY-ID.*[A-Z0-9]{16,}'
    '"PK[A-Z0-9]{16,}"'
    '"AK[A-Z0-9]{16,}"'

    # Telegram bot tokens (digits:alphanum format)
    '[0-9]{8,12}:[A-Za-z0-9_-]{30,}'

    # Stripe live + test keys
    '"sk_live_[A-Za-z0-9]{20,}"'
    '"sk_test_[A-Za-z0-9]{20,}"'
    '"pk_live_[A-Za-z0-9]{20,}"'

    # AWS access keys
    'AKIA[A-Z0-9]{16,}'

    # Generic hardcoded password=, api_key=, secret= assignments
    # Allows bare env-var refs like os.environ["API_KEY"]
    '(api[_-]?key|secret[_-]?key|password)\s*=\s*"[A-Za-z0-9+/=_-]{20,}"'
    "(api[_-]?key|secret[_-]?key|password)\s*=\s*'[A-Za-z0-9+/=_-]{20,}'"

    # Hardcoded bearer tokens
    'Bearer\s+[A-Za-z0-9._-]{30,}'
)

# Ignore patterns — files we explicitly skip.
EXCLUDE_PATHS=(
    ':!**/*.env.example'
    ':!**/secrets.env.example'
    ':!**/test_secret_scan*'
    ':!docs/postmortems/**'
    ':!compliance_incidents/**'
    ':!**/*.lock'
    ':!**/node_modules/**'
    ':!**/.next/**'
    ':!**/build/**'
    ':!**/dist/**'
    ':!data/**'
    ':!actions-runner/**'
    ':!frontend/.next/**'
    ':!**/__pycache__/**'
)

# Files to scan: all tracked files except excluded paths.
echo "🔍 Scanning tracked files for credential patterns..."
echo

n_hits=0
for pattern in "${PATTERNS[@]}"; do
    while IFS= read -r line; do
        if [ -z "$line" ]; then continue; fi
        n_hits=$((n_hits + 1))
        echo "  ⚠️  $line"
    done < <(git grep -E -n "$pattern" -- "${EXCLUDE_PATHS[@]}" 2>/dev/null || true)
done

if [ "$n_hits" -gt 0 ]; then
    echo
    echo "❌ FAIL: $n_hits credential-shaped match(es) found in tracked files."
    echo
    echo "Either:"
    echo "  - Move the secret to .env (gitignored) and reference via os.environ"
    echo "  - If it's a false positive, add the file to EXCLUDE_PATHS"
    echo "  - If it's a placeholder/example, name the file *.example"
    echo
    exit 1
fi

# Also scan launchd plists specifically — credentials inline are the recurring bug.
plist_hits=$(git ls-files -- '**/*.plist' | xargs -I{} grep -lE '(ALPACA_API_KEY|ALPACA_API_SECRET|TELEGRAM_BOT_TOKEN|STRIPE_SECRET_KEY)' {} 2>/dev/null | grep -v '\.example$' || true)
if [ -n "$plist_hits" ]; then
    echo "❌ FAIL: launchd plist(s) contain credential env-var DECLARATIONS:"
    echo "$plist_hits" | sed 's/^/  ⚠️  /'
    echo
    echo "Plists must NOT carry secrets even by env-var. Use scripts/launchd/run_with_env.sh"
    echo "to source ~/.config/form4/secrets.env at runtime instead."
    exit 1
fi

echo "✅ Clean — no credentials in tracked files."
exit 0

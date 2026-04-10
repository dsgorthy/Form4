#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Form4 Deployment — supports prod and sandbox environments
# =============================================================================
#
# Usage:
#   ./deploy/deploy.sh                     # deploy prod (default)
#   ./deploy/deploy.sh --env sandbox       # deploy sandbox
#   ./deploy/deploy.sh --env prod api      # deploy prod API only
#   ./deploy/deploy.sh --env sandbox frontend  # deploy sandbox frontend only
#   ./deploy/deploy.sh --status            # show prod status
#   ./deploy/deploy.sh --env sandbox --status  # show sandbox status
#   ./deploy/deploy.sh --rollback          # rollback prod
#   ./deploy/deploy.sh --env sandbox --rollback  # rollback sandbox
#
# Environments:
#   prod    → form4.app (live Stripe/Clerk, prod DB)
#   sandbox → sandbox.form4.app (test Stripe/Clerk, sandbox DB)
#
# =============================================================================

REPO_DIR="/Users/openclaw/trading-framework"
LOG_DIR="$REPO_DIR/logs"

cd "$REPO_DIR"
mkdir -p "$LOG_DIR"

# Parse --env flag
ENV="prod"
REMAINING_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            ENV="$2"
            shift 2
            ;;
        *)
            REMAINING_ARGS+=("$1")
            shift
            ;;
    esac
done
set -- "${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}"

# Environment-specific config
if [ "$ENV" = "sandbox" ]; then
    COMPOSE_FILES="-f docker-compose.yml -f docker-compose.sandbox.yml"
    ENV_FILE=".env.sandbox"
    DOMAIN="sandbox.form4.app"
    API_PREFIX="sandbox-"
    LABEL="SANDBOX"
else
    COMPOSE_FILES="-f docker-compose.yml -f docker-compose.prod.yml"
    ENV_FILE=".env"
    DOMAIN="form4.app"
    API_PREFIX=""
    LABEL="PRODUCTION"
fi

LOG_FILE="$LOG_DIR/deploy-${ENV}-$(date +%Y%m%d-%H%M%S).log"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$LABEL] $*" | tee -a "$LOG_FILE"; }

# --- Status command ---
if [[ "${1:-}" == "--status" ]]; then
    echo "=== $LABEL CONTAINERS ==="
    docker compose $COMPOSE_FILES ps 2>/dev/null || echo "No containers"
    echo ""
    if [ "$ENV" = "prod" ]; then
        echo "=== LAUNCHD JOBS ==="
        for job in daily-content breaking-signal portfolio-runner insider-fetch form4-notifications backfill-returns congress-scraper ceowatcher-reader; do
            status=$(launchctl list 2>/dev/null | grep "com.openclaw.${job}" | awk '{print $1}' || true)
            if [ -n "$status" ]; then
                if [ "$status" = "-" ]; then echo "  $job: loaded (scheduled)"
                else echo "  $job: running (PID $status)"
                fi
            else echo "  $job: NOT LOADED"
            fi
        done
        echo ""
    fi
    echo "=== DATABASES ($ENV) ==="
    if [ "$ENV" = "sandbox" ]; then
        for db in strategies/insider_catalog/sandbox/insiders.db strategies/insider_catalog/sandbox/prices.db; do
            if [ -f "$db" ]; then echo "  $db: $(ls -lh "$db" | awk '{print $5}')"
            else echo "  $db: NOT INITIALIZED (run: python3 pipelines/db_sync.py init-sandbox)"
            fi
        done
    else
        for db in strategies/insider_catalog/insiders.db strategies/insider_catalog/prices.db strategies/insider_catalog/research.db; do
            if [ -f "$db" ]; then echo "  $db: $(ls -lh "$db" | awk '{print $5}')"
            else echo "  $db: MISSING"
            fi
        done
    fi
    echo ""
    echo "=== ENDPOINTS ==="
    api_code=$(curl -sf -o /dev/null -w "%{http_code}" "https://${DOMAIN}/api/v1/health" 2>/dev/null || echo "000")
    fe_code=$(curl -sf -o /dev/null -w "%{http_code}" "https://${DOMAIN}/" 2>/dev/null || echo "000")
    echo "  API (/api/v1/health): HTTP $api_code"
    echo "  Frontend (/): HTTP $fe_code"
    exit 0
fi

# --- Logs command ---
if [[ "${1:-}" == "--logs" ]]; then
    docker compose $COMPOSE_FILES logs --tail=50 -f
    exit 0
fi

# --- Rollback command ---
if [[ "${1:-}" == "--rollback" ]]; then
    log "Rolling back — restarting with existing images..."
    docker compose $COMPOSE_FILES down
    docker compose $COMPOSE_FILES up -d
    log "Rollback complete"
    exit 0
fi

# --- Deploy ---
SERVICE="${1:-}"

log "=== Starting deploy${SERVICE:+ ($SERVICE only)} ==="
log "Environment: $ENV | Domain: $DOMAIN | Env file: $ENV_FILE"

# Pre-deploy checks
log "Pre-deploy checks..."

CHANGED=$(git diff --name-only HEAD 2>/dev/null | grep "\.py$" || true)
if [ -n "$CHANGED" ]; then
    for f in $CHANGED; do
        if [ -f "$f" ]; then
            python3 -c "import py_compile; py_compile.compile('$f', doraise=True)" 2>&1 || {
                log "SYNTAX ERROR in $f — aborting deploy"
                exit 1
            }
        fi
    done
    log "  Python syntax: OK"
fi

# Check sandbox DB exists
if [ "$ENV" = "sandbox" ]; then
    if [ ! -f "strategies/insider_catalog/sandbox/insiders.db" ]; then
        log "Sandbox DB not initialized. Running init-sandbox..."
        python3 pipelines/db_sync.py init-sandbox 2>&1 | tee -a "$LOG_FILE"
    fi
fi
log "  Databases: OK"

# Build + restart
if [ -n "$SERVICE" ]; then
    # Map service name for sandbox (e.g., "api" → "sandbox-api")
    if [ "$ENV" = "sandbox" ]; then
        SERVICE="${API_PREFIX}${SERVICE}"
    fi
    log "Building $SERVICE..."
    docker compose $COMPOSE_FILES build "$SERVICE" 2>&1 | tee -a "$LOG_FILE"
    log "Restarting $SERVICE..."
    docker compose $COMPOSE_FILES up -d "$SERVICE" 2>&1 | tee -a "$LOG_FILE"
else
    log "Building all $LABEL services..."
    if [ "$ENV" = "sandbox" ]; then
        docker compose $COMPOSE_FILES build sandbox-api sandbox-frontend 2>&1 | tee -a "$LOG_FILE"
        # Rolling restart: one service at a time to avoid simultaneous downtime
        log "Rolling restart: sandbox-api..."
        docker compose $COMPOSE_FILES up -d --no-deps sandbox-api 2>&1 | tee -a "$LOG_FILE"
        sleep 3
        log "Rolling restart: sandbox-frontend..."
        docker compose $COMPOSE_FILES up -d --no-deps sandbox-frontend 2>&1 | tee -a "$LOG_FILE"
    else
        docker compose $COMPOSE_FILES build 2>&1 | tee -a "$LOG_FILE"
        # Rolling restart: one service at a time to avoid simultaneous downtime
        log "Rolling restart: api..."
        docker compose $COMPOSE_FILES up -d --no-deps api 2>&1 | tee -a "$LOG_FILE"
        sleep 3
        log "Rolling restart: frontend..."
        docker compose $COMPOSE_FILES up -d --no-deps frontend 2>&1 | tee -a "$LOG_FILE"
    fi
fi

# Cleanup old images
docker image prune -f 2>&1 | tee -a "$LOG_FILE"

# Health check
log "Health check..."
sleep 5

HEALTHY=true
api_code=$(curl -sf -o /dev/null -w "%{http_code}" "https://${DOMAIN}/api/v1/health" 2>/dev/null || echo "000")
if [ "$api_code" != "200" ]; then
    log "WARNING: API health check failed (HTTP $api_code)"
    HEALTHY=false
fi

fe_code=$(curl -sf -o /dev/null -w "%{http_code}" "https://${DOMAIN}/" 2>/dev/null || echo "000")
if [ "$fe_code" != "200" ]; then
    log "WARNING: Frontend health check failed (HTTP $fe_code)"
    HEALTHY=false
fi

if $HEALTHY; then
    log "=== Deploy successful ==="
else
    log "=== Deploy completed with warnings ==="
    docker compose $COMPOSE_FILES logs --tail=10 2>&1 | tee -a "$LOG_FILE"
fi

# Keep only last 10 deploy logs per environment
ls -t "$LOG_DIR"/deploy-${ENV}-*.log 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

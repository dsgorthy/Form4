#!/bin/bash
# Install (or update) the Dagster webserver + daemon launchd services.
#
# Run ON STUDIO as derekg:
#   bash /Users/derekg/trading-framework/dataplane/deploy/install_dagster_services.sh
#
# Idempotent: re-running boots out old instances, refreshes DAGSTER_HOME
# config + plists from the repo, and bootstraps fresh. These services are
# STUDIO-ONLY (listed in trading-framework/CLAUDE.md) — never load on Mini.
set -euo pipefail

DAGSTER_HOME=/Users/derekg/dataplane_dagster_home
REPO=/Users/derekg/trading-framework
VENV=/Users/derekg/dataplane_venv
LA="$HOME/Library/LaunchAgents"
DEPLOY="$REPO/dataplane/deploy"
UI_PORT=3030

# ── Sanity checks ────────────────────────────────────────────────────
[ -x "$VENV/bin/dagster-daemon" ]    || { echo "FATAL: $VENV/bin/dagster-daemon missing"; exit 1; }
[ -x "$VENV/bin/dagster-webserver" ] || { echo "FATAL: $VENV/bin/dagster-webserver missing"; exit 1; }
[ -f "$REPO/.env" ]                  || { echo "FATAL: $REPO/.env missing"; exit 1; }
bash -c "set -a; . $REPO/.env; set +a" >/dev/null 2>&1 \
  || { echo "FATAL: $REPO/.env does not source cleanly under bash"; exit 1; }

# ── Stop existing instances first (also frees the UI port for the check)
for svc in dagster-daemon dagster-webserver; do
  launchctl bootout "gui/$(id -u)/com.openclaw.$svc" 2>/dev/null || true
done
sleep 2

if lsof -nP -iTCP:$UI_PORT -sTCP:LISTEN >/dev/null 2>&1; then
  echo "FATAL: port $UI_PORT is held by another process:"
  lsof -nP -iTCP:$UI_PORT -sTCP:LISTEN
  exit 1
fi

# ── Refresh DAGSTER_HOME config from the repo ───────────────────────
mkdir -p "$DAGSTER_HOME/logs"
cp "$REPO/dataplane/dagster.yaml"  "$DAGSTER_HOME/dagster.yaml"
cp "$DEPLOY/workspace.yaml"        "$DAGSTER_HOME/workspace.yaml"

# ── Freshen the dbt manifest so the dbt asset graph loads correctly ──
( cd "$REPO/dataplane/dbt_project" \
  && DBT_PROFILES_DIR="$REPO/dataplane/dbt_project" "$VENV/bin/dbt" parse -q ) \
  || { echo "FATAL: dbt parse failed"; exit 1; }

# ── Validate the code location imports before installing services ───
( cd "$REPO/dataplane" \
  && set -a && . "$REPO/.env" 2>/dev/null && set +a \
  && DAGSTER_HOME="$DAGSTER_HOME" \
     DBT_PROFILES_DIR="$REPO/dataplane/dbt_project" \
     "$VENV/bin/python" -c "from dagster_project.definitions import defs; print('defs OK')" ) \
  || { echo "FATAL: definitions module failed to import"; exit 1; }

# ── Install + bootstrap ──────────────────────────────────────────────
for svc in dagster-daemon dagster-webserver; do
  cp "$DEPLOY/com.openclaw.$svc.plist" "$LA/"
  launchctl bootstrap "gui/$(id -u)" "$LA/com.openclaw.$svc.plist"
done

sleep 6
echo "── launchctl ──"
launchctl list | grep dagster || { echo "FATAL: services not running"; exit 1; }
echo "── UI port ──"
lsof -nP -iTCP:$UI_PORT -sTCP:LISTEN || echo "(webserver not listening yet — check $DAGSTER_HOME/logs/webserver.err)"
echo "OK — UI at http://100.78.9.66:$UI_PORT (tailnet only)"

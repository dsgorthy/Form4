#!/bin/bash
# Install (or update) the Pyrrho Dataplane Desk launchd service.
#
# Run ON STUDIO as derekg:
#   bash /Users/derekg/trading-framework/dataplane/deploy/install_pyrrho_desk_service.sh
#
# Idempotent: boots out the existing service first, then bootstraps.
# STUDIO-ONLY (must not autoload on Mini — see trading-framework/CLAUDE.md).
set -euo pipefail

VENV=/Users/derekg/dataplane_venv
REPO=/Users/derekg/trading-framework
LA="$HOME/Library/LaunchAgents"
DEPLOY="$REPO/dataplane/deploy"
UI_PORT=3031
SVC=com.openclaw.pyrrho-desk

[ -x "$VENV/bin/python" ] || { echo "FATAL: $VENV/bin/python missing"; exit 1; }

# Boot out the existing instance + free the port
launchctl bootout "gui/$(id -u)/$SVC" 2>/dev/null || true
sleep 2

if lsof -nP -iTCP:$UI_PORT -sTCP:LISTEN >/dev/null 2>&1; then
  echo "FATAL: port $UI_PORT held by:"
  lsof -nP -iTCP:$UI_PORT -sTCP:LISTEN
  exit 1
fi

# Validate the module imports cleanly before installing
( cd "$REPO/dataplane" \
  && PYRRHO_DATAPLANE_DSN="dbname=pyrrho_data_dev host=localhost" \
     "$VENV/bin/python" -c "from dataplane.desk import gather_status, render_html; print('desk module OK')" ) \
  || { echo "FATAL: dataplane.desk failed to import"; exit 1; }

mkdir -p /Users/derekg/dataplane_dagster_home/logs
cp "$DEPLOY/$SVC.plist" "$LA/"
launchctl bootstrap "gui/$(id -u)" "$LA/$SVC.plist"

sleep 3
echo "── launchctl ──"
launchctl list | grep pyrrho-desk || { echo "FATAL: service not running"; exit 1; }
echo "── port $UI_PORT ──"
lsof -nP -iTCP:$UI_PORT -sTCP:LISTEN || echo "(not listening yet — check log)"
echo "OK — Pyrrho Desk at http://100.78.9.66:$UI_PORT (tailnet only)"

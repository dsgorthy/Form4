#!/bin/bash
# Nightly Postgres backups for Studio.
#
# Before 2026-08-12 there were NO database backups of any kind on this box:
# no dump jobs, no ~/backups, and `tmutil destinationinfo` reported "No
# destinations configured". form4 alone holds ~1.65M insider trades, 40k
# congress disclosures, 385k PIT scores and 23.5M option rows, none of it
# reproducible from source — EDGAR full-text search only serves a rolling
# window, ThetaData is cancelled, and Capitol Trades rate-limits deep
# pagination. A disk failure would have been unrecoverable.
#
# Design notes:
#   -Fc  custom format: compressed, and restorable selectively with
#        pg_restore (single table, schema-only, etc). Plain SQL would be
#        several times larger and all-or-nothing to restore.
#   Every dump is verified with `pg_restore --list` immediately after
#        writing. An unverified backup is not a backup — a truncated dump
#        looks fine on disk and only fails when you need it.
#   Off-box copy to the Mini over Tailscale, because a backup sitting on
#        the same disk as the database does not survive the failure it
#        exists for. Non-fatal if the Mini is unreachable.
#
# Usage:
#   scripts/backup_databases.sh                 # all databases
#   scripts/backup_databases.sh form4           # one database
#   RETENTION_DAYS=14 scripts/backup_databases.sh
set -uo pipefail

export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin

BACKUP_ROOT="${BACKUP_ROOT:-/Users/derekg/backups/postgres}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
OFFBOX_HOST="${OFFBOX_HOST:-100.102.80.61}"          # Mac Mini over Tailscale
OFFBOX_USER="${OFFBOX_USER:-derekg}"
OFFBOX_DIR="${OFFBOX_DIR:-/Users/derekg/backups/studio-postgres}"
STAMP="$(date +%Y%m%d_%H%M%S)"

# Ordered by how painful the loss would be, so the important ones land first
# if the run is interrupted.
DATABASES=("${@:-form4 pyrrho_data_dev pyrrho_prod dagster_runs}")
read -r -a DATABASES <<< "${DATABASES[*]}"

mkdir -p "$BACKUP_ROOT"

log() { echo "[$(date +%H:%M:%S)] $*"; }

overall_rc=0
declare -a SUMMARY=()

for db in "${DATABASES[@]}"; do
    out="$BACKUP_ROOT/${db}_${STAMP}.dump"
    log "dumping $db -> $(basename "$out")"
    start=$(date +%s)

    if ! pg_dump -Fc -d "$db" -f "$out" 2>"$out.err"; then
        log "  FAILED: $(tail -2 "$out.err" | tr '\n' ' ')"
        SUMMARY+=("$db: DUMP FAILED")
        overall_rc=1
        rm -f "$out"
        continue
    fi
    rm -f "$out.err"

    # Verify: a dump that pg_restore cannot read is worthless, and truncated
    # dumps are indistinguishable from good ones by size alone.
    if ! pg_restore --list "$out" >/dev/null 2>&1; then
        log "  FAILED VERIFY — dump is not readable, discarding"
        SUMMARY+=("$db: VERIFY FAILED")
        overall_rc=1
        rm -f "$out"
        continue
    fi

    elapsed=$(( $(date +%s) - start ))
    size=$(du -h "$out" | cut -f1)
    tables=$(pg_restore --list "$out" 2>/dev/null | grep -c "TABLE DATA" || echo "?")
    log "  ok: $size in ${elapsed}s, $tables table(s) verified"
    SUMMARY+=("$db: $size, ${elapsed}s, $tables tables")
done

# Prune old local dumps.
pruned=$(find "$BACKUP_ROOT" -name "*.dump" -type f -mtime +"$RETENTION_DAYS" -print -delete 2>/dev/null | wc -l | tr -d ' ')
log "pruned $pruned dump(s) older than ${RETENTION_DAYS}d"

# Off-box copy. A same-disk backup does not survive a disk failure.
if ssh -o ConnectTimeout=10 -o BatchMode=yes "${OFFBOX_USER}@${OFFBOX_HOST}" "mkdir -p '$OFFBOX_DIR'" 2>/dev/null; then
    if rsync -a --delete-after \
        --include="*_${STAMP}.dump" --include="*/" --exclude="*" \
        "$BACKUP_ROOT/" "${OFFBOX_USER}@${OFFBOX_HOST}:${OFFBOX_DIR}/" 2>/dev/null; then
        log "off-box copy -> ${OFFBOX_HOST}:${OFFBOX_DIR} ok"
    else
        log "off-box copy FAILED (local dumps still good)"
        overall_rc=1
    fi
else
    log "off-box host ${OFFBOX_HOST} unreachable — local dumps only"
    overall_rc=1
fi

log "=== SUMMARY ==="
for s in "${SUMMARY[@]}"; do log "  $s"; done
log "free space: $(df -h "$BACKUP_ROOT" | tail -1 | awk '{print $4}')"
exit "$overall_rc"

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
#
#   form4            product + research data, almost none of it re-fetchable
#   job_search_prod  Tailorly production (14GB) — was covered by NOTHING
#   pyrrho_data_dev  the dataplane: signal_observations / signal_definitions
#   design_quiz_prod small but live
#   dagster_runs     materialization history; losing it loses lineage, not data
#
# pyrrho_prod / pyrrho_staging were removed 2026-08-12 when the Pyrrho product
# was decommissioned. Final archival dumps live in
# backups/postgres-archive/pyrrho-final and on the Mini.
DATABASES=("${@:-form4 job_search_prod pyrrho_data_dev design_quiz_prod dagster_runs}")
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

# Prune old local dumps. -maxdepth 1 so a sibling archive directory can never
# be caught by a retention rule written for the nightly set.
pruned=$(find "$BACKUP_ROOT" -maxdepth 1 -name "*.dump" -type f -mtime +"$RETENTION_DAYS" -print -delete 2>/dev/null | wc -l | tr -d ' ')
log "pruned $pruned local dump(s) older than ${RETENTION_DAYS}d"

# Off-box copy. A same-disk backup does not survive a disk failure.
#
# The mirror is pruned BEFORE the transfer, by us, over ssh. Two reasons, both
# learned on 2026-09-09 when the Mini hit 99% full and the off-box copy had been
# failing silently for two nights:
#
#   1. rsync --delete cannot do this job. An --exclude pattern also PROTECTS the
#      receiver's matching files from deletion, so the previous
#      `--delete-after --include="*_${STAMP}.dump" --exclude="*"` deleted
#      nothing on any night -- including every night it logged success. The
#      mirror grew unbounded to 118 GB / 28 days against a 7-day retention while
#      this script reported "off-box copy ok".
#   2. Pruning first frees the space the transfer is about to need. Pruning
#      after inverts that: once the mirror is full the transfer fails, so the
#      prune never runs, so the mirror stays full. That is the loop the Mini was
#      stuck in, and it cannot unstick itself.
#
# Scoped by database name rather than to *.dump, because the mirror also holds
# the final archival dumps of the decommissioned Pyrrho databases. Those have no
# live source left to re-dump from, so they must never age out.
if ssh -o ConnectTimeout=10 -o BatchMode=yes "${OFFBOX_USER}@${OFFBOX_HOST}" "mkdir -p '$OFFBOX_DIR'" 2>/dev/null; then
    remote_pruned=$(ssh -o ConnectTimeout=30 -o BatchMode=yes "${OFFBOX_USER}@${OFFBOX_HOST}" \
        "n=0; for db in ${DATABASES[*]}; do \
             c=\$(find '$OFFBOX_DIR' -maxdepth 1 -type f -name \"\${db}_*.dump\" -mtime +$RETENTION_DAYS -print -delete 2>/dev/null | wc -l); \
             n=\$((n+c)); \
         done; echo \$n" 2>/dev/null | tr -d ' ')
    log "pruned ${remote_pruned:-?} mirror dump(s) older than ${RETENTION_DAYS}d"

    if rsync -a \
        --include="*_${STAMP}.dump" --include="*/" --exclude="*" \
        "$BACKUP_ROOT/" "${OFFBOX_USER}@${OFFBOX_HOST}:${OFFBOX_DIR}/" 2>/dev/null; then
        remote_free=$(ssh -o ConnectTimeout=10 -o BatchMode=yes "${OFFBOX_USER}@${OFFBOX_HOST}" \
            "df -h '$OFFBOX_DIR' | tail -1 | awk '{print \$4}'" 2>/dev/null)
        log "off-box copy -> ${OFFBOX_HOST}:${OFFBOX_DIR} ok (${remote_free:-?} free there)"
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

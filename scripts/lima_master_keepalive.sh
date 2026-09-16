#!/bin/bash
# Supervised ssh ControlMaster for the Colima VM. RUNS ON THE STUDIO under
# launchd (scripts/launchd/com.derekg.lima-master-keepalive.plist).
#
# WHY THIS EXISTS
#
# Every published port on this box (80 form4, 443, 8080 tailorly, 8082
# design-quiz) and the Docker socket are ssh port-forwards, and all of them
# live inside ONE process: the ControlMaster Lima starts at `colima start`
# with ControlPersist=yes. Lima daemonises it under launchd (parent pid 1)
# and nothing supervises it. When it dies, every forward dies with it while
# the VM, dockerd and all containers keep running: the Cloudflare tunnels
# 502 and `docker ps` on the host cannot connect. Lima re-issues a forward
# only when the guest agent reports a port coming UP; a new master gets
# nothing. Verified 2026-09-16: the ports came back only because a hand-run
# `ssh -L` client pushed them onto the new master through the mux.
#
# It died twice in 36 hours -- 2026-09-15 22:53:08 (12 min outage) and
# 2026-09-16 04:48:34 (5h11m outage; nothing on the box creates a master
# until a human runs ssh) -- each time as a clean client-side disconnect
# ("disconnected by user" in the VM's sshd log) with no log line on the host.
# Whatever signals it is not observable here; supervising it is.
# docs/rca_2026-09-16_colima_ssh_master.md has the full account.
#
# WHAT IT DOES, every CYCLE seconds
#
#   1. If a live master exists (Lima's own, or ours), make sure the Docker
#      socket and every published container port are forwarded on it.
#      `ssh -O forward` on an already-forwarded port fails harmlessly.
#   2. If none exists, become one: `ssh -N` with ControlMaster=auto, which
#      unlinks a stale socket first (ControlMaster=yes would instead
#      "disable multiplexing" and leave us as a useless side session). Then
#      run step 1 against ourselves.
#
# A bare `ssh -N` mux client with nothing to forward exits immediately, so
# "attach to the master and die with it" is not an option; polling is.
# Bash 3.2 (the macOS default): no associative arrays, no mapfile.
set -u

CFG="${LIMA_SSH_CONFIG:-$HOME/.colima/_lima/colima/ssh.config}"
HOST="${LIMA_SSH_HOST:-lima-colima}"
DOCKER_SOCK="${COLIMA_DOCKER_SOCK:-$HOME/.colima/default/docker.sock}"
DOCKER="${DOCKER_BIN:-/opt/homebrew/bin/docker}"
CYCLE="${KEEPALIVE_CYCLE:-15}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# stdin: `docker ps --format '{{.Ports}}'` lines. stdout: unique host ports
# published on 0.0.0.0 or [::], one per line, ascending.
#   "443/tcp, 2019/tcp, 0.0.0.0:8082->80/tcp, [::]:8082->80/tcp"  ->  8082
published_host_ports() {
    tr ',' '\n' \
    | awk '/->[0-9]+\/tcp[[:space:]]*$/ {
             sub(/^[[:space:]]+/, "")
             split($0, a, "->")
             n = split(a[1], b, ":")
             if (b[n] ~ /^[0-9]+$/) print b[n]
           }' \
    | sort -un
}

master_pid() {
    # "Master running (pid=32185)" -> 32185; empty when there is none.
    ssh -F "$CFG" -O check "$HOST" 2>&1 | sed -n 's/.*pid=\([0-9]*\)).*/\1/p'
}

listening() { lsof -nP -iTCP:"$1" -sTCP:LISTEN -t >/dev/null 2>&1; }

docker_ok() { "$DOCKER" version >/dev/null 2>&1; }

reconcile() {
    if ! docker_ok; then
        # Lima forwards the socket only at VM start. A stale socket file makes
        # the master's bind fail, so clear it first (nothing answers on it).
        rm -f "$DOCKER_SOCK"
        if ssh -F "$CFG" -O forward -L "$DOCKER_SOCK:/var/run/docker.sock" "$HOST" >/dev/null 2>&1 && docker_ok; then
            log "forwarded docker socket"
        else
            log "docker socket forward failed"
            return
        fi
    fi
    local p
    for p in $("$DOCKER" ps --format '{{.Ports}}' 2>/dev/null | published_host_ports); do
        listening "$p" && continue
        # Mirror Lima's own rule (lima.yaml hostIP 0.0.0.0): same bind, nothing new exposed.
        if ssh -F "$CFG" -O forward -L "0.0.0.0:$p:0.0.0.0:$p" "$HOST" >/dev/null 2>&1; then
            log "forwarded port $p"
        else
            log "forward of port $p failed and nothing listens on it"
        fi
    done
}

become_master() {
    # The master runs a remote `sleep infinity` instead of -N, and logs at
    # VERBOSE, so that its death explains itself in this log. With -N,
    # OpenSSH deliberately swallows a SIGTERM (exit 0, no message) -- the
    # one case we most need to see. With a remote command:
    #   "Killed by signal N."  + status 255  -> something on the host signalled it
    #   "Connection to ... closed by remote host" / "Timeout, server ... not
    #   responding"           + status 255  -> the transport or the VM
    #   no line                + status 0    -> an `ssh -O exit` request
    #   no line                + status 143  -> the remote sleep was killed
    # VERBOSE does not log per-connection channel traffic, so this costs nothing.
    ssh -F "$CFG" -o LogLevel=VERBOSE \
        -o ControlMaster=auto -o ControlPersist=no \
        -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=no -o StreamLocalBindUnlink=yes \
        -L "$DOCKER_SOCK:/var/run/docker.sock" \
        "$HOST" -- exec sleep infinity &
    local pid=$! i
    OUR_SSH=$pid
    for i in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        [ "$(master_pid)" = "$pid" ] && { log "became ControlMaster, pid $pid"; return 0; }
        kill -0 "$pid" 2>/dev/null || break
    done
    # Someone else won the race or the VM is not answering; drop ours and retry next cycle.
    log "could not become master (ssh pid $pid, master '$(master_pid)')"
    kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; OUR_SSH=""
    return 1
}

OUR_SSH=""

# Our master died: collect its exit status and the exact second, next to the
# ssh's own VERBOSE line above it. This is the evidence the 09-15/16 deaths
# never left behind (see the table in become_master).
reap_our_master() {
    [ -n "$OUR_SSH" ] || return 0
    if ! kill -0 "$OUR_SSH" 2>/dev/null; then
        local rc
        wait "$OUR_SSH" 2>/dev/null; rc=$?
        log "our master (pid $OUR_SSH) died, exit status $rc"
        OUR_SSH=""
    fi
}

main() {
    log "start: cfg=$CFG cycle=${CYCLE}s"
    local last=""
    while true; do
        local m
        reap_our_master
        m=$(master_pid)
        if [ -z "$m" ]; then
            log "no live ControlMaster; taking over"
            become_master && m=$(master_pid)
        fi
        if [ -n "$m" ]; then
            [ "$m" != "$last" ] && log "master pid $m"
            last=$m
            reconcile
        else
            last=""
        fi
        sleep "$CYCLE"
    done
}

# `source scripts/lima_master_keepalive.sh` loads the functions for tests.
if [ "${BASH_SOURCE[0]:-}" = "$0" ]; then
    main
fi

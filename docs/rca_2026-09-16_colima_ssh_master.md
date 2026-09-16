# RCA: form4.app, trytailorly.com and interiordesignfordummies.com down 5h11m on 2026-09-16

Second outage of the same class in 36 hours. The first (2026-09-15
22:53–23:06, ~12 min) was attributed to the wrong cause and closed with a fix
that did not address it; this document supersedes that account.

## Summary

Every published port on the Studio (80 form4, 443, 8080 tailorly, 8082
design-quiz) and the Docker socket are ssh port-forwards that live inside
**one unsupervised process**: the ssh ControlMaster that Lima starts when
Colima boots. At 04:48:34 that process closed its connection cleanly and
exited. All forwards died with it. The VM, dockerd and all ten containers
kept running and serving — to nobody, because nothing on the host listened
on 80/8080/8082 any more, so all three Cloudflare tunnels returned 502.
Nothing on the box recreates a master or its forwards; service came back
only when a human ran ssh at 09:59.

## Impact

| | |
|---|---|
| Window | 2026-09-16 04:48:34 → ~10:02 PT (5h11m); 443 forward not restored until 10:23 (unused by the tunnels) |
| Down | form4.app, trytailorly.com, interiordesignfordummies.com — all 502 at the edge |
| Also broken | `docker` CLI on the host and therefore any CI deploy (docker.sock is one of the forwards) |
| Not affected | Data. Ingestion, Dagster, strategy runners and PostgreSQL run on the host, not through the VM. The API containers stayed up and unreachable. |
| Previous | 2026-09-15 22:53:08 → 23:05:40 (12.5 min), same mechanism, plus a planned 5-min Colima restart |

## Timeline (PT)

**2026-09-15**
- 22:53:08 — ssh master (up since the Aug 14 boot) exits; caddy inside the VM logs its last request the same second. Uptime monitor FAILs from 22:53:06.
- 22:55 — on-box monitor alerts (3 consecutive). 23:00 — off-box watchdog pages.
- 23:05:40 — stopgap `ssh -N -L 80 -L 8080` restores the sites.
- 23:25:37 → 23:32:03 — planned Colima restart with `mountInotify: false` (the suspected cause). New master starts 23:30:34. Two of three cloudflared tunnels die with the VM stop and need a kickstart.

**2026-09-16**
- 04:48:34 — VM sshd: `Received disconnect from 192.168.5.2 port 44799:11: disconnected by user` — the master (session opened 23:30:34) closed cleanly. Nothing on the host logs anything at that second.
- 04:49:06 — uptime.log `FAIL [1]`. 05:00 — off-box watchdog pages. No one is awake.
- ~09:50 — Derek reports the site down.
- 09:59:24 — `colima ssh` (diagnosis) creates a new master. It has **no forwards**; the sites stay down.
- 10:00:13, 10:01:39 — two hand-run `ssh -N -L 80 -L 8080 -L 8082 …` clients. Each pushes its forwards onto the master through the mux, then falls back to its own session, fails to bind the same ports itself and exits — but the forwards it registered stay on the master. **This is what restored 80/8080/8082.** Sites 200 by ~10:02.
- 10:02:52 — docker.sock-only client (own session) restores the Docker CLI.
- 10:23:08 — reconcile pass of the new keepalive restores 443, the one forward the stopgap did not carry.

## How the forwards work (and why one process is the whole edge)

```
cloudflared (host)  ──►  127.0.0.1:80 / :8080 / :8082
                            │  ssh -L listeners owned by ONE process:
                            ▼
              ssh: ~/.colima/_lima/colima/ssh.sock [mux]   (parent: launchd, ControlPersist=yes)
                            │  TCP 127.0.0.1:59274
                            ▼
                    VM sshd  ──►  caddy containers
docker CLI  ──►  ~/.colima/default/docker.sock  (a unix-socket forward on the same master)
```

- Lima starts the master explicitly at `colima start` with `-o ControlMaster=auto -o ControlPersist=yes` and **no ServerAlive options**, then detaches it. Nothing supervises it. (hostagent log: "Explicitly start ssh ControlMaster".)
- Lima's hostagent adds a TCP forward with `ssh -O forward -L …` **when the guest agent reports a port coming up** — at boot and on container (re)starts. It never re-issues forwards for a new master, and the docker socket forward is issued once at boot.
- Consequently a master death is permanent until Colima restarts or something else requests the forwards.

## Root cause

**Architectural.** The edge of three production sites depends on one
daemonised ssh client that nothing supervises, with no keepalive and no
reconnect, and no component that re-establishes the forwards when a new
connection appears. The proximate event — the master closing cleanly — has
happened twice in 36 hours after 32 days of stability; its trigger is not
identified (see Open question). The outage *duration* (5h11m for a
sub-second event) is entirely the absence of supervision and detection.

## What the 09-15 analysis got wrong

1. **"mountInotify churn drops the connection."** Plausible from the
   per-minute `chmod` sessions the inotify daemon ran, and disabling it was
   still right (thousands of pointless sessions a day), but the master died
   again 5h18m into a VM with inotify off. Not the cause.
2. **"Lima re-adds the forwards once a master exists."** Written on the
   09-16 morning after seeing the ports return within seconds of a new
   master. The hostagent log shows no forwarding activity at all between
   the boot and 10:23; the sshd log shows my own `ssh -L` clients at
   10:00:13 and 10:01:39 are what carried them. Lima does nothing.
3. **"The docker.sock stopgap keeps the master alive."** It has its own
   sshd session (a mux forward request that fails — stale socket file — makes
   ssh fall back to a direct connection). It neither depends on nor protects
   the master.

## Detection

- `scripts/uptime_monitor.sh` saw the outage from the first minute and
  wrote the alert — and **exited 0 every run**, so `pipeline_runs` recorded
  `form4_uptime` as `ok` for five hours and the off-box status check could
  not see it. Fixed: a failed check now exits 1 with the failing endpoints
  on stderr.
- The off-box watchdog on the Mini paged at 05:00, 12 minutes in. Correct,
  and useless at 5 am. The fix has to be automatic recovery, not louder paging.

## Fix

`scripts/lima_master_keepalive.sh` under `com.derekg.lima-master-keepalive`
(launchd, RunAtLoad + KeepAlive; plist in `scripts/launchd/`). Every 15 s:

1. If a live master exists (Lima's or ours), make sure the docker socket
   and every host port the containers publish (`docker ps`) are forwarded
   on it. Idempotent: `-O forward` on an already-forwarded port fails harmlessly.
2. If none exists, become one (`ssh -N`, `ControlMaster=auto`, which unlinks a
   stale socket; `ServerAliveInterval=30`), then run step 1 against ourselves.

Design constraints learned the hard way, so nobody re-learns them:
- A bare `ssh -N` mux client with nothing to forward **exits immediately**;
  "attach and die with the master" is not available. Poll instead.
- `ControlMaster=yes` on an existing/stale socket "disables multiplexing"
  and leaves a useless side session; `auto` handles the stale case.
- The master applies *its own* options to forwards requested through the
  mux, so a client's `StreamLocalBindUnlink` does not help; the script
  removes a dead docker.sock file before asking.
- Bind `0.0.0.0` to mirror Lima's own rule; nothing new is exposed.

Expected recovery after a master death: ≤ ~20 s, all ports plus docker.sock,
no container touched, no Colima restart. Tested read-only against the live
master on 09-16 (it correctly added only the missing 443).

**Install and the failover test need Derek's approval** — a new launchd
agent on production plus a deliberate ~20 s outage to prove the takeover
works before it has to work at 4 am.

## Open question: what closes the master?

Facts: the VM's sshd received a normal client-initiated disconnect both
times (`:11: disconnected by user`), so not a network failure (it is
localhost TCP), not an sshd-side kill, not the VM. The Studio never sleeps
(`pmset sleep 0`, no sleep/wake events). Nothing is scheduled at 22:53 or
04:48. No process on the host runs `ssh -O exit`/`stop`. No loginwindow or
session event. The master's parent is launchd (pid 1) — no terminal, no
SIGHUP source. Not inotify (second death had it off). Two deaths, 5h18m and
~32 days of life, no pattern yet.

How we will learn: the keepalive logs the master pid and every takeover to
the second. At the next death, `log show --start <t-60s> --end <t+5s>`
across the whole system is the first thing to read. Until then the trigger
is unknown and the system no longer cares.

## Follow-ups

1. **Structural:** point the cloudflared tunnels at the VM's own IP
   (`network.address: true`, then `http://<vm-ip>:80`) so serving stops
   depending on ssh forwards at all. Needs a Colima restart (~5 min down);
   docker.sock would still need the master.
2. Two of three cloudflared tunnels die on a Colima stop and their
   `KeepAlive=true` does not bring them back — kickstart them after any
   restart. Still unexplained.
3. `brew services restart colima` stops the VM but never starts it on this
   box; use `launchctl kickstart -k gui/$(id -u)/homebrew.mxcl.colima`.
4. The off-box watchdog could check host listeners on 80/8080/8082 directly
   (over ssh) to name this failure class in the page instead of "502".

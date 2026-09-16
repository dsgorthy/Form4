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
| Collateral of that restart | Six long-running agents stopped at 23:25:37 and stayed dead 12–19 h: **dagster-daemon** (every Dagster schedule — notification scanner, strategy intraday, price refresh — silent 23:25 → 11:17), **the GitHub deploy runner** (pushes to main queued instead of deploying), dagster-webserver, Ollama, and the tailorly + design-quiz tunnels (those two were kickstarted the same night). Found 11:10–11:20 on 09-16. |

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
- 11:02 — fix pushed. The deploy job **queues**: the self-hosted runner has been dead since 23:25:37 (its log: "Runner execution been cancelled" at that second).
- 11:03:00 — keepalive installed (needed `launchctl kickstart`; `bootstrap` left it "not running"). 11:03:25 controlled failover: master killed → all forwards back 11:03:32, site 200 at +9 s.
- 11:14 — runner kickstarted; deploy runs and succeeds. 11:17 — dagster-daemon, dagster-webserver and Ollama kickstarted; Dagster's first scheduled run since 23:25 lands at 11:17:15.

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

## The restart's collateral: a second, silent outage

`brew services restart colima` at 23:25:37 on 09-15 did not just stop the
VM. At that same second launchd stopped six unrelated long-running user
agents and left each at `- 0` (no pid, "exit 0") without restarting them —
KeepAlive=true (both tunnels) or no KeepAlive (the runner) made no
difference. Nothing in the unified log names the actor. The two tunnels were
noticed and kickstarted that night; the other four were not:

| agent | consequence | dead for |
|---|---|---|
| `com.openclaw.dagster-daemon` | every Dagster schedule stopped — notification scanner, strategy intraday, open-position price refresh; last runs 23:15–23:25, next 11:17 | 11h52m |
| `actions.runner.dsgorthy-Form4.dereks-mac-studio` | a push to main no longer deploys; today's sat queued 12 min | 11h49m |
| `com.openclaw.dagster-webserver` | Dagster UI | ~12h |
| `com.ollama.server` | local LLM | ~12h |

The Mini watchdog paged for the Dagster *symptom* on every tick from 00:00
(`notification_scanner last run 695m ago`) — correct, unread overnight, and
it never said *why*. `launchctl list` on the Studio said `0` for all of them.

**Rule from this: a Colima restart on this box is a full-service restart.**
After one, kickstart and verify every agent in the watchdog's
`MUST_RUN_AGENTS` list. The watchdog now reads `launchctl list` over ssh
each tick and pages a loaded-but-not-running agent by name with its
kickstart command (`evaluate_must_run_agents`, tested).

## Detection

- `scripts/uptime_monitor.sh` saw the outage from the first minute and
  wrote the alert — and **exited 0 every run**, so `pipeline_runs` recorded
  `form4_uptime` as `ok` for five hours and the off-box status check could
  not see it. Fixed: a failed check now exits 1 with the failing endpoints
  on stderr.
- The off-box watchdog on the Mini paged at 05:00, 12 minutes in. Correct,
  and useless at 5 am. The fix has to be automatic recovery, not louder paging.
- Neither check could name a dead daemon. Added: the watchdog's must-run-agents
  check (above).
- Side effect of the exit-code fix, expected: a deploy restarts the frontend, so
  one `form4_uptime` run per deploy records `failed` (11:18:07 today, `/ → 502`);
  the next minute's run is `ok` and supersedes it.

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

**Installed and verified 2026-09-16 11:03** with a controlled failover:
master killed 11:03:25 → keepalive took over 11:03:31 → new master with all
four ports and docker.sock 11:03:32 → form4.app 200 at +9 s. Total planned
outage 7–9 s. One install gotcha: `launchctl bootstrap` left the job
"not running" despite RunAtLoad; `launchctl kickstart -k` was required.

## Open question: what closes the master? (still open — narrowed, not answered)

What the VM's sshd logged both times, `Received disconnect … :11:
disconnected by user`, is the DISCONNECT packet the OpenSSH client sends
whenever its main loop ends — on a normal end, on an `ssh -O exit` request,
**and on SIGTERM/SIGHUP/SIGINT** (the packet goes out before the signal is
reported). So it proves the client chose to leave over a healthy connection;
it does not rule out a signal, as the first draft of this document claimed.

Ruled out on 09-16 (this time with `/usr/bin/log` — every earlier "the
unified log is silent" statement had run zsh's builtin `log` and seen nothing):
- The VM, the transport and sshd: the client sent the disconnect; localhost TCP.
- Sleep/wake: `pmset sleep 0`, no power events. No loginwindow/session events.
- Lima: `limactl` runs `ssh -O exit` only in the hostagent's shutdown
  cleanup (Lima v2.1.1 `hostagent.go`); the hostagent ran on through both
  deaths and logged only time-sync ticks at those seconds.
- Scripts: nothing under `~` on the Studio or in this repo / the studio CLI
  on the Mini kills ssh, matches `mux`, or sends `-O exit`/`-O stop`.
- Scheduled work: no launchd job or Dagster schedule fires at 22:53 or 04:48;
  launchd logged no job activity at either second except the uptime monitor.
- Updates: no OS install, no Homebrew formula changed in the window.
- The master is `setsid`-ed into its own process group (verified with a
  throwaway master), so a process-group kill of Lima's processes cannot reach it.
- Not inotify: the second death happened with it off.

Exact lifetimes: master 1 ran Aug 14 15:01 → Sep 15 22:53:08 (~32 d);
master 2 ran 23:30:34 → 04:48:34 (5 h 18 m 00 s to the second). No common
period; no OpenSSH timer fits (`ControlPersist=yes` never expires; no
ServerAlive; RekeyLimit is by volume).

What remains: a signal to that specific pid from a process that leaves no
log, or an `ssh -O exit` from something not yet found. The keepalive's
master is now instrumented to say which: it runs a remote `sleep infinity`
(a `-N` client swallows SIGTERM silently, by OpenSSH design) at
`LogLevel=VERBOSE`, and the script reaps it and logs the exit status:

| in `logs/lima-master-keepalive.log` | meaning |
|---|---|
| `Killed by signal N.` then `died, exit status 255` | something on the host signalled it — then `sudo eslogger signal` for a day would name the sender |
| `Connection to 127.0.0.1 closed by remote host` / `Timeout, server … not responding`, status 255 | transport or VM |
| no ssh line, `exit status 0` | an `ssh -O exit` request from some process on the host |
| no ssh line, `exit status 143` | the remote sleep was killed inside the VM |

Either way the sites now recover in ~7 s, so the answer is for curiosity and
for whatever else that actor might be killing.

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
4. Why does a Colima restart stop unrelated launchd agents, and why does
   launchd not restart KeepAlive jobs afterwards? Unified log had nothing at
   23:25:37. Until known: treat every Colima restart as a full-service restart.
5. The runner's plist has no KeepAlive at all; the tunnels' KeepAlive did not
   help either, so adding one is not the fix — the watchdog check is.

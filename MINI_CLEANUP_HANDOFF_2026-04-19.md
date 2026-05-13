# Handoff: Mini Form4/Trading-Framework Service Cleanup (2026-04-19)

## TL;DR

12 trading-framework launch agents were **removed from the Mac Mini** on 2026-04-19 ~20:40 PDT. These services were running in parallel with their counterparts on Mac Studio, producing duplicate work and creating a potential double-order risk. Form4/trading-framework now runs exclusively on Studio per `/Users/openclaw/CLAUDE.md` architecture. Follow-up items below — one of which is a real bug (`trial-emails` crash-looping on both machines, not just Mini).

## Context

Derek was debugging severe high-performance screenshare lag on the Mini after a 19:30 PDT reboot. Investigation found:
- Cloudflared tunnels for form4.app and trytailorly.com active on Mini (removed at ~20:15 PDT)
- GitHub Actions runner and form4-* notification/tail services on Mini (removed at ~20:15 PDT)
- Docker Desktop VM holding 2.86 GB RAM for no workload (removed at ~20:25 PDT)
- All 22 prediction-markets services duplicated on Mini — handed off to PM Claude instance at ~20:35 PDT (see `/Users/openclaw/prediction-markets/MINI_CLEANUP_HANDOFF_2026-04-19.md`)
- 12 trading-framework services duplicated on Mini — **this doc, removed at ~20:40 PDT**

Per `/Users/openclaw/CLAUDE.md` the Mini is dev-only. All production-facing workloads should be on Studio. Mini should be near-instant to reboot with no background churn.

## What was running on Mini at time of discovery

### Actively executing strategies (⚠️ double-run risk)

| Service | Mini PID | Studio PID | Entry point |
|---|---|---|---|
| `com.openclaw.quality-momentum` | 793 | 1020 | `strategies/cw_strategies/cw_runner.py --config .../quality_momentum.yaml` |
| `com.openclaw.reversal-dip` | 776 | 1023 | `strategies/cw_strategies/cw_runner.py --config .../reversal_dip.yaml` |
| `com.openclaw.tenb51-surprise` | 772 | 1026 | `strategies/cw_strategies/cw_runner.py --config .../tenb51_surprise.yaml` |

Each was running Python 3.9 (`/Library/Developer/CommandLineTools/...Python.app`) on Mini; Studio runs Python 3.12. Version mismatch worth checking — Mini was running a potentially different dependency set against the same data.

**If any of these three submit real orders to a broker, dual-run means potential duplicate order submission.** If all broker interaction is gated by a separate service (e.g., `trading-bot` or a central order manager), the strategy processes themselves may only emit signals to a queue and duplicates would be filtered. **Please verify the full signal-to-order path.**

### Crash-looping on BOTH Mini and Studio (real bug, not a Mini artifact)

| Service | Mini status | Studio status |
|---|---|---|
| `com.openclaw.trial-emails` | exit 1, restart loop | exit 1, restart loop |

**This is a genuine bug in `trial-emails`** — same code, same failure, on both machines. It's not a Mini-specific configuration issue. Please investigate:
- Check `/Users/openclaw/trading-framework/logs/` on Studio for the stderr trail.
- Likely candidates: SMTP creds missing, a recent schema migration broke the query, hitting a rate limit.
- Because `KeepAlive=true`, this is restarting every ~30s on Studio, loading Python + project deps (~200 MB RSS) before dying. Non-trivial background drag.

### Idle / scheduled (on both)

- `com.openclaw.backfill-returns`
- `com.openclaw.breaking-signal`
- `com.openclaw.ceowatcher-reader`
- `com.openclaw.daily-content`
- `com.openclaw.insider-fetch`
- `com.openclaw.intraday-backfill`
- `com.openclaw.position-rules-test`
- `com.openclaw.strategy-health`

These run on schedules, not continuously. They weren't actively executing at audit time, but on Mini they would fire alongside Studio's copies and likely make duplicate API calls / DB writes against the same targets.

### Mini-only (NOT on Studio — status unclear, NOT removed)

| Service | State on Mini | Question |
|---|---|---|
| `com.openclaw.trading-bot` | loaded, exit 1 (crash-looping) | Legacy pre-form4 rename? Or still in use somewhere? If used, why not on Studio? |
| `com.openclaw.catchup-cleanup` | loaded, idle (timer-driven) | What does it clean up, and is it Mini-specific dev data or something that should be on Studio? |

These two were intentionally **left in place** pending your (PM / Form4 Claude instance) determination. If they're dead code, `rm ~/Library/LaunchAgents/com.openclaw.{trading-bot,catchup-cleanup}.plist` on Mini. If they belong on Studio, `studio deploy` them.

## What was done on Mini

For each of the 12 services listed under "Actively executing" + "Crash-looping" + "Idle / scheduled":

1. `launchctl bootout gui/$(id -u) <plist-path>` — terminates any running instance and deregisters.
2. `rm <plist-path>` — permanent deletion from `~/Library/LaunchAgents/`.
3. Verified `launchctl list` shows none of the 12 labels remaining.

No trading-framework source code, logs, configs, or database data was touched. The `/Users/openclaw/trading-framework/` checkout on Mini is untouched — it just isn't autostarting strategy/job processes anymore.

## Follow-up items for Form4 Claude

### Priority 1 — safety verification for the 3 actively-duplicating strategies

Before today, `quality-momentum`, `reversal-dip`, and `tenb51-surprise` were running simultaneously on Mini and Studio. Answer these in order:

1. **Do these strategies submit orders directly, or emit signals to a queue / order manager?**
   - If direct: dual-run = duplicate order risk. Need broker-level idempotency or strategy-level exclusive lock (DB advisory lock preferred — see PM `pm-bet-handler` handoff for the same concern).
   - If via queue: likely safe if the order-consumer deduplicates, but verify the dedup logic handles two producers emitting the same signal at the same bar.

2. **Audit the order/signal log for duplicates during the dual-running window.** Estimated window: last Mini reboot prior to 2026-04-19 19:30 through 2026-04-19 20:40. Exact prior uptime is unknown but likely weeks — check `last reboot` output or the oldest strategy signal row originating from the Mini's hostname/machine-id if you log that.

3. **Check the data pipeline for duplicate fills or duplicate signal rows** — any table with a `signals`, `trades`, `fills`, or `orders` name should have no dupes on `(strategy, ticker, bar_time)` for the window above.

### Priority 2 — fix `trial-emails` crash loop

Running on both machines with exit status 1 and `KeepAlive=true`. Each restart loads the Python runtime for nothing. Steady background drag on Studio.

- Locate the service entry point. Plist on Studio at `~/Library/LaunchAgents/com.openclaw.trial-emails.plist` (or equivalent path) will name the script.
- Read recent stderr. Fix the underlying error or gate `KeepAlive` with a `SuccessfulExit` condition so it stops hammering.

### Priority 3 — review Mini-only unclear services

- `com.openclaw.trading-bot` — crash-looping on Mini, not on Studio. Determine: is this live code? If yes, fix it and deploy to Studio. If no, delete.
- `com.openclaw.catchup-cleanup` — scheduled on Mini, not on Studio. Determine: Mini-dev-specific, or should be on Studio?

Both plists are still present at `~/Library/LaunchAgents/com.openclaw.{trading-bot,catchup-cleanup}.plist` on Mini.

### Priority 4 — confirm Studio inventory

Run on Studio (Tailscale `100.78.9.66` / `derekg@dereks-mac-studio`):

```bash
launchctl list | grep -E '^[0-9-]' | awk '{print $3}' | grep -E '^com\.openclaw' | sort
```

Expected to contain all 12 services this doc describes, plus form4-error-tail, form4-notifications, form4-seed-positions, form4-uptime, and tailorly-tunnel. If anything is missing, recover via `studio deploy form4` (or the trading-framework equivalent).

### Priority 5 — update CLAUDE.md and deploy workflow guards

- `/Users/openclaw/trading-framework/CLAUDE.md` — add explicit "runs on Studio only, NOT on Mini" statement with list of services that should *never* be auto-loaded on any dev machine.
- `studio deploy form4` — consider adding a pre-check that warns if matching plists exist in `~/Library/LaunchAgents/` on the machine running the deploy. Prevents this regression.

## Not touched

- `/Users/openclaw/trading-framework/` source tree on Mini
- `/Users/openclaw/trading-framework/logs/` on Mini (may have useful last-crash stderr for `trial-emails` etc.)
- Local Postgres `form4` database on Mini, if one exists (Mini runs its own Postgres 17 — worth auditing separately whether Mini has a stale `form4` DB)
- `form4-error-tail`, `form4-notifications`, `form4-seed-positions`, `form4-uptime`, cloudflared form4 tunnel — these were removed earlier in the same cleanup session (see backup at `~/Library/LaunchAgents.disabled-mini-form4-tailorly/`)
- GitHub Actions runner `actions.runner.dsgorthy-Form4.Openclaws-Mac-mini` — also removed earlier in the same session

## Evidence at time of handoff

### Mini inventory (before cleanup)

```
- 0 com.openclaw.backfill-returns
- 0 com.openclaw.breaking-signal
- 0 com.openclaw.ceowatcher-reader
- 0 com.openclaw.daily-content
- 1 com.openclaw.insider-fetch
- 0 com.openclaw.intraday-backfill
- 0 com.openclaw.position-rules-test
793 0 com.openclaw.quality-momentum
776 0 com.openclaw.reversal-dip
- 0 com.openclaw.strategy-health
772 0 com.openclaw.tenb51-surprise
- 0 com.openclaw.trial-emails   (was exit 1 previously — momentarily reporting 0 at probe time)
```

### Studio inventory (unchanged)

```
- 0 com.openclaw.backfill-returns
- 0 com.openclaw.breaking-signal
- 0 com.openclaw.ceowatcher-reader
- 0 com.openclaw.daily-content
83693 1 com.openclaw.form4-error-tail
- 1 com.openclaw.form4-notifications
- 0 com.openclaw.form4-seed-positions
- 0 com.openclaw.form4-uptime
- 0 com.openclaw.insider-fetch
- 1 com.openclaw.intraday-backfill
- 0 com.openclaw.position-rules-test
1020 0 com.openclaw.quality-momentum
1023 0 com.openclaw.reversal-dip
- 0 com.openclaw.strategy-health
638 0 com.openclaw.tailorly-tunnel
1026 0 com.openclaw.tenb51-surprise
- 1 com.openclaw.trial-emails
```

### Active Mini Python at audit

```
Python 3.9 /Users/openclaw/trading-framework/strategies/cw_strategies/cw_runner.py --config .../quality_momentum.yaml
Python 3.9 /Users/openclaw/trading-framework/strategies/cw_strategies/cw_runner.py --config .../reversal_dip.yaml
Python 3.9 /Users/openclaw/trading-framework/strategies/cw_strategies/cw_runner.py --config .../tenb51_surprise.yaml
```

### Active Studio Python (parallel workload)

Confirmed via earlier SSH: Studio is running all three `cw_runner.py` strategies under Python 3.12 alongside Mini's 3.9 copies.

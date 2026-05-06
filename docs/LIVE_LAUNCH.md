# Day-14 Live Launch — quality_momentum

This is the playbook for the moment Derek flips QM into real-money trading. It assumes everything in `docs/LIVE_ACCOUNT_SETUP.md` is done (live account funded, credentials installed, SMS gateway configured) and that the 14-day implementation plan is complete.

## T-1 day: Final pre-flight

1. **Run the pre-flight validator on Studio.**
   ```bash
   ssh derekg@100.78.9.66
   cd ~/trading-framework
   # Live preflight — full gate including credentials + funding
   python3 scripts/preflight/live_launch_check.py --strategy quality_momentum --mode live

   # Paper preflight — same gates minus the live-creds check; useful any
   # day to confirm the paper system itself is healthy
   python3 scripts/preflight/live_launch_check.py --strategy quality_momentum --mode paper
   ```
   All gates must show `[✓]` blocker-severity. Warnings are OK; blockers are not.

2. **Two consecutive paper days, zero unresolved divergences.**
   ```bash
   psql -d form4 -c "SELECT * FROM alpaca_reconciliation WHERE resolved_at IS NULL AND severity IN ('critical','warn');"
   ```
   Empty result.

3. **Daily summary delivered for 7+ consecutive weekdays** (check inbox).

4. **Critical SMS path verified** end-to-end:
   ```bash
   python3 -m framework.alerts.sms test "live launch dress rehearsal"
   ```
   You receive the SMS within 5 minutes.

5. **Kill switch tested.**
   ```bash
   studio halt-trading quality_momentum
   studio trading-status   # should show TRADING_HALTED_QUALITY_MOMENTUM=true
   # ... wait for next scan cycle, observe runner skipping entries ...
   studio resume-trading quality_momentum
   ```

6. **Order audit completeness clean:**
   ```bash
   python3 scripts/audit/order_completeness.py --strategy quality_momentum
   # Expect: "All entries + exits since cutoff have order_audit provenance."
   ```

## T+0: Launch sequence

Pick a Tuesday or Wednesday during US market hours. **Avoid Mondays (weekend news catch-up) and Fridays (4 PM weekly cadence kicks in).**

### Step 1 — Final guardrail confirmation (5 min)

```bash
ssh derekg@100.78.9.66
cd ~/trading-framework

# Live cred + account check
python3 scripts/verify_live_creds.py --strategy quality_momentum --min-equity 9500

# Preflight all-green
python3 scripts/preflight/live_launch_check.py --strategy quality_momentum

# Smoke run on the live yaml in dry mode (no orders submitted)
python3 strategies/cw_strategies/cw_runner.py \
  --config strategies/cw_strategies/configs/quality_momentum_live.yaml \
  --once --dry-run
# Expect: scan_signals fires, no STALE_INPUT_HALT, no exceptions.
```

### Step 2 — Load the live runner (1 min)

```bash
cp ~/trading-framework/scripts/launchd/com.openclaw.quality-momentum-live.plist \
   ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/com.openclaw.quality-momentum-live.plist
launchctl list | grep quality-momentum-live   # expect a PID > 0
tail -f ~/trading-framework/logs/quality-momentum-live.log
```

The live runner starts immediately with `RunAtLoad: true`. Watch the log:
- `Loaded config: quality_momentum (1 theses)`
- `LIVE TRADING ENABLED — orders route to https://api.alpaca.markets/v2`
- The first market-hours scan should reach `scan_signals: <N> candidates ...`.

### Step 3 — First entry (when one fires)

When the first live entry happens you'll see:
1. SMS `[FORM4-CRIT]` if anything went sideways (timeout, exception, guardrail reject)
2. The Resend `BUY` alert email
3. New `strategy_portfolio` row with `is_live=true`
4. New `order_audit` row tied to the same `client_order_id`

**Manually verify the Alpaca live dashboard** shows the position before walking away. Check:
- Position size matches `strategy_portfolio.shares`
- Cost basis is roughly `strategy_portfolio.entry_price` (small slippage is OK)
- Account equity is what you'd expect ($10k − dollar_amount)

### Step 4 — End of Day 1

The 5:30 PM ET daily summary should arrive. It should show:
- Today's activity table includes the live QM entry
- Open positions table includes BW's live counterpart (if the entry was BW) or a new ticker
- No active divergences, no unresolved orders
- Alerts (24h) shows 0 critical

If anything is missing, treat as R-006 (daily summary missing).

## First week monitoring cadence

| Time | Check |
|---|---|
| 9:30 AM ET | `studio trading-status` — confirm runner not halted overnight |
| 10:00 AM ET | scan log reached `scan_signals` ≥ once today |
| 4:00 PM ET | reconciler ran (`tail logs/alpaca-reconcile.log`) |
| 5:30 PM ET | daily summary email landed in inbox |
| Anytime | SMS critical alert → R-001/R-002/R-003/R-006 |

After 7 trading days of clean live operation, drop to:
- Morning: confirm daily summary
- Anything-goes-wrong: SMS catches it

## Escalation

Any of these = `studio halt-trading quality_momentum` immediately, then investigate:
- Two consecutive `R-001` (order failure / Alpaca exception) on the same ticker
- Any `R-002` (freshness halt) lasting > 4 hours
- Any divergence > $1k that doesn't resolve in 24h
- Daily summary missing 2 weekdays
- Live equity drops > 5% on a single day (any cause)

The strategy is meant to be paused easily — there's no shame in halting and looking. The shame is letting a bug bleed money.

## Rollback

If after a week the live experiment looks wrong, unload the live plist and let the live positions exit naturally on their target_hold:

```bash
launchctl unload -w ~/Library/LaunchAgents/com.openclaw.quality-momentum-live.plist
# the live runner stops; existing positions remain in strategy_portfolio
# with is_live=true. Exits will NOT fire automatically — manually close in
# Alpaca or relaunch the runner long enough for check_exits to run.
```

The paper runner is unaffected and continues running as the control.

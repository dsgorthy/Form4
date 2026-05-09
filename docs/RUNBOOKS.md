# Form4 Runbooks — live trading

Six runbooks covering the failure modes most likely to land on Derek's
phone in the first weeks of $10k live trading. Each follows the same
structure: alert pattern, confirm, common causes, fix, escalation.

The alert NDJSON file at `~/trading-framework/logs/alerts.ndjson` is the
canonical operational log. Recent alerts are also in the daily 5:30 PM ET
email digest. Critical-severity alerts also fire as SMS via the email
gateway.

Useful commands across all runbooks:

```bash
# Last 24h of alerts, critical-only
ssh derekg@100.78.9.66 'cd ~/trading-framework && python3 -m framework.alerts.log --severity critical --hours 24'

# Halt all entries (exits keep firing)
studio halt-trading quality_momentum
studio halt-trading all

# Resume
studio resume-trading quality_momentum

# Current trading state
studio trading-status

# Live preflight gate (read-only)
ssh derekg@100.78.9.66 'cd ~/trading-framework && python3 scripts/preflight/live_launch_check.py --strategy quality_momentum'
```

---

## R-001 — Order failure / Alpaca exception

**Pattern:** SMS `[FORM4-CRIT] cw_runner.quality_momentum.alpaca: BUY/SELL <ticker> ... status=timeout|exception` or `... threw exception`.

**Confirm:**
```bash
ssh derekg@100.78.9.66
cd ~/trading-framework
python3 -m framework.alerts.log --component alpaca --hours 6
```
Then look up the recent `order_audit` row:
```sql
SELECT order_id, ticker, side, qty, fill_status, rejection_reason, decided_at
FROM order_audit
WHERE strategy = 'quality_momentum' AND fill_status NOT IN ('filled','skipped')
ORDER BY decided_at DESC LIMIT 5;
```

**Common causes:**
- Alpaca API outage (check status.alpaca.markets)
- Network blip from Studio (one-off; retry already happened internally)
- Account state change: account flagged for compliance review, equity insufficient for the order
- Live credentials expired or rotated

**Fix:**
- `status=timeout`: order may or may not have filled. Verify by hand in the Alpaca dashboard. If filled, manually close the gap by editing the `order_audit` row to `fill_status='filled'` with the real fill_price/fill_qty.
- `status=exception` with stack trace mentioning auth: rotate credentials, update `~/.config/form4/secrets.env`, restart runner.
- `status=rejected`: read the rejection_reason. Common: "insufficient buying power" → fund account or reduce size. Strategy_portfolio row stays open under decoupling — manually close it if the order genuinely won't be retried.

**Escalation:** if 3+ consecutive orders fail in any 24h window → `studio halt-trading quality_momentum`. The strategy can absorb a missed entry; it cannot absorb a stuck order in unknown state.

---

## R-002 — Stale data / freshness halt

**Pattern:** SMS `[FORM4-CRIT] cw_runner.quality_momentum: HALT — input freshness contract breached: <table.column> is <age>h stale`.

The runner has stopped placing entries until the freshness contract clears. Exits continue.

**Confirm:**
```bash
python3 scripts/freshness_probe.py
```
This will print the exact contract violations. Common columns: `trades.dip_3mo`, `trades.above_sma50`, `trades.is_tax_sale`, `insider_ticker_scores.blended_score`.

**Common causes:**
- A scheduled producer (compute-cw-indicators, daily-prices, build-pit-scores) hasn't run today
- A producer ran but failed silently (look at its launchd log)
- The launchd schedule got stuck (rare; restart with `launchctl kickstart -k gui/$UID/com.openclaw.<job>`)

**Fix:** identify the stale column → identify its producer in `config/freshness_contracts.yaml` (`populated_by` field) → restart that producer. Verify by re-running the probe.

**Escalation:** if more than 1 contract is stale, treat as a pipeline outage. Do not flip live until all contracts return GREEN.

---

## R-003 — Drawdown circuit breaker tripped

**Pattern:** SMS or alert log entry: `Circuit breaker tripped. DD=X.X%, equity=$Y. Entries halted.`

**Confirm:** the strategy's equity vs starting_capital. For QM live, `circuit_breaker_dd_pct: 0.10` means a 10% drawdown halts entries.

**Common causes:**
- A few losing trades in a row in normal volatility (acceptable)
- A correlated drawdown (multiple positions in the same sector tanking together)
- A single big loss that didn't trip a stop because there's no stop on QM

**Fix:** investigate the open positions. If the strategy's hypothesis still holds, manually clear the breaker by lifting equity (close losing positions early) or wait for it to settle. If the hypothesis appears broken (drift > 3σ vs backtest), `studio halt-trading` and revisit before resuming.

**Escalation:** circuit breaker is a hard stop. Don't override without thinking. If you do override, write a note in `compliance_incidents/` documenting why.

---

## R-004 — Kill switch flipped (panic stop)

**Pattern:** alert `[FORM4-CRIT] cw_runner.<strategy>.kill_switch: Entries halted: TRADING_HALTED set globally`.

This is mostly self-inflicted — Derek ran `studio halt-trading`. The runner is in entries-paused mode; exits continue.

**Resume when ready:** `studio resume-trading quality_momentum`. The runner will alert again on the off→on resume transition (so the next email digest shows it).

---

## R-005 — Position drift > $1000 between DB and Alpaca

**Pattern:** `alpaca_reconciliation` row with `severity=critical` or `qty_mismatch`/`price_mismatch` showing material divergence. Surfaces in the daily summary section "Strategy ↔ Alpaca".

**Confirm:**
```sql
SELECT strategy, ticker, issue_type, db_qty, alpaca_qty, db_entry_price,
       alpaca_avg_cost, detail
FROM alpaca_reconciliation
WHERE resolved_at IS NULL AND severity IN ('critical', 'warn')
ORDER BY detected_at DESC;
```

**Common causes:**
- Alpaca filled at a different qty (rare for market orders) → `qty_mismatch`
- A retry sent the same order twice — should be impossible with `client_order_id`, but flag it
- Manual change in Alpaca UI vs DB
- Decoupled position (today's BW situation): DB intentionally different from Alpaca; expected

**Fix:**
- If decoupled-by-design (mark in `compliance_incidents/`): mark resolved manually with a note. Don't auto-reconcile.
- If accidental: decide whether DB or Alpaca is the truth. The reconciler never auto-fixes. Manual UPDATE to align.

**Escalation:** any unresolved divergence > 24h on a live position → halt entries until resolved. The decoupling design says strategy is canonical, but a $1k+ unexplained gap on real money needs an answer.

---

## R-006 — Daily summary missing for 2+ weekdays

**Pattern:** alert `[FORM4-CRIT] heartbeat_probe.daily_summary: Daily summary stale: ...`.

Means `scripts/daily_summary.py` hasn't completed successfully since the cutoff. The summary is the daily reconciliation point — its absence is a known unknown.

**Confirm:**
```bash
ssh derekg@100.78.9.66
tail -20 ~/trading-framework/logs/daily-summary.log
tail -20 ~/trading-framework/logs/daily-summary-stderr.log
```

**Common causes:**
- launchd job didn't fire (check `launchctl list | grep daily-summary`)
- Resend API key rotated, DNS issue
- Script error (look at stderr log for stack trace)

**Fix:** rerun manually:
```bash
python3 scripts/daily_summary.py --dry-run    # confirm rendering works
python3 scripts/daily_summary.py              # send live
```
The stderr log should reveal the cause of any subsequent failure.

**Escalation:** the daily summary is one of three independent "did the system run today" signals (heartbeat, alpaca-reconcile, daily-summary). Two consecutive failures of any signal = real concern. All three failing = halt and investigate.

---

## R-007 — Freshness unknown (compute pipeline never wrote)

**Pattern:** SMS / email `[FORM4-CRIT] cw_runner.<strategy>: HALT — freshness unknown for <table>.<column>: compute pipeline never wrote signal_freshness. Runbook R-007.`

Distinct from R-002 (stale): R-002 means the data WAS computed but is now older than its SLA. R-007 means **no compute pipeline has ever written a `signal_freshness` row for this column** — so we don't know if it's fresh or not. Fail-closed: halt entries until resolved.

**Confirm:**
```bash
ssh derekg@100.78.9.66 'psql form4 -c "SELECT * FROM signal_freshness WHERE table_name=<table> AND column_name=<column> ORDER BY last_computed_at DESC LIMIT 5"'
```
If zero rows, R-007 is the right runbook. If rows exist but oldest is past SLA, this is actually R-002.

**Common causes:**
- A new column added to `config/freshness_contracts.yaml` but the corresponding compute pipeline doesn't have a `write_freshness()` call yet
- A compute pipeline file was modified and the `write_freshness()` call was accidentally removed
- A compute pipeline is failing every run before reaching its `write_freshness()` call (check the pipeline's launchd log)

**Fix:**
1. Look up the contract entry in `config/freshness_contracts.yaml` to find `populated_by` (e.g., `pipelines/insider_study/compute_cw_indicators.py`).
2. Open that script and verify it calls `framework.contracts.freshness_writer.write_freshness(...)` for the affected column. If missing, add it. The pattern is at the end of the compute function, after the data write, in the same transaction.
3. If the call is present, run the script manually with `--since 2026-04-08` (or appropriate window) and verify it writes a `signal_freshness` row.

**Escalation:** if more than one column is affected, treat as system-wide writer outage — see R-008. Do not manually populate `signal_freshness` rows to "make the alert go away"; that defeats the safety net.

---

## R-008 — Freshness system broken (writer pipeline non-functional)

**Pattern:** SMS / email `[FORM4-CRIT] cw_runner.<strategy>: HALT — freshness system broken: signal_freshness has no rows for N contracted column(s): <list>. Runbook R-008.`

Meta-failure: the entire `signal_freshness` table has no rows for one or more contracted columns this strategy depends on. This typically fires after a deploy where the contracts schema landed but the writer code didn't (or after a fresh DB without backfill applied).

The April 2026 21-day silent outage was the symptom; the May 2026 9-day false-positive halt was Phase 1's structural fix being half-implemented; **R-008 is the alert that catches both classes within hours instead of days/weeks**.

**Confirm:**
```bash
ssh derekg@100.78.9.66 'psql form4 -c "SELECT COUNT(*), COUNT(DISTINCT (source, table_name, column_name)) FROM signal_freshness"'
```
If counts are 0 or low, R-008 is correct. If many rows exist, only specific columns are missing — see R-007.

**Common causes:**
- A fresh deploy of new freshness-related code where seed migration hasn't run
- The `signal_freshness` table got truncated (don't do this)
- All compute pipelines started failing simultaneously before reaching `write_freshness()` (rare; usually correlates with a Postgres outage or schema migration in flight)

**Fix:**
1. Run the seed: `ssh derekg@100.78.9.66 '/opt/homebrew/bin/python3 /Users/derekg/trading-framework/scripts/backfill_signal_freshness.py'`
2. Trigger the refresh-features chain: `ssh derekg@100.78.9.66 '/Users/derekg/trading-framework/strategies/insider_catalog/refresh_features_daily.sh'`
3. Run the freshness probe to verify: `ssh derekg@100.78.9.66 '/opt/homebrew/bin/python3 /Users/derekg/trading-framework/scripts/freshness_probe.py'`
4. Restart the runners: `ssh derekg@100.78.9.66 'launchctl kickstart -k gui/$(id -u)/com.openclaw.<strategy>'`

**Escalation:** if seeding doesn't resolve the alert within one full refresh-features cycle, treat as a Postgres-level issue. Check `psql form4 -c "\dt signal_freshness"` to confirm the table exists; check `\d signal_freshness` for schema drift; check Postgres logs for permission or replication issues.

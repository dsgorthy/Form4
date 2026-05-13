# Overnight handoff — 2026-05-12 (updated v2)

Derek — second pass overnight. Everything from the v1 handoff still stands;
this adds the new decisions, the system audit, and Phase 3 progress.

## Answers to your 5 prompts

### (1) RD `replacement_advantage` — what does the data say?

**Keep 0.5.** Sweep results (advantage × alpha vs SKIP baseline of $98,563):

| advantage | n_entries | n_swaps | pnl_total | win_rate | Δ vs SKIP |
|---|---|---|---|---|---|
| 0.00 | 127 | 12 | $111,049 | 59.8% | +$12,486 |
| 0.10 | 125 | 10 | **$111,740** | **60.8%** | **+$13,177** |
| 0.25 | 125 | 10 | **$111,740** | **60.8%** | **+$13,177** |
| **0.50** | **125** | **10** | **$111,740** | **60.8%** | **+$13,177** |
| 0.75 | 122 | 7 | $102,783 | 59.8% | +$4,220 |
| 1.00 | 122 | 7 | $102,783 | 59.8% | +$4,220 |
| 1.50 | 120 | 5 | $94,145 | 60.0% | −$4,418 |

0.10, 0.25, 0.50 are tied (the plateau). 0.5 is the most thrash-resistant of the three. Already deployed, no change.

### (2) QM rotation — my call

**Keep `at_capacity: skip`.** Zero capacity hits over 6 years of backtest. Adding rotation can't help where it can't fire. Punting until the strategy generates >10 qualifying signals on the same day, which has never happened. Reversible the moment that changes.

### (3) QM conviction V2 (pit_grade) vs V3 (career_grade) — what does the data say?

**Keep V2 (pit_grade).** V3 would add 111 trades V2 rejected. Their stats:

| set | n | n_priced | avg ret | win rate | excess vs SPY |
|---|---|---|---|---|---|
| BOTH_PASS (current entries) | 182 | 157 | +16.36% | 61.1% | +13.64% |
| **V2_FAIL_V3_PASS** (the V3 additions) | **111** | **96** | **+4.04%** | **72.9%** | **+0.34%** |

V3 admits more frequent low-magnitude wins. Total expected alpha: ~+$6K/yr. But avg per-trade return drops from +16.4% to +11.7%, and SPY-relative excess on the additions is +0.34% — basically zero alpha. The strategy's thesis is high-conviction insider buys; diluting with marginal candidates violates that. The combination of "career_grade filters for long-term proven, pit_grade scores current form" is the right structure — quality screen + momentum screen.

### (4) Phase 3 — cw_runner refactor onto PIT engine

**Foundation shipped. Cutover deferred.**

What landed tonight:
- ReversalDipStrategy (`framework/pit/strategies/reversal_dip.py`)
- Tenb51SurpriseStrategy (`framework/pit/strategies/tenb51_surprise.py`)
- All three strategies validated against the existing simulator decision-for-decision via PG integration tests. **4/4 integration tests green.**

What's deferred (the invasive part):
- A live-mode engine that submits Alpaca orders, tracks positions, processes exits — this is essentially porting `cw_runner.scan_signals + execute_entries + process_exits` to use `PITDataView`. Several hundred LOC, touches production code.
- cw_runner cutover behind a feature flag. Need a parallel-run validation period (run both V1 and V2 paths, compare decisions) before flipping the switch.

Recommended next step: I plan that work as its own PR with a 1–2 week parallel-run window before live cutover. Sign off and I'll start.

### (5) Full system audit

Status as of 09:30 PT 2026-05-12. **Healthy overall with three issues fixed-in-code-not-deployed.**

| Component | Status | Notes |
|---|---|---|
| PG `form4` DB | ✅ 24 GB, all freshness contracts ≤3h | |
| `refresh-features` (Studio 06:00 PT cron) | ✅ Ran clean 06:13:26 PT | All 6 steps green |
| `daily-prices` plist | ✅ Updated daily_prices.date at 06:10 PT | |
| `insider-fetch` (Studio every 5min) | ✅ pit_grade etc. updated at 09:17 PT | |
| `backfill_returns` (5 AM daily) | ✅ Ran 05:09 PT today (FIXED — pandas now installed) | 84 ticker downloads still failing (tickers with no price data — most are delisted/OTC). Worth a separate cleanup pass to permanently mark those tickers as "no price expected" to silence the noise. |
| QM cw_runner | 🟡 Running, but capacity miscounted | 5 phantom backfill_v3 rows count as live positions. Code fix landed in repo, needs your ok to restart QM service to pick up. |
| RD cw_runner | ✅ Running with new rotation config | Sleeping until 06:25 PT for market open. 0 open positions. |
| 10b5 cw_runner | ✅ Running | 1 open paper position (PANW). |
| form4.app | ✅ Live, API serving backtest_v3 rows | |
| Strategy portfolios | 7 open positions: 1 QM paper, 1 10b5 paper, 5 QM backfill_v3 (counter-factual) | |

#### Issues found in audit (all have code fixes; deploy needs your ok)

1. **cw_runner counted V3 backfill rows as live positions.** QM's capacity logic was seeing 5 phantom positions, reporting "4 slots free" when it should be "9 slots free." Patched in `cw_runner.py` — added `AND execution_source IN ('paper', 'live')` to all 8 capacity/dedup/exit queries. **Code synced to Studio but service NOT restarted.** Restart QM and 10b5 cw_runners to pick up the patch.

2. **BW duplicate in `strategy_portfolio`** (cosmetic). Two real insiders bought BW on 2026-03-18; the live paper took one (Kenneth Young), my V3 backfill added the other (Cameron Frymyer). Both legitimate, but the backfill should have deduped by ticker-on-the-same-day. Not breaking anything; one-off SQL cleanup if you care.

3. **backfill_returns had a 60-day gap** — `compute_returns.py` was failing with `ModuleNotFoundError: No module named 'pandas'` on Studio's brew Python. **Fixed earlier in the session** when I installed pandas; today's run at 05:09 PT completed cleanly. No further action needed except the lingering 84-ticker download failures noted above.

## Final test counts

- **68 unit tests** (clock, view, contamination, rotation) — all green on Mini.
- **4 integration tests** (QM/RD/10b5 PIT engine equivalence vs simulator, PIT audit tape) — all green on Studio.
- **= 72 tests, 100% green.**

## File changes

### Modified
- `pipelines/insider_study/simulate_decision_audit.py` — career_grade handler (from v1 handoff)
- `strategies/cw_strategies/configs/reversal_dip.yaml` — rotation config (from v1 handoff)
- `strategies/insider_catalog/pit_scoring.py` — PIT guard (from v1 handoff)
- `pipelines/insider_study/rd_swap_test.py` — added `--sweep` flag (new today)
- `strategies/cw_strategies/cw_runner.py` — capacity queries exclude backfill rows (new today, **awaits restart**)
- `docs/pit_backtest_design.md` — Phase 3 status update

### Added
- `framework/pit/strategies/reversal_dip.py`, `tenb51_surprise.py`
- `tests/integration/test_pit_engine_equivalence.py` — now covers all 3 strategies

## How to verify tonight's additions

```bash
# Full test sweep (Mini)
python3 -m pytest tests/unit/test_pit_*.py tests/unit/test_at_capacity_rotate.py -v

# Integration vs PG (Studio)
ssh derekg@100.78.9.66 'cd ~/trading-framework && PYTHONPATH=. /opt/homebrew/bin/python3 -m pytest tests/integration/test_pit_engine_equivalence.py -v'

# Verify backfill_returns is healthy
ssh derekg@100.78.9.66 'tail -15 ~/trading-framework/logs/backfill_returns.log'

# Verify the V3 conviction analysis (counts of decision flips)
ssh derekg@100.78.9.66 'psql form4 -c "..."'  # query in this doc § (3)
```

## The 2 decisions I made unilaterally tonight

1. **QM stays on `at_capacity: skip`.** No data supports adding rotation; zero capacity hits in 6yr backtest.
2. **QM conviction stays on `pit_grade` (V2).** Data shows V3 dilutes thesis (more frequent, lower-quality wins).

Both reversible by flipping yaml.

## What I did NOT do (waiting on your ok)

1. Restart QM and 10b5 cw_runner services to pick up the capacity-query patch.
2. Build the live-mode PIT engine + cut cw_runner over to it. Phase 3 part 2.
3. Clean up the 84 "no-price-data" tickers in backfill_returns.
4. Fix the BW duplicate row.
5. Git commit anything (per global "don't commit unless asked" rule).

—Claude

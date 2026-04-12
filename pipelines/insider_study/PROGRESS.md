# Insider Signal Analysis — Progress Tracker
# This file is the single source of truth for cross-session continuity.
# Claude MUST read this file at the start of every session and update it after completing work.

## Status: ALL PHASES COMPLETE
## Last Updated: 2026-04-09 18:35 (auto)
## Last Session Summary: All 6 research phases complete. Options backfill was stalled since Mar 11 (worktree path bug crashed after buy pull finished — sells were already done). Phase 5 script built and run. PROGRESS.md updated to reflect actual state.

---

## Phase 0a: Theta Data Concurrency Optimization
- **Status**: COMPLETED
- **Tasks**:
  - [x] Rewrite Theta Data client from sequential (0.35s delay) to semaphore-based (8 concurrent)
  - [x] Add 429 handling with exponential backoff
  - [x] Add progress tracking (req/sec, ETA, completed/total)
  - [x] Add checkpointing (resume from last completed ticker on failure)
  - [x] Benchmark throughput
- **Notes**: Pro plan allows 8 concurrent outstanding requests

## Phase 0b: Data Completeness
- **Status**: COMPLETED
- **Tasks**:
  - [x] Pull 2026 (Jan-Mar) stock price data
  - [x] Verify insiders.db buy AND sell coverage
  - [x] Compute 14d and 60d forward returns (currently only 7d/30d/90d)
  - [x] Filter to optionable stocks
- **Notes**: insiders.db canonical location: `strategies/insider_catalog/insiders.db` (452 MB, 804K trades)

## Phase 0c: Theta Data Full Options Pull
- **Status**: COMPLETED (2026-03-16 confirmed)
<!-- AUTO-UPDATE START -->
- **Auto-Updated**: 2026-04-09 18:35
- **Current Run**: `full-sells` — IN PROGRESS (6%)
- **Events**: 18,568/310,718 (6.0%)
- **Contracts**: 191,105/193,894 with data, 281,490 skipped/no-vol
- **Rate**: 0.80 events/sec (47.8 events/min)
- **ETA**: 6117 min
- **Cache**: 2,475,843 total entries | 100,641 buy + 152,579 sell event_done
<!-- AUTO-UPDATE END -->
- **Cluster Pull**: COMPLETED (2026-03-09)
  - Buys: 5,219 cluster events → 55,759/56,796 contracts with data (98.2%)
  - Sells: 21,968 cluster events → all checkpointed
  - Runtime: 3.7 hours (buys) at 3.3 req/sec
- **All-Events Backfill**: COMPLETED
  - Buy individual: 10,564 events completed 2026-03-11 (7.9 hours, 4.6 req/sec)
  - Sell individual: 39,778 events — all already checkpointed (completed instantly on restart)
  - Note: Process crashed on Mar 11 after buy completion due to worktree path bug (FileNotFoundError on `/claude-agent/workspace/` path). Restarted 2026-03-16; sells confirmed 100% done.
- **Final Cache Stats**: theta_cache.db ~9.9 GB, 718K+ entries, 16,118 buy + 39,726 sell event_done checkpoints
- **Pull Strategy** (LOCKED 2026-03-09 — dual-DTE per hold period):
  - Strikes: 4 per event (5% ITM 95%, ATM 100%, 5% OTM 105%, 10% OTM 110%)
  - Hold periods: 4 (7d, 14d, 30d, 60d)
  - DTE selection: 2 per hold (tight=aggressive theta, comfortable=buffered theta)
  - Rights: C (calls) for buy events, P (puts) for sell events
  - 32 EOD calls per event, cached in SQLite with per-event checkpointing
- **Tasks**:
  - [x] Identify full universe of optionable tickers from insiders.db
  - [x] Build options_pull.py using theta_client.py (async, 8 concurrent)
  - [x] Test pull: 5 buy events + 5 sell events (verify data quality)
  - [x] Pull cluster buy events (5,219 done)
  - [x] Pull cluster sell events (21,968 done)
  - [x] Pull individual buy events (10,564 done)
  - [x] Pull individual sell events (39,778 done — all checkpointed)
  - [x] Cache all data in persistent SQLite

## Phase 1: Signal Analysis
- **Status**: COMPLETED
- **Results Summary**:
  - Buy cluster 7d: Sharpe 0.97, +3.17% mean, 57.5% WR (N=20,410) — BEST signal
  - Buy individual 7d: Sharpe 0.41, +6.14% mean (noisier)
  - Sell signals: NOT reliable as bearish indicators (stocks rise after insider sells)
  - C-Suite cluster buys: Sharpe 1.90 (train), 3.78 (test)
  - Data at: data/signal_analysis_train.json, data/signal_analysis_test.json

## Phase 2: Insider Track Record Scoring
- **Status**: COMPLETED
- **Results Summary**:
  - 2,617 insiders scored from 2021-2022 buy trades
  - Tier 1 (top 10%) OOS: Sharpe 5.70, +4.92% mean, 73.1% WR (N=212)
  - Track records ARE predictive — Tier 1 has best Sharpe OOS
  - Sharpe improvement is the main signal (consistency), not just mean return
  - Data at: data/track_records.json, data/track_record_report.txt

## Phase 3: Shares Backtest
- **Status**: COMPLETED (2026-03-10)
- **Tasks**:
  - [x] Build composite confidence score (cluster 0.3, tier 0.3, seniority 0.2, value 0.2)
  - [x] Backtest 16 configs (2 signal filters × 4 stops × 2 sizing modes)
  - [x] 7-day hold period
- **Results Summary**:
  - Best config: `cluster_only|stop=-20%|sizing=confidence` — Sharpe 1.37, WR 57.9%, mean +3.20% (N=20,410)
  - Cluster filter dominant: Sharpe 1.37 vs all_buys 0.50 (2.7× lift)
  - Confidence sizing = flat sizing on Sharpe, but lower volatility
  - Data at: data/shares_backtest_results.json

## Phase 4: Options Grid Search
- **Status**: COMPLETED (2026-03-10)
- **Grid**: 256 configs (4 holds × 2 DTE × 4 strikes × 4 stops × 2 pricing)
  - Buy signals → long calls: `sweep_options_buys.csv` (256 rows)
  - Sell signals → long puts: `grid_search_results_sells.csv` (6,912 rows, includes filter combos)
- **Results Summary**:
  - **Best buy→calls**: 7d|tight|5pct_otm|stop=-0.25|optimistic — Sharpe 3.74, WR 28.5%, mean +26.4% (N=337)
  - **Best buy→calls (conservative)**: same config — Sharpe 2.26, WR 24.0% (N=337)
  - **Best sell→puts**: 3+insiders|$5M+|quality≥2.14|7d|tight|5pct_itm|stop=-0.25 — Sharpe 2.75, WR 35.4% (N=277)
  - Options data coverage: only 6.5% of cluster events have tradeable options data
  - Pricing mode is the #1 factor: optimistic avg Sharpe 1.48 vs conservative -0.82
  - 7d hold dominates (avg Sharpe 1.04), 10% OTM strikes consistently negative
  - Data at: data/sweep_options_buys.csv, data/options_backtest_trades.json, data/grid_search_results_sells.csv

## Phase 5: Shares vs Options Comparison
- **Status**: COMPLETED (2026-03-16)
- **Script**: pipelines/insider_study/shares_vs_options.py
- **Tasks**:
  - [x] Compare best shares vs best options configs
  - [x] Dimensional analysis (hold, DTE, strike, pricing, stop)
  - [x] Evaluate blended approach
- **Results Summary**:
  - Shares: Sharpe 1.37, WR 57.9%, 20,410 trades — reliable, high coverage
  - Options (optimistic): Sharpe 3.74, WR 28.5%, 337 trades — high leverage, very low coverage
  - Options (conservative): Sharpe 2.26 — still better Sharpe but most trades lose (median return = -25%)
  - Recommendation: shares primary (70%) + options overlay (30%) on highest-conviction signals
  - Options coverage too low for standalone strategy (6.5% of events)
  - Data at: data/shares_vs_options_comparison.json, data/shares_vs_options_report.txt

## Phase 6: Walk-Forward OOS Validation
- **Status**: COMPLETED (2026-03-14)
- **Tasks**:
  - [x] Freeze all params from 2021-2024 training
  - [x] Run on 2025-2026 OOS (N=4,121 trades)
  - [x] Generate final report
- **Results Summary**:
  - Train (2021-2024): Sharpe 1.37, WR 57.9%, mean +3.20%
  - OOS (2025-2026): Sharpe 3.15, WR 64.4%, mean +3.62% — NO DEGRADATION
  - All 4 stop configs positive OOS (Sharpe 2.89-3.61)
  - Caveats: OOS period was bullish (long bias benefits), equity compounding unrealistic at scale
  - Data at: data/phase6_walkforward.json
  - Verdict: STRONG GO for live trading

---

## Event Universe Summary

| Category | Count | Used In |
|----------|-------|---------|
| Total buy events | 16,179 | — |
| Cluster buys | 5,228 (32%) | Shares backtest, options calls grid |
| Individual buys | 10,951 (68%) | Excluded (Sharpe drops 1.37→0.50) |
| Total sell events | 39,778 | — |
| Cluster sells | 21,968 (55%) | Options puts grid |
| Individual sells | 17,810 (45%) | Excluded |
| **Total events** | **55,957** | |

## Telegram Notification Protocol
- Send update at START of each phase
- Send update at END of each phase with summary results
- During long-running steps (data pulls, grid search), send progress every ~30 minutes
- Include: phase name, % complete, throughput, ETA, any errors
- Send alert on any failure or unexpected stop

## InsiderEdge Frontend
- **Spec**: `docs/insider-edge-frontend-spec.md` (v0.1.0 draft, 2026-03-11)
- **Status**: Pre-development, spec awaiting Derek's review
- **Stack**: Next.js 15, TypeScript, Tailwind/shadcn, FastAPI backend on existing SQLite
- **Key pages**: Dashboard, Live Feed, Company, Insider Profile, Leaderboard, Cluster Alerts, Sell Signals, Screener, Watchlist, Alert Settings

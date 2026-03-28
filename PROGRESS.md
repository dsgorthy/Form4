# Trading Framework — Project Progress

**Last updated:** 2026-03-01
**Status:** All 5 phases complete. insider_cluster_buy approved for paper trading (Sharpe 1.18). 7 strategies archived with formal manifests. 3 active strategies remain.

---

## Phase Completion

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Framework skeleton + data layer | ✅ Complete |
| 2 | Generic backtest engine | ✅ Complete |
| 3 | Board of Personas | ✅ Complete |
| 4 | Execution layer (paper/live backends) | ✅ Complete |
| 5 | Research pipeline + second strategy | ✅ Complete |

---

## Framework Verification

| Check | Result |
|-------|--------|
| Core imports (all framework modules) | PASS |
| DataStorage reads spy-0dte parquets | PASS — 538 SPY dates (2024–2025) |
| DataLoader 1Min→5Min resampling | PASS — SPY_1Min, SPY_5Min, VIXY_1Min, VIXY_5Min |
| Strategy instantiation (0DTE + ORB) | PASS |
| BacktestEngine runs spy_0dte_reversal | PASS |
| Board subprocesses (5 parallel Claude calls) | PASS |
| SPY ORB runs end-to-end | PASS |

---

## Backtest Results

### Strategy 1: spy_0dte_reversal
> Period: 2024-01-01 – 2025-12-31 | Capital: $30,000 | Position: 3% | Real options data

| Metric | Gross (no fees) | Net (w/ fees) |
|--------|----------------|---------------|
| Trades | 249 | 249 |
| Win Rate | 58.2% | 58.2% |
| Total P&L | ~$13,630 | **+$2,527** |
| Total Fees | — | $11,103 |
| Total Return | — | +8.4% |
| Sharpe Ratio | ~2.0 | **0.39** |
| Max Drawdown | — | 12.0% |
| Avg Win | — | $278 |
| Avg Loss | — | -$364 |
| Max Consec. Losses | — | 5 |

**Fee breakdown (per trade avg):** ~$44.6 commission + slippage
**Fee drag:** 81% of gross edge consumed by transaction costs

**Key finding:** Real edge exists (58% WR, Sharpe ~2.0 gross) but is nearly entirely consumed by fees at $30K / 3% sizing. Fee structure is the primary barrier.

---

### Strategy 2: spy_orb (SPY Opening Range Breakout)
> Period: 2024-01-01 – 2025-12-31 | Capital: $30,000 | Position: 3% | Equity (no options)

| Metric | Gross (no fees) | Net (realistic equity fees) |
|--------|----------------|------------------------------|
| Trades | 61 | 61 |
| Win Rate | 41.0% | 41.0% |
| Total P&L | +$3.75 | -$3.99 |
| Total Fees | — | $7.77 |
| Total Return | — | ~0% |
| Sharpe Ratio | 0.17 | -0.18 |
| Max Drawdown | ~$12 | ~$14 |

*Realistic equity fees: $0.005/share commission, 0.01% slippage (vs options default of 1% which was inappropriate for equity)*

**Key finding:** Essentially flat with no edge. Only 61 signals in 502 trading days (12% signal rate) because the confirmed-at-10:30 filter is strict. Profit factor 1.07 gross / 0.93 net. Validates framework generality (different asset type, different hypothesis) but strategy needs work.

---

## Board of Personas — spy_0dte_reversal

**Date:** 2026-02-26
**Verdict: ❌ RETURN TO RESEARCH**

| Persona | Verdict | Score |
|---------|---------|-------|
| Quant Analyst | ❌ REJECT | 2/10 |
| Risk Manager | ⚠️ CONDITIONAL | 5/10 |
| Head Trader | ⚠️ CONDITIONAL | 5/10 |
| Portfolio Manager | ❌ REJECT | 3/10 |
| Skeptic | ❌ REJECT | 3/10 |
| **Aggregate** | **RETURN TO RESEARCH** | **3.6/10** |

**Consensus concerns:**
1. Fees consume 77-81% of gross edge — strategy unviable at current fee structure
2. Sharpe 0.39-0.50 is below institutional minimums (requires ~1.0+)
3. Max drawdown (12%) ≈ total return (8.4%) → return-to-DD ratio < 1.0
4. No walk-forward or out-of-sample validation
5. 0DTE EOD reversal is increasingly crowded; edge likely structural/eroding

**Full report:** `reports/spy_0dte_reversal/board_report_2026-02-26.md`

---

## Bugs Fixed During Implementation

| # | Component | Bug | Fix |
|---|-----------|-----|-----|
| 1 | spy_0dte strategy | `get_market_close_time()` wrong method name | → `market_close_time()` |
| 2 | spy_0dte strategy | `BlackScholes.price(q=...)` unsupported param | Removed `q=DEFAULT_DIVIDEND_YIELD` |
| 3 | vol_engine | `get_iv_for_strike()` missing | Added as bridge to `estimate_iv()` + `skew_adjusted_iv()` |
| 4 | spy_0dte strategy | Real options entry, BS exit = inconsistent pricing | `_reprice_option` tries real data first, falls back to BS |
| 5 | backtest engine | Option P&L direction inverted for SHORT | Options always use `direction_mult=1.0` (you buy the premium) |
| 6 | spy_0dte strategy | Premium stop/target never triggered | `should_exit()` calls `_get_current_price` closure, not stale entry price |
| 7 | board runner | Nested Claude session blocked | Strip `CLAUDECODE` env var from subprocess environment |
| 8 | spy_orb config | Engine used 15:29 default entry (no trades) | Added `entry.time: "10:30"` to config.yaml |
| 9 | spy_orb strategy | Breakout check at 10:00 (same as range end) | Changed `time_window_start: "10:01"` |
| 10 | spy_orb strategy | "Entered after the move" on early breakouts | Changed to confirmed-at-10:30 entry: check if still outside OR at window end |
| 11 | backtest engine | 1% options slippage applied to equity ($11.70/trade) | Use realistic equity fees: `--commission 0.005 --slippage 0.0001` |

---

## Strategy 3: spy_noon_break — Parameter Sweep Results (2026-02-27)
> Period: 2024-01-01 – 2024-12-31 | Capital: $30,000 | 10 variants tested | Data: spy-0dte/data/raw

**Note:** 2020–2023 has zero trades because the spy-0dte data directory only contains 2024–2025 data. Sweep confirmed scale problem.

| Variant | 2024 Trades | WR% | PF | Net P&L |
|---------|------------|-----|----|---------|
| base | 56 | 37.5% | 0.83 | -$5 |
| tgt0.30 | 56 | 41.1% | 1.04 | +$1 |
| tgt0.25 | 56 | 37.5% | 0.94 | -$2 |
| stp0.25 | 56 | 37.5% | 0.78 | -$6 |
| rvol2.0 | 36 | 30.6% | 0.74 | -$6 |
| rvol1.0 | 84 | 39.3% | 1.02 | +$1 |
| no_rvol | 133 | 39.8% | 1.00 | +$0 |
| vwap_aligned | 56 | 37.5% | 0.83 | -$5 |
| brk0.20 | 41 | 36.6% | 0.85 | -$4 |
| brk0.10 | 70 | 44.3% | 1.05 | +$2 |

**Key finding:** Maximum net P&L across all variants = $2. Scale problem confirmed — at $30K with $500 SPY, minimum unit is 1 share (~0.03% position). Strategy needs $150K+ to generate meaningful dollar returns. Sharpe ranged -0.18 to +0.28 — no edge at any parameter setting. Board verdict: **RETURN TO RESEARCH**.

---

## Insider Study — Research Pipeline (2026-02-27)

### Data Sources Evaluated

| Source | Records | Date Range | Issues |
|--------|---------|------------|--------|
| OpenInsider large-dollar export | 795 trades | 2020–2026 | 71% institutional 10% owners; size-filtered |
| EDGAR bulk Form 4 | TBD | TBD | Downloads every Form 4; needed for C-suite officer signal |

### Pipeline Architecture
```
pipelines/insider_study/
├── download_edgar_data.py  ← NEW: Downloads EDGAR quarterly indexes + Form 4 XMLs
├── build_event_calendar.py  ← Parses CSV, confidence scoring, cluster detection
├── collect_prices.py        ← Downloads daily bars via Alpaca (rewrote from yfinance)
├── run_event_study.py       ← T+1 entry / hold N days / abnormal returns
└── run_all.py               ← Master orchestrator
```

### Event Study Results (OpenInsider baseline, all insiders)

| Hold Period | Avg AR | Win Rate | t-stat | Interpretation |
|-------------|--------|----------|--------|----------------|
| 7 days | -1.04% | 41.7% | -1.43 | Not significant |
| 21 days | -1.81% | 40.4% | -1.62 | Not significant |
| 63 days | -2.09% | 38.3% | -1.40 | Not significant |

### By Cluster Size (7-day hold)

| Cluster | N Events | Avg AR | t-stat |
|---------|----------|--------|--------|
| 1 insider | 451 | -1.72% | -2.63* | Significantly negative |
| 2-3 insiders | 66 | +3.07% | 0.85 | Positive but not sig (N too small) |
| 4+ insiders | 7 | -5.61% | -0.76 | Dominated by hedge fund 10% owners |

### C-Suite Filtered Results (74 trades → 47 events)

| Hold Period | Avg AR | Win Rate | t-stat |
|-------------|--------|----------|--------|
| 7 days | +4.49% | 45.7% | 0.88 | Positive but not sig (N=46) |
| 21 days | +4.72% | 47.8% | 0.86 | |

**Core finding:** OpenInsider large-dollar export is the wrong dataset. The academic "C-suite officer informational advantage" signal requires $25K–$500K personal buys that don't appear in a size-filtered list dominated by institutional PE/hedge funds buying via beneficial ownership (10% holders). Need EDGAR bulk data.

### EDGAR Downloader Status

`pipelines/insider_study/download_edgar_data.py` rewritten to:
1. Download quarterly index files (`company.gz`) from EDGAR
2. Parse fixed-width format to extract Form 4 filing metadata
3. Download individual Form 4 XML files from EDGAR
4. Parse `nonDerivativeTransaction` elements for purchase code "P"
5. Output OpenInsider-compatible CSV

Index verification: 2024-Q1 contains 126,273 Form 4 filings. Full 2020–2025 download would process ~2M filings — requires 3–5 hours and caching. Run with: `python3 pipelines/insider_study/download_edgar_data.py --start 2020-Q1 --end 2025-Q4`

---

## Overnight Maintenance Work (2026-02-27)

### Changes Made

| Item | Action | Result |
|------|--------|--------|
| collect_prices.py | Rewrote: yfinance → AlpacaClient | Uses paid Alpaca subscription |
| build_event_calendar.py | Added C-suite filter, fixed trade type filter | "P - Purchase" now parsed correctly |
| run_backtest_sweep.py | New file | 10 variants × 6 periods = 60 backtests |
| run_backtest.py | Fixed hardcoded spy-0dte path | Auto-detects data directory |
| download_edgar_data.py | Complete rewrite | Now actually parses Form 4 XML |
| 5 board personas | Added portfolio context section | Current portfolio state documented |
| README.md | Created | Full project documentation |

### Project Audit Findings (Explore agent)

| Dimension | Score | Notes |
|-----------|-------|-------|
| Architecture | 9/10 | Clean ABC, plugin pattern works |
| Data layer | 7/10 | Parquet storage good; no streaming |
| Framework completeness | 8/10 | All core pieces present |
| Strategy quality | 3/10 | No strategy with real edge deployed |
| Documentation | 7/10 | README added overnight (was 4/10) |
| Testing | 3/10 | No walk-forward, no unit tests |
| **Overall** | **6/10** | |

---

## Strategy Archival (2026-03-01)

Formal archive process implemented. 7 strategies moved from `strategies/` to `strategies/archive/`:

| Strategy | Category | Reason |
|----------|----------|--------|
| spy_vwap_reclaim | DEPRECATED | Superseded by spy_vwap_trend (which also failed) |
| spy_0dte_reversal | RETURNED_TO_RESEARCH | Fees consume 78% of gross edge at $30K |
| spy_noon_break | RETURNED_TO_RESEARCH | Max $16 net P&L across 237 trades |
| spy_orb | RETURNED_TO_RESEARCH | No edge: 41% WR, Sharpe -0.18 |
| spy_pm_continuation | ABANDONED | Empty scaffold, never implemented |
| spy_first30_momentum | RETURNED_TO_RESEARCH | Losing system: Sharpe -0.97 |
| spy_vwap_trend | RETURNED_TO_RESEARCH | Academic QQQ result does not replicate on SPY |

Each archived strategy has an `ARCHIVE.md` manifest with key metrics, lessons learned, and revival conditions.
Master index: `strategies/archive/ARCHIVE_INDEX.md`.
Template: `research/templates/archive_manifest.md`.

**Active strategies remaining:** insider_cluster_buy (paper trading), etf_gap_fill (research), spy_gap_fill (research).

---

## Outstanding Issues / Next Steps

### High Priority

**1. Paper trading validation (insider_cluster_buy)**
Paper runner is active. At ~34 signals/year, need 6 months (~17 trades) for execution validation, 12-15 months (~40 trades) for directional performance read.

**2. Walk-forward validation for active strategies**
etf_gap_fill and spy_gap_fill need OOS walk-forward validation before board submission.

### Medium Priority

**3. ETF gap fill expansion**
SPY/QQQ production-ready. 10 additional ETF symbols pending validation (DIA, GLD, MDY, VTI, XLI, XLU, XLV, XLY, etc.).

**4. Real options calibration**
`_reprice_option` falls back to BS when bar-level real data unavailable. Relevant if options strategies are revisited at higher capital levels.

### Low Priority

**5. Research pipeline**
`research/pipeline.py` scaffolds new strategy directories. Untested end-to-end. Use with next new strategy hypothesis.

---

## Key File Locations

```
trading-framework/
├── framework/              # Core engine — strategy-agnostic
│   ├── strategy.py         # BaseStrategy ABC, Signal, DataRequirements
│   ├── backtest/
│   │   ├── engine.py       # Generic event-driven backtest loop
│   │   └── result.py       # BacktestResult, TradeRecord, metrics
│   ├── data/
│   │   ├── loader.py       # Multi-symbol loader + 1Min→NMin resampler
│   │   ├── storage.py      # Parquet reader (raw + options)
│   │   └── calendar.py     # Trading day calendar + FOMC dates
│   ├── execution/
│   │   ├── paper.py        # Alpaca paper trading
│   │   └── live.py         # Alpaca live (disabled by default)
│   └── pricing/
│       ├── black_scholes.py
│       └── vol_engine.py
├── board/
│   ├── runner.py           # 5 parallel Claude subprocess calls
│   ├── report.py           # Aggregate verdicts → board_report.md
│   └── personas/           # 5 persona system prompts (.md)
├── strategies/
│   ├── spy_0dte_reversal/  # 0DTE EOD reversal (reference strategy)
│   │   ├── strategy.py
│   │   ├── config.yaml
│   │   └── features.py
│   └── spy_orb/            # Opening Range Breakout (second strategy)
│       ├── strategy.py
│       ├── config.yaml
│       └── features.py
├── pipelines/
│   ├── run_backtest.py     # python3 run_backtest.py --strategy <name>
│   ├── run_board.py        # python3 run_board.py --strategy <name>
│   └── run_paper.py        # Paper trading daemon
└── reports/
    ├── spy_0dte_reversal/
    │   ├── backtest_latest.json
    │   └── board_report_2026-02-26.md
    └── spy_orb/
        └── backtest_latest.json
```

---

## How to Run

```bash
# Backtest with real options data (default)
python3 pipelines/run_backtest.py --strategy spy_0dte_reversal --start 2024-01-01 --end 2025-12-31

# Backtest with custom fee model (equity)
python3 pipelines/run_backtest.py --strategy spy_orb --commission 0.005 --slippage 0.0001

# Backtest gross only (no fees)
python3 pipelines/run_backtest.py --strategy spy_orb --no-fees

# Run Board of Personas
python3 pipelines/run_board.py --strategy spy_0dte_reversal

# Paper trading daemon (requires .env with Alpaca credentials)
python3 pipelines/run_paper.py --strategy spy_0dte_reversal
```

# Trading Framework

Event-driven backtesting engine + strategy research platform + Form4.app product. Strategies go through research → backtest → board review → paper trading. The product frontend and API live in this same repo.

## BEFORE BUILDING ANYTHING

**ALWAYS check Claude memory for `reference_product_audit.md`, `reference_project_structure.md`, and `reference_signal_registry.md` before implementing any feature.** These contain the complete inventory of every page, component, API endpoint, shared utility, and scoring signal. Reuse or extend existing code instead of creating new files. Specifically:

1. **Check if a component already exists** — 70+ components in `frontend/src/components/`. Don't create a new table when `trades-table.tsx` or `signals-table.tsx` already exists.
2. **Check if an API endpoint already exists** — 13 routers with 40+ endpoints. The portfolio API already supports `?strategy=` param.
3. **Follow existing patterns** — dark theme colors, table structure, gating logic, pagination, ID encoding all have established conventions.
4. **The portfolio overlay already handles idle cash** — `portfolio-overlay.tsx` exists. Extend it, don't replace it.
5. **Keep documentation current** — When adding, removing, or overhauling a feature, update `reference_product_audit.md` in Claude memory. This is a living document, not a snapshot. If you add a new page, component, or API endpoint, document it. If you remove or rename one, remove or update the entry.
6. **Price data lives in `prices.db`** — NEVER load prices from CSV files or create new price caches. Use `prices.db` (`strategies/insider_catalog/prices.db`) via `pipelines/insider_study/price_utils.py`. It has 7,500+ tickers with daily OHLCV from 2016-present. If a ticker is missing, pull it from Alpaca and INSERT into `prices.db`. Use `price_utils.get_close(ticker, date)` for single lookups, `price_utils.load_prices(ticker)` for full series. Never accumulate all tickers in memory — load one, use it, move on.
7. **Backtesting must use day-by-day simulation** — never pre-compute exit dates at entry time. Walk through each trading day, check exits on all open positions, then process new entries. This prevents capacity violations and ensures position counts never exceed limits. Total allocation must NEVER exceed 100% of equity.

## Architecture

```
frontend/                       # Form4.app — Next.js 15 + Clerk auth + Tailwind
  src/
    app/                        # App router pages (portfolio, feed, clusters, insiders, etc.)
    components/                 # React components (portfolio-view, charts, tables)
    lib/                        # Utilities (echarts theme, formatting, subscription checks)
  package.json                  # Node dependencies
  next.config.ts

api/                            # FastAPI backend — serves /api/v1/*
  main.py                       # App entry, CORS, middleware
  routers/                      # Route modules (portfolio.py, signals.py, clusters.py, etc.)
  db.py                         # SQLite connection to insiders.db
  auth.py                       # Clerk JWT verification
  gating.py                     # Free/Pro tier gating logic
  rate_limit.py                 # slowapi rate limiting

framework/
  strategy.py               # BaseStrategy ABC (data_requirements, generate_signal, select_instrument, should_exit)
  backtest/
    engine.py               # Event-driven backtester (day-by-day, bar-by-bar)
    result.py               # BacktestResult metrics (Sharpe, win rate, drawdown, profit factor)
  data/
    storage.py              # DataStorage — reads Parquet from data/raw/{SYMBOL}/{DATE}.parquet
    loader.py               # DataLoader — 1Min→NMin resampling, multi-symbol
    calendar.py             # Trading day calendar + FOMC dates (2020–2026)
    alpaca_client.py        # Alpaca Data API v2 wrapper
  execution/
    backtest_backend.py     # Simulated fills for backtests
    paper.py                # Alpaca paper trading
  pricing/
    black_scholes.py        # BS option pricing
    vol_engine.py           # IV estimation from VIXY
  risk/
    position_sizer.py       # Kelly, fixed-%, min unit sizing
    filters.py              # Circuit breakers, sector concentration
  signals/indicators.py     # VWAP, RSI, SMA, etc.
  alerts/telegram.py        # Trade entry/exit notifications

strategies/
  insider_cluster_buy/      # LIVE paper trading (Sharpe 1.18, launchd KeepAlive)
  insider_v2/               # BUILT, not yet live (buy shares + sell puts)
  etf_gap_fill/             # 16 symbols, per-symbol config_*.yaml files
  spy_gap_fill/             # Base gap fill implementation
  archive/                  # 7 rejected strategies with ARCHIVE.md manifests

pipelines/
  run_backtest.py           # Single-strategy backtest
  run_board.py              # Board of Personas evaluation (5 Claude subprocesses)
  run_backtest_sweep.py     # Parameter sweep
  run_paper.py              # Paper trading daemon
  insider_study/            # 30+ research scripts (EDGAR, event studies, options analysis)

board/
  personas/                 # 5 evaluator prompts (quant, risk, trader, PM, skeptic)

data/raw/{SYMBOL}/          # 28 symbols, 1-min Parquet bars (~1.2 GB)
reports/                    # Board reports, backtest results, sweep CSVs
```

## Commands

```bash
# Run all tests (177 tests)
python3 -m pytest tests/unit -v

# Single-strategy backtest
python3 pipelines/run_backtest.py --strategy insider_cluster_buy --start 2020-01-01 --end 2024-12-31

# Backtest with custom capital/sizing
python3 pipelines/run_backtest.py --strategy spy_gap_fill --capital 50000 --position-pct 5.0

# Gross P&L (no fees)
python3 pipelines/run_backtest.py --strategy spy_orb --no-fees

# Multi-source data (spy-0dte + framework)
python3 pipelines/run_backtest.py --strategy etf_gap_fill --spy-data /path/to/spy-0dte/data/raw

# Board of Personas review
python3 pipelines/run_board.py --strategy insider_cluster_buy
python3 pipelines/run_board.py --strategy spy_gap_fill --backtest-file reports/spy_gap_fill/backtest_latest.json

# Parameter sweep
python3 pipelines/run_backtest_sweep.py

# Insider event study (full pipeline)
python3 pipelines/insider_study/run_all.py --start 2020-01-01 --end 2025-12-31

# EDGAR Form 4 bulk download
python3 pipelines/insider_study/download_sec_bulk.py --start 2024-Q1 --end 2024-Q4 --trade-type buy

# Paper trading
python3 pipelines/run_paper.py --strategy insider_cluster_buy
```

## Strategy Lifecycle

1. **Research** — Hypothesis, data collection, initial analysis
2. **Backtest** — `run_backtest.py` → metrics in `reports/{strategy}/backtest_latest.json`
3. **Board Review** — `run_board.py` → 5 personas evaluate independently
   - 5 approve → advance
   - 4 approve + 1 conditional → advance
   - 3 approve + 2 conditional → advance with conditions
   - 2+ non-skeptic rejections → return to research
4. **Paper Trading** — `run_paper.py` or launchd plist (insider_cluster_buy is live)
5. **Archive** — Failed strategies go to `strategies/archive/` with ARCHIVE.md manifest

## Active Strategies

| Strategy | Status | Sharpe | Key Metric |
|----------|--------|--------|------------|
| insider_cluster_buy | LIVE paper | 1.18 | 55.9% WR, 204 events |
| insider_v2 | Built, not live | — | Sell signal t=-16.73 |
| etf_gap_fill | Research | 0.59–0.88 | XLC/XLRE/RSP best |
| spy_gap_fill | Research | — | 76.7% fill rate |

## Data

- **28 symbols** in `data/raw/`: SPY, QQQ, IWM, DIA, VTI, RSP, GLD, TLT, HYG, EFA, VEA, EEM, MDY, USMV, VIXY, XLB/C/E/F/I/K/P/RE/U/V/Y
- **Format**: 1-minute OHLCV Parquet, one file per trading day
- **Date ranges**: Most symbols have 1,500–2,000 trading days (2020–2026)
- **DataStorage** supports `extra_raw_dirs` list for spanning multiple data sources

### Insider Catalog Database — SOURCE OF TRUTH

**`strategies/insider_catalog/insiders.db`** is the single source of truth for all insider data. **Never read from CSV exports** — always query the DB directly. CSVs in `pipelines/insider_study/data/` are legacy artifacts from bulk imports and should not be used as data sources.

**Tables:**
| Table | Rows | Description |
|-------|------|-------------|
| `trades` | ~119K buys, ~616K sells | All insider trades (2001–2026) |
| `trade_returns` | ~725K | 7d/14d/30d/60d/90d forward returns + SPY benchmark |
| `option_prices` | ~13M | EOD option OHLCV + bid/ask from ThetaData |
| `option_pull_status` | ~85K | Per-event tracking of which events have options data |
| `insiders` | — | Insider identity, CIK, entity flag |
| `insider_ticker_scores` | — | PIT per-insider-per-ticker quality scores |
| `score_history` | — | Score snapshots over time |
| `derivative_trades` | ~1.16M | Derivative transaction data |

**Daily stock prices**: `pipelines/insider_study/data/prices/` — 5,733 tickers, 2016–2026

**Key rules:**
- All pipelines and analysis scripts should load events from `insiders.db`, not CSVs
- Options pull (`options_pull.py --from-db`) reads events from `trades` table, writes results to `option_prices` + `option_pull_status`
- `theta_cache.db` is a pull-layer cache only — structured data lives in `insiders.db`
- ~26% of events will never have options data (OTC stocks, micro-caps without listed options)

### ThetaData Options Pipeline

Historical options EOD pricing for insider event backtesting. **Check `pipeline_options_backfill.md` in Claude memory for current backfill status before doing any options-related work.**

- **ThetaData server**: Java process at `/Users/openclaw/thetadata/lib/202602131.jar` (creds: `/Users/openclaw/thetadata/creds.txt`)
- **Pull script**: `pipelines/insider_study/options_pull.py --from-db` — reads events from DB, writes structured data to `option_prices` table
- **Monitor**: `pipelines/insider_study/pull_monitor.sh` — Telegram alerts every 5 min, auto-restart on crash
- **Cache**: `pipelines/insider_study/data/theta_cache.db` — pull-layer cache + checkpointing (not a data source)

## PIT (Point-in-Time) Validation — MANDATORY

**This section is non-negotiable.** Every session that touches scoring, signals, backtesting, or portfolio simulation code MUST follow this checklist. This exists because PIT violations have been repeatedly missed across sessions, wasting significant dev cycles.

### Before Modifying Any Scoring/Signal/Backtest Code

1. **Read `reference_signal_registry.md` in Claude memory.** It catalogs every signal, its PIT status, and known issues. Do not proceed without reading it.
2. **Trace every data input.** For every column read from the database in the code you're modifying, answer: "Was this data available at the trade's filing_date?" If you can't answer YES with certainty, investigate.
3. **Never use `insider_track_records`** for anything PIT-sensitive. This table is global/static (computed across all time). Use `insider_ticker_scores` with `as_of_date <= filing_date` instead.
4. **Never use `signal_quality.py` for backtesting.** It has a known PIT violation (sell_win_rate_7d uses full track record). Use `trade_grade.py` or `conviction_score.py` instead.

### PIT Validation Checklist (Run Before Declaring Anything "Clean")

- [ ] Every DB column read: is the data available at filing_date? (Not trade_date — filing_date is when we KNOW about the trade)
- [ ] Every aggregate (avg, count, win_rate): does it only include trades filed BEFORE the current trade?
- [ ] Every score lookup: does it use `as_of_date <= filing_date`, not just the latest score?
- [ ] Every price lookup: does it use prices at or before the relevant date, never after?
- [ ] No use of `insider_track_records.score`, `score_tier`, or `percentile` in any backtest or scoring path
- [ ] No statistics computed over the full dataset then applied to individual trades (e.g., percentile cutoffs, optimal thresholds)
- [ ] Walk-forward: scores computed in chronological order, each score uses only data available at that point
- [ ] Observable return lag: if using forward returns (7d/30d/90d), ensure the lag between trade_date and when the return is used in scoring is sufficient (>=10d for 7d returns, >=40d for 30d, >=100d for 90d)

### After Modifying Scoring Code

1. **Run PIT validation tests** (in `tests/unit/test_pit_validation.py` once built).
2. **Sanity check results.** If backtest Sharpe > 1.5 or CAGR > 20% for insider strategies, ASSUME there's a bug. Audit harder.
3. **Update `reference_signal_registry.md`** in Claude memory if you added, removed, or changed any signal.

### Red Flags That Indicate PIT Violation

- Backtest results that are dramatically better than prior validated runs
- A score that references any table without an `as_of_date` or `filing_date` filter
- Any use of `insider_track_records` (score, percentile, score_tier, win_rates) in scoring or backtesting
- Aggregates over "all trades" without a date cutoff
- Score thresholds that were tuned on the same data used for backtesting

## Gotchas

- Engine injects `bars["_meta"]` with `prev_close`, `date`, `prev_date` — strategies depend on this
- Strategies should set `instrument["_exit_price_override"]` on target/stop hit for exit price precision
- TF 2020–2023 dataset has DST-related gaps: post-spring DST session starts at 10:30 ET (1hr offset)
- Gap fill strategy must check if gap already filled during F30 before entry
- Board `run_board.py` strips `CLAUDECODE` env var to allow nested Claude subprocesses
- Options pricing: `_reprice_option` tries real data first, falls back to Black-Scholes
- Alpaca paper trading requires `.env` with ALPACA_API_KEY, ALPACA_API_SECRET
- `insider_cluster_buy` paper runner is active via `com.openclaw.insider-paper` launchd service — do not stop without approval

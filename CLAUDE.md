# Trading Framework

Event-driven backtesting engine + strategy research platform. Strategies go through research → backtest → board review → paper trading.

## Architecture

```
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

## Gotchas

- Engine injects `bars["_meta"]` with `prev_close`, `date`, `prev_date` — strategies depend on this
- Strategies should set `instrument["_exit_price_override"]` on target/stop hit for exit price precision
- TF 2020–2023 dataset has DST-related gaps: post-spring DST session starts at 10:30 ET (1hr offset)
- Gap fill strategy must check if gap already filled during F30 before entry
- Board `run_board.py` strips `CLAUDECODE` env var to allow nested Claude subprocesses
- Options pricing: `_reprice_option` tries real data first, falls back to Black-Scholes
- Alpaca paper trading requires `.env` with ALPACA_API_KEY, ALPACA_API_SECRET
- `insider_cluster_buy` paper runner is active via `com.openclaw.insider-paper` launchd service — do not stop without approval

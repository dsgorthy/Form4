# Trading Framework

A generalized event-driven trading framework. Any trading hypothesis can move from
rough idea → backtest → Board of Personas evaluation → paper trading → live execution
without touching core infrastructure.

---

## Architecture

```
Hypothesis → Strategy (BaseStrategy subclass)
                 ↓
          BacktestEngine
          (event-driven, bar-by-bar)
                 ↓
          BacktestResult
          (trades, equity curve, metrics)
                 ↓
          Board of Personas
          (5 parallel Claude evaluations)
                 ↓
          Verdict: advance to paper | return to research
                 ↓
          PaperBackend (Alpaca paper API)
                 ↓
          LiveBackend (Alpaca live — disabled by default)
```

**Key design principle:** Strategies are plugins. The framework handles data loading,
position sizing, exit simulation, P&L tracking, fee modeling, and evaluation.
A strategy only needs to implement 4 methods.

---

## Quick Start

```bash
# Backtest a strategy
python3 pipelines/run_backtest.py --strategy spy_noon_break --start 2020-01-01 --end 2024-12-31

# Backtest with explicit data directory
python3 pipelines/run_backtest.py --strategy spy_noon_break \
    --spy-data /path/to/raw/bars \
    --start 2024-01-01 --end 2024-12-31

# Gross P&L (no fee deduction)
python3 pipelines/run_backtest.py --strategy spy_noon_break --no-fees

# Run Board of Personas evaluation
python3 pipelines/run_board.py --strategy spy_noon_break

# Parameter sweep across all variants and years
python3 pipelines/run_backtest_sweep.py

# Insider event study pipeline
python3 pipelines/insider_study/run_all.py --start 2020-01-01 --end 2025-12-31
```

---

## Strategy Interface

Every strategy subclasses `BaseStrategy` from `framework/strategy.py`:

```python
class BaseStrategy(ABC):
    def data_requirements(self) -> DataRequirements:
        """Declare which symbols/timeframes/lookback you need."""

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        """Given today's bars up to entry time, return long/short/None signal."""

    def select_instrument(self, signal: Signal, bars, date: str) -> dict:
        """Given a signal, return the instrument to trade (equity or option)."""

    def should_exit(self, position: dict, bars: Dict[str, pd.DataFrame]) -> Optional[str]:
        """Called bar-by-bar after entry. Return exit reason or None to hold."""
```

`bars` is keyed `"SYMBOL_TIMEFRAME"` — e.g. `bars["SPY_1Min"]`, `bars["VIXY_5Min"]`.

---

## Adding a New Strategy

```bash
# Create the scaffold
python3 research/pipeline.py --name my_strategy --description "Short description"
```

This creates:
```
strategies/my_strategy/
├── strategy.py    # Subclass BaseStrategy here
├── config.yaml    # All parameters
└── features.py    # Optional: feature engineering helpers
```

Then implement the 4 methods and backtest:
```bash
python3 pipelines/run_backtest.py --strategy my_strategy
```

---

## Data Setup

Data is stored as Parquet files:
```
data/raw/
├── SPY/          # One .parquet per trading day (1-minute bars)
│   ├── 2024-01-02.parquet
│   └── ...
└── VIXY/
    └── ...

data/options/     # 0DTE options chains (bid/ask/iv per strike)
```

`DataStorage` reads these. The default path is `data/` within the framework root,
but can be overridden:

```python
from framework.data.storage import DataStorage
storage = DataStorage(raw_dir="/path/to/raw", options_dir="/path/to/options")
```

To collect new data, use Alpaca:
```bash
# Collect historical minute bars (edit the date range in the script)
python3 pipelines/collect_historical.py
```

---

## Board of Personas

After a backtest, run the strategy through 5 independent Claude evaluations:

```bash
python3 pipelines/run_board.py --strategy spy_noon_break
```

**The 5 personas:**
| Persona | Focus |
|---------|-------|
| Quant Analyst | Statistical edge (Sharpe, WR, overfitting, walk-forward) |
| Risk Manager | Max drawdown, consecutive losses, tail risk |
| Head Trader | Fill realism, liquidity, slippage assumptions |
| Portfolio Manager | Diversification, correlation, capital efficiency |
| Skeptic | Default reject — edge durability, data mining risk |

**Advance rules:**
- 5 approve OR 4 approve + 1 conditional → advance to paper
- 3 approve + 2 conditional → advance with tracked conditions
- Any 2 non-skeptic rejections → return to research

**Output:** `reports/{strategy}/board_report_{date}.md`

---

## Current Strategy Status

| Strategy | Type | Board Status | Notes |
|----------|------|--------------|-------|
| insider_cluster_buy | Event-driven swing (small/midcap equities) | APPROVED — paper trading | Sharpe 1.18, walk-forward validated, 204 events |
| etf_gap_fill | ETF equity, intraday gap fill | Active research | SPY/QQQ production, 10-ETF expansion |
| spy_gap_fill | SPY equity, intraday gap fill | Active research | Multiple variants under test |

### Archived Strategies

7 strategies archived to `strategies/archive/`. See [`ARCHIVE_INDEX.md`](strategies/archive/ARCHIVE_INDEX.md) for details, lessons learned, and revival conditions.

| Strategy | Archived | Reason | Sharpe (net) |
|----------|----------|--------|-------------|
| spy_0dte_reversal | 2026-03-01 | Fees consume 78% of edge at $30K | 0.50 |
| spy_noon_break | 2026-03-01 | Near-zero edge ($16 net P&L) | 0.28 |
| spy_orb | 2026-03-01 | No edge (41% WR) | -0.18 |
| spy_first30_momentum | 2026-03-01 | Losing system | -0.97 |
| spy_vwap_trend | 2026-03-01 | QQQ result does not transfer to SPY | -0.62 |
| spy_vwap_reclaim | 2026-03-01 | Deprecated; superseded by vwap_trend | N/A |
| spy_pm_continuation | 2026-03-01 | Empty scaffold, never implemented | N/A |

---

## Fee Model

**Options (spy_0dte_reversal):**
- Commission: $0.65/contract per leg (Alpaca rate)
- Slippage: ~1% of premium per side (0DTE ATM spread at 3:29 PM)
- Effective: ~$44.60/trade average at $30K / 3% sizing

**Equity (spy_noon_break, spy_orb):**
- Commission: $0 (Alpaca charges nothing for equity)
- Slippage: 0.01% one-way (SPY bid-ask ≈ $0.01 on ~$500 = 0.002%)
- Effective: ~$0.10-0.30/trade negligible

**Scale effect:** Fees are fixed per contract; doubling account from $30K → $60K approximately halves fee drag as a % of P&L (more contracts per trade, fixed commission per contract).

---

## Project Layout

```
trading-framework/
├── framework/              # Core engine — strategy-agnostic
│   ├── strategy.py         # BaseStrategy ABC, Signal, DataRequirements
│   ├── backtest/
│   │   ├── engine.py       # Event-driven loop, position sizing, exit simulation
│   │   └── result.py       # BacktestResult, TradeRecord, metrics, JSON export
│   ├── data/
│   │   ├── loader.py       # Multi-symbol 1Min→NMin resampling
│   │   ├── storage.py      # Parquet reader (raw bars + options chains)
│   │   ├── alpaca_client.py # Alpaca Data API v2 wrapper
│   │   └── calendar.py     # Trading day calendar + FOMC dates (2020–2026)
│   ├── execution/
│   │   ├── base.py         # ExecutionBackend ABC
│   │   ├── paper.py        # Alpaca paper trading
│   │   └── live.py         # Alpaca live (disabled by default)
│   ├── pricing/
│   │   ├── black_scholes.py
│   │   └── vol_engine.py   # IV estimation from VIXY
│   └── alerts/
│       └── telegram.py     # Trade entry/exit Telegram notifications
│
├── board/                  # Board of Personas evaluation system
│   ├── runner.py           # 5 parallel Claude subprocess calls
│   ├── report.py           # Aggregate verdicts → board_report.md
│   └── personas/           # 5 persona system prompts
│
├── strategies/             # One directory per strategy (self-contained)
│   ├── insider_cluster_buy/# Insider cluster buy (APPROVED, paper trading)
│   ├── etf_gap_fill/       # ETF gap fill (active research)
│   ├── spy_gap_fill/       # SPY gap fill (active research)
│   └── archive/            # Archived strategies (see ARCHIVE_INDEX.md)
│       ├── spy_0dte_reversal/
│       ├── spy_noon_break/
│       ├── spy_orb/
│       ├── spy_first30_momentum/
│       ├── spy_vwap_trend/
│       ├── spy_vwap_reclaim/
│       └── spy_pm_continuation/
│
├── pipelines/
│   ├── run_backtest.py         # Standalone backtest runner
│   ├── run_backtest_sweep.py   # Multi-variant parameter sweep
│   ├── run_board.py            # Board of Personas runner
│   ├── run_paper.py            # Paper trading daemon
│   ├── collect_historical.py   # Alpaca historical data collection
│   └── insider_study/          # Standalone research pipeline
│       ├── run_all.py          # Master: calendar → prices → study
│       ├── build_event_calendar.py  # Parse Form 4s, confidence scoring
│       ├── collect_prices.py   # Download daily bars via Alpaca
│       └── run_event_study.py  # T+1 entry / hold N days / abnormal returns
│
├── reports/                # Generated by run_board.py
│   ├── spy_0dte_reversal/
│   └── spy_noon_break/
│
├── data/                   # Shared data store (raw bars + options)
├── research/               # Strategy scaffolding tools
├── config/framework.yaml   # Framework-level settings
├── .env                    # API credentials (not committed)
└── PROGRESS.md             # Project status, bugs fixed, outstanding issues
```

---

## Environment Setup

```bash
pip install pandas numpy pyyaml pytz requests

# Copy and fill in credentials
cp .env.example .env
# Edit .env: ALPACA_API_KEY, ALPACA_API_SECRET, TELEGRAM_BOT_TOKEN
```

---

## Known Limitations / Outstanding Work

1. **Paper trading validation** — insider_cluster_buy paper runner is active; need 6+ months of execution data before drawing performance conclusions
2. **Walk-forward validation** — etf_gap_fill and spy_gap_fill still need OOS validation before board submission
3. **Real options calibration** — `_reprice_option` falls back to BS when bar-level real data unavailable; verify coverage density if options strategies are revisited

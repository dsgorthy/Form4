# Trading Framework

Event-driven backtesting engine + strategy research platform + Form4.app product. Strategies go through research → backtest → board review → paper trading. The product frontend and API live in this same repo.

## BEFORE BUILDING ANYTHING

**ALWAYS check Claude memory for `reference_product_audit.md`, `reference_project_structure.md`, and `reference_signal_registry.md` before implementing any feature.** These contain the complete inventory of every page, component, API endpoint, shared utility, and scoring signal. Reuse or extend existing code instead of creating new files. Specifically:

1. **Check if a component already exists** — ~65 components in `frontend/src/components/`. Don't create a new table when `trades-table.tsx` or `signals-table.tsx` already exists.
2. **Check if an API endpoint already exists** — 20 routers with 68 endpoints. The portfolio API already supports `?strategy=` param.
3. **Follow existing patterns** — dark theme colors, table structure, gating logic, pagination, ID encoding all have established conventions.
4. **The portfolio overlay already handles idle cash** — `portfolio-overlay.tsx` exists. Extend it, don't replace it.
5. **Keep documentation current** — When adding, removing, or overhauling a feature, update `reference_product_audit.md` in Claude memory. This is a living document, not a snapshot. If you add a new page, component, or API endpoint, document it. If you remove or rename one, remove or update the entry.
6. **All data lives in PostgreSQL on Mac Studio** — Database `form4` runs on Studio (not Mini). Schemas: `public` (insiders, trades, scores), `prices` (daily_prices, option_prices), `research` (derivative_trades, footnotes), `notifications`. Use `from config.database import get_connection` for all DB access. Use `price_utils.get_close(ticker, date)` for single price lookups. Never use `sqlite3` directly — the compat layer in `config/database.py` handles SQL translation automatically. **DB-touching scripts must run on Studio** (`ssh derekg@100.78.9.66` or a launchd service there); Mini has no local `form4` DB.
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
  db.py                         # Postgres shim — re-exports get_connection/get_db from config.database (NOT SQLite)
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
    base.py                 # ExecutionBackend ABC + OrderResult
    backtest_backend.py     # Simulated fills for backtests
    paper.py                # Alpaca paper trading
    live.py                 # Alpaca live trading
    service.py              # ExecutionService (Stage 4) — consumes TradeIntent → broker → Fill (NOT YET WIRED into cw_runner)
  decision/                 # Stage 3 — pure decision engine (NO I/O, NO DB)
    types.py                # CandidateFact, PositionState, StrategyConfig, TradeIntent, ExitIntent
    filters.py              # evaluate_filters (shared between sim + live)
  observability/            # Stage 2.5 — pipeline run telemetry
    pipeline_runner.py      # pipeline_run() context manager — records to pipeline_runs table
  pricing/
    black_scholes.py        # BS option pricing
    vol_engine.py           # IV estimation from VIXY
  risk/
    position_sizer.py       # Kelly, fixed-%, min unit sizing
    filters.py              # Circuit breakers, sector concentration
  signals/indicators.py     # VWAP, RSI, SMA, etc.
  alerts/telegram.py        # Trade entry/exit notifications

strategies/
  cw_strategies/            # LIVE paper trading — runs 3 yaml-configured strategies via cw_runner.py
  insider_catalog/          # Insider data fetch/backfill/scoring (fetch_latest.py, compute_returns.py, pit_scoring.py)
  etf_gap_fill/             # 16 symbols, per-symbol config_*.yaml files
  spy_gap_fill/             # Base gap fill implementation
  archive/                  # Rejected strategies with ARCHIVE.md manifests

pipelines/
  run_backtest.py           # Single-strategy backtest
  run_board.py              # Board of Personas evaluation (5 Claude subprocesses)
  run_backtest_sweep.py     # Parameter sweep
  run_paper.py              # Paper trading daemon
  insider_study/            # 30+ research scripts (EDGAR, event studies, options analysis)

board/
  personas/                 # 5 evaluator prompts (quant, risk, trader, PM, skeptic)

data/raw/{SYMBOL}/          # 26 symbols, 1-min Parquet bars (research-only, not refreshed)
reports/                    # Board reports, backtest results, sweep CSVs
migrations/                  # SQL migrations (applied via psql -f, NOT auto-run)
```

## Position state — single canonical table

All position state lives in **`strategy_portfolio`**. Discriminated by:
- `is_live boolean` — false for non-real-money rows
- `execution_source text` — `backtest | simulated | alert | paper | live`

Readers (e.g. `/portfolio`, `/admin/strategies/{name}/positions`,
`/paper-trading/dashboard`) filter on these two columns. There is no
sim/paper/live split table — the 2026-05-22 Stage 2 refactor created
`sim_portfolio`/`paper_trades`/`live_trades`/`backtest_archive` as
side tables, never reached cutover, and was rolled back 2026-06-07
(migration `2026-06-07_consolidate_to_strategy_portfolio.sql`). The
drift detector (Stage 5) was removed in the same change since there's
no longer anything to compare across tables.

What did survive from the refactor:
- **Stages 0-1**: simulator dup fix + Studio-only launchd guard.
- **Stage 2.5**: ✅ `pipeline_runs` table + `framework.observability.pipeline_run()`
  context manager. New batch jobs wrap their entry point in it. Surfaces at
  `/admin/pipelines`.
- **Stage 3 (scaffold)**: `framework/decision/` has shared `evaluate_filters`
  + dataclass contracts. cw_runner still has inline filter logic. New
  decision logic should land here when extracting shared paths.

When touching cw_runner or simulate_strategy_portfolio, prefer to extract
shared logic into `framework/decision/` rather than reimplementing in place.

## Commands

```bash
# Run all tests
python3 -m pytest tests/unit -v

# Backtest a historical strategy (archived/research)
python3 pipelines/run_backtest.py --strategy spy_gap_fill --capital 50000 --position-pct 5.0

# Gross P&L (no fees)
python3 pipelines/run_backtest.py --strategy spy_orb --no-fees

# Multi-source data (spy-0dte + framework)
python3 pipelines/run_backtest.py --strategy etf_gap_fill --spy-data /path/to/spy-0dte/data/raw

# Board of Personas review
python3 pipelines/run_board.py --strategy spy_gap_fill --backtest-file reports/spy_gap_fill/backtest_latest.json

# Parameter sweep
python3 pipelines/run_backtest_sweep.py

# Insider event study (full pipeline)
python3 pipelines/insider_study/run_all.py --start 2020-01-01 --end 2025-12-31

# EDGAR Form 4 bulk download
python3 pipelines/insider_study/download_sec_bulk.py --start 2024-Q1 --end 2024-Q4 --trade-type buy

# Live paper trading (one of three productized strategies)
python3 strategies/cw_strategies/cw_runner.py --config strategies/cw_strategies/configs/quality_momentum.yaml
```

## Strategy Lifecycle

1. **Research** — Hypothesis, data collection, initial analysis
2. **Backtest** — `run_backtest.py` → metrics in `reports/{strategy}/backtest_latest.json`
3. **Board Review** — `run_board.py` → 5 personas evaluate independently
   - 5 approve → advance
   - 4 approve + 1 conditional → advance
   - 3 approve + 2 conditional → advance with conditions
   - 2+ non-skeptic rejections → return to research
4. **Paper Trading** — launchd plist running `cw_runner.py --config configs/{strategy}.yaml` with dedicated per-strategy Alpaca account
5. **Archive** — Failed strategies go to `strategies/archive/` with ARCHIVE.md manifest

## Active Strategies — exactly 3, alert-only

**Names live in `api/public_fields.STRATEGIES`, which is the single source of
truth for the key → public label mapping.** Do not retype a display name
anywhere else; `tests/unit/test_strategy_registry.py` fails the build if a
surface drifts from the registry or if a retired strategy reappears on a live
one. The internal keys are deliberately unchanged — they are written into
`strategy_portfolio.strategy`, plist labels, yaml filenames and env prefixes,
so renaming a key is a data migration and the label is the only part that has
to move.

| Key | Public name | Status | CAGR | Key Metric |
|-----|-------------|--------|------|------------|
| quality_notrend | **A-List Buys** | LIVE alert-only | 64.3% blended (+45.2 vs SPY), 3x33% | A+/A insider buys, no chart condition. 141 closed sim trades. The strongest book: the trend filter QM applies costs more in trades foregone than it saves — QM fills 2.6 of 10 slots against notrend's 6.4. |
| quality_momentum | **Insider Breakout** | LIVE alert-only | 63.9% blended (+42.7 vs SPY), 5x20%, A+/A/B, -20% stop, 44% max DD | Same insider grade, plus above SMA50 and SMA200. 55 closed sim trades. Kept as the A/B control. |
| reversal_dip | **Insider Dip Buys** | LIVE alert-only | 38.1% blended (+16.4 vs SPY), 4x25%, ON WATCH (verified 2026-08-23) | 10+ consecutive sells then a buy, into a 25%+ 3-month drawdown. Genuinely lumpy — went dark Dec 2025–Feb 2026 and again Jun–Aug 2026, then fired 3 in a month. Sparse alerts are expected, not a fault. |

CAGRs are as of 2026-08-22, after the TRANCHE CORRECTION: the scorer had been
counting execution lots as separate trades, so a purchase filled in five
tranches scored as five successful trades. Fixing it moved 21% of career
grades, almost all downward, and forced both sizing/gate changes above. See
`docs/published_returns_methodology.md` and the memory
`feedback_filing_not_lot_grouping`.

**Two things to carry from that.** Insider Breakout's max drawdown roughly
doubled — the published `max_drawdown` says 31.5% but it samples equity only at
trade exits; a daily blended curve says **49.9%**, and that is what a holder
experiences. It comes from widening the gate to A+/A/B, not from sizing: A+/A
gives ~31% CAGR at 21% DD but leaves the book 85% in cash. And **Insider Dip
Buys was not in the A/B** while its primary filter (`consecutive_sells_before`)
changed materially, so treat its figure as unverified.

An earlier correction landed 2026-08-20 (the stop). Two earlier
corrections landed 2026-08-19, and they
pull in opposite directions:

- **Look-ahead removed** (`filed_at` was Eastern for 2026 rows but read as UTC,
  so 37 of 278 positions entered a session early): **−1.0 to −1.3 CAGR points**,
  $21,017 across the three books.
- **After-bell filings now fill at the NEXT SESSION'S OPEN**, not its close.
  76% of filings arrive after the bell, and this is what a subscriber acting on
  the alert would actually get: **+1.5 to +6.7 points**.

A third correction landed 2026-08-19: `is_largest_ever` was wrong on 23.7% of
flags (a `--since` window was being mistaken for an insider's whole career).
It feeds conviction, which gates entry, so false flags were admitting trades
that had not earned their score. Removing them moved the three books
**+$79,930** and cut six unqualified positions out of Insider Breakout.

**Every position has been audited individually.** On 2026-08-23 all 172
simulated positions across the three books were walked one at a time and each
stored value re-derived from source — 2,040 checks, **zero exceptions, zero PIT
violations**. Entry sessions, fill prices to the cent, hold lengths, all 19 stop
exits, career grades re-scored at filing date, and every signal input replayed
through the same `entry_timing` / `evaluate_filters` the live runner uses. All
three defects found during that audit were in the audit scripts, not the
pipeline. Details in `docs/published_returns_methodology.md`.

**Two known biases, both directional and both small.** Roughly 2% of
qualifying candidates are dropped because the ticker has no usable price
series — NH delisted between the filing and the entry, DRTTF was never
covered — and that always removes the worst outcomes rather than a random
sample. And when a held position's price series ends, the simulator marks it to
the LAST CLOSE IT SAW, never to zero — a delisting exits at its last quote. That
matters more since the stop moved to −50%, because less now sits between a
collapsing position and its time exit.

**THE STOP IS NOT THE SAME ON EVERY BOOK. Never write "the stop" without
naming one.** A-List Buys and Insider Dip Buys carry −50%; Insider Breakout
carries **−20%** as of 2026-08-23. All three are declared in the strategy yaml
and read from there by both surfaces;
`tests/unit/test_stop_is_config_driven.py` fails the build if a module constant
reappears or the two disagree.

- **−50% on A-List / Dip Buys is a backstop and has never fired** — but it
  clears by 2.5 points, not a wide margin: A-List's worst holding closed at
  −47.5% (LRHC), its second worst at −47.1% (COE). Dip Buys' worst is −24.8%.
- **−20% on Breakout is a working stop and has fired 19 times** out of 85
  positions, averaging −22.2% on exit (13 of the 19 gapped through the level).
  It cut the daily drawdown 49.9% → 43.8% *and raised* CAGR.

Until 2026-08-20 `simulate_strategy_portfolio.py` carried
`STOP_LOSS_PCT = -0.30` as a module constant while all three yamls said
`stop_loss_pct: null` — and `cw_runner` reads the yaml, so for three months the
published book simulated a stop the live alerts never applied. Removing it was
worth +5.3 CAGR points on A-List and +3.2 on Breakout with max drawdown
unchanged.

The stop is evaluated on the CLOSE, not the intraday low, and that choice now
carries real weight: re-measured 2026-08-23, four Breakout positions traded
through −20% intraday and closed back above it, and one of them is **PDYN,
+286.9% — the best trade in the book, saved by 0.7 of a point**. A low-based
rule would have cost those four roughly 351 P&L points.

**THE FRAGILE BOOK IS INSIDER BREAKOUT, NOT A-LIST — re-measured 2026-08-23.**

±0.25 perturbation on conviction, 14 draws, current configs:

| | min | median | max | published | below |
|---|---|---|---|---|---|
| A-List | 53.4% | **67.6%** | 80.3% | 63.3% | 3 of 14 |
| Breakout | 33.7% | 49.2% | 67.4% | 64.3% | **13 of 14** |
| Dip Buys | 31.6% | 35.3% | 37.1% | 36.5% | 10 of 14 |

The August finding — A-List published near the top of a wide band — has
INVERTED. A-List's published figure now sits *below* its perturbed median;
Breakout is the 13-of-14 book with a 34-point band. Two consequences: read
Breakout as the fragile one, and **A-List's gate looks mis-set**, since pure
noise improves its median by 4.3 points.

**Annual returns, 2026-08-23** — and Breakout's CAGR is ONE YEAR:

| year | A-List | Breakout | Dip Buys | SPY |
|---|---|---|---|---|
| 2023 | +65.4% | +34.2% | +52.1% | +24.8% |
| 2024 | +41.1% | **+231.2%** | +46.2% | +24.0% |
| 2025 | +69.2% | +18.6% | +26.6% | +16.6% |
| 2026 | +43.2% | +21.8% | **+10.1%** | +12.1% |

That year is close to one position: PDYN was **+489.9% at the 2024 mark**
before giving back to +296% at its January exit — the same position that
survived its stop by 0.7 of a point. One intraday tick → one position → one
year → the headline. A-List is the healthiest here, with no single year
carrying it.

The superseded 2026-08-20 study, kept for the shape of the argument:

Measured 2026-08-20 **under the old −30% stop, and NOT yet re-run under −50%.**
The sleeve figures below are therefore stale in level. The *shape* of the
finding — a wide band driven by the conviction gate — is a property of the gate
and is not expected to change, but do not quote these numbers as current. Re-run
before publishing any band. Perturbing conviction by ±0.25 — less than any
single component of the score — and re-running 14 times gave quality_notrend:

| | CAGR |
|---|---|
| min | 46.2% |
| **median** | **49.7%** |
| max | 55.7% |
| published | **55.4% — 13 of 14 draws fall below it** |

The mechanism is the CONVICTION GATE, not tie-breaking. Tie-break-only noise
(+0..0.004) moved the book **$0** across 14 seeds. But `min_conviction` is 1.5
on A-List and Breakout and **3.0 on Dip Buys** — it is not one global number,
and the simulator and `cw_runner` DEFAULT APART (1.5 vs 5.0), inert only
because all three yamls declare it. `tests/unit/test_conviction_gate_is_config_driven.py`
keeps it that way. The score is built from ~12 half-point components, so any ±0.5 change to
one input pushes a whole band of candidates across the floor. Forcing
`is_largest_ever` on gives 148 trades and $346,851; off gives 126 and $425,851;
correct gives 141 and $495,797 — **non-monotonic**, so it is which trades get
admitted, not how many.

Practical consequences:
- Read the sleeve figure as a band, not a point. The ±5 spread was measured on
  a 55.4% sleeve under the old stop; the spread is the durable part, the centre
  needs re-measuring.
- Any change to any conviction input will move the book by tens of percent.
  That is not a bug being reintroduced; it is this design.
- The gate threshold is doing more work than any individual signal.
  **`min_conviction` was swept 2026-08-23 and deliberately NOT changed:** the
  in-sample optimum fails out of sample on all three books (A-List best-in 1.5
  / best-out 2.0; Breakout 0.0 / 2.0; Dip Buys 0.0 / 1.5). It is not tunable on
  this data. But **2.0 wins out-of-sample on both A-List and Breakout** and
  beats shipped on A-List over the full period too (71.2% vs 63.3%), which
  agrees with the perturbation result that noise improves A-List's median.
  Two independent signals say A-List's 1.5 is too low. Needs a proper
  walk-forward, and it moves live alerts, so it is Derek's call.

Still unvalidated out of sample at this level, and the 6.8% drawdown is
flattered by the book sitting ~41% in cash. Idle cash held in SPY; 1x leverage — the 2x book was built,
measured at 61% average gross exposure against a 200% ceiling, and shelved.

**Retired 2026-08-18:** `tenb51_surprise` (Sharpe 0.68). Runner unloaded, plist
archived to `~/retired_plists/` on Studio, absent from every live surface. The
yaml, the PIT strategy class and its ~200 simulated rows remain so the decision
is reversible — re-add it to `ACTIVE_STRATEGIES` and `STRATEGY_CONFIG` to
resume.

| Research strategies | | | | |
|-----|-------------|--------|------|------------|
| quality_momentum_live | — | LIVE money (pre-launch, plist not installed, $0 deployed) | — | $10k allocation, 5 max concurrent, 10% circuit-breaker |
| quality_momentum_2x | — | Built, not published | — | Levered in name only at current fill rates; `ALLOWED_STRATEGIES` excludes it |
| etf_gap_fill | — | Research | 0.59–0.88 Sharpe | XLC/XLRE/RSP best |
| spy_gap_fill | Research | — | 76.7% fill rate |
| spy_intraday_momentum | Research (untracked, NOT yet board-reviewed) | — | 0DTE SPY ATM call/put on Gao-Han-Li-Zhou intraday-momentum signal; backtest shows implausible compounding — needs sizing review |

**Constraint:** Never run multiple strategies through the same Alpaca config. Each trading strategy reads its own `ALPACA_API_KEY_{prefix}` / `ALPACA_API_SECRET_{prefix}` from `.env`, with the prefix declared in the strategy yaml as `alpaca_env_prefix`. Shared read-only credentials for bar-reading processes live in `ALPACA_DATA_API_KEY` / `ALPACA_DATA_API_SECRET`.

## Data

- **28 symbols** in `data/raw/`: SPY, QQQ, IWM, DIA, VTI, RSP, GLD, TLT, HYG, EFA, VEA, EEM, MDY, USMV, VIXY, XLB/C/E/F/I/K/P/RE/U/V/Y
- **Format**: 1-minute OHLCV Parquet, one file per trading day
- **Date ranges**: Most symbols have 1,500–2,000 trading days (2020–2026)
- **DataStorage** supports `extra_raw_dirs` list for spanning multiple data sources

### Insider Catalog Database — SOURCE OF TRUTH

**PostgreSQL database `form4`** on Mac Studio (`derekg@100.78.9.66`) is the single source of truth for all insider data. Access via `from config.database import get_connection` from code running on Studio. **Never read from CSV exports** or SQLite files — always query PG directly. The old SQLite files (`insiders.db`, `prices.db`, `research.db`) are archived backups. Mini is dev-only; it has no `form4` DB of its own.

**Connection:** `from config.database import get_connection, get_db`
- `get_connection()` for scripts (individual connection)
- `get_db()` for API (pooled, context manager)
- SQL compat layer auto-translates `?` → `%s`, `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`, `datetime('now')` → `NOW()::text`, PRAGMAs → no-op

**Tables:**
| Table | Rows | Description |
|-------|------|-------------|
| `trades` | ~1.65M total | All insider trades (2001–2026); refreshed every 5 min by `insider-fetch` plist |
| `trade_returns` | ~725K | 7d/14d/30d/60d/90d forward returns + SPY benchmark; nightly via `backfill-returns` |
| `prices.option_prices` | ~23.5M | EOD option OHLCV + bid/ask from ThetaData; **DORMANT — see `pipeline_options_backfill.md`** |
| `prices.option_pull_status` | ~314K | Per-event tracking of which events have options data |
| `insiders` | — | Insider identity, CIK, entity flag |
| `insider_ticker_scores` | ~358K | PIT per-insider-per-ticker quality scores; daily 09:30 PT via `refresh-features` |
| `score_history` | ~541K | Score snapshots over time |
| `research.derivative_trades` | ~1.16M | Derivative transaction data (legacy; superseded by `trades.is_derivative=1`) |

**Daily stock prices**: `pipelines/insider_study/data/prices/` — 5,733 tickers, 2016–2026

**Key rules:**
- All pipelines and analysis scripts must load events from PostgreSQL `form4`, not CSVs or SQLite. Use `from config.database import get_connection`.
- Options pull (`options_pull.py --from-db`) reads events from `trades` table, writes results to `option_prices` + `option_pull_status`.
- `theta_cache.db` was a pull-layer cache; the file is currently MISSING on Studio (data has been migrated to `option_prices`).
- ~26% of events will never have options data (OTC stocks, micro-caps without listed options).

### ThetaData Options Pipeline

Historical options EOD pricing for insider event backtesting. **Check `pipeline_options_backfill.md` in Claude memory for current backfill status before doing any options-related work.**

- **ThetaData server**: Java process at `/Users/derekg/thetadata/lib/202602131.jar` on Studio (creds: `/Users/derekg/thetadata/creds.txt`); listens on `127.0.0.1:25503` per `config.toml`. **Currently NOT running** — see `pipeline_options_backfill.md`.
- **Pull script**: `pipelines/insider_study/options_pull.py --from-db` — reads events from DB, writes structured data to `option_prices` table. Variants: `options_pull_longdte.py`, `options_pull_targeted.py`.
- **Monitor**: `pipelines/insider_study/pull_monitor.sh` is referenced in older docs but **the file does not currently exist** on Mini, Studio, or in the repo. Restore or remove the dependency before relaunching the pull.
- **Cache**: `pipelines/insider_study/data/theta_cache.db` was the pull-layer cache; **the file is currently MISSING**. Resume relies entirely on PG `option_pull_status` for per-event dedup, not the cache.
- **Pipeline status**: DORMANT since 2026-04-09. ~6-week freshness gap. Last `MAX(trade_date)=2026-03-27`. **Always check `pipeline_options_backfill.md` in Claude memory** before any options work.

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

## Common Tasks

**"Run a backtest"** — Always clarify: which strategy (name from `strategies/`), capital amount, date range (or "all available"), include fees or `--no-fees`, any parameter overrides. Command: `python3 pipelines/run_backtest.py --strategy NAME --capital N`. Output lands in `reports/{strategy}/`.

**"Add/modify a strategy"** — Requires: which `BaseStrategy` methods to implement (`data_requirements`, `generate_signal`, `select_instrument`, `should_exit`), an existing strategy to copy patterns from (suggest one), data source (1-min bars, daily prices, insider DB). New strategies go in `strategies/`. Read `framework/strategy.py` first.

**"Board review"** — Requires: strategy name and path to backtest JSON. Command: `python3 pipelines/run_board.py --strategy NAME --backtest-file reports/NAME/backtest_latest.json`. 5 personas evaluate independently. Approval rules in Strategy Lifecycle section above.

**"Anything with a score, grade, star or badge"** — `api/ratings.py` is the single definition of every rating this product publishes: Insider Rating (A+/A/B/C/Unrated, from `career_grade`) and Trade Rating (Exceptional/Strong/Notable/Routine/Weak, from the 0-100 trade score). Tags are 1-to-many and never rate. Never add a new scale, never retype a band name or threshold, never render `pit_grade` or `conviction` as a user-facing rating. `frontend/src/lib/ratings.ts` mirrors it; `tests/unit/test_ratings_parity.py` fails the build on drift. Read `reference_rating_taxonomy.md` in Claude memory first.

**"PIT audit / scoring change"** — MANDATORY: read `reference_signal_registry.md` from Claude memory first. Then follow the full PIT Validation Checklist above. Never skip this even if the change seems safe.

**"Insider pipeline work"** — Clarify which step: fetch (`fetch_latest.py`), compute returns (`compute_returns.py`), score (`pit_scoring.py`), or options pull (`options_pull.py --from-db`). Always specify date range. DB source is PostgreSQL `form4` — never CSV or SQLite.

## Gotchas

- Engine injects `bars["_meta"]` with `prev_close`, `date`, `prev_date` — strategies depend on this
- Strategies should set `instrument["_exit_price_override"]` on target/stop hit for exit price precision
- TF 2020–2023 dataset has DST-related gaps: post-spring DST session starts at 10:30 ET (1hr offset)
- Gap fill strategy must check if gap already filled during F30 before entry
- Board `run_board.py` strips `CLAUDECODE` env var to allow nested Claude subprocesses
- Options pricing: `_reprice_option` tries real data first, falls back to Black-Scholes
- Alpaca paper trading requires `.env` with per-strategy trading credentials (`ALPACA_API_KEY_QUALITY_MOMENTUM`, `ALPACA_API_KEY_REVERSAL_DIP`) and shared read-only data credentials (`ALPACA_DATA_API_KEY` / `ALPACA_DATA_API_SECRET`). See `.env` header comment for the convention
- Three runners are live via `com.openclaw.quality-notrend`, `com.openclaw.quality-momentum` and `com.openclaw.reversal-dip` launchd services (all run `cw_runner.py`) — do not stop without approval. `com.openclaw.tenb51-surprise` was unloaded 2026-08-18 and its plist archived to `~/retired_plists/` on Studio
- **Studio-only launch agents — must never autoload on Mini.** Running the same launchd service on both machines against the same Alpaca paper account risks duplicate order submission (`submit_order` in `framework/execution/paper.py` passes no `client_order_id`, so Alpaca has no server-side dedup). The services confined to Studio: `quality-momentum`, `quality-notrend`, `reversal-dip`, `trial-emails`, `backfill-returns`, `breaking-signal`, `ceowatcher-reader`, `daily-content`, `insider-fetch`, `intraday-backfill`, `position-rules-test`, `strategy-health`, `form4-error-tail`, `form4-notifications`, `form4-seed-positions`, `form4-uptime`, `tailorly-tunnel`, `dagster-daemon`, `dagster-webserver` (dataplane orchestration — plists + install script in `dataplane/deploy/`; UI on `100.78.9.66:3030`, tailnet only; port 3000 is held by `pyrrho-staging-frontend` container), `pyrrho-desk` (Pyrrho Dataplane Desk dashboard — `100.78.9.66:3031`, tailnet only; install script `install_pyrrho_desk_service.sh`), `pg-backup` (nightly verified `pg_dump` of form4/pyrrho_data_dev/pyrrho_prod/dagster_runs at 03:15 PT, rsynced off-box to the Mini — `scripts/backup_databases.sh`, plist in `scripts/launchd/`). `~/.local/bin/studio` has a `guard_studio_only_plists` pre-check that fails `studio deploy form4` / `studio deploy pm` if any `com.openclaw.*` plist other than `claude-agent`, `etsy-bot`, `prank-mail-bot` is present on the deploying machine.

# ETF Gap Fill — Satellite Strategy

**Version:** 1.0.0
**Type:** Intraday mean reversion (satellite / diversifier)
**Last updated:** 2026-02-27

---

## Overview

Morning gaps in liquid ETFs tend to mean-revert and fill the prior-day close within the same
trading session. This strategy fades those gaps after a 30-minute confirmation window (F30),
targeting the fill with a leveraged equity position.

The strategy is deliberately narrow — it only trades high-conviction setups — which keeps
drawdowns minimal and correlation to other strategies near zero. It is sized and intended as
a **satellite allocation** within a larger portfolio, not a standalone return driver.

---

## Hypothesis

When a liquid ETF opens above or below its prior close (a gap), institutional arbitrage and
reversion flows tend to push price back toward that anchor by the afternoon session. The
strongest predictors of same-day fill are:

1. **Gap size**: small gaps (0.05–0.40% depending on symbol) fill at 60–85%+ rates
2. **F30 direction**: if the first 30 minutes *fade* the gap, fill probability jumps ~10pp
3. **Pre-fill check**: if price already crossed prev\_close during F30, no entry (avoid chasing)
4. **Vol regime**: VIXY > 80 signals crisis-level dislocation — skip these days entirely

The edge is structural (ETF arbitrage, index rebalancing pressure) rather than alpha-decay-prone
pattern trading. It has been stable across the full 2020–2025 backtest period including COVID,
rate-hike cycle, and 2022 bear market.

---

## Empirical Validation (SPY baseline, 2020–2026, N=420 gap days)

| Gap bucket | All fills | F30 fade | F30 continue |
|---|---|---|---|
| Small (0.05–0.15%) | 79.3% | **93.9%** | 60.5% |
| Medium (0.15–0.30%) | 70.1% | **84.8%** | 51.0% |
| All (0.05–0.30%) | 74.0% | **88.7%** | 55.1% |

Gate threshold for new symbols: ≥60% fill rate on qualifying setups (gap in range + F30 fade).

---

## Validated Symbols

| Symbol | Description | Gap range | Stop | WR% | Sharpe | Trades/yr | Status |
|---|---|---|---|---|---|---|---|
| SPY | S&P 500 | 0.05–0.30% | 0.25% | 84.5% | 0.80 | ~10 | ✅ Production |
| QQQ | Nasdaq 100 | 0.15–0.40% | 0.15% | 62.0% | 0.57 | ~19 | ✅ Production |
| IWM | Russell 2000 | 0.05–0.30% | 0.25% | 66.7% | -0.48 | ~8 | ❌ Rejected |

SPY + QQQ combined portfolio: Sharpe **0.91**, max DD **0.22%**, correlation **+0.019**.

Symbols pending evaluation (data collected, backtest in progress):
DIA, MDY, VTI, XLF, XLE, XLK, XLV, XLI, XLP, XLU, XLY, XLB, TLT, HYG, GLD, EEM, EFA, VNQ

### Why IWM was rejected

IWM had 66.7% WR under baseline SPY parameters — below the ~75.5% break-even threshold
for that stop width. The parameter sweep found no configuration with Sharpe > 0.10 using
F30 fade. The "best" IWM variants (Sharpe 0.62) required removing the F30 filter and relying
on pure R:R arithmetic (32% WR × large reward), which is not the mean-reversion edge — it is
mechanical P:L ratio optimization that is likely to be fragile out-of-sample. IWM excluded.

---

## Entry Logic

- **Entry time:** 10:00 AM (after F30 = first 30 min of session)
- **Gap calculation:** `(open - prev_close) / prev_close × 100`
- **Gap range:** symbol-specific (see table above); outside range = skip
- **F30 direction:** must fade the gap (mandatory signal confirmation)
- **Pre-fill guard:** if price already crossed prev\_close during F30 = skip
- **VIXY filter:** skip if VIXY > 80 (crisis/dislocation days)
- **Direction:** gap-up → short (fade); gap-down → long (fade)

---

## Exit Logic

- **Target:** previous day's close (the gap fill level)
- **Stop:** symbol-specific `stop_pct` adverse move from entry
- **Time stop:** 15:30 ET — hard cut if no fill
- **Max hold:** 3 trading days (position carries overnight if target/stop not hit)
- **Expiry override:** options configs close same day; leveraged configs allow multi-day

---

## Instrument

All production configs use **synthetic 3x leveraged equity**:

```
virtual_price = underlying_price / 3.0
num_units = (capital × position_pct) / virtual_price
P&L = (exit_price - entry_price) × direction × leverage × num_units
```

This replicates SPXL/TQQQ exposure using the underlying ETF's data series.
- Commission: $0 (equity at Alpaca)
- No options bid-ask drag
- Multi-day holds supported (unlike 0DTE options)

---

## Portfolio Properties

| Metric | SPY only | QQQ only | SPY + QQQ |
|---|---|---|---|
| Annual P&L (at 5% pos, $30K) | +$46/yr | +$49/yr | +$94/yr |
| Annual P&L (at 10% pos, $30K) | +$91/yr | +$97/yr | **+$188/yr** |
| Annual return (10% pos, $30K) | 0.30% | 0.32% | **0.63%** |
| Sharpe | 0.80 | 0.57 | **0.91** |
| Max DD | 0.29% | 0.36% | **0.22%** |
| Daily return correlation | — | — | +0.019 |
| Same-day co-occurrence | — | — | 17/6yr (2.8/yr) |

### Why Sharpe *improves* in combination

Correlation of +0.019 (near-zero) means SPY and QQQ gap fill signals are almost fully
independent. Combined daily P&L variance ≈ sum of individual variances, while mean P&L
adds linearly. The result is a better Sharpe than either individual strategy.

### Capital utilization

~29 trades/year × ~1.5 day avg hold = ~44 capital-at-work days out of 252 = **17%**.
The other 83% of the year, capital sits idle in this strategy. This is by design —
the strategy only takes high-conviction setups. The idle capital should be deployed
in complementary strategies.

### Worst-case same-day exposure

At 5% position size × 3x leverage per symbol:
- 1 symbol fires: 15% effective exposure
- 2 symbols fire: 30% effective exposure
- All N symbols fire: 15% × N effective exposure

With 2 validated symbols, max same-day exposure is **30%** — manageable.
P(both fire same day) ≈ 2.8 days/year empirically.

---

## Risk Characteristics

- **Max drawdown (6yr, SPY+QQQ):** $196 combined on $60K = 0.22%
- **Max consecutive losses:** 2 (per individual strategy)
- **Worst regime:** 2022 bear market — strategy still profitable (mean reversion persists)
- **Tail risk:** gaps that widen rather than fill (stop loss handles this, capped at `stop_pct`)
- **Model risk:** synthetic 3x assumes perfect leverage with no tracking error (vs real SPXL)

---

## Adding a New Symbol

1. **Collect data:**
   ```bash
   python3 pipelines/collect_data.py --symbols XYZ --start 2020-01-01 --end 2025-12-31
   ```

2. **Run analysis gate** (≥60% fill rate required):
   ```bash
   python3 pipelines/analyze_gap_fill.py --symbols XYZ
   ```

3. **Parameter sweep** (find best gap range + stop):
   ```bash
   python3 pipelines/sweep_gap_fill.py --symbol XYZ --start 2020-01-01 --end 2025-12-31
   ```

4. **Create config YAML** using the best Sharpe variant from the sweep:
   ```bash
   # Copy config_template.yaml → config_xyz.yaml
   # Set data.primary_symbol, entry.min/max_gap_pct, exit.stop_pct
   ```

5. **Run portfolio backtest** including the new symbol:
   ```bash
   python3 pipelines/etf_gap_fill_runner.py
   ```

6. Update this document's Validated Symbols table.

---

## Acceptance Criteria (Board Thresholds)

For a symbol to be added to the production portfolio:

| Metric | Threshold |
|---|---|
| Analysis gate fill rate | ≥ 60% (small gap + F30 fade + clean) |
| Individual Sharpe | ≥ 0.40 |
| Minimum trades (backtest) | ≥ 15 over test period |
| Max DD per symbol | ≤ 2.0% of capital |
| F30 fade required | Yes (no-fade variants rejected regardless of Sharpe) |

---

## Relationship to spy_gap_fill

`etf_gap_fill` is a namespace extension, not a fork:

- `strategy.py` re-exports `SPYGapFillStrategy` unchanged (5-line thin wrapper)
- All logic changes go in `strategies/spy_gap_fill/strategy.py` and propagate automatically
- The only differences are per-symbol config YAMLs and this documentation
- SPY and QQQ configs also exist in `spy_gap_fill/` for historical reference

---

## Implementation Notes

- **Runner:** `pipelines/etf_gap_fill_runner.py` — discovers configs automatically from this dir
- **Data:** stored in `trading-framework/data/raw/{SYMBOL}/` (one parquet per trading day)
- **VIXY data:** shared across all symbols as the vol filter; already collected 2020–2025
- **Resume-safe:** all collection and backtests are idempotent (skip existing files)
- **Scope of 3x synthetic leverage:** P&L tracks the underlying ETF × 3; does not account for
  daily rebalancing drag of real leveraged ETFs (SPXL, TQQQ) over multi-day holds

# Archive Manifest — spy_orb

**Archived:** 2026-03-01
**Original location:** `strategies/spy_orb/`

---

## Reason for Archival

**Category:** RETURNED_TO_RESEARCH

No edge detected. 41% win rate, Sharpe -0.18, total P&L -$3.99 over 61 trades. Only 12% signal rate (61 signals in 502 trading days) because the confirmed-at-10:30 breakout filter is strict. Not formally board-evaluated due to sparse signals, but backtest results are conclusive: profit factor 0.93 net with no viable parameter path forward at current design.

The strategy validated framework generality (different asset type, different hypothesis from the reference strategy) even though no trading edge was found.

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | 2024-01-01 — 2025-12-31 |
| Capital | $30,000 |
| Total Trades | 61 |
| Win Rate | 41.0% |
| Sharpe Ratio (net) | -0.18 |
| Sharpe Ratio (gross) | 0.17 |
| Total P&L (net) | -$3.99 |
| Max Drawdown | ~$14 |
| Profit Factor | 0.93 (net) / 1.07 (gross) |

---

## Board Verdict

**Date:** N/A — not formally evaluated
**Verdict:** NOT EVALUATED
**Score:** N/A
**Report:** N/A (backtest data: `reports/spy_orb/backtest_latest.json`)

### Key Findings
1. Only 12% signal rate — confirmed-at-10:30 filter is too strict
2. 41% win rate with insufficient payoff asymmetry
3. Opening range breakout on SPY does not carry predictive power for afternoon direction

---

## Lessons Learned

1. **Breakout confirmation filters trade off signal rate for quality — but can overshoot.** The 10:30 confirmation requirement reduced trades to 61/502 days without improving win rate.
2. **Framework validation succeeded even though strategy failed.** spy_orb was the second strategy implemented and confirmed the BaseStrategy plugin architecture works for different instruments and hypotheses.
3. **Opening range on SPY is well-arbitraged.** The opening range breakout is one of the oldest and most widely known patterns — institutional and HFT participants have compressed any edge.

---

## Conditions for Revival

This strategy could be reconsidered if:

1. **Stronger breakout confirmation** added (volume surge, gap alignment, opening range width filter)
2. **Wider entry window or multiple entry attempts** to increase signal rate above 12%
3. **Trend filter** (SPY above/below 20-day SMA) to restrict trades to directionally favorable regimes

**Revival process:** Copy this directory back to `strategies/spy_orb/`, remove ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — BaseStrategy subclass implementing confirmed-at-10:30 opening range breakout
- `config.yaml` — Strategy parameters (range window, breakout threshold, stop/target)
- `features.py` — Feature engineering (opening range calculation, breakout detection)
- Backtest data: `reports/spy_orb/backtest_latest.json`

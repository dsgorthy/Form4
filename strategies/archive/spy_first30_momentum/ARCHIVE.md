# Archive Manifest — spy_first30_momentum

**Archived:** 2026-03-01
**Original location:** `strategies/spy_first30_momentum/`

---

## Reason for Archival

**Category:** RETURNED_TO_RESEARCH

Board returned to research (3.6/10, 4 rejects + 1 conditional). Strategy loses money: negative Sharpe -0.97, negative expectancy -$0.15/trade, profit factor 0.67. The academic signal (Gao, Han, Li & Zhou 2018: first 30-min return predicts last 30-min return) does not survive in the 2024-2025 low-VIX bull regime on SPY. 33.85% win rate is worse than a coin flip for a directional momentum strategy — an outright refutation of the hypothesis as implemented.

The 0.12% stop is almost certainly too tight for SPY's intraday volatility, causing stops to trigger on noise before the directional signal can express.

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | 2024-2025 |
| Capital | $30,000 |
| Total Trades | 65 |
| Win Rate | 33.85% |
| Sharpe Ratio (net) | -0.97 |
| Total P&L (net) | -$9.95 |
| Max Drawdown | 0.04% |
| Profit Factor | 0.674 |
| Max Consecutive Losses | 10 |

---

## Board Verdict

**Date:** 2026-02-26
**Verdict:** RETURN TO RESEARCH
**Score:** 3.6/10
**Report:** `reports/spy_first30_momentum/board_report_2026-02-26.md`

### Consensus Concerns
1. Negative expectancy (-$0.15/trade) — strategy destroys capital on every marginal trade
2. 10 max consecutive losses triggers automatic reject (threshold: 8)
3. 33.85% win rate with only 1.29:1 payoff ratio — structurally insufficient (needs ~44% to break even)
4. 0.12% stop is too tight — noise triggers stops before directional signal expresses
5. Widely published signal (Gao et al. 2018, covered by Quantpedia, AlphaArchitect) — likely arbitraged by institutional systematic traders

---

## Lessons Learned

1. **Academic signals can be regime-dependent.** The first-30-minute momentum effect may only work in high-VIX environments. The 2024-2025 low-VIX bull market is the wrong regime for this signal.
2. **Stop calibration matters more than entry.** The 0.12% stop vs 0.25% target looks reasonable on paper (1:2 risk-reward) but the stop gets hit far too frequently — the 30-min intraday noise exceeds the stop tolerance.
3. **Widely published signals face decay.** Gao et al. (2018) has been covered extensively in practitioner literature. Any persistent intraday momentum signal in SPY has been arbitraged by HFT and systematic desks.

---

## Conditions for Revival

This strategy could be reconsidered if:

1. **Regime filter** restricting trades to VIX > 20 environments (where momentum persistence is theoretically stronger)
2. **Wider stop-loss** (0.20%+) to avoid noise-triggered exits
3. **Extended backtest** across high-VIX periods (2018, 2020, 2022) to test if the signal is regime-conditional

**Revival process:** Copy this directory back to `strategies/spy_first30_momentum/`, remove ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — BaseStrategy subclass implementing first-30-minute momentum entry at 3:30 PM
- `config.yaml` — Strategy parameters (signal threshold, stop/target %, VIX bounds)
- Board report: `reports/spy_first30_momentum/board_report_2026-02-26.md`

# Archive Manifest — spy_vwap_trend

**Archived:** 2026-03-01
**Original location:** `strategies/spy_vwap_trend/`

---

## Reason for Archival

**Category:** RETURNED_TO_RESEARCH

Board returned to research (4.0/10, 4 rejects + 1 conditional). Strategy loses money: negative Sharpe -0.62, negative expectancy -$0.22/trade, profit factor 0.77. The academic citation (Zarattini & Aziz 2023: Sharpe 2.1 on QQQ, $25K to $192K) does not replicate on SPY. The VWAP deviation threshold-based entry at a fixed time fails to capture any tradeable edge on the world's most liquid equity instrument.

Massive divergence between cited results (Sharpe 2.1 on QQQ) and actual performance (Sharpe -0.62 on SPY) suggests the original paper's edge either doesn't transfer across instruments, is overfitted to QQQ's momentum characteristics, or relied on a specific bull market regime.

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | 2024-2025 |
| Capital | $30,000 |
| Total Trades | 47 |
| Win Rate | 36.17% |
| Sharpe Ratio (net) | -0.62 |
| Total P&L (net) | -$10.29 |
| Max Drawdown | 0.06% |
| Profit Factor | 0.765 |
| Max Consecutive Losses | 8 |

---

## Board Verdict

**Date:** 2026-02-26
**Verdict:** RETURN TO RESEARCH
**Score:** 4.0/10
**Report:** `reports/spy_vwap_trend/board_report_2026-02-26.md`

### Consensus Concerns
1. Negative expectancy (-$0.22/trade) — strategy destroys capital systematically
2. Academic QQQ results (Sharpe 2.1) do not replicate on SPY (Sharpe -0.62) — possible instrument-specific overfitting
3. 36% win rate with only 1.35:1 payoff ratio — needs ~43% to break even
4. 7 free parameters with no walk-forward validation
5. VWAP is the most widely known institutional benchmark — simple threshold entries are consumed by faster participants

---

## Lessons Learned

1. **Academic results on one instrument don't transfer automatically.** QQQ and SPY have different microstructure dynamics. Always validate on the target instrument before investing development time.
2. **Beware of high-Sharpe academic claims.** Zarattini & Aziz's Sharpe 2.1 on QQQ either overfitted to a specific regime or relied on parameters that don't generalize. Replication on different instruments/periods is essential.
3. **Seven free parameters with N=47 trades is a recipe for curve-fitting.** Reduce parameters through principled feature selection before drawing conclusions about edge.

---

## Conditions for Revival

This strategy could be reconsidered if:

1. **Tested on QQQ** (the original academic instrument) with extended data covering multiple regimes
2. **Parameters reduced** through principled feature selection (currently 7 free parameters)
3. **Walk-forward validation** across multiple regimes confirms positive OOS expectancy

**Revival process:** Copy this directory back to `strategies/spy_vwap_trend/`, remove ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — BaseStrategy subclass implementing VWAP deviation trend entry at 10:30 AM
- `config.yaml` — Strategy parameters (VWAP deviation threshold, sustained bars, stop/target %, VIXY bounds)
- Board report: `reports/spy_vwap_trend/board_report_2026-02-26.md`

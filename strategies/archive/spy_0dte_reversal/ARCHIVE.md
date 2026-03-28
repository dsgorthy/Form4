# Archive Manifest — spy_0dte_reversal

**Archived:** 2026-03-01
**Original location:** `strategies/spy_0dte_reversal/`

---

## Reason for Archival

**Category:** RETURNED_TO_RESEARCH

Board returned to research (3.6/10, 3 rejects + 2 conditional). Fees ($11,056/year) consume 78% of gross edge ($14,217 gross P&L), leaving net Sharpe of 0.50 — below the institutional 1.0 minimum. The strategy has genuine edge at the gross level (58.9% WR, ~2.0 Sharpe gross) but is untradeable at $30K capital due to options fee structure. Max drawdown (11.86%) exceeds total return (10.54%), yielding a return-to-DD ratio of 0.89.

The 0DTE EOD mean-reversion concept has theoretical grounding in dealer gamma hedging and MOC order flow, but the trade has become increasingly crowded since CBOE launched daily SPX expirations in 2022.

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | 2024-01-01 — 2025-12-31 |
| Capital | $30,000 |
| Total Trades | 241 |
| Win Rate | 58.9% |
| Sharpe Ratio (net) | 0.50 |
| Sharpe Ratio (gross) | ~2.0 |
| Total P&L (net) | +$3,161 |
| Max Drawdown | 11.86% |
| Profit Factor | 1.085 |
| Fee Drag | 78% of gross edge |

---

## Board Verdict

**Date:** 2026-02-26
**Verdict:** RETURN TO RESEARCH
**Score:** 3.6/10
**Report:** `reports/spy_0dte_reversal/board_report_2026-02-26.md`

### Consensus Concerns
1. Fees consume 78% of gross edge — strategy is a fee-transfer mechanism to the broker at $30K
2. Sharpe 0.50 net is below institutional minimums; return-to-DD ratio < 1.0
3. No walk-forward validation despite 6+ tunable parameters — overfitting risk
4. 0DTE options in final 26 minutes carry extreme gamma/spread risk not captured in backtest
5. $13.12/trade expectancy — a single tick of adverse slippage eliminates the entire edge

---

## Lessons Learned

1. **Fee-to-edge ratio is the viability gatekeeper for options strategies.** Real edge exists (Sharpe ~2.0 gross) but fees at small account sizes can render it untradeable. Always model net-of-fees before investing development time.
2. **0DTE options carry tail risk that backtests understate.** End-of-day liquidity evaporation, bid-ask spread widening, and stop-gap risk in the 3:29-3:55 PM window are difficult to capture historically.
3. **Crowding matters.** The 0DTE EOD reversal has become one of the most popular retail and institutional trades since 2022. Edge persistence should be evaluated against competition dynamics.

---

## Conditions for Revival

This strategy could be reconsidered if:

1. **Account capital reaches $150K+** — fee drag drops below 20% of gross edge as position sizing scales with capital but per-contract fees remain fixed
2. **Broker commissions negotiated below $0.30/contract** (vs current $0.65 Alpaca rate)
3. **Walk-forward validation confirms OOS Sharpe > 1.0 gross** across multiple regimes

**Revival process:** Copy this directory back to `strategies/spy_0dte_reversal/`, remove ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — BaseStrategy subclass implementing 0DTE EOD mean-reversion entry/exit logic
- `config.yaml` — Strategy parameters (entry time, stop/target %, VIX bounds, strike offset)
- `features.py` — Feature engineering helpers (intraday move calculation, VIX regime detection)
- Board report: `reports/spy_0dte_reversal/board_report_2026-02-26.md`
- Backtest data: `reports/spy_0dte_reversal/backtest_latest.json`

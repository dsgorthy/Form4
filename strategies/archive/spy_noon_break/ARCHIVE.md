# Archive Manifest — spy_noon_break

**Archived:** 2026-03-01
**Original location:** `strategies/spy_noon_break/`

---

## Reason for Archival

**Category:** RETURNED_TO_RESEARCH

Board returned to research (3.8/10, 4 rejects + 1 conditional). Maximum net P&L of $16.19 across 237 trades — economically indistinguishable from zero. 10-variant parameter sweep confirmed the result is not parameter-sensitive: no variant produces meaningful P&L (range: -$6 to +$2). The edge simply does not exist at $30K equity scale where minimum unit is 1 SPY share.

The noon range breakout hypothesis (U-shaped volume, institutional re-engagement after lunch) has theoretical grounding but SPY is too efficiently priced for a simple breakout signal to generate alpha against algorithmic and institutional participants.

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | 2024-01-01 — 2024-12-31 |
| Capital | $30,000 |
| Total Trades | 237 |
| Win Rate | 44.3% |
| Sharpe Ratio (net) | 0.28 |
| Total P&L (net) | +$16.19 |
| Max Drawdown | 0.10% |
| Profit Factor | 1.095 |
| Max Consecutive Losses | 9 |

### Parameter Sweep Summary (best/worst of 10 variants)

| Variant | Trades | WR% | Net P&L |
|---------|--------|-----|---------|
| brk0.10 (best) | 70 | 44.3% | +$2 |
| stp0.25 (worst) | 56 | 37.5% | -$6 |

---

## Board Verdict

**Date:** 2026-02-26
**Verdict:** RETURN TO RESEARCH
**Score:** 3.8/10
**Report:** `reports/spy_noon_break/board_report_2026-02-26.md`

### Consensus Concerns
1. $16.19 total P&L on $30K is statistically indistinguishable from zero — no demonstrated edge
2. $0.07/trade expectancy — a single tick of slippage flips this negative
3. 9 max consecutive losses triggers automatic reject threshold (>8)
4. 10+ tunable parameters with no walk-forward validation on negligible edge = overfitting risk
5. SPY is the most efficiently priced instrument on Earth — simple breakout has no structural persistence

---

## Lessons Learned

1. **Scale problem is real.** At $30K with ~$500 SPY, minimum unit is 1 share. Tiny per-share P&L becomes economically meaningless. Strategy needs $150K+ or options leverage to generate meaningful returns.
2. **Parameter sweep insensitivity confirms null result.** When 10 variants all produce near-zero P&L, the signal genuinely doesn't exist — it's not a calibration problem.
3. **Efficiently priced instruments are the hardest to find edge on.** SPY intraday patterns are monitored by thousands of algorithms. Simple range breakout signals are consumed by faster participants.

---

## Conditions for Revival

This strategy could be reconsidered if:

1. **Account capital reaches $150K+** enabling 5-10 share positions where per-share P&L becomes meaningful
2. **Options overlay added** to amplify the small equity edge (e.g., ATM calls for noon breakout direction)
3. **Volume confirmation filter** or order flow data added to distinguish real breakouts from noise

**Revival process:** Copy this directory back to `strategies/spy_noon_break/`, remove ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — BaseStrategy subclass implementing noon range breakout logic
- `config.yaml` — Base parameters (breakout threshold, stop/target %, VWAP filter)
- `config_1dte_options.yaml` — Experimental options overlay variant
- `features.py` — Feature engineering (range calculation, VWAP alignment, relative volume)
- Board report: `reports/spy_noon_break/board_report_2026-02-26.md`

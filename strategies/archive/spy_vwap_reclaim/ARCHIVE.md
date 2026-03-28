# Archive Manifest — spy_vwap_reclaim

**Archived:** 2026-03-01
**Original location:** `strategies/spy_vwap_reclaim_deprecated/`

---

## Reason for Archival

**Category:** DEPRECATED

Original prototype for VWAP-based trading on SPY. Superseded by `spy_vwap_trend`, which refined the entry logic from VWAP reclaim (bounce) to VWAP deviation (trend). Neither strategy produced positive edge on SPY. The `_deprecated` suffix was the original ad-hoc archival mechanism; this formal archive replaces it.

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | Unknown |
| Capital | $30,000 |
| Total Trades | Unknown |
| Win Rate | Unknown |
| Sharpe Ratio (net) | Unknown |
| Total P&L (net) | Unknown |

No formal board evaluation or documented backtest results. Backtest data may exist at `reports/spy_vwap_reclaim/backtest_latest.json`.

---

## Board Verdict

**Date:** N/A — not evaluated
**Verdict:** NOT EVALUATED
**Score:** N/A
**Report:** N/A

---

## Lessons Learned

1. **VWAP reclaim/deviation signals on SPY do not generate sufficient alpha.** The most liquid instrument is the hardest to find edge on — this was confirmed by both the reclaim (this strategy) and deviation (spy_vwap_trend) variants.
2. **Iterate, but know when to stop.** The reclaim → trend evolution was a reasonable research progression, but both failing confirms the VWAP thesis on SPY is likely exhausted.

---

## Conditions for Revival

**Unlikely.** The concept was already iterated into `spy_vwap_trend`, which also failed (Sharpe -0.62, board returned to research). VWAP-based strategies on SPY appear to have no tradeable edge.

**Revival process:** Copy this directory back to `strategies/spy_vwap_reclaim/`, remove ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — BaseStrategy subclass implementing VWAP reclaim bounce logic
- `config.yaml` — Strategy parameters

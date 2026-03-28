# Archive Manifest — {strategy_name}

**Archived:** {YYYY-MM-DD}
**Original location:** `strategies/{original_dir_name}/`

---

## Reason for Archival

**Category:** {REJECTED | RETURNED_TO_RESEARCH | ABANDONED | DEPRECATED}

{1-3 paragraphs explaining why this strategy was archived. Include the specific
board verdict, the core failure mode, and any relevant context.}

---

## Key Metrics at Time of Archival

| Metric | Value |
|--------|-------|
| Backtest Period | {start} — {end} |
| Capital | ${amount} |
| Total Trades | {N} |
| Win Rate | {X.X%} |
| Sharpe Ratio (net) | {X.XX} |
| Sharpe Ratio (gross) | {X.XX} |
| Total P&L (net) | ${amount} |
| Max Drawdown | {X.X%} |
| Profit Factor | {X.XX} |
| Fee Drag | {X%} of gross edge |

---

## Board Verdict

**Date:** {YYYY-MM-DD}
**Verdict:** {RETURN TO RESEARCH | REJECT | NOT EVALUATED}
**Score:** {X.X}/10
**Report:** `reports/{strategy_name}/board_report_{date}.md`

### Consensus Concerns
1. {concern 1}
2. {concern 2}
3. {concern 3}

---

## Lessons Learned

1. {lesson 1}
2. {lesson 2}
3. {lesson 3}

---

## Conditions for Revival

This strategy could be reconsidered if:

1. {condition 1}
2. {condition 2}
3. {condition 3}

**Revival process:** Copy this directory back to `strategies/{name}/`, remove
ARCHIVE.md, update the archive index, and re-run the Board of Personas evaluation.

---

## Files Preserved

- `strategy.py` — {brief description}
- `config.yaml` — {brief description}
- Board report: `reports/{name}/board_report_{date}.md`
- Backtest data: `reports/{name}/backtest_latest.json`

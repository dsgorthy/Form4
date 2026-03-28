# Strategy Specification Template

## Strategy Overview
**Name:** [strategy_slug]
**Version:** 1.0.0
**Date:** [YYYY-MM-DD]
**Author:** [author]

## Hypothesis
**Core Thesis:** [One sentence description of the edge being exploited]

**Why does this edge exist?**
[2-3 paragraphs explaining the structural or behavioral reason this inefficiency exists and why it should persist]

**Why hasn't it been arbitraged away?**
[Explanation of barriers to entry, capacity constraints, or behavioral persistence]

## Strategy Logic

### Entry Conditions
- **Entry Time:** [HH:MM ET]
- **Direction Rule:** [How direction is determined]
- **Required Conditions:** [List of conditions that must all be true]
- **Optional Conditions:** [Conditions that improve but don't require a trade]

### Exit Conditions
- **Take Profit:** [Condition or level]
- **Stop Loss:** [Condition or level]
- **Time Stop:** [Maximum hold time or time]
- **Other Exits:** [Any other exit triggers]

### Instrument Selection
- **Asset Type:** [options | equity | futures]
- **Symbol(s):** [Primary symbol and any secondary symbols used]
- **Selection Method:** [How the specific instrument is chosen]

## Data Requirements
- **Primary Symbol:** [e.g. SPY]
- **Secondary Symbols:** [e.g. VIXY]
- **Timeframes:** [e.g. 1-minute, 5-minute]
- **Options Data:** [yes/no — if yes, describe what's needed]
- **Lookback Period:** [e.g. 30 days of history]

## Parameters

| Parameter | Default | Range | Notes |
|-----------|---------|-------|-------|
| [param_1] | [value] | [min-max] | [description] |
| [param_2] | [value] | [min-max] | [description] |

## Risk Management
- **Position Size:** [% of capital per trade]
- **Max Daily Loss:** [% of capital]
- **Stop Loss:** [% of position or $ amount]
- **Max Positions:** [number of simultaneous open positions]

## Known Risks
1. [Risk 1 and mitigation]
2. [Risk 2 and mitigation]
3. [Risk 3 and mitigation]

## Validation Plan
- **Backtest Period:** [e.g. 2024-01-01 to 2025-12-31]
- **Walk-Forward Test:** [yes/no — describe if yes]
- **Out-of-Sample Period:** [reserved test period]
- **Paper Trading Period:** [planned duration]
- **Success Criteria:** [Sharpe >, WR >, max DD <, etc.]

## Research Notes
[Any additional notes, references, related strategies, or background research]

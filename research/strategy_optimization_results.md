# Strategy Optimization Results — Post-2021 Regime

**Date:** 2026-03-27
**Data:** 41K+ insider buys (trade_returns), 430 portfolio trades, post-2021

---

## Key Findings Summary

### 1. HOLD PERIOD: Shorter is better. 7 days is optimal.
- **7d return is the highest across ALL quality filters** — alpha peaks early and decays
- PIT WR>=70%: **7d +2.10%** vs 30d +0.80% vs 90d +0.21%
- C-Suite + PIT WR>=60%: **7d +2.33%** vs 30d +1.50%
- Beyond 30d, returns go negative for most filters. 180d and 365d are deeply negative.
- **Abnormal returns confirm this**: 7d alpha is +1.66% (PIT WR>=70%), 30d drops to +0.28%, 90d is -2.05%
- **Recommendation: Switch from 30-day to 7-14 day holds.** The alpha is front-loaded.

### 2. STOP LOSS: -10% is the sweet spot.
- At -15% stop: only 10.2% of trades stopped, avg survivors +1.29%
- At -10% stop: 29.1% stopped, avg survivors +4.43%
- At -8% stop: 34.4% stopped, avg survivors +5.27%
- **Trades that hit -15% drawdown: 0 of 44 recovered.** The -15% stop is too loose.
- At -10%, we stop 29% of trades but the survivors average +4.43% — much better than the +1.29% at -15%
- **Recommendation: Tighten stop to -10%.** Cuts losers faster, survivors perform much better.

### 3. TARGET GAIN: +5% to +8% targets beat time exits.
- For PIT WR>=70% trades:
  - +5% target: 76.6% hit rate, overall avg +1.13% (vs +0.80% from 30d hold)
  - +10% target: 53.1% hit rate, overall avg +1.06%
  - +8% target: 57.8% hit rate, overall avg +0.69%
- Targets lock in profits before they reverse. The current 30d time exit lets winners decay.
- **Recommendation: Implement +5% target gain OR trailing stop from +5% peak.** Don't let winners sit for 30 days.

### 4. INSIDER ROLE: CFO and VP beat CEO. Chairman is terrible.
- **CFO**: +2.65% at 30d, +1.62% abnormal, 52.5% WR (best role)
- **VP**: +2.43% at 30d, +1.59% abnormal, 59.3% WR (highest WR!)
- **President**: +1.40%, +0.83% abnormal
- **CEO**: +0.41%, -0.13% abnormal (barely positive)
- **Chairman**: **-3.09%**, -3.67% abnormal, 41.7% WR (TERRIBLE — do not trade)
- **Director**: +0.25%, -0.43% abnormal (noise)
- **Recommendation: Weight CFO/VP/President higher. Penalize or exclude Chairman and Director-only.**

### 5. TRADE VALUE: $25K-$100K is the sweet spot. Large trades are worse.
- **<$25K**: +1.88% but 27.7% WR (volatile, unreliable)
- **$25K-$100K**: +0.96%, +0.22% abnormal, 47.9% WR (best risk-adjusted)
- **$100K-$500K**: +0.04%, -0.67% abnormal (noise)
- **$500K-$1M**: +0.13%, -0.61% abnormal (noise)
- **$1M-$5M**: +0.20%, -0.40% abnormal
- **$5M+**: **-1.26%**, -2.08% abnormal, 44.3% WR (WORST bucket)
- Large trades ($5M+) are the worst performers. This contradicts the intuition that "bigger = more conviction."
- Likely explanation: $5M+ trades are often 10b5-1 plan executions, not opportunistic buys.
- **Recommendation: Penalize $5M+ trades. Best signal is $25K-$500K range.**

### 6. SELL SIGNAL / SHORTING: Marginal, not worth the complexity.
- All sells post-2021: 30d abnormal **-1.08%** (stocks go down after insider sells)
- Non-routine sells: -1.20% abnormal
- Sells > $10M: -1.36% abnormal, 51.1% short WR
- The signal exists but is weak — 51% WR means very thin edge, and shorting costs (borrow, squeeze risk) would eat it.
- **Recommendation: Do NOT short.** The sell signal alpha is ~1% and doesn't justify the complexity and risk. Focus on optimizing the buy side.

### 7. SCALING IN: Do NOT average down. Dips don't recover.
- Trades down -5% to -2% at 7d: 30d avg return **-4.99%**, 28.8% WR
- Trades down -10% to -5% at 7d: 30d avg **-7.37%**, 26.1% WR
- Trades down -15% to -10% at 7d: 30d avg **-15.21%**, 10.5% WR
- **Early losers continue to lose.** There is ZERO mean reversion in insider buys that start poorly.
- Conversely, trades up +2% to +5% at 7d: 30d avg +1.44%, 64.6% WR
- **Recommendation: Do NOT scale into losers. Consider scaling into winners (add at +2-3%).**

### 8. RARE REVERSALS: The single best signal.
- Post-2021: **+2.30% at 30d, +1.58% abnormal, 54.2% WR**
- Also strong at 90d: +5.62% return, +2.12% abnormal
- This is the ONLY filter that shows positive and growing returns at longer horizons.
- **Recommendation: Rare reversals should get maximum position size (10%) and possibly longer hold.**

### 9. OPTIMAL COMBINED FILTER (post-2021):
Best combinations ranked by 30d abnormal return:

| Filter | N | 30d Return | 30d Abnormal | WR |
|--------|---|-----------|-------------|-----|
| **Rare reversal** | 1,826 | +2.30% | **+1.58%** | 54.2% |
| **C-Suite + PIT WR>=60%** | 2,240 | +1.50% | **+1.11%** | 53.1% |
| **PIT WR>=70%** | 4,380 | +0.80% | +0.28% | 50.3% |
| Non-routine + PIT WR>=60% | 7,966 | +0.15% | -0.45% | 48.1% |

---

## Recommended New Strategy Configuration

### Entry Criteria (tighter)
- Min PIT WR: 60% (was implicit via quality score)
- Min PIT trade count: 3 (unchanged)
- Exclude: 10% owners, Chairman role, "no PIT history" insiders
- Bonus: Rare reversals, CFO/VP/President, holdings change >= 10%
- Penalize: $5M+ trade value, Director-only role

### Position Sizing (variable, same as now but informed)
- Rare reversal: 10% (max)
- C-Suite + PIT WR>=60%: 8%
- PIT WR>=70%: 7%
- Base (PIT WR>=60%, n>=3): 5%

### Exit Rules (major change)
- **Hold period: 14 days** (down from 30) — alpha is front-loaded
- **Hard stop: -10%** (down from -15%) — losers don't recover
- **Target gain: +8%** — exit when hit, don't let it decay
- **Trailing stop: 5% from peak** (down from 10%) — tighter trail given shorter hold
- No scaling into losers. Optional: add at +3% if within first 7 days.

### What NOT to do
- Do not short sell signals (marginal alpha, high complexity)
- Do not average down on losers (they don't recover)
- Do not trade Chairman buys (-3% avg return)
- Do not hold beyond 30 days (returns go negative at 90d+)

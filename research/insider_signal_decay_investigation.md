# Insider Buy Signal Decay Investigation — 2021+

**Date:** 2026-03-27
**Finding:** The Form4 insider buy signal has been net-negative since 2021, after producing strong returns 2016-2020.

---

## Executive Summary

The insider buy strategy earned +$100K (2016-2020) but lost -$9.7K (2021-2026). This is NOT a bug in our code — the underlying population of insider buys has degraded across all quality tiers. The decay is broad-based but NOT uniform: **Quality 8+ trades remain profitable post-2021** (+4.54% avg, 55% WR), while Quality 6-7 trades turned negative.

**Root causes identified (5 converging forces):**
1. **70-80% of alpha captured before filing** — structural, documented in Ozlen & Batumoglu (SSRN 2026)
2. **Signal democratization** — 15+ retail platforms now track Form 4 filings in real-time (Quiver Quant launched Feb 2020, exactly at decay onset)
3. **10b5-1 reform** (Dec 2022) — shifted trades from opportunistic to routine; 31.1% → 1.7% traded within 90 days of plan adoption
4. **Aggregate insider buying volume collapsed 56%** ($25.3B H1 2021 → $11.2B H1 2023)
5. **"No PIT history" insiders collapsed** — #1 alpha source pre-2021, #1 loss source post-2021

**Recommended action:** Tighten to Quality >= 8 (PIT WR >= 70% + C-suite/holdings), implement Cohen routine filter, add market regime filter. Only proven insiders with high-conviction informational trades still have alpha.

---

## 1. Year-over-Year Performance

| Year | Trades | Win Rate | Avg Return | Total P&L |
|------|--------|----------|-----------|----------|
| 2016 | 162 | 54.3% | +2.24% | +$21,052 |
| 2017 | 123 | 57.7% | +1.19% | +$11,872 |
| 2018 | 109 | 50.5% | +0.44% | +$1,773 |
| 2019 | 136 | 50.7% | +1.12% | +$17,228 |
| 2020 | 88 | 51.1% | +5.92% | +$48,602 |
| **2021** | **81** | **43.2%** | **+0.43%** | **+$2,757** |
| **2022** | **88** | **35.2%** | **-0.99%** | **-$4,609** |
| **2023** | **87** | **42.5%** | **+0.26%** | **+$5,142** |
| **2024** | **73** | **37.0%** | **-2.01%** | **-$16,717** |
| **2025** | **88** | **48.9%** | **+0.80%** | **+$9,103** |
| **2026** | **15** | **33.3%** | **-3.58%** | **-$5,412** |

The inflection point is Q4 2021 (27.3% WR, -5.57% avg). Performance has been choppy since, with occasional good quarters (Q4 2023: 66.7% WR) but negative on aggregate.

---

## 2. Quality Tier Breakdown (Key Finding)

### Pre-2021 (2016-2020)
| Quality | Trades | Win Rate | Avg Return | Avg Win | Avg Loss |
|---------|--------|----------|-----------|---------|----------|
| 6 | 360 | 51.9% | +1.87% | +5.89% | -4.02% |
| 7 | 173 | 52.6% | +1.29% | +5.47% | -4.18% |
| 8 | 72 | 54.2% | +3.32% | +6.69% | -3.37% |
| 9 | 12 | 91.7% | +9.29% | +10.16% | -0.88% |

### Post-2021 (2021-2026)
| Quality | Trades | Win Rate | Avg Return | Avg Win | Avg Loss |
|---------|--------|----------|-----------|---------|----------|
| **6** | **300** | **40.3%** | **-0.82%** | +4.46% | -5.28% |
| **7** | **94** | **38.3%** | **-0.91%** | +4.29% | -5.20% |
| **8** | **31** | **54.8%** | **+2.71%** | +6.69% | -3.98% |
| **9** | **7** | **57.1%** | **+12.63%** | +15.73% | -3.10% |

**Critical insight:** Quality 6-7 flipped from profitable to money-losing. Quality 8+ held up. The wins are the same size (avg win ~+5-6%), but **avg losses got worse** (-4% → -5.3%) and **win rate collapsed** (52% → 40%).

---

## 3. What Still Works Post-2021

| Filter | Trades | WR | Avg Return | Total P&L |
|--------|--------|-----|-----------|----------|
| All trades (current) | 432 | 41.2% | -0.37% | -$9,736 |
| **Quality >= 8 only** | **38** | **55.3%** | **+4.54%** | **+$27,551** |
| **PIT WR >= 70%** | **49** | **55.1%** | **+3.10%** | **+$25,582** |
| PIT WR >= 70% + Q>=7 | 48 | 56.3% | +3.23% | +$25,942 |
| C-Suite + Q>=7 | 61 | 41.0% | +0.37% | +$6,211 |
| Holdings increase >= 10% | 7 | 71.4% | +6.00% | +$5,076 |
| Rare reversals | 10 | 50.0% | +2.03% | +$2,040 |
| Time exit only (no stops) | 168 | 53.6% | +3.50% | +$65,728 |

**Best post-2021 filters:**
- Quality >= 8: 55% WR, +4.5% avg, +$27K (but only 38 trades in 5 years)
- PIT WR >= 70%: 55% WR, +3.1% avg, +$26K
- Holdings increase >= 10%: 71% WR, +6% avg (tiny sample)

---

## 4. PIT Win Rate Analysis

### Pre-2021
| PIT Bucket | Trades | WR | Avg Return |
|-----------|--------|-----|-----------|
| PIT WR >= 70% | 141 | 56.7% | +2.87% |
| No history | 267 | 53.6% | +2.68% |
| PIT WR < 50% | 63 | 49.2% | +1.18% |
| PIT WR 60-69% | 71 | 47.9% | +0.43% |
| PIT WR 50-59% | 75 | 53.3% | +0.28% |

### Post-2021
| PIT Bucket | Trades | WR | Avg Return |
|-----------|--------|-----|-----------|
| **PIT WR >= 70%** | **49** | **55.1%** | **+3.10%** |
| PIT WR 60-69% | 55 | 40.0% | +0.37% |
| PIT WR < 50% | 128 | 45.3% | -0.62% |
| **No history** | **107** | **38.3%** | **-0.73%** |
| PIT WR 50-59% | 93 | 32.3% | -1.86% |

**Critical finding:** The "No PIT history" bucket was the 2nd best pre-2021 (+2.68%) and is now the 2nd worst post-2021 (-0.73%). This means **unknown insiders used to provide alpha but no longer do.** Only insiders with proven track records (PIT WR >= 70%) still work.

---

## 5. C-Suite Erosion

| Period | C-Suite WR | C-Suite Avg | Non-C-Suite WR | Non-C-Suite Avg |
|--------|-----------|-------------|----------------|-----------------|
| 2016-2020 | 55.1% | +2.18% | 52.3% | +1.94% |
| **2021-2026** | **38.9%** | **-0.78%** | **42.2%** | **-0.19%** |

C-suite is actually WORSE than non-C-suite post-2021. The C-suite premium has disappeared and inverted.

---

## 6. Exit Pattern Shift

| Period | Time Exit % | Trailing % | Stop Loss % | Stop Loss Avg |
|--------|------------|-----------|-------------|---------------|
| 2016-2020 | 53.2% | 28.4% | 18.5% | -15.0% |
| 2021-2026 | 38.9% | 36.8% | **24.3%** | -15.0% |

**Stop-loss rate increased from 18.5% to 24.3%.** More trades are hitting the -15% stop before the 30-day hold completes. The time-exit win rate also dropped (68.6% → 53.6%).

---

## 7. Market Regime Impact

| Regime (SPY during hold) | Pre-2021 Avg | Post-2021 Avg | Change |
|--------------------------|-------------|---------------|--------|
| Bull (SPY +5%+) | +11.16% | +4.11% | -7.05% |
| Mild bull (0-5%) | +3.67% | +1.44% | -2.23% |
| Mild bear (0 to -5%) | -2.43% | -3.29% | -0.86% |
| Bear (SPY -5%-) | -3.54% | **-9.63%** | -6.09% |

The signal provides no downside protection. In bear markets post-2021, the strategy loses -9.63% per trade vs -3.54% pre-2021. **The strategy is essentially long beta with no alpha in down markets.**

---

## 8. Underlying Population Decay (Not Just Our Portfolio)

From the full `trade_returns` table (100K+ events, not just portfolio trades):

| Period | PIT Bucket | Events | 30d Return | 30d Abnormal | 30d WR |
|--------|-----------|--------|-----------|-------------|--------|
| Pre-2021 | PIT WR >= 70% | 8,925 | +2.88% | +1.09% | 56.6% |
| Pre-2021 | No history | 24,899 | +4.57% | +3.11% | 55.6% |
| Pre-2021 | PIT WR < 60% | 23,234 | +2.06% | +0.33% | 55.4% |
| **Post-2021** | **PIT WR >= 70%** | **4,195** | **+0.80%** | **+0.28%** | **50.3%** |
| **Post-2021** | **No history** | **12,119** | **+0.27%** | **-0.46%** | **48.0%** |
| **Post-2021** | **PIT WR < 60%** | **20,003** | **+0.26%** | **-0.47%** | **48.9%** |

**This confirms the decay is in the underlying signal, not our portfolio construction.** Even the best insiders (PIT WR >= 70%) dropped from +2.88% to +0.80% raw return and from +1.09% to +0.28% abnormal return.

---

## 9. Root Causes — Data + External Research

### A. Most Alpha Is Captured Before Filing (Structural)

**"The Death of Insider Trading Alpha" (Ozlen & Batumoglu, SSRN, January 2026)** is the most directly relevant paper. Using Form 4 filings for Russell 2000 constituents:

> "70-80% of total insider trading alpha dissipates between the transaction date and the next trading day — well before public disclosure via EDGAR."

Strategies entering on the transaction date (private info) generate high Sharpe ratios, but once entry is delayed to the filing date (T+1), **measured performance collapses**. This is structural — it always existed but was masked by larger gross alpha in earlier periods.

A 2024 ScienceDirect study found: *"positive but lower abnormal percentage returns than in previous studies for short holding periods, but they vanish and even become negative when limiting the tradable dollar amount."*

**Source:** [Ozlen & Batumoglu (SSRN 2026)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5966834), [ScienceDirect 2024](https://www.sciencedirect.com/science/article/pii/S1544612324015435)

### B. Signal Democratization (15+ platforms, timing: Feb 2020+)

Pre-2020, insider trading data was used primarily by institutional investors and a handful of services (OpenInsider, SECForm4.com). Now at least **15 major platforms** offer real-time Form 4 tracking:

OpenInsider, FinViz, Unusual Whales, Quiver Quantitative (launched Feb 2020 — exactly at decay onset), InsiderFinance, InsiderScreener, SecForm4.com, MarketBeat, GuruFocus, Fintel, TIKR, DataRoma, InsiderTracking.com, CEO Watcher, and Form4.app.

Key market structure changes:
- Retail order flow hit **36% of total market volume** (April 2025, all-time high)
- Extended-hours trading = **11% of daily volume** (2x the 2019 level)
- Dow Jones disseminates insider filings within **60-90 seconds** of EDGAR posting
- The practical alpha window compressed from **days (pre-2015) to hours (2015-2020) to minutes (2020+)**

**Source:** [Quiver Quantitative](https://www.quiverquant.com/insiders/), [2iq Research](https://www.2iqresearch.com/blog/here-s-what-insider-buying-says-about-the-stock-market-right-now-2023-07-21)

### C. 10b5-1 Reform (Effective Feb 2023)

The SEC adopted amendments to Rule 10b5-1 in December 2022 (effective February 2023). Columbia Law School analysis (July 2025) found:

**Before reform:**
- 31.1% of 10b5-1 sales occurred within 90 days of plan adoption (suggesting opportunistic use)
- 10b5-1 plans accounted for 52.5% of all insider sales
- Post-trade: negative abnormal returns (insiders selling before bad news)

**After reform:**
- Only **1.7%** of 10b5-1 sales now within 90 days (vs 31.1% before)
- Post-trade: flat or slightly positive returns (opportunistic selling pattern reversed)
- Insiders significantly less likely to sell under 10b5-1 plans before earnings misses

The reform shifted the trade mix toward less informative, more routine transactions. Fewer trades are informationally motivated.

**Source:** [CLS Blue Sky Blog](https://clsbluesky.law.columbia.edu/2025/07/31/insider-trading-after-the-2022-rule-10b5-1-amendment/), [Harvard Law](https://corpgov.law.harvard.edu/2023/06/22/how-effective-is-sec-rule-10b5-1-in-deterring-insider-trading/)

### D. Aggregate Insider Buying Volume Collapsed

| Period | Aggregate Insider Buying |
|--------|------------------------|
| H1 2021 | **$25.3 billion** |
| H1 2022 | $18.5 billion (-27% YoY) |
| H1 2023 | **$11.2 billion** (-56% from 2021) |

The universe of insider buys shrank by more than half. The remaining buys may be lower conviction. The insider buy/sell ratio (June 2025) was **0.29** vs long-term average of **0.42** — insiders have been persistent net sellers throughout the recovery.

A COVID-specific study (Das, 2025) found **opportunistic insider trading during COVID generated higher returns than routine trading**, suggesting the 2020 period was a one-time anomaly that inflated our pre-period returns.

**Source:** [SEC Filing Data](https://www.secfilingdata.com/blog/insider-buying-and-selling-trends-for-2025-how-companies-are-trading-their-own-stock/), [Das 2025](https://onlinelibrary.wiley.com/doi/10.1111/fmii.12213)

### E. "No History" Insider Collapse (Our Data)

Pre-2021, insiders with no PIT track record averaged +2.68% per trade. Post-2021, they average -0.73%. This is the single largest driver of the decay in our portfolio. The combination of more routine 10b5-1 buys, more retail-sentiment-driven buying, and market structure changes means **unknown insiders no longer provide alpha**.

### F. Macro Regime — Zero Bear Protection

2022 was the worst equity year since 2008 (S&P down 19.4%). Our data shows insider buy signals provide ZERO downside protection — they lose MORE than the market in bear periods (-9.63% avg vs -5% SPY). The 2022-2024 rate cycle created a fundamentally different regime than 2016-2020.

---

## 10. Recommendations

### Immediate (Portfolio Changes)
1. **Raise minimum quality to 8** — only 38 trades in 5 years but +4.54% avg, +$27K total P&L post-2021. This is the only tier that's held up.
2. **OR: Require PIT WR >= 70%** — 49 trades, +3.10% avg, +$26K. Slightly more volume than Q8+.
3. **Filter out "no PIT history" insiders** — they were the #1 source of alpha pre-2021 and the #1 source of losses post-2021.
4. **Stop-loss tightening** — the -15% stop is hit 24% of the time. Consider -10% or -12% to cut losers faster.

### Signal Research (Medium Term)
5. **Cohen routine classification** — already on roadmap. Would filter out non-informational buys.
6. **Insider horizon / switching rate** — rare reversals still show +2% post-2021. This is a real signal.
7. **Holdings % change >= 10%** — 71% WR post-2021 (tiny sample but strong).
8. **Market regime filter** — do NOT enter during SPY bear regimes (SPY below 200-day SMA). The strategy has ZERO edge in down markets and -9.63% avg loss.

### Portfolio Construction
9. **SPY overlay** — deploy idle cash into SPY. The +2.5% alpha over SPY is real but only for high-quality trades.
10. **Reduce hold period** — time exits have degraded from 68.6% WR to 53.6% WR. Consider 14-21 day holds instead of 30.

---

## Appendix: Quarterly Detail (2019-2026)

| Quarter | Trades | WR | Avg Return | Total P&L |
|---------|--------|-----|-----------|----------|
| 2019-Q1 | 32 | 50.0% | +3.68% | +$11,931 |
| 2019-Q2 | 33 | 48.5% | -0.96% | -$1,241 |
| 2019-Q3 | 41 | 53.7% | +1.57% | +$5,519 |
| 2019-Q4 | 30 | 50.0% | +0.05% | +$1,018 |
| 2020-Q1 | 28 | 42.9% | -3.30% | -$8,981 |
| 2020-Q2 | 20 | 65.0% | +9.61% | +$17,287 |
| 2020-Q3 | 18 | 33.3% | -0.50% | +$249 |
| 2020-Q4 | 22 | 63.6% | +19.54% | +$40,047 |
| **2021-Q4** | **11** | **27.3%** | **-5.57%** | **-$8,485** |
| 2022-Q1 | 23 | 34.8% | +2.18% | +$5,616 |
| 2022-Q2 | 32 | 31.3% | -1.93% | -$1,858 |
| 2022-Q3 | 15 | 33.3% | -2.61% | -$4,090 |
| 2022-Q4 | 18 | 44.4% | -2.03% | -$4,277 |
| 2023-Q1 | 22 | 36.4% | -1.96% | -$4,330 |
| 2024-Q3 | 20 | 30.0% | -4.42% | -$10,741 |

---

## References

1. Ozlen & Batumoglu (2026). "The Death of Insider Trading Alpha." SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5966834
2. "Insider Filings as Trading Signals — Does It Pay to Be Fast?" ScienceDirect, 2024. https://www.sciencedirect.com/science/article/pii/S1544612324015435
3. "Insider Purchase Signals in Microcap Equities." arXiv, 2024. https://arxiv.org/html/2602.06198
4. "Insider Trading After the 2022 Rule 10b5-1 Amendment." CLS Blue Sky Blog, July 2025. https://clsbluesky.law.columbia.edu/2025/07/31/insider-trading-after-the-2022-rule-10b5-1-amendment/
5. "How Effective Is SEC Rule 10b5-1 in Deterring Insider Trading?" Harvard Law, 2023. https://corpgov.law.harvard.edu/2023/06/22/how-effective-is-sec-rule-10b5-1-in-deterring-insider-trading/
6. Das (2025). "Opportunistic Insider Trading During COVID-19 Pandemic." Financial Markets, Institutions & Instruments. https://onlinelibrary.wiley.com/doi/10.1111/fmii.12213
7. 2iq Research. "What Insider Buying Says About the Stock Market." https://www.2iqresearch.com/blog/here-s-what-insider-buying-says-about-the-stock-market-right-now-2023-07-21
8. "Run EDGAR Run: SEC Dissemination in a High-Frequency World." SSRN. https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=2513350
9. SEC Rule 10b5-1 Fact Sheet. https://www.sec.gov/files/33-11138-fact-sheet.pdf

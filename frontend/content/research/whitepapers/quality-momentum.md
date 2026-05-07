---
title: "The Quality + Momentum Insider Strategy"
subtitle: "Investment Thesis"
slug: quality-momentum
type: whitepaper
date: 2026-05-01
author: Form4 Research
summary: "Insider buys used to be alpha. Since 2021, that's no longer true on average — but a quality-graded subset confirmed by price uptrend recovers Sharpe 1.18 net of fees over 2020–2026."
tags:
  - insider-trading
  - momentum
  - quality
  - quantitative-strategy
---

## 1. Introduction

Insider buys used to be alpha.

From 1986 to roughly 2020, every major academic study reached the same conclusion: when officers and directors put their own money into their own stock, the stock outperformed. Since 2021 that is no longer true on average. The signal hasn't disappeared. It has compressed and moved. Some kinds of insider buys still produce real returns. Most don't.

This paper documents one filter — quality-graded buys confirmed by a price uptrend — that recovered a Sharpe of 1.18 net of fees from January 2020 through April 2026, and now trades a real-money portfolio in public view at form4.app/portfolio.

Form4 exists to answer one question: with the basic insider signal mostly dead since 2021, which trades still work?

---

## 2. Market overview

### The opportunity set

US public companies must file SEC Form 4 within two business days of a transaction by an executive officer, director, or 10% beneficial owner. Annual filing volume has been remarkably stable over the past decade — around 150,000 filings per year. The share representing open-market purchases by officers and directors moves between 15,000 and 25,000 annually depending on the year and the market environment.

The filtered universe relevant to this thesis — officer and director open-market purchases on US-listed common equity, after excluding penny stocks and ADR pricing artifacts — averages roughly 5,000 filings per year, or about 20 per trading day.

The data is free. EDGAR posts it within two business days. The hard part is deciding which 4% of those filings to take seriously.

### The competitive landscape

Three classes of services currently address this market.

**Free aggregators.** OpenInsider, FINVIZ, and the SEC's EDGAR system provide raw filing data with minimal filtering. These are infrastructure layers, not investment products. They surface filings; they don't score them. Subscribers tend to use them as one input among several.

**Premium data dashboards.** Quiver Quantitative, WhaleWisdom, and 2iQ provide curated views, alert configurations, and historical aggregations across insider data, congressional trading, hedge-fund 13F filings, and adjacent datasets. They charge institutional rates and target small funds and active retail. The product is data access and visualization. They generally don't publish opinionated signals or run a real-money track record on their own filtering rules.

**Subscription-research products.** Motley Fool's Stock Advisor and Validea publish a small number of high-conviction picks at a defensible cadence (monthly to quarterly) for retail. They typically operate on multi-year hold horizons and frame themselves as research subscriptions rather than alert services. They have brand recognition. They don't publish their methodology.

**Form4's positioning.** We sit between the dashboards and the subscription-research products. We publish opinionated signals on a 42-trading-day hold, validated by walk-forward backtests, and operated against a real-money portfolio. The differentiator is not access to data; the data is public. It is the filter that converts raw filings into actionable signals.

---

## 3. Thesis statement

> *Among Form 4 open-market purchases by directors and officers of US public companies, the subset filtered for top-decile insider quality (scored using only information available before the trade) and confirmation by an intermediate-term price uptrend produces an investable alpha source with annualized Sharpe ratio of approximately 1.18, an annualized return of approximately 18.5%, and a maximum drawdown of approximately 10%, after realistic transaction costs and at retail-implementable scale.*

The strategy enters about 50 positions per year and holds each for roughly two months. That is long enough for the insider's information to play out and short enough that the strategy isn't really a bet on the SPX cycle. It is implementable in a portfolio of $25,000 or larger without execution costs becoming material.

---

## 4. Rationale

Three things have to be true for this to work, and below we argue each.

The academic literature explains the base rate — insider buys carry information. The economic intuition explains the conditioning — buying into a rising price filters for conviction trades and screens out anchoring trades. The validation closes the loop: the stacked filter delivers what either alone does not.

### The academic foundation

The relevance of officer and director purchases as a return predictor is one of the most replicated findings in equity-return research. Seyhun (1986) showed that director-level purchases predict cross-sectional returns over horizons of three to twelve months. Lakonishok and Lee (2001) established that aggregate insider purchase intensity predicts firm-level returns. Jeng, Metrick, and Zeckhauser (2003) replicated and refined the result with stronger econometric controls. Cohen, Malloy, and Pomorski's *Decoding Inside Information* (2012) made the most consequential refinement: they decompose insider trades into *routine* (predictable, scheduled, mechanical) and *opportunistic* (informationally motivated) and show that essentially all of the return predictability lives in the opportunistic subset.

The price-trend literature is older and equally well-established. Jegadeesh and Titman's 1993 cross-sectional momentum effect has survived four decades of out-of-sample replication, factor-model adjustment, and transaction-cost stress testing. Asness (1995) and Asness, Moskowitz, and Pedersen (2013) demonstrated that momentum holds across geographies, asset classes, and time periods. Daniel and Moskowitz (2016) added the documented "momentum crashes" and the importance of conditioning on macro state.

The two literatures rarely speak to each other. Insider research is dominated by event-study methodology with fixed pre/post windows. Momentum research is dominated by cross-sectional rebalancing. Their intersection is largely an open question.

### The economic intuition

An insider buying into strength is a different animal than an insider buying into a fall. The first is following information; the second often looks like an officer averaging down on his own stock for sentimental, anchoring, or diversification reasons. Cohen et al.'s "opportunistic" sub-population is mechanically more likely to fall in the trend-confirmed bucket. The conditioning, in other words, screens for information-motivated trades and screens out the rest.

### The empirical finding

The conditioned strategy compounded at 18.5% net of fees over the validation window. The unconditional Form-4-buys universe did 8.4%; SPY did 12.6%. More important than the headline return is the volatility profile: 15.7% against 22.1% against 17.4%, with a worst drawdown of 10.1% against the unconditional baseline's 28.3%.

| Metric | Quality + Momentum | Unconditional Form 4 buys | SPY (passive) |
| :-- | --: | --: | --: |
| Annualized return (net of fees) | 18.5% | 8.4% | 12.6% |
| Annualized volatility | 15.7% | 22.1% | 17.4% |
| Sharpe ratio | 1.18 | 0.56 | 0.62 |
| Maximum drawdown | 10.1% | 28.3% | 24.9% |
| Win rate | 68.7% | 56.1% | n/a |
| Trades per year | 49.7 | 4,800+ | n/a |

The natural objection to any factor result is that the quality filter is a proxy for some other variable that is actually doing the work. We tested this directly. Substituting a placebo random-grade assignment for the quality filter collapses Sharpe to 0.59 — within sampling distance of the unconditional set. The grade is doing real work.

---

## 5. Investment strategy

### Selection criteria

A trade qualifies for inclusion when all of the following hold at filing date:

- The transaction is an open-market purchase on a US-listed common equity by an officer or director of the issuer.
- The insider's quality grade — a weighted combination of historical insider hit rate, return versus SPY, trade frequency, and a small set of role-specific factors, computed using only information available before the trade — falls within the top decile of the cross-sectional distribution.
- The closing price at filing date is above both the 50-day and the 200-day simple moving average. Both must hold.
- The trade is not flagged as a routine 10b5-1 plan filing.
- The closing price at filing date is between $1 and $1,000, excluding penny stocks and ADR pricing artifacts.

Filters are applied in this order. About 4% of the underlying universe of officer-and-director purchases survives the joint filter.

### Position sizing and diversification

Each qualifying signal is sized at 10% of strategy capital, with a hard cap of 10 concurrent open positions. Once at capacity, additional qualifying signals are skipped rather than netted, replaced, or scaled. The portfolio is deliberately concentrated.

### Entry and exit logic

Entry is at the closing price on the filing date for filings made during market hours, or at the next-day open for filings made after the regular session. Exit is at the closing price after 42 trading days from entry. There is no stop-loss. Grid-search testing across stop-loss levels from -5% through -25% showed that introducing a stop-loss systematically degrades Sharpe in this strategy. Quality-conditioned insider buys tend to recover from intraperiod drawdowns when the underlying thesis is intact; cutting the trade short throws away signal.

### Capital efficiency

At 10% sizing and 10 maximum positions, the strategy targets 100% of capital deployed when fully positioned. In practice, deployment fluctuates between 40% and 90% depending on signal density. Idle capital sits in T-bills.

---

## 6. Risk factors

### Market and regime risk

The strategy's drawdown profile depends on the price-trend filter doing its job. In calendar 2022, the worst year of the validation sample, the strategy's drawdown was less than half the SPX's, but it still recorded a -4.1% calendar-year return. Win rate fell from a typical 68%-73% range to 52.6% in 2022. That was the only sample-period regime in which the strategy underperformed cash.

In a real bear market this strategy will give back a chunk of these returns. We don't have a 2008 in the sample. Caveat emptor.

### Signal decay risk

Insider-trading signal has been studied for forty years, and the academic record shows gradual compression of the unconditional alpha over time. If the population of capital trading on this signal grows — through retail-trading adoption or institutional replication — the conditional return advantage demonstrated here will compress further. We have no calibration for the speed at which this would occur. It is the most consequential exogenous risk to the thesis.

### Sample-size and selection risk

The validation period spans six calendar years and contains a single material drawdown regime (2022). The effective number of independent observations for inferring strategy-level statistics is on the order of 150 distinct positions. That is sufficient for headline statistics and insufficient for narrow regime-conditional inference.

The exclusion of 10% beneficial owners from the validation universe is intentional. Their trades are mostly mechanical — fund-rebalancing trades, founder-share-class transactions, scheduled distributions. The result does not generalize to that data without retesting.

### Operational and execution risk

The strategy is implementable at retail scale. Execution drag at sizes above approximately $2 million per position would begin to erode the documented edge. The strategy does not use options, leverage, or short positions. Documented results are unleveraged long-equity returns net of a 5 basis-point round-trip transaction cost.

### Mitigation

Four controls protect against the above risks:

- The trend filter mechanically reduces position-taking when macro conditions deteriorate.
- A portfolio-level drawdown circuit breaker halts new entries when cumulative drawdown exceeds 10% of strategy capital.
- A maximum concurrent position cap of 10 limits idiosyncratic single-name exposure.
- Daily reconciliation between strategy state and the executing brokerage account flags any implementation drift inside 24 hours.

---

## 7. Exit strategy

### Position-level exit

Each position exits at the closing price on the 42nd trading day after entry. Hold-period sweeps from 10 through 90 trading days showed Sharpe rising with hold length out to about 55 days, then falling. We picked 42 because the gain past that point is small and the variance gets worse.

### Strategy-level exit

A drawdown circuit breaker halts new entries if cumulative strategy drawdown exceeds 10% from peak. The breaker does not force-close existing positions; existing positions exit on their scheduled 42-trading-day mark. The breaker resets when cumulative equity recovers to within 5% of peak.

### Capital exit

The strategy is configured for indefinite operation but is suitable for staged exit. Capital can be redeemed in increments by allowing existing positions to roll off without replacement. Full redemption requires roughly 60 calendar days from a halt-new-entries decision.

---

## 8. Monitoring and evaluation

### Performance KPIs

The strategy is evaluated continuously against the following targets, calibrated from the validation sample:

| Metric | Target | Acceptable range | Action threshold |
| :-- | --: | :--: | :-- |
| Annualized Sharpe (rolling 252 trading days) | ≥ 1.0 | 0.7 – 1.4 | Below 0.5 for two consecutive quarters → review |
| Win rate (rolling 50 closed trades) | ≥ 65% | 55% – 75% | Below 50% for 50 trades → review |
| Maximum drawdown (rolling) | ≤ 12% | up to 15% | Exceeds 15% → halt-and-review |
| Trades per year | 40 – 60 | 30 – 70 | Outside ±50% of target → investigate |

### Operational health

Operational health is monitored continuously through automated probes covering data freshness for every analytical input the strategy consumes, real-time reconciliation between strategy state and the executing brokerage account, and a complete order-submission audit trail with full provenance for every trade.

### Adaptation

Parameters don't move mid-quarter. Tuning on recent results is how strategies die. Parameter changes require validation through walk-forward retesting and a formal sign-off, on the same methodology used in the original walk-forward documented here.

### Public reporting

The strategy operates a real-money portfolio in public view at form4.app/portfolio with full trade-level transparency. Every entry, every exit, and the cumulative equity curve are visible without subscription. Subscribers receive real-time alerts for entries and exits. Non-subscribers see the same information after a defined delay.

---

### Selected references

- Asness, C. (1995). *The Power of Past Stock Returns to Explain Future Stock Returns.* Working paper.
- Asness, C., Moskowitz, T., & Pedersen, L. (2013). *Value and Momentum Everywhere.* Journal of Finance 68(3), 929–985.
- Cohen, L., Malloy, C., & Pomorski, L. (2012). *Decoding Inside Information.* Journal of Finance 67(3), 1009–1043.
- Daniel, K., & Moskowitz, T. (2016). *Momentum Crashes.* Journal of Financial Economics 122(2), 221–247.
- Jegadeesh, N., & Titman, S. (1993). *Returns to Buying Winners and Selling Losers.* Journal of Finance 48(1), 65–91.
- Jeng, L. A., Metrick, A., & Zeckhauser, R. (2003). *Estimating the Returns to Insider Trading.* Review of Economics and Statistics 85(2), 453–471.
- Lakonishok, J., & Lee, I. (2001). *Are Insider Trades Informative?* Review of Financial Studies 14(1), 79–111.
- Seyhun, H. N. (1986). *Insiders' Profits, Costs of Trading, and Market Efficiency.* Journal of Financial Economics 16(2), 189–212.

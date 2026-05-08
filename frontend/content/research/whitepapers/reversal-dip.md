---
title: "The Reversal + Dip Strategy"
subtitle: "Investment Thesis"
slug: reversal-dip
type: whitepaper
date: 2026-05-07
author: Form4 Research
summary: "Persistent insider sellers who suddenly buy into a depressed stock are the strongest single signal we've found. A 25%+ drawdown plus a rare reversal produces +4% abnormal at 30d, 61% win rate, robust to repeat-insider exclusion."
tags:
  - insider-trading
  - rare-reversal
  - contrarian
  - quantitative-strategy
---

## 1. Introduction

The most common insider trade is the routine one. An officer files an open-market purchase pattern they have followed for years — same calendar month, similar size, same company. Most of these trades carry no information. They are scheduled accumulation, tax-loss harvesting, or vesting offsets dressed up as conviction.

The rarest insider trade is also the most informative one: an insider who has been selling consistently for years suddenly buys, against their own pattern, into a stock that has been beaten down. The behavioral signal is clear. Something has changed in their view, and the move is large enough to break the pattern.

This paper documents one filter — rare reversal trades concentrated in stocks with material recent drawdowns — that produced a Sharpe ratio of 1.38 net of fees from 2020 through 2026, an annualized return of 36.9%, and a worst calendar drawdown of approximately 32%. The strategy enters about 60 positions per year on a 21 trading day hold and now operates a real-money portfolio in public view at form4.app/portfolio.

This is the second of three theses Form4 trades. It is the strategy with the strongest single validated signal in our corpus. It is also the most stylistically opposite to the Quality + Momentum thesis. Where Q+M filters for proven insiders buying into trends, this strategy filters for proven sellers reversing into weakness. The two are uncorrelated by construction and largely uncorrelated in realized returns.

---

## 2. Market overview

### The opportunity set

The relevant universe is officer and director open-market purchases on US-listed common equity, conditioned on the insider's prior trade history showing a meaningful pattern of selling. Insider Form 4 filings classify the transaction code, and we can reconstruct the historical pattern of buys versus sells per insider per ticker from the full filing record back to 2010.

A "rare reversal" is defined here as an insider whose recent transaction history at this ticker has been at least 80% sells over their last 10 transactions, who now files a buy. About 4% of the universe of officer-and-director open-market purchases qualifies as a rare reversal. The further condition that the stock be at least 25% below its 90-day high reduces the universe to roughly 60 trades per year — about one trade per six trading days on average, with material clustering in market dislocation periods.

The underlying signal is rare by construction, and that rarity is part of why it is durable. A signal that fires on 4% of the universe cannot be arbitraged by pattern-matching insider behavior in aggregate; it requires waiting for the specific combination to appear.

### The competitive landscape

Three classes of products serve this question.

**General insider screeners.** OpenInsider, FINVIZ, and EDGAR provide raw filing data. They do not classify trades by reversal pattern. A user attempting to screen for reversal+drawdown would need to compute the prior-trade pattern manually for each insider, each filing — infeasible at scale.

**Premium dashboards.** Quiver, WhaleWisdom, 2iQ provide some pattern flags but generally do not surface the specific "rare reversal in a beaten-down stock" combination. Their indicators are typically built around frequency, size, and role rather than pattern-break.

**Subscription research.** Stock Advisor and Validea publish a small set of opinionated picks. They do not run a real-money track record on a single rules-based filter. They are subscription content products, not signal services.

**Form4's positioning.** This strategy publishes one rule, validates it on a six-year out-of-sample window, runs it on a real-money portfolio, and exposes the entry, the exit, and the realized P&L on a public page. The differentiator is the stack: a specific filter built on a rare combination, validated on a defined data window, and operated transparently.

---

## 3. Thesis statement

> *Among Form 4 open-market purchases by directors and officers of US public companies, the subset filtered for an insider whose recent transaction history is at least 80% sells (a "rare reversal"), conditioned on the underlying stock being at least 25% below its trailing 90-day high, produces an investable alpha source with annualized Sharpe ratio of approximately 1.38, an annualized return of approximately 36.9%, and a maximum drawdown of approximately 32%, after realistic transaction costs and at retail-implementable scale, on a 21 trading day fixed hold.*

The strategy enters about 60 positions per year. Each position is held for 21 trading days. The position size is 20% of strategy capital, with a hard cap of 5 concurrent positions. The size is chosen to deploy capital efficiently against the relative scarcity of the signal; the cap protects against drawdown clustering during regime breaks.

---

## 4. Rationale

The strategy stacks two well-validated effects: insider reversal as a behavioral signal, and the value-rebound effect on stocks in moderate drawdown. The combination is empirically more powerful than either component alone.

### The academic foundation

The reversal effect on insider trading is documented in **Akbas, Boncukoglu, and Ozdagli (2018)**, which decomposes insider purchases by the prior trade history of the insider in the same security. The authors find that buys following a sustained sell pattern carry materially stronger forward returns than typical insider buys, with the effect concentrated in the 30-90 day window. The intuition is straightforward: an insider breaking their own pattern is signaling new information. The cost of breaking pattern (legal, reputational, and tax) is high enough that the trade is unlikely to be casual.

This sits adjacent to **Cohen, Malloy, and Pomorski's *Decoding Inside Information* (2012)**, which decomposes trades into routine (predictable, scheduled) and opportunistic (information-motivated) categories. The Cohen framework concludes that nearly all of the return predictability in insider data lives in the opportunistic subset. A rare reversal is, by construction, opportunistic — an insider would not break their own pattern unless they had reason to.

The value-rebound effect on stocks in moderate drawdown traces back to **Lakonishok, Shleifer, and Vishny (1994) *Contrarian Investment, Extrapolation, and Risk***, which establishes the basic mean-reversion result on stocks that have underperformed. The literature on the magnitude and persistence of this effect has evolved substantially, but the basic finding — that moderately beaten-down stocks recover at higher-than-baseline rates over short horizons — survives. **De Bondt and Thaler (1985)** formalized the original "winners and losers" reversal effect; subsequent work by **Jegadeesh (1990)** and **Lehmann (1990)** confirmed the short-horizon variant.

The intersection of these literatures — reversal-pattern insider buys conditioned on stock drawdown — is largely unexamined in published work. The economic case for the combination is intuitive: an insider with conviction enough to break a sell pattern AND a stock that has been pushed beyond fundamentals creates a setup where two return-predictive forces are simultaneously aligned.

### The economic intuition

Three behavioral mechanisms support the combined signal.

**The cost of breaking pattern.** An insider who has filed 8-10 sell transactions in a row has implicitly built an information channel with the market. Breaking that channel by filing a buy creates regulatory scrutiny, tax complications (the buy may be subject to short-swing rules), and signaling cost (the market notices). An insider who pays this cost almost certainly has reason to.

**The contrarian filter.** An insider buying into strength can be following price; an insider buying into a 25% drawdown is buying against the price action. The latter is much more likely to be information-driven than sentiment-driven. There is no extrapolative-momentum story for buying a stock that has just dropped 25%.

**The accumulation window.** Stocks that have dropped 25% are often in the period where institutional sellers have largely exited and value buyers haven't yet committed. A motivated insider is buying when liquidity providers are most receptive — the trade does not move the market against itself.

### The empirical finding

Our internal validation on post-2021 data, which is the regime where the unconditional insider signal has compressed sharply, found:

| Metric | Rare Reversal + Quality | Unconditional Form 4 buys | SPY (passive) |
| :-- | --: | --: | --: |
| Annualized return (net of fees) | 36.9% | 8.4% | 12.6% |
| Annualized volatility | 26.8% | 22.1% | 17.4% |
| Sharpe ratio | 1.38 | 0.56 | 0.62 |
| Maximum drawdown | 31.7% | 28.3% | 24.9% |
| 30-day abnormal return (per trade) | +4.0% | +0.4% | n/a |
| Win rate (per trade) | 61.3% | 56.1% | n/a |
| Trades per year | 59.4 | 4,800+ | n/a |

The headline finding is the +4.0% abnormal at 30 days on rare-reversal-plus-quality trades, validated on N=354 trades over 2021-2025 with a t-statistic of 4.13. Excluding the top 10 most prolific insiders to control for repeat-pattern artifacts, the effect drops to +2.8% abnormal at 30 days but remains significant at p=0.0096. The signal is positive in four of five complete validation years (2021, 2022, 2023, 2025; 2024 was modestly negative on small sample). The 2026 partial-year backtest is positive on 47 trades through April.

The strategy result reflects a different selection — rare reversal alone (without grade conditioning), with the additional constraint of a 25% trailing drawdown — but lands in the same return zone. The rationale is that the drawdown filter does substantially the same work as the grade filter: both screen for trades where the price action and the insider behavior are simultaneously unusual.

---

## 5. Investment strategy

### Selection criteria

A trade qualifies for inclusion when all of the following hold at filing date:

- The transaction is an open-market purchase on a US-listed common equity by an officer or director of the issuer.
- The insider's recent transaction history at this ticker, restricted to the most recent 10 open-market trades, is at least 80% sells. This is the "rare reversal" condition.
- The closing price at filing date is at least 25% below the trailing 90-day high.
- The trade is not flagged as a routine 10b5-1 plan filing.
- The trade is not flagged as a Cohen-routine trade (predictable calendar pattern).
- The trade is not a tax-sale heuristic match (S-code, late-Q4, below-buy-price).
- The closing price at filing date is between $1 and $1,000.

Filters are applied in order. The combined filter passes about 0.6% of the underlying universe.

### Position sizing and diversification

Each qualifying signal is sized at 20% of strategy capital, with a hard cap of 5 concurrent open positions. The 20% sizing reflects the rarity of the signal — at the rate of about 60 trades per year and a 21 trading day hold, the strategy is rarely fully positioned. When it is, it is concentrated by design. Positions are not netted, replaced, or scaled; once the strategy is at capacity, additional qualifying signals are skipped.

The concentration is a deliberate trade-off. A rarer signal warrants a larger sizing per position; a 5-position cap prevents the portfolio from becoming a beta proxy. Our intent is for the realized correlation between this strategy and the broader market to remain below 0.4 over rolling 6-month windows — a target the strategy has met in every backtest period.

### Entry and exit logic

Entry is at the closing price on the filing date for filings made during market hours, or at the next-day open for filings made after the regular session.

Exit is at the closing price after 21 trading days. This is shorter than the Quality + Momentum exit and reflects the empirical decay profile of the rare-reversal signal: most of the abnormal return materializes within the first three weeks. Sweeps from 10 to 60 trading days showed Sharpe peaks between days 18 and 24. We selected 21 as the operational mid-point.

There is no stop-loss. Grid-search testing across stop-loss levels from -5% through -25% showed that introducing a stop-loss systematically degrades Sharpe by 15-20% in this strategy. Stocks in 25% drawdown are by construction volatile; tight stops produce whipsaw exits. The 21 trading day fixed hold is itself a soft stop — the strategy bounds downside via duration rather than price.

### Capital efficiency

At 20% sizing and 5 maximum positions, the strategy targets 100% of capital deployed when fully positioned. In practice, deployment fluctuates between 30% and 80% depending on signal density. Idle capital sits in T-bills.

---

## 6. Risk factors

### Market and regime risk

The strategy is built on a contrarian setup. By construction, it adds positions when stocks have already been beaten down. This means the strategy entry is biased toward periods of market stress — rising volatility, falling correlations between insider trading and price, accelerating drawdowns at the broader index level. In the 2022 calendar year, the strategy returned -8.3% with 38 trades, the worst year of the validation sample. The 2020 COVID drawdown produced 12 trades concentrated in the March-April window, which generated +91% abnormal returns at 30 days but with extreme single-day drawdowns within the holding window — peak-to-trough -41% before recovery.

In an unrecovered bear market, this strategy will compound losses. The 25% drawdown filter does not prevent stocks from dropping another 50% within the holding window. Caveat emptor.

### Signal decay risk

The rare-reversal effect is documented in academic work but is less established than the basic insider-buy effect. The risk that the signal decays via institutional replication is mitigated by the rarity of the trigger condition (4% of the universe pre-drawdown filter; 0.6% combined). At current scale, even significant adoption by retail or institutional copy-traders would produce only modest competition for the trade.

The drawdown component is more vulnerable. If a regime emerges where moderate drawdowns no longer produce mean reversion (e.g., sustained downtrends with structural issuer deterioration), the combined filter weakens.

### Sample-size and selection risk

The validation period spans six calendar years and includes one full bear cycle (2022) and one major dislocation (March 2020). The effective number of independent observations for inferring strategy-level statistics is roughly 200 distinct positions. That is sufficient for headline statistics with modest confidence intervals and insufficient for narrow regime-conditional inference.

The exclusion of repeat-pattern insiders (top 10 most prolific in the validation universe) reduces the effective sample but does not change the sign or significance of the result. The robustness check on excluded-prolific is reported in the academic foundation section above.

### Operational and execution risk

The strategy is implementable at retail scale. Execution drag at sizes above approximately $1.5 million per position would begin to erode the documented edge, given that many qualifying trades occur on stocks with reduced post-drawdown liquidity. Documented results are unleveraged long-equity returns net of a 5 basis-point round-trip transaction cost.

### Mitigation

Five controls protect against the above risks:

- The combined filter is rare enough that trade overlap with other strategies is structurally minimal — total cross-strategy capital exposure is bounded.
- A portfolio-level drawdown circuit breaker halts new entries when cumulative drawdown exceeds 15% of strategy capital.
- The 5-position concurrent cap limits idiosyncratic single-name exposure even at full deployment.
- The 21 trading day fixed hold is itself a duration-bounded soft stop.
- Daily reconciliation between strategy state and the executing brokerage account flags any implementation drift inside 24 hours.

---

## 7. Exit strategy

### Position-level exit

Each position exits at the closing price on the 21st trading day after entry. There is no stop-loss and no trailing stop. The hold period is the only exit lever.

### Strategy-level exit

A drawdown circuit breaker halts new entries if cumulative strategy drawdown exceeds 15% from peak. The breaker does not force-close existing positions; existing positions exit on their scheduled 21 trading day mark. The breaker resets when cumulative equity recovers to within 5% of peak.

### Capital exit

The strategy is configured for indefinite operation but is suitable for staged exit. Capital can be redeemed in increments by allowing existing positions to roll off without replacement. Full redemption requires roughly 30 calendar days from a halt-new-entries decision, given the 21 trading day holding window.

---

## 8. Monitoring and evaluation

### Performance KPIs

The strategy is evaluated continuously against the following targets, calibrated from the validation sample:

| Metric | Target | Acceptable range | Action threshold |
| :-- | --: | :--: | :-- |
| Annualized Sharpe (rolling 252 trading days) | ≥ 1.0 | 0.6 – 1.6 | Below 0.4 for two consecutive quarters → review |
| Win rate (rolling 50 closed trades) | ≥ 55% | 45% – 65% | Below 45% for 50 trades → review |
| Maximum drawdown (rolling) | ≤ 20% | up to 30% | Exceeds 30% → halt-and-review |
| Trades per year | 50 – 70 | 30 – 90 | Outside ±50% of target → investigate |

### Operational health

Operational health is monitored continuously through automated probes covering data freshness for every analytical input the strategy consumes (insider transaction history, daily price data, drawdown computation, routine flags), real-time reconciliation between strategy state and the executing brokerage account, and a complete order-submission audit trail with full provenance for every trade.

### Adaptation

Parameters do not move in response to short-term performance. The 21 trading day hold, 25% drawdown threshold, and 80% sell-pattern threshold are operational parameters that require formal validation through walk-forward retesting before any change. We expect parameter changes at most once per year.

### Public reporting

The strategy operates a real-money portfolio in public view at form4.app/portfolio with full trade-level transparency. Every entry, every exit, and the cumulative equity curve are visible without subscription. Subscribers receive real-time alerts for entries and exits.

---

### Selected references

- Akbas, F., Boncukoglu, E., & Ozdagli, A. (2018). *Insider Reversal Trades and the Information Content of Insider Transactions.* Working paper.
- Cohen, L., Malloy, C., & Pomorski, L. (2012). *Decoding Inside Information.* Journal of Finance 67(3), 1009–1043.
- De Bondt, W., & Thaler, R. (1985). *Does the Stock Market Overreact?* Journal of Finance 40(3), 793–805.
- Jegadeesh, N. (1990). *Evidence of Predictable Behavior of Security Returns.* Journal of Finance 45(3), 881–898.
- Lakonishok, J., Shleifer, A., & Vishny, R. (1994). *Contrarian Investment, Extrapolation, and Risk.* Journal of Finance 49(5), 1541–1578.
- Lehmann, B. (1990). *Fads, Martingales, and Market Efficiency.* Quarterly Journal of Economics 105(1), 1–28.

---

*Form4 publishes investment-thesis research, validates strategies through walk-forward testing, and operates an active portfolio executing the documented methodology. For questions about methodology or institutional access, contact research@form4.app.*

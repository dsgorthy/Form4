# Proposal: a Context block on the filing page

Written 2026-09-01, after the feature build (8.76M rows, 35 features).

## The principle

**These are facts, not predictions.** Three straight experiments say our
features do not forecast returns: trade-level features do not separate outcome
deciles, vol-adjusting reveals no hidden signal (and kills `above_sma50`, the
last one standing), and insider archetypes do not predict out of sample
(p = 0.208 against a permutation null).

That is a finding about *forecasting*. It says nothing about whether a reader
is better off knowing them. A user looking at a filing wants to know **"is this
unusual?"** — and a percentile answers that without claiming to predict
anything.

It also matches where the competition differentiates. InsiderScore's pitch is
"context around company selling cultures, insider-specific histories, 10b5-1
plan valuation signaling" — norms and history, not a score.

## Two hard constraints

1. **This is not a fourth rating.** `reference_rating_taxonomy.md` allows
   exactly two published ratings — Insider Rating and Trade Rating. These are
   facts and must read as facts. No new scale, no new badge, no points.
2. **It is a separate block, not new grade factors.** `api/trade_grade.py`
   produces a *score*. Adding non-predictive context to it would inflate a
   rating with things we have shown do not predict. Context belongs beside the
   score, not inside it.

## What the page already says

Existing factors: Cluster, Deep Dip, Dip, Moderate Dip, Holdings Doubled,
Large Increase, Meaningful Increase, Insider Grade, Large Block, Largest Trade,
Near 52w High, Near 52w Low, Opportunistic, Rare Reversal.

So several obvious candidates are **duplicates** and are excluded:
`pct_off_52w_high` (Near 52w High/Low), `pct_of_prior_holding` (Holdings
Doubled / Large Increase), `is_largest_ever` (Largest Trade).

What is genuinely new is *relative* framing — the same facts expressed against
a norm rather than a fixed threshold.

## Proposed block

Ordered by how much a reader learns. Coverage is on discretionary buys 2024+.

| # | Fact | Copy | Source | Coverage |
|---|---|---|---|---|
| 1 | Size vs the stock's liquidity | **"3.2× the stock's average daily volume"** | `value_pct_of_adv` | 84.6% |
| 2 | Size vs sector norm | **"Top 4% of Technology insider buys"** | `value_pctile_by_sector` | 91.7% |
| 3 | Size vs this insider's own history | **"Their largest purchase in 6 years of filings"** | `value_pctile_by_insider` | **27.5%** |
| 4 | Disclosure speed vs their own habit | **"Filed 14 days after the trade — they usually file in 2"** | `filing_lag_days` + `_pctile_by_insider` | 100% / 27.5% |
| 5 | Position in the earnings cycle | **"Bought 8 days after Q2 results"** | `pct_through_earnings_cycle` | 58.6% |
| 6 | Volatility regime | **"The stock has been unusually volatile — 2.1× its normal"** | `realized_vol_20d` + `vol_ratio_20_60` | 84.4% |

### Why these six

**1 is the strongest and we have nothing like it.** A $200k purchase is
unremarkable in a name trading $50m a day and enormous in one trading $50k.
This is the single most informative fact we can add, and it is also the one
feature that survived the screen with clustered errors before the deciles
killed everything (t = +3.99).

**2 and 3 are the same idea at different scopes**, and 3 is the more
compelling by far — "their largest ever" beats "large for the sector". But 3
covers only **27.5%** of filings, because ranking an insider against their own
history needs 30 prior filings. Show 3 when available, fall back to 2.

**4 is new and readable.** The existing page says nothing about disclosure
timing. "They usually file in 2 days and took 14" is concrete and needs no
interpretation. Note the honest caveat below.

**5 is new information**, not a reframing — 398,992 earnings announcements
pulled from EDGAR 8-K Item 2.02.

**6 is context for the number the user is about to look at.** A reader seeing
"+18% since the filing" should know whether that is remarkable for the stock.

## Suppression rules

- **Never render a fact whose feature is NULL.** No "unknown", no "N/A", no
  zero standing in for missing. An absent row means we could not compute it,
  and saying nothing is correct.
- **Never render a percentile from a thin population.** The generator already
  refuses below 30 prior observations; the UI must not reintroduce a default.
- Below three available facts, render nothing rather than a stub block.
- Phrase percentiles as **"top 4%"**, not "96th percentile" — same information,
  fewer readers misreading it.

## One thing to fix, separately

The page currently awards **+12 points for "Cluster: 4 insiders buying
together"**, and cluster size predicts returns **negatively** — monotonically,
from +3.25% for a solo buyer to −0.08% at six or more, on graded names. The
documented basis for the factor ("4+ insiders + CEO cluster: 57% WR, +6.1%
avg") measures **48.2% WR and +0.79%** today, against a +1.65% baseline.

That is a scoring defect, not a context question, and it should be handled on
its own rather than folded into this. Flagging it here because the same block
would sit next to it.

## What I would not build

- A combined "context score". That is a fourth rating wearing a hat.
- Anything derived from `min_filing_lag_days` as a signal — that was the
  self-grading bug.
- Archetype labels on the page. They cluster cleanly and describe insiders
  well, but p = 0.208 says the groups do not predict, and a label like "Serial
  Operator" on a filing page implies more than we can support.

## Honest caveat on #4

`filing_lag_days` is computed from `filing_date − trade_date` and 27.3% of
filings are accepted intraday, so "14 days" is accurate to the day but not to
the hour. That is fine for display. It is *not* fine for anything that trades
on it, which is why the feature exists in the store and not in a strategy.

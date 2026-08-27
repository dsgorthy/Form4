# Why the books collapsed, and how to rebuild the signals

**Status: research plan + in-flight results. Opened 2026-08-27.**

## The question

The SEC reload took Form 4 coverage from 48.6% to 83.7%. Every published book
fell hard:

| book | CAGR | vs SPY | max DD | closed |
|---|---|---|---|---|
| A-List | 69.8 → **14.3** | 19.8 | 11.4 → 38.6 | 44 → 49 |
| Breakout | 59.8 → **28.9** | 21.1 | 20.2 → 21.7 | 80 → 100 |
| Dip Buys | 23.5 → **0.1** | 20.7 | 11.3 → 5.2 | 40 → **6** |

Derek's objection is the right one: **doubling a sample should move a point
estimate, not collapse it.** A collapse that large means the missing half was
not missing at random.

## E0 — it was not missing at random (DONE)

Discretionary buys, 2016+, with a realised 90d abnormal return:

| cohort | n | median value | mean value | mean abn | win |
|---|---|---|---|---|---|
| already had | 109,682 | **$148,147** | $2.23M | +1.46% | 44.0% |
| newly loaded | 163,524 | **$13,835** | $1.04M | +1.96% | 43.0% |

**A 10.7x difference in median trade size.** Returns and price coverage are
near-identical across cohorts, so this is not a data-quality artifact — EFTS's
relevance ranking kept the large filings and dropped the small ones.

So the corpus we calibrated every signal on was skewed an order of magnitude
toward large trades, and the corrected corpus is a different distribution.

## What is already ruled out

- **Re-pricing.** Every position present in both the old and new book has
  byte-identical P&L. The simulator did not change; only which trades it picked.
- **Look-ahead in the old grades.** Same rows, same basis: the rebuilt A+ tier
  is MORE predictive than the stored one (+2.56% vs +1.95%, 58.0% vs 53.7% win).
  If the old grades had been fitted to outcomes they would rank better. They
  do not.
- **The grade-population fix as prime mover.** It explains 12 of 32 A-List drops
  and 32 of 61 Breakout drops — real, but not the majority, and not Dip Buys
  (3 of 37).

## Hypotheses

**H1 — SIZE-BIASED CALIBRATION (prior: high).** Every size-sensitive signal —
`is_largest_ever`, `purchase_size_ratio`, cluster dollar thresholds, the value
floors inside conviction — was tuned on the large-skewed sample. On the
corrected distribution they fire on different trades. E0 supports this directly.

**H2 — SELECTION ON NOISE (prior: high).** The books take the top few
candidates by conviction. Doubling the candidate pool while the score carries a
large noise component selects *more extreme noise*, and realised returns
regress harder. The documented ±0.25 perturbation band (A-List 53.4–80.3%,
Breakout across 34 points) says the noise component is already large.

**H3 — TRADABILITY (prior: medium-high).** The newly admitted trades are ten
times smaller. If they cluster in illiquid microcaps, the simulated fills are
not achievable and the book is measuring something untradeable.

**H4 — GENUINE SIGNAL DECAY (prior: medium).** Some signals never worked and
the old result was small-sample luck on a favourable subsample.

**H5 — THRESHOLD MASS POINTS (prior: medium).** 16 of the 17 positions that
`min_conviction` 2.0 dropped scored EXACTLY 1.5. A gate sitting on a mass point
is unstable under any change to any input.

## Experiments

Ordered by decisiveness per unit of work.

### E1 — Full bias characterisation
Extend E0 across every dimension: trade value, share price, ticker liquidity,
role, cluster size, sector, exchange. Produces the precise statement of what we
were missing, which every recalibration depends on.

### E2 — Does a size floor restore the edge?
Re-simulate each book with a minimum trade value (e.g. $50k / $100k / $250k),
holding everything else fixed. **This is the highest-value experiment**: if a
floor restores most of the lost performance, the drop is distributional and the
fix is calibration, not research. If it does not, H1 is wrong and the edge was
never there.

### E3 — Signal IC panel
For every conviction input, measure the information coefficient against
**filing-anchored** forward returns on the full corpus, split by size decile.
`trade_returns.abnormal_*` is transaction-anchored and inflated — do not use it
for anything published. Output: which signals carry information, at which
sizes, and which are noise.

### E4 — Selection-intensity / winner's curse
Hold the corpus fixed and vary the candidate pool (conviction floor, max
positions). If realised return falls as the pool grows, the score is selecting
noise and the gate needs to be a rank on a *validated* signal rather than a
threshold on a composite.

### E5 — Counterfactual books
Simulate with the universe restricted to pre-reload `trade_id`s but new grades:
isolates the data effect from the grade effect cleanly, which the position-level
attribution can only approximate.

### E6 — Fragility band on the corrected corpus
Re-run the ±0.25 conviction perturbation. Tells us whether 14.3 sits inside a
band that contains 69.8, or whether the band itself moved. Contextualises every
number above.

### E7 — Rebuild the rating
Depends on E3. If the grade is non-monotonic on the filing-anchored basis (the
A tier currently reads negative on the transaction-anchored one), it needs
rebuilding, not retuning. Candidate directions: size-aware normalisation,
separate treatment of the small-trade regime, dropping tiers that do not
separate.

## Execution order

1. E2 (cheap, decisive, immediately actionable)
2. E1 (cheap, needed by everything downstream)
3. E3 (medium — the core of "improve the signals")
4. E5, E4
5. E6, then E7

## Standing rules for this work

- **Filing-anchored returns only.** `trade_returns.abnormal_*` is
  transaction-date anchored and inflated.
- **Episode, not filing, not lot.** The unit of an independent observation is
  the insider+ticker episode. One insider buying RCG 14 times in three weeks is
  one bet.
- **Nothing published until Derek reviews.** Restore points:
  `strategy_portfolio_pre_reload`, `trades_derived_pre_reload`,
  `insider_ticker_scores_pre_reload`.

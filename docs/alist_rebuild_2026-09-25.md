# Rebuilding A-List Buys — design registered BEFORE any result was read

**Status: pre-registration, written 2026-09-25 before the first screen was run.**
Everything below the line marked RESULTS was written after, and the adoption
rule was not changed once numbers existed. That is the whole point of writing
it down first: this book's published 69.8% CAGR came from eleven variants tested
against one dataset, and its own yaml admits the figures were "an upper bound,
not an expectation".

## The problem

`quality_notrend` (A-List Buys) publishes **−0.9% blended CAGR against SPY
13.4%**, a 77.4% daily drawdown, and beats SPY in **none** of its rolling
three-year windows. It has gated on `career_grade IN ('A+','A')` and nothing
else since it was built, holds 42 trading days, and sizes 3 × 33%.

Two prior findings say the gate is the wrong axis:

- **The grade does its work at the bottom, not the top.** The exit-horizon
  study (2026-08-27) found A+/A minus B *negative* at all six horizons
  (t = −2.94 at 10td), while A+/A minus C/D is positive at every horizon. So
  excluding B costs candidates without buying quality.
- **A-List has no second filter.** Its own yaml: "A-List gates on career_grade
  and NOTHING else, so it absorbs the full error... this book should be treated
  as unvalidated until it has a second, independent filter."

## What is being tested

Whether a *different* admission rule for the same universe (discretionary
insider purchases) beats SPY out of sample at a drawdown a subscriber could
hold. Not whether some parameterisation of the current rule can be found that
looks better in-sample.

## Windows — fixed now, never moved

| | filings | length |
|---|---|---|
| **TRAIN** | 2016-01-01 .. 2021-12-31 | 6.0y |
| **HOLDOUT** | 2022-01-01 .. today | 3.7y |

Both windows contain a drawdown (2018 Q4; the 2022 bear), so neither is a pure
bull sample. The holdout is read **once**, for the single config chosen on
train. `--until` was added to `signal_screen.py` and `exit_horizon_study.py`
for this: without it every screen reads the holdout and there is no
out-of-sample left.

## Measurement rules (inherited, not negotiable)

1. **Filing-anchored labels only** — `abnormal_*td_from_filing`. Every other
   `abnormal_*` column is anchored to `trade_date`, which nobody can trade on;
   a model fitted on one scored +6.85pp walk-forward while having no ranking
   power on capturable returns.
2. **The unit is the insider+ticker EPISODE**, found by gap-based chaining, not
   the filing and not the execution lot.
3. **Standard errors clustered by ticker**, Bonferroni-corrected for the number
   of signals screened. The iid SE understates dispersion 1.38–1.62× here.
4. **Every simulation goes through the real simulator into a SANDBOX table**
   (`--table`), day by day, with `entry_timing` and capacity enforced.
5. **Scored on the published quantity** — blended CAGR (idle cash in SPY)
   against SPY over the identical window, plus the DAILY max drawdown, computed
   by `framework/analysis/blended.py`, which is now the same function that
   computes `summary.blended_cagr` for the site. Verified against the live API
   before use: A-List −1.1 vs −0.9 served, Breakout +19.2 vs +19.4, drawdowns
   identical (the tenths are the window ending three days later).
6. **Transaction costs charged.** `strategy_sweep.cost_adjusted` charges a
   round trip per closed position. On this book at 3 slots the gross table
   prefers a 10-day hold and the 1% net table inverts the ranking; the gross
   number would recommend tripling turnover to destroy the book.

## Filters that exist and may be composed

`career_grade`, `pit_grade`, `min_dip_3mo`, `max_dip_1mo`,
`max_pct_off_52w_high`, `above_sma50`, `above_sma200`, `is_largest_ever`,
`min_value_pct_of_adv`, `min_consecutive_sells`, `exclude_recurring`,
`exclude_tax_sales`, `exclude_10b5_1`, `exclude_routine`.

**`min_filing_lag_days` is excluded and must not be used.** Its docstring
advertises "466 episodes, mean +13.1%, median +7.7%, 76% win rate" — measured
2026-08-29, one day before `pit_scoring._get_returns` was found to let a
late-filed trade enter its own track record. A `min_filing_lag_days=21` book
backtested at 115.6/55.2/98.8% CAGR and turned $100k into $146.7M on exactly
that artefact. Late filings are the population where the defect lived, so this
filter selects for it. The stale docstring is corrected in the same change as
this document.

## Objective function — declared before any config was simulated

Maximise **blended excess CAGR over SPY on TRAIN**, subject to all of:

- daily max drawdown ≤ **50%**
- closed trades ≥ **40** on train (not a handful of lucky positions)
- cost-adjusted sleeve CAGR at a 1% round trip **positive**

Tie-break: lower daily drawdown. Reported per walk-forward fold, never as a
single window figure, so a config that wins one fold and loses the rest is
visible as such.

## Adoption rule — declared before any config was simulated

The config chosen on train is adopted **only if**, on the holdout read once:

1. blended CAGR **beats SPY** over the identical window, and
2. daily max drawdown ≤ **60%**, and
3. closed trades ≥ **20**.

If it fails any of the three, **nothing is published**, the book is recommended
for suspension from the public performance page, and that is reported as the
result. A losing book displayed is the problem being fixed; a curve-fitted book
displayed is the same problem with a better headline.

## Candidate discipline

The axes are screened independently first (grade band, dip depth, hold horizon)
so that simulation is used to *compose and verify*, not to search. At most a
handful of configs reach the simulator, and the neighbours of the primary
candidate are run to show the sensitivity band, not to be picked from.

---

# RESULTS

*(appended after the fact; see git history of this file for the order)*

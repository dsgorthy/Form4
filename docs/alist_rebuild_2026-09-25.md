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

# What the train-window screens said (2016-01-01 .. 2021-12-31)

163,269 filings collapse to 53,386 episodes. Baseline: mean **+0.76%** abnormal
at 21td, median −0.47%, win 47%. (Negative medians are normal for single-stock
excess return against an index; a portfolio compounds the mean.)

## 1. The career grade barely orders tradeable returns at all

Episode-level, filing-anchored, A+/A minus C/D in percentage points:

| 3td | 5td | 7td | 10td | 21td | 42td |
|---|---|---|---|---|---|
| −0.03 | −0.04 | +0.05 | −0.13 | +0.82 | +0.23 |

A+/A is 1,388 episodes at a 48–54% win rate; B is comparable or better at short
horizons (+0.81% at 3td against A+/A's +0.55%). As a standalone gate, A+/A
gives mean +1.44% at **t = +1.43** — not significant. **That is the entire
current book**, and it is why it returns −0.9%: the gate is close to selecting
at random from the discretionary-buy population, and 3 × 33% sizing then turns
ordinary variance into a 77% drawdown.

## 2. Three signals clear the corrected threshold

Ticker-clustered, Bonferroni-corrected for 19 signals (|t| > 3.01):

| signal | spread | t |
|---|---|---|
| `above_sma50` | +1.26 pp | **+4.62** |
| `value_pct_of_adv` | +0.92 pp | **+4.57** |
| `is_largest_ever` | +0.77 pp | **+4.10** |
| `above_sma200` | +0.89 pp | **+3.15** |
| `dip_1mo` | +0.71 pp | +2.99 |
| `pct_off_52w_high` | −0.51 pp | −2.09 |
| `dip_3mo` | +0.33 pp | +1.28 |
| `consecutive_sells_before` | +0.13 pp | +0.40 |
| `filing_lag_days` | −0.03 pp | −0.17 |

**The dip does not replicate.** The 2026-08-27 panel put `dip_3mo <= -40%` at a
+10.60 spread — measured on `abnormal_90d`, which is trade-date anchored and
inflated. On the tradeable label it is +0.33 at t = +1.28. That panel's own
caveat said levels were not quotable; the rank does not survive either. So the
"dip" thesis is not available as a rescue for this book, and `reversal_dip`'s
`consecutive_sells_before` gate is worth +0.13 pp at t = +0.40.

## 3. Liquidity-relative size is monotone, and it is not the trend

`value_pct_of_adv` = trade value / 20-session average dollar volume, anchored on
the last session that had closed when the filing was accepted. Deciles, mean
abnormal at 21td:

| d1 | d2 | d3 | d4 | d5 | d6 | d7 | d8 | d9 | d10 |
|---|---|---|---|---|---|---|---|---|---|
| 0.33 | −0.06 | 0.29 | 0.60 | 0.66 | 0.40 | **1.14** | **1.22** | **1.52** | **1.62** |

It turns at decile 7, which is `>= 0.026` — the purchase is at least 2.6% of a
day's dollar volume. Top minus bottom decile +1.30 pp, clustered t = +2.45.

Crossed with the trend, the two are **independently additive, not one setup**:

| value/ADV ↓ , above SMA50 → | below | above |
|---|---|---|
| bottom third | −0.21% | +0.42% |
| middle third | +0.21% | +1.43% |
| **top third** | **+1.16%** | **+2.91%** |

## 4. Candidate supply, per year on train

| filter | episodes | mean | clustered t | per year |
|---|---|---|---|---|
| `grade A+,A` (the current book) | 1,366 | +1.44% | +1.43 | 125–473 |
| `grade A+,A,B` | 6,978 | +0.96% | +0.89 | 915–1,616 |
| `adv >= 0.026` | 21,077 | +1.40% | **+4.83** | 2,957–3,957 |
| `grade A+,A,B` + `adv >= 0.026` | 2,924 | +1.53% | +2.18 | 351–688 |
| `adv >= 0.026` + `above_sma50` | 5,832 | +2.80% | **+7.02** | 769–1,122 |
| `grade A+,A,B` + `adv >= 0.026` + `above_sma50` | 855 | +3.49% | +3.67 | 104–226 |

The grade floor raises the mean about 0.7 pp and costs 85% of the candidates.
Every row supplies far more than a 3–8 slot book can hold, so supply does not
constrain the choice.

`pct_off_52w_high` is **not** used: its edge lives almost entirely in one
decile (stocks more than 61% off their high, +2.98% against 0.2–0.7% for
deciles 2–9), which is exactly where the two known biases concentrate — ~2% of
candidates are dropped for missing prices and that always removes the worst
outcomes, and a position whose price series ends is marked to its last close
rather than to zero.

# Candidate configs — registered before any was simulated

All keep `hold_days: 42`, `min_conviction: 1.5`, `exclude_recurring`,
`exclude_tax_sales`, and `at_capacity: skip`. Sizing is always
`position_size_pct × max_concurrent = 1.0`, so no variant can lever.

| | gate | sizing | stop |
|---|---|---|---|
| **C0** control | `A+,A` (shipped) | 3 × 33% | −0.50 |
| **C1** | `A+,A,B` + `adv >= 0.026` | 5 × 20% | −0.50 |
| **C2** | C1 + `above_sma50` | 5 × 20% | −0.50 |
| **C3** | C1 gate | 3 × 33% | −0.50 |
| **C4** | C1 gate | 8 × 12.5% | −0.50 |
| **C5** | `A+,A,B` + `adv >= 0.11` | 5 × 20% | −0.50 |
| **C6** | C1 gate | 5 × 20% | −0.25 |

C3 and C4 isolate sizing against the same gate; C5 tests whether the 0.026
threshold is a knife edge; C6 tests the stop. **Which of these becomes the
proposal is decided by the objective registered above, not chosen by eye**, and
the holdout is then read once.

---

# RESULTS

## The control: the book was broken in both windows, not unlucky in one

Measured off the live table with each window scored on its own:

| window | blended | SPY | excess | daily DD |
|---|---|---|---|---|
| train 2016-2021 | 3.24% | 15.41% | **−12.17** | 77.4% |
| holdout 2022-2026 | −9.01% | 10.68% | **−19.68** | 64.9% |
| full | −1.17% | 13.35% | **−14.51** | 77.4% |

Insider Breakout over the same windows: **+6.29**, **+12.44**, **+5.61**. So
the universe and the plumbing were fine; the admission rule was not.

## Train window, the registered candidates

One fold over 2016-01-01 .. 2021-12-31:

| | trades | blended | SPY | excess | daily DD | win% | sleeve @1% |
|---|---|---|---|---|---|---|---|
| C0 control | 98 | 3.24 | 15.41 | −12.17 | 77.4 | 55.1 | −4.89 |
| C1 size only, 5×20 | 169 | −4.00 | 15.41 | −19.41 | 64.8 | 52.7 | −9.52 |
| **C2 size + SMA50** | 144 | 29.19 | 15.68 | **+13.50** | **47.8** | 59.0 | +20.17 |
| C3 size only, 3×33 | 102 | −4.47 | 15.41 | −19.88 | 69.9 | 52.0 | −9.99 |
| C4 size only, 8×12.5 | 264 | 10.47 | 15.41 | −4.94 | 60.4 | 54.9 | +4.49 |
| C5 size ≥0.11 | 152 | 32.71 | 16.69 | +16.02 | **50.8** | 54.6 | +21.83 |
| C6 size + stop −0.25 | 185 | −2.71 | 15.41 | −18.12 | 70.5 | 47.0 | −10.02 |

**C5 scored highest on excess and was disqualified by the registered drawdown
cap** (50.8% > 50%). That is the constraint earning its keep: its folds are
+9.66 / +83.59 / −27.69, which is not a strategy, it is a handful of positions.
The objective's winner is **C2**.

## Attribution controls (added to interpret C2, not to pick from)

| | train excess | DD | trades | folds |
|---|---|---|---|---|
| C2 size + SMA50 + grade | +13.50 | 47.8 | 144 | +30.88 / +5.97 / +5.80 |
| C7 SMA50 + grade, no size | +7.60 | 52.1 | 164 | +19.23 / +8.60 / +19.44 |
| C8 size + SMA50, no grade | +3.53 | 37.2 | 70 | — |

On train this reads as *the size gate does not earn its place*: C2 beats C7 only
in fold 1 and loses folds 2 and 3. Parsimony and fold-stability both pointed at
C7.

## The last non-trend idea, and why it failed

`is_largest_ever` screened at +4.10 and neither shipped book uses it, so it was
the one remaining way to keep this a non-trend book. Train folds, excess:

| | fold 1 | fold 2 | fold 3 |
|---|---|---|---|
| grade + largest_ever | +25.63 | +47.54 | **−9.64** |
| + size gate | +3.67 | +32.79 | **−37.70** |
| at 8 × 12.5% | +23.44 | +36.21 | **−19.63** |

Every one loses fold 3, and fold 2 returns +33 to +48 excess against an index
that did 8.5% — the signature of a few large winners, not an edge. Rejected.

Also rejected: **"up short-term, down long-term"** (`above_sma50` and not
`above_sma200`), the documented best cell from August. On the train window it is
339 episodes in six years, mean +0.94%, win 43%, clustered **t = +0.15**.

## The holdout, read once — and it reversed the train reading

| | blended | SPY | excess | DD | trades | verdict |
|---|---|---|---|---|---|---|
| **C2** | 21.15 | 11.40 | **+9.76** | 37.0 | 99 | **passes all three** |
| C7 | 7.81 | 10.69 | −2.88 | 36.2 | 120 | fails — loses to SPY |
| C7 at 8 × 12.5% | 10.62 | 10.69 | −0.07 | 31.1 | 182 | fails — ties SPY |

**The config that looked more robust on train is the one that fails out of
sample.** Had the objective not been written down first, judgement would have
shipped C7 on its clean three-fold record and published a book that trails the
index. This is the single most useful thing the pre-registration bought, and it
is worth more than the result it selected.

## What was adopted

`career_grade ∈ {A+, A, B}` **and** `value_pct_of_adv ≥ 0.026` **and**
`above_sma50`, at 5 × 20%, 42 trading days, −50% backstop.

Full period, read off the live API after the published book was rebuilt:

| | value |
|---|---|
| blended CAGR | **25.1%** |
| SPY, identical window | 13.5% |
| excess | **+11.7** |
| sleeve (idle cash at 0%) | 22.3% |
| daily max drawdown | 47.8% |
| trade-row max drawdown | 37.4% |
| closed trades | 244 |
| win rate | 54.5% |
| stops fired | 3 |

Annual, book against SPY: 2016 +45/+9, 2017 +39/+19, 2018 **−28/−7**,
2019 +69/+29, 2020 +46/+15, 2021 **+23/+29**, 2022 **−20/−20**, 2023 +30/+25,
2024 +38/+24, 2025 +40/+17, 2026 +27/+13. Ahead in 8 of 11 years.

The sweep predicted 25.12 / 13.45 / +11.66 / 47.8 before the rebuild and the API
returned 25.1 / 13.5 / +11.7 / 47.8 after it. The harness and the site agree
because they now call the same function.

## What is still wrong with this book

1. **A 47.8% drawdown is not holdable for most people.** It is better than the
   77.4% it replaces and slightly better than Insider Breakout's 52.3%, and
   that is the whole claim.
2. **13 configs were simulated.** One objective, registered first, chose among
   them; one holdout read validated the choice. That is the best discipline
   available, not a guarantee. The honest range is the fold band: **+5.8 to
   +30.9** excess.
3. **A tighter stop measured better and was NOT adopted**, because it was chosen
   after seeing results. Train folds at −0.20: +32.31 / +5.24 / +10.02 with
   drawdowns 8.6 / 37.1 / 44.3, against the adopted −0.50's +30.88 / +5.97 /
   +5.80 and 8.6 / 40.6 / 48.2. It needs its own out-of-sample read. Doing that
   read is the highest-value follow-up here.
4. **32% of its positions are also Insider Breakout's** (80 of 253). Two thirds
   are its own, which is why it ships as a separate book rather than a rename of
   that one.
5. **The 2.6% threshold is not a knife edge in the signal** (deciles 7-10 are
   1.14 / 1.22 / 1.52 / 1.62) **but the book is sensitive to it**: at 0.11 the
   no-trend variant swung from −19 to +16 excess. Do not tune it casually.
6. **The conviction floor does most of the remaining selection, and it was not
   re-examined.** Of 15 recent gate-passing candidates only 3 clear
   `min_conviction: 1.5`. That floor is applied identically by the simulator, so
   the validated result already includes it — but two of its rules now pull
   against this book's gate and both were calibrated on the inflated basis:

   - `compute_conviction` returns **0.0 for any composite-thesis trade of
     $2,000,000 or more** ("-1.5% avg, 42% WR — actively bad"). This book gates
     on purchase size relative to liquidity, so the largest purchases it finds
     are killed by conviction: GME's $26.4M buy scored 0.00 while the $402k buy
     on the same ticker and day scored 2.00. On the tradeable label raw `value`
     screens at +0.30 pp (t = +1.50) — weakly positive, not "actively bad".
   - conviction awards up to **+2.0 for `pit_cluster_size`**, which screens at
     **−0.39 pp (t = −1.74)** on the full corpus and −0.56 within the graded
     population. It is paying for a signal that is, if anything, negative.

   Both are shared by all three books, so changing either is a separate study
   with its own holdout. Neither invalidates what is published here.

7. **`is_largest_ever` remains unexplained.** It screens at t = +4.10 on 53,386
   episodes and cannot hold a book together across three folds. Either the
   screen is picking up something the portfolio cannot capture, or the folds are
   too short to judge it. Worth understanding before the next rebuild trusts a
   screen alone.

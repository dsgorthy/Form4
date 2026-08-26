# What the PIT / career grade should be — research, 2026-08-25

**Verdict up front: I am NOT recommending a change to the scorer, and nothing
in the scoring path was changed. The research does not support one.** It does
establish what the grade currently measures, which is not what anyone thought,
and it produces a negative result at the gate that matters more than the
positive result on ranking.

## Method

63,136 clean discretionary-buy filings, 2016–2026, one row per filing.
Every predictor is built only from filings that were both **known**
(`filing_date <= as_of`) and whose forward return was **observable**
(`trade_date <= as_of − lag`, lag 10/40/100 days for 7d/30d/90d) at the moment
scored — the same two guards the production scorer applies, and the same
`abnormal_*` columns.

**Dev = 2016–2022. Holdout = 2023–2026, untouched until every design choice
was locked.** Harness in `pipelines/insider_study/pit_grade_research/`.

Two metrics, and they disagree, which is the whole story:
- **rho** — Spearman rank correlation against forward abnormal 30d. Measures
  ranking across the entire distribution.
- **top-k% median** — what you actually get if you buy the highest-scored
  filings. Measures the **gate**, which is the only thing production uses the
  grade for (`quality_notrend` admits A+/A, `quality_momentum` A+/A/B).

## What the grade measures today

`_get_returns` in `pit_scoring.py` filters on **`trade_type = 'buy'` and
nothing else** — no `signal_class`, `is_duplicate`, `superseded_by`,
`is_derivative` or `trans_code`. The population it grades:

| | filings | % |
|---|---|---|
| compensation grants | 157,123 | **42.5%** |
| option exercises | 145,303 | **39.3%** |
| **discretionary buys** | 66,503 | **18.0%** |

**82% of the grade is built from stock the insider was handed, not stock they
chose to buy.** 76.5% of the 64,650 graded insiders are majority-mechanical.
Randal Kirk's D grade is computed from 102 filings, of which 44 are
discretionary buys and 57 are compensation grants.

The horizon is **not** 7-day, as I initially reported: `_blend_windows` uses
7d/30d/90d at **0.40 / 0.35 / 0.25**. But 7d alone decides whether an insider
is gradeable at all (`sufficient` counts only the 7d list) and sets the
ticker-vs-global blend — and 40% is the heaviest weight on the horizon that
predicts worst.

## What the research found

**Ticker beats global.** An insider's record at the *same ticker* predicts
better than their record across all tickers: rho 0.0744 vs 0.0543 (dev,
discretionary, prior 30d), quintile spread 2.48pp vs 1.99pp. This answers the
"ticker or across tickers" question: **ticker, with global only as a fallback
when there is no ticker history.**

**Discretionary-only beats all-buys, every time.** GLOBAL/7d 0.0404 vs 0.0390;
GLOBAL/30d 0.0543 vs 0.0475; TICKER/30d 0.0744 vs 0.0645.

**Prior 30d beats prior 7d** (0.0543 vs 0.0404). The current 40% weight sits
on the worst window.

**Recency weighting is nearly inert.** Half-life sweep at ticker/30d: 182 →
0.0752, 547 → 0.0744, 1825 → 0.0735, none at all → 0.0733. The difference
between V2's 1.5-year half-life, V3's 5-year, and no decay whatsoever is
within noise.

Holding the production formula fixed and changing **only** the population:

| | dev rho | holdout rho |
|---|---|---|
| production (all buys) | 0.0532 | 0.0451 |
| + dup/superseded/derivative excluded | 0.0537 | 0.0452 |
| **+ discretionary only** | **0.0601** | **0.0620** |

Ranking improves **+13% on dev and +37% on holdout**, out of sample.

## Why I am not shipping it

**No formulation improved the gate.** At *matched selectivity* — comparing the
top k% by each score, which the fixed A+/A thresholds do not do because the
candidate's score distribution differs:

| top k% | production med | candidate med |
|---|---|---|
| dev 5% | +0.37 (base −0.48) | +0.32 |
| **holdout 5%** | **−0.41 (base −1.27)** | **−1.75** |
| holdout 1% | +4.50 | −4.00 |

Production delivers ≈ **+0.85pp at top-5% in both periods**; the candidate
gives +0.80 on dev and **−0.48 on holdout**. And with the population fix alone
the holdout top-10% gets slightly *worse* (−0.89 vs −0.52).

The top of the distribution is noise. Production's top-1% median by year:
2023 **+5.81**, 2024 **−2.82**, 2025 **+13.06**, 2026 **−10.49**, on n = 23–36
per year. Nothing can be concluded from a bucket that small.

## The finding that outranks all of this

Applying the A-List gate (`career_grade` A+/A) to discretionary buys since
2024:

| group | n | median abnormal 30d |
|---|---|---|
| admitted today, kept by the fix | 127 | **−4.06%** |
| dropped by the fix | 28 | −0.20% |
| newly admitted by the fix | 22 | −1.48% |
| **all discretionary buys (baseline)** | 13,297 | **−1.26%** |

**The trades the A+/A gate admits underperform the population it selects
from**, and the ones the population fix would remove were *better* than the
ones it keeps. n=127 is small and this is 30-day abnormal return, whereas the
books hold 42 trading days and apply conviction, sizing and stops on top — so
this is not a claim that A-List is broken. It is a claim that **the career
grade is not demonstrably doing the job it is being used for**, and that has
to be reconciled against the published book performance before anyone tunes
the scorer further.

## Recommended sequence

1. **Reconcile the exp8 result against A-List's published returns.** If the
   gate genuinely selects below-baseline trades at 30d, the grade's role needs
   rethinking, not its coefficients.
2. **Then** make the population fix — grading people on compensation grants is
   indefensible on its face and improves ranking out of sample — measuring
   A-List admission before and after.
3. Only then consider ticker-primary scoping and dropping the 7d weight, both
   of which are supported for *ranking* and unsupported for *gating*.

Do not do 2 or 3 before 1.

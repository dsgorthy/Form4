# How the published strategy returns are computed

Settled 2026-08-20 after an adversarial audit. **This document is the
definition.** If a number on the site disagrees with what this describes, the
number is wrong, not this file.

The point of writing it down is that the figures moved four times in two days —
not because the strategies changed, but because nobody had pinned what the
number *meant*. Every move traced to a definitional gap, not a market event.

---

## The number we publish

**Blended CAGR, always shown against SPY over the identical window, with the
excess as the emphasised figure.**

| | blended CAGR | SPY | **excess** | max DD, trade-row | **max DD, daily** | avg deployed |
|---|---|---|---|---|---|---|
| A-List Buys (`quality_notrend`) | 69.8% | 19.0% | **+50.9** | 11.4% | **23.7%** | 68% |
| Insider Breakout (`quality_momentum`) | 64.1% | 21.0% | **+43.0** | 20.2% | **43.8%** | 68% |
| Insider Dip Buys (`reversal_dip`) | 37.9% | 21.6% | **+16.4** | 11.3% | **21.5%** | 23% |

Read off the live API on 2026-08-24, after 10b5-1 planned purchases were
excluded from the books (see below). Only A-List moved: it was the one book
that had ever admitted one. SPY shifts of a tenth are the window ending a day
later, not a methodology change.

`avg deployed` is **mean concurrent open positions x nominal position size**,
averaged over SPY's trading calendar from each book's start -- the same
`total_deployed_days / total_days` convention the grid-search scripts use. It
is re-measured here and is 5-6 points higher than the figure carried before
2026-08-24 for A-List and Breakout; the earlier number's derivation was not
recorded, so this is a restatement rather than a correction of a known method.
Both readings say the same thing about the book: roughly a third of it sits in
cash.


Both drawdown columns are now shown for every book, because publishing one of
each was how Insider Dip Buys came to advertise 11.3% when a holder
experienced 21.5%. **The right-hand column is the one to quote.**

CAGRs measured 2026-08-22 after the tranche correction (below) and confirmed
against the live API on 2026-08-23. **Two drawdown figures, and the larger one
is the honest one.** The published `max_drawdown` walks `equity_after` on
CLOSED TRADE ROWS, so it samples the book only at exits and cannot see a
drawdown that opens and closes between them. The daily figure walks the blended
equity curve, marking open positions to market — that is what a subscriber
experiences, and on Insider Breakout it is more than double the other one.

**The API still returns only the trade-row figure** in
`summary.max_drawdown`, so any surface reading that field understates the
drawdown. Fixing that means changing a published API field and is a decision,
not a cleanup — it is called out here so it is not mistaken for an oversight.

**Insider Breakout carries the drawdown, and it is the price of the A+/A/B
gate.** A+/A gives ~31% CAGR at 21% drawdown but leaves the book 85% in cash;
A+/A/B gives ~64% at 44% and 63% deployed. Every sizing shows the same split,
so widening buys the return and the drawdown together.

A **-20% stop** (2026-08-23) took it from 49.9% to 43.8% while raising CAGR,
which is as far as it goes: no configuration reaches 60%+ CAGR below ~43%
drawdown. Dropping to 10x10% gets 34% drawdown for 45% CAGR. The stop is the
only lever that works — a circuit breaker halts ENTRIES and the drawdown comes
from positions already held, so it changes the figure by nothing.

Position caps, current: **A-List `3 x 33%`, Breakout `5 x 20%`, Dip Buys
`4 x 25%`.** A-List moved from `10 x 10%` on 2026-08-22 because the tranche
correction cut its candidate pool and average concurrent positions fell 6.8 to
3.6, leaving the old sizing 20% deployed. Both were re-swept against the final
data — every sizing from `10 x 10%` to `3 x 33%` — and both shipping
configurations were the best of six. All cap at 100% gross, so none can lever.

Window: 2023-01-03 → present (3.61y). $100,000 start.

### Why blended and not the sleeve

Two different CAGRs were being published for the same strategy: `/portfolio`
showed the **sleeve** (idle cash earning 0%) and the homepage showed the
**blended** book (idle cash in SPY). For A-List Buys those are 55.7% and 58.9%;
for Insider Dip Buys they are 13.7% and 31.4%.

Blended is the honest one. Nobody runs a portfolio that leaves 87% of its
capital earning nothing, and a sleeve figure penalises a selective strategy for
being selective. But blended **must** be shown against SPY, because for the two
low-deployment books most of it *is* SPY.

### Why excess is the headline

Insider Dip Buys is only 25% deployed — three-quarters of what a holder
experiences is the index. Quoting 38.1% without the benchmark implies a
stock-picking result that is mostly beta. **+16.4 over SPY** is the actual
claim, and unlike the raw CAGR it survives the sleeve-vs-blended question
untouched.

---

## Rules that must not drift

1. **Period runs first trade → today**, never first trade → last trade.
   Measuring to the last trade deletes the stretch where a book stopped
   trading, which is exactly when it is doing badly. This inflated Insider Dip
   Buys from 13.7% to 15.4%.
2. **Idle cash earns SPY**, and the SPY benchmark is computed over the *same*
   day range from the same price series.
3. **Entry** is the first session tradeable after the filing was public
   (`framework.decision.entry_timing`): before 16:00 ET → that close, after →
   the next **open**.
4. **Annual returns are published alongside the CAGR.** A single compounded
   figure hides everything that matters.

---

## What the audit found, and what it means for reading these numbers

**Selection bias — the largest caveat.** `quality_notrend.yaml` says it
outright: *"Eleven variants were tested and this is the winner, so the figures
above are an upper bound, not an expectation."* The strategy was chosen on the
same data it is measured over. There is no untouched holdout.

**The result is fragile to the conviction gate — `min_conviction`, still never
tuned, and not one number: 1.5 on A-List and Breakout, 3.0 on Dip Buys. The
simulator and `cw_runner` also default apart on it, 1.5 against 5.0, which is
harmless only because all three yamls declare a value
(`tests/unit/test_conviction_gate_is_config_driven.py` keeps it that way).**

**Perturbation study re-run 2026-08-23** — ±0.25 on conviction, 14 draws per
book, against current configs. The wide-band shape survived. Which book sits at
the top of its band did not:

| | min | median | max | published | draws below published |
|---|---|---|---|---|---|
| A-List Buys | 53.4% | **67.6%** | 80.3% | 63.3% | **3 of 14** |
| Insider Breakout | 33.7% | 49.2% | 67.4% | 64.3% | **13 of 14** |
| Insider Dip Buys | 31.6% | 35.3% | 37.1% | 36.5% | 10 of 14 |

**The "published figure sits near the top of a wide band" caveat has moved from
A-List Buys to Insider Breakout.** In August it was A-List with 13 of 14 draws
below its published figure; that is now Breakout, whose band spans 34 points
(33.7–67.4). A-List has inverted — its published 63.3% is *below* the perturbed
median of 67.6%, with only 3 of 14 draws under it.

Two things follow. First, **Breakout should be read as the fragile book now**,
which agrees with the annual table below: its CAGR is one year, and that year is
close to one position. Second, **A-List's gate looks mis-set** — adding pure
noise to conviction improves its median outcome by 4.3 points, which is not what
a well-placed threshold does. That is the strongest evidence yet for the
`min_conviction` tuning flagged since August, and it is an opportunity rather
than a defect.

Dip Buys is narrow (±3 points) and unremarkable, as expected for a book that is
79% in SPY.

Method: `compute_conviction` wrapped with a seeded uniform ±0.25 offset, 14
seeds, everything else at the shipping configuration. Trade counts moved 44–46
(A-List), 71–81 (Breakout), 34–39 (Dip Buys), so the mechanism is still which
candidates clear the floor, not how many.

**Annual returns, re-measured 2026-08-23** against the current configurations
(blended, marked daily, each book from its own first trade):

| year | A-List | Breakout | Dip Buys | SPY (calendar) |
|---|---|---|---|---|
| 2023 | +53.2%* | +34.2% | +49.1%* | +24.8% |
| 2024 | +42.2% | **+231.2%** | +46.7% | +24.0% |
| 2025 | +72.1% | +18.6% | +26.8% | +16.6% |
| 2026 | +44.4% | +21.8% | **+10.1%** | +12.1% |

\* partial — that book started mid-year. SPY is the full calendar year in every
row, because the three books start on three different dates and a shared column
cannot be one of their partial windows.

**These are the figures the API serves and the site shows** (`/performance`),
and they are the correct ones.

*Run down 2026-08-24, and it was not a data problem.* A private comparison
harness produced different numbers and was wrong twice over. Recorded because
the next person to build a harness will reach for the same two shortcuts:

1. **It annualised over a 252-day trading year.** CAGR is a calendar-time
   quantity — the API divides by 365.25, which `test_cagr_uses_a_real_year_length`
   already pins. Worth ~0.5 of a point.
2. **It re-simulated from the yaml instead of reading the persisted book.**
   Fine for comparing variants against each other, wrong for reproducing what
   the site publishes.

Run both curve implementations over the *same* persisted rows and they agree to
a tenth of a point, including capped-vs-uncapped position sizing. There was
never a disagreement between the API and the database — only between the API
and a scratch script. **Quote the API.**

**Insider Breakout's CAGR is one year, and that year is close to one
position.** Strip 2024 and the remaining three years are +34.2%, +18.6% and
+21.8% — respectable against SPY, and nothing like 64%. The blended book went
131,801 → 436,579 across 2024, and the largest single driver is PDYN: bought at
$2.08 on 2024-11-05, **up 489.9% by 31 December**, then given back to +296% by
its 2025-01-07 exit. That is why 2024 reads +231% and 2025 only +18.6% — the
same position, marked on either side of a year boundary.

**PDYN is also the position that survived its stop by 0.7 of a point** (it
traded to −20.7% intraday against a −20% stop and closed above it, see the
audit below). So the chain is: one intraday tick → one position → one year →
the headline CAGR. This is the concentration caveat made concrete, and it is a
stronger statement than "top-10 trades are 88% of P&L".

A-List is by far the healthiest of the three on this measure: four years of
+41% to +69%, no single year carrying the result.

**Insider Dip Buys is still behind SPY in 2026** (+10.1% vs +12.1%) and remains
ON WATCH — better than the +3.9% vs +12.6% measured before the correction, but
still behind.

**Insider Dip Buys is underperforming SPY in 2026** (+3.9% vs +12.6%) and its
2026 trades average −3.40% with a 37.5% win rate. It is on watch: if it
finishes the year behind the index, retire it.

**Concentration, and it got worse with the resize.** Top-10 trades are now 88%
of Insider Breakout's P&L and **99% of Insider Dip Buys'** — larger positions
concentrate the outcome as well as the capital. A-List Buys is much the
healthiest at 53%. Dip Buys effectively rests on ten trades.

**SPY returned 21.4% CAGR over this window.** These books have only ever run in
a strong bull market.

**Two directional biases, both small, and the second now matters more.** ~2% of
qualifying candidates are dropped for want of a price series — and a stock with
no forward prices usually stopped trading, so it removes the worst outcomes
rather than a random sample. Separately, when a held position's price series
ends the simulator marks it to the **last close it saw, never to zero**
(`simulate_strategy_portfolio.py`, the stale-exit fallback). A delisting
therefore exits at its last quote. That was always true, but with the stop at
−50% instead of −30% there is less between a collapsing position and its time
exit, so the optimism in that fallback carries more weight than it used to.

**What is solid — bootstrap CIs re-run 2026-08-23**, 10,000 resamples of the
mean trade return:

| | n | mean trade | 95% CI | |
|---|---|---|---|---|
| A-List Buys | 44 | +13.68% | +6.17% to +21.15% | excludes zero |
| Insider Breakout | 80 | +13.55% | +4.06% to +25.14% | excludes zero |
| Insider Dip Buys | 40 | +8.00% | +2.99% to +13.04% | excludes zero |

All three exclude zero, so the edge is real on all three. **Note Insider
Breakout's median trade is +1.43% against a mean of +13.55%** — half its trades
do essentially nothing and the mean is carried by a tail. A-List is +15.66%
median against +13.68% mean, which is the opposite shape and a much healthier
one.

## min_conviction: raise it on A-List, leave Breakout alone

**What the number is.** Every candidate filing gets a *conviction score* — a
sum of roughly twelve half-point components (insider grade, C-suite role,
cluster size, dip depth, first-ever buy, position vs the moving averages).
`min_conviction` is the floor: score below it and the candidate is rejected. It
is 1.5 on A-List and Breakout, 3.0 on Dip Buys, and has never been tuned.

**The score works, weakly.** On A-List's 44 taken positions the rank
correlation between conviction and P&L is **+0.18** — real but close to noise.
The useful structure is at the bottom, not across the range:

| conviction | n | mean | median | win |
|---|---|---|---|---|
| **1.5–2.0** | **17** | **+6.75%** | +14.81% | **59%** |
| 2.0–2.5 | 14 | +20.06% | +18.30% | 79% |
| 2.5–3.0 | 7 | +12.51% | +9.81% | 71% |
| 3.0+ | 6 | +19.83% | +17.55% | 67% |
| all | 44 | +13.68% | +15.66% | 68% |

**39% of the book sits in the bottom bucket and earns half the book's return.**

**Walk-forward.** Pick the floor that looked best on data before a cut date,
then measure what it actually earned after. Four cuts:

| | A-List | Insider Breakout |
|---|---|---|
| floor picked at every cut | **2.0** | 1.0 |
| average gain vs shipped | **+20.0 pts** | **−34.1 pts** |
| cuts where tuning helped | 2 of 4 | **0 of 4** |

And as a fixed floor over the whole period:

| floor | A-List | Breakout |
|---|---|---|
| 1.0 | 49.2% | **68.3%** |
| **1.5 (shipped)** | 62.4% | 64.2% |
| **2.0** | **70.8%** | 42.3% |
| 2.5 | 33.1% | 30.6% |

**A-List: raise 1.5 → 2.0.** It is the walk-forward pick at every cut, the best
fixed floor over the full period (+8.4 points), and max drawdown is unchanged
(23.2% against 22.6%). The mechanism is visible in the bucket table rather than
being a curve-fit.

**Breakout: do not touch it.** Its in-sample pick (1.0) lost out of sample at
every single cut, by 30 points or more three times out of four.

**Caveats that belong with this.** A-List drops from 44 trades to 38, and it
already fires only ~13 times a year — the floor buys return by taking fewer
alerts. The +20.0 average gain is dominated by one cut (+53.6); the median cut
gained far less. And this is still one book over 3.6 years in a bull market.


---

## The per-trade audit, 2026-08-23

Every one of the 172 simulated positions across the three books was walked
individually and each stored value re-derived from source — 2,040 checks in
two independent passes. **Zero exceptions, zero PIT violations.**

| check | n | what it re-derived |
|---|---|---|
| entry session | 172 | `filed_at` replayed through `framework.decision.entry_timing.entry_fill` |
| no look-ahead | 172 | before 16:00 ET → that close, after → next open. 129 (75%) were after-bell |
| entry price | 172 | matched to the cent against `prices.daily_prices` |
| exit price | 164 | that session's actual close |
| hold length | 164 | SPY session calendar vs. `hold_days` in the yaml |
| stop discipline | 164 | level rebuilt from yaml; every close walked entry→exit |
| career grade | 172 | re-scored at filing date, filing-grouped returns only |
| consecutive sells | 172 | recounted as filings, not lots |
| eligibility replay | 172 | every yaml filter through the shared `evaluate_filters` |
| SMA50 / SMA200 | 172 / 169 | recomputed from prior closes only |
| dip_3mo | 172 | point-to-point 90-calendar-day change |
| input as-of dates | 172 | every dated input ≤ its own `filing_date` |

All 19 stop exits landed on the **first** close at or below the level, and no
time exit concealed a breach. Entry timing and eligibility were replayed
through the same two functions `cw_runner` calls, so a divergence between the
published book and the live alerts would have surfaced here as a failure.

**Three defects were found and all three were in the audit, not the
pipeline** — worth recording, because it is the reason to trust the result:
the consecutive-sells recount assumed the audited trade was the last event in
its filing (false for SST, where one accession holds both a buy and a sell);
`dip_3mo` was recomputed as decline-from-peak rather than point-to-point; and
a column alias dropped `consecutive_sells_before`, flagging all 40 Dip Buys
positions ineligible. Every time the stored value was right.

Two things the audit surfaced that are not defects but change what should be
said: the A-List backstop clears by 2.5 points rather than a wide margin, and
Insider Breakout's best position survived its stop by 0.7. Both are recorded
above.

This audit verifies that every position is what the pipeline claims it is. It
does not touch the two caveats that sit above it and are not defects: these
configurations were selected on the same data they are measured over, and the
books have only ever run in a bull market.

---

## 10b5-1 planned purchases are not admitted, from 2026-08-24

The books have always refused `planned_sell` — the sell side only ever admitted
discretionary classes. The buy side did not apply the same test, so
`planned_buy` was admitted. That was an asymmetry rather than a decision, and
nobody had measured it.

Measured over the full history (2019–2026), abnormal return against SPY:

| class | n | abnormal 30d | beat SPY |
|---|---|---|---|
| `discretionary_buy` | 112,831 | **+1.71%** | 47.3% |
| `planned_buy` | 1,020 | **−2.22%** | 43.0% |

Negative, not merely weaker, and the gap is 3.93 points. A purchase scheduled
months in advance says nothing about what the insider thinks on the day it
executes — that is what "planned" means.

**Exactly one `planned_buy` ever reached a published book**: COE in A-List on
2026-05-19, which closed **−43.2%**, one of that book's two worst positions.
Removing it:

| | before | after |
|---|---|---|
| A-List blended CAGR | 64.6% | **69.8%** |
| A-List max DD, trade-row | 13.1% | **11.4%** |
| A-List P&L | $453,213 | **$512,433** |
| A-List closed trades | 44 | 44 |

Insider Breakout and Insider Dip Buys are byte-identical before and after —
neither ever held one.

Two things worth stating plainly. **The +5.2 points are a consequence of the
rule, not the reason for it**; the rule would be right if the number had gone
the other way, and it is applied on the strength of the 112,831-vs-1,020
comparison rather than on one position. And **the trade count did not move**:
the freed slot admitted a replacement. That is the same non-monotonic
admission behaviour documented under the conviction gate — what changes the
book is *which* trades are admitted, not how many.

Enforced in both surfaces — `simulate_strategy_portfolio.py` and
`cw_runner.py` — as `(t.signal_class IS NULL OR t.signal_class <> 'planned_buy')`.
The NULL arm is deliberate: unclassified rows still trade, and
`tests/unit/test_planned_buys_are_excluded.py` fails the build if either
surface drops the exclusion or the NULL guard.

## Every time the numbers moved, and why they should stop

| date | change | cause |
|---|---|---|
| 2026-08-18 | 52.3% → 43.5% | `filed_at` read in the server timezone |
| 2026-08-19 | 43.1% → 48.8% | `filed_at` Eastern for 2026 rows; after-bell fills moved to the next open |
| 2026-08-19 | 48.8% → 55.4% | `is_largest_ever` wrong on 23.7% of flags |
| 2026-08-20 | definition settled | sleeve vs blended, and the period bug |
| 2026-08-20 | per-strategy position caps | one shared `10 x 10%` left two books ~85% in SPY |
| 2026-08-20 | stop moved −30% → −50% | the −30% was a simulator-only override the live runner never applied |
| 2026-08-22 | tranche correction | the scorer counted execution lots as separate trades; 21% of grades moved, A-List resized to `3 x 33%`, Breakout's gate widened to A+/A/B |
| 2026-08-23 | Breakout stop −50% → −20% | a working stop: daily drawdown 49.9% → 43.8% with CAGR up. 19 of its 85 positions now exit on it |
| 2026-08-23 | **no change** — per-trade audit | all 172 positions re-derived from source, 2,040 checks, zero exceptions. The figures survived verification rather than moving |
| 2026-08-24 | A-List 64.6% → 69.8% | `planned_buy` admitted on the buy side while `planned_sell` was refused on the sell side. One position ever, COE, −43.2% |

Every move before 2026-08-20 was a definitional or data defect, none a market
event, and each now has a regression test: `test_entry_timing_eastern`,
`test_cumulative_signal_windows`, `test_published_returns`.

The 08-23 stop change is a deliberate parameter decision, taken before the
figures are marketed rather than after. Everything above it was a defect, and
so is the 08-24 row — an asymmetry between how the two sides of the book
treated 10b5-1, which is a defect even though correcting it happened to help.

Anything that moves these numbers from here should be a new trade or a
decision made on purpose — not a discovery. That has now been said twice and
been wrong twice, so the honest version is: the defects found so far were all
found by looking, and nobody has yet done a pass that came back empty on the
first try.

---

## The stops — −50% on two books, −20% on Insider Breakout

`simulate_strategy_portfolio.py` carried `STOP_LOSS_PCT = -0.30` as a module
constant from 2026-05-12. All three yamls said `stop_loss_pct: null`, and
`cw_runner.py` — the live alert runner — reads the yaml. **For three months the
published book simulated a stop that subscribers' alerts never applied.**

Measured before changing it, split at 2025-06:

| | −30% in | no-stop in | −30% out | no-stop out |
|---|---|---|---|---|
| A-List | 43.6% | 50.0% | 79.3% | 80.9% |
| Breakout | 38.7% | 43.1% | 42.3% | 42.3% |
| Dip Buys | 34.8% | 34.8% | 40.1% | 40.2% |

No variant was ever better with the stop. Max drawdown was unchanged in-sample
(23.5% either way on A-List) and the stop bought 1.2 points out-of-sample while
costing 1.6 of CAGR.

**−50% and no-stop produced identical results on all three books** at the time
that was measured. Nothing any of them held has closed below −50% in 3.6 years,
so on A-List Buys and Insider Dip Buys the backstop is still inert in every
published figure while capping a genuine blowup. That is why the answer there
is −50% rather than no stop: it costs nothing measurable and the sample
contains no bankruptcy, only a bull market.

**Inert is not the same as far away.** The per-trade audit on 2026-08-23 found
A-List's worst holding closed at **−47.5%** (LRHC) and its second worst at
−47.1% (COE). The backstop has never fired, but it cleared by 2.5 points, not
by a comfortable margin. Insider Dip Buys is genuinely far off it at −24.8%.

**Insider Breakout is different and its stop is not a backstop.** On
2026-08-23 it moved to **−20%**, which is a working stop: it has closed **19 of
that book's 85 positions**, averaging −22.2% on exit. It was taken because
tightening cut the daily drawdown from 49.9% to 43.8% *and raised* CAGR — the
one lever that improved both. Anything written about "the stop" that does not
name a book is now wrong; the value lives in each strategy yaml and the two
surfaces read it from there.

The parameter now lives in the yaml, which both the simulator and the live
runner read. `tests/unit/test_stop_is_config_driven.py` fails the build if a
module-level constant reappears or if the two surfaces resolve different values
from the same config.

**What this does not do.** The stop is still evaluated on the CLOSE, not the
intraday low, and re-measured against the current configurations on 2026-08-23
that choice is worth far more than it used to be. Four positions — all in
Insider Breakout, all against its new −20% level — traded through the stop
during the day and closed back above it:

| | entry | worst intraday | actual result |
|---|---|---|---|
| PDYN | 2024-11-05 | −20.7% | **+286.9%** |
| ENVX | 2023-04-25 | −30.7% | +0.4% |
| FOUR | 2024-06-07 | −20.2% | −1.8% |
| DNTH | 2023-09-29 | −25.6% | −14.0% |

A low-based rule would have cut all four at −20%, costing roughly **351 points
of P&L** between them. The close-only check is doing real work as a whipsaw
filter.

**It also means Insider Breakout's headline rests on 0.7 of a percentage
point.** PDYN is the single best position in that book and it survived because
it closed above a level it had already traded through. That is not an argument
for changing the rule — it is an argument for knowing how much of the figure
sits on one position, which the concentration caveat above quantifies at 88%
for the top ten.

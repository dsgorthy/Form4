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

| | blended CAGR | SPY | **excess** | max DD | avg deployed |
|---|---|---|---|---|---|
| A-List Buys (`quality_notrend`) | 64.3% | 19.1% | **+45.2** | 13.1% / 22.6% | 62% |
| Insider Breakout (`quality_momentum`) | 63.9% | 21.2% | **+42.7** | **43.8%** | 63% |
| Insider Dip Buys (`reversal_dip`) | 38.1% | 21.7% | **+16.4** | 11.3% | 25% |

Measured 2026-08-22 after the tranche correction (below). **Two drawdown
figures, and the larger one is the honest one.** The published `max_drawdown`
walks `equity_after` on CLOSED TRADE ROWS, so it samples the book only at
exits and cannot see a drawdown that opens and closes between them. The second
figure walks the blended equity curve daily, marking open positions to market.
For Insider Breakout the difference is 31.5% against 49.9% — a subscriber
experiences the second.

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

**The result is fragile to the conviction gate — `min_conviction`, still 1.5
and still never tuned — and the 2026-08-20 perturbation study is now STALE** — it was run before the tranche correction
changed both `career_grade` and `is_largest_ever`, which are two of the gate's
inputs. Its finding was a ±5 band around a 50% sleeve from a ±0.25 perturbation,
less than one of the ~12 half-point components. The *shape* is a property of
the gate and should survive; the centre must be re-measured before anyone
quotes a band. Do not reuse those numbers.

**Year concentration and the annual table below are STALE** — measured
2026-08-20, before the correction rebuilt every book. Re-run before quoting.

| year | A-List | Breakout | Dip Buys | SPY |
|---|---|---|---|---|
| 2023 | +63.9% | +44.9% | +48.4% | +24.8% |
| 2024 | +26.3% | +47.5% | +38.4% | +24.0% |
| 2025 | +86.5% | +34.8% | +43.8% | +16.6% |
| 2026 | +40.8% | +41.6% | **+3.9%** | +12.6% |

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

**What is solid.** The bootstrap CIs quoted here on 2026-08-20 predate the
correction and are stale in level, but the direction held under it: the
unfitted trade-level test after regrading showed trades keeping A+/A returning
+17.8% median against +6.4% for those losing it. The corrected grade ranks
better even where the book it feeds does worse. Re-run the CIs before quoting.

**Insider Dip Buys is now verified** (2026-08-23). Its primary filter lost a
third of its qualifying signals to the tranche correction, so the threshold was
re-tested: `min_consecutive_sells` is flat from 10 down to 6 (36.5 / 36.4 /
36.6% CAGR) and worse below, and 10 has the best Sharpe. Sizing was swept from
`6 x 17%` to `2 x 50%`; the shipped `4 x 25%` is best on Sharpe. No change
warranted — but note it is only 19% deployed, so four-fifths of what a holder
experiences is SPY, and its Sharpe of 0.70 is the weakest of the three.

---

## Why the numbers moved four times, and why they should stop

| date | change | cause |
|---|---|---|
| 2026-08-18 | 52.3% → 43.5% | `filed_at` read in the server timezone |
| 2026-08-19 | 43.1% → 48.8% | `filed_at` Eastern for 2026 rows; after-bell fills moved to the next open |
| 2026-08-19 | 48.8% → 55.4% | `is_largest_ever` wrong on 23.7% of flags |
| 2026-08-20 | definition settled | sleeve vs blended, and the period bug |
| 2026-08-20 | per-strategy position caps | one shared `10 x 10%` left two books ~85% in SPY |
| 2026-08-20 | stop moved −30% → −50% | the −30% was a simulator-only override the live runner never applied |
| 2026-08-22 | tranche correction | the scorer counted execution lots as separate trades; 21% of grades moved, A-List resized to `3 x 33%`, Breakout's gate widened to A+/A/B |

Every move before 2026-08-20 was a definitional or data defect, none a market
event, and each now has a regression test: `test_entry_timing_eastern`,
`test_cumulative_signal_windows`, `test_published_returns`.

The last two rows are deliberate parameter decisions, taken before the figures
are marketed rather than after. The rows above them were defects. Anything that
moves these numbers from here should be a new trade or a decision made on
purpose — not a discovery.

---

## The stop, and why it is −50%

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

**−50% and no-stop produced identical results on all three books.** Nothing any
of them held has ever closed below −50% in 3.6 years, so the backstop is inert
in every published figure while still capping a genuine blowup. That is why the
answer is −50% rather than removing the stop: it costs nothing measurable and
the sample contains no bankruptcy, only a bull market.

The parameter now lives in the yaml, which both the simulator and the live
runner read. `tests/unit/test_stop_is_config_driven.py` fails the build if a
module-level constant reappears or if the two surfaces resolve different values
from the same config.

**What this does not do.** The stop is still evaluated on the CLOSE, not the
intraday low. Modelling it against the low was measured on 2026-08-20 and would
have converted four of 255 positions into −30% losses, two of which actually
finished positive (CATX +8.2%, ENVX +0.4%). All four touched the level intraday
and recovered — the close-only check is acting as a whipsaw filter and is worth
keeping.

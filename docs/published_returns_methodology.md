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

| | blended CAGR | SPY | **excess** | avg deployed |
|---|---|---|---|---|
| A-List Buys (`quality_notrend`) | 62.8% | 21.2% | **+41.7** | 60% |
| Insider Breakout (`quality_momentum`) | 48.0% | 21.2% | **+26.8** | 42% |
| Insider Dip Buys (`reversal_dip`) | 35.9% | 21.2% | **+14.7** | 25% |

Measured 2026-08-20 after the stop correction below. The SPY figure drifts by a
tenth or two as the window end moves; it is recomputed from the same day range
on every measurement rather than carried forward.

Position caps were set per strategy on 2026-08-20 rather than sharing one
`10 x 10%`. Breakout and Dip Buys had `max_concurrent: 10` against books that
hold 2.4 and 1.6 positions, so the slot count was decoration and only the 10%
size was live — leaving 81% and 86% of their capital in SPY. Breakout is now
`5 x 20%`, Dip Buys `4 x 25%`; A-List keeps `10 x 10%` because its slot count
genuinely binds. All three cap at 100% gross, so none can lever.

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

Even after the resize, Insider Dip Buys is only 28% deployed — roughly
seven-tenths of what a holder experiences is the index. Quoting 37.4% without
the benchmark implies a stock-picking result that is mostly beta. **+16.0 over
SPY** is the actual claim, and unlike the raw CAGR it survives the
sleeve-vs-blended question untouched.

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

**The result is fragile to the conviction gate.** `min_conviction` is 1.5 and
the score is ~12 half-point components, so a ±0.25 perturbation — less than one
component — gives A-List Buys a CAGR range of **46.2%–55.7%, median 49.7%**,
with the unperturbed 55.4% above 13 of 14 draws. Tie-breaking contributes
nothing (+0..0.004 noise moves the book $0); it is the gate admitting a
different *set* of trades. Read the sleeve figure as ~50% ± 5.

**One year carries A-List Buys.** 2025 is 47% of total P&L. 2024 averaged
+2.72% per trade.

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

**What is solid.** Bootstrap CI on the mean trade excludes zero for all three:
A-List +12.73% [+8.22, +17.54], Breakout +12.76% [+5.63, +21.02], Dip Buys
+6.03% [+1.94, +10.14]. There is a real edge at the trade level. The dispute is
only over its size — and for Dip Buys the interval now nearly touches zero.

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

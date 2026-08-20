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
| A-List Buys (`quality_notrend`) | 58.6% | 21.4% | **+37.2** | 59% |
| Insider Breakout (`quality_momentum`) | 45.3% | 21.4% | **+24.0** | 41% |
| Insider Dip Buys (`reversal_dip`) | 37.4% | 21.3% | **+16.0** | 28% |

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

**Two directional biases, both small.** ~2% of qualifying candidates are
dropped for want of a price series — and a stock with no forward prices usually
stopped trading, so it removes the worst outcomes rather than a random sample.
The −30% stop is evaluated on the close, so gaps fill below it (two of twelve
at −43.6% and −42.3%).

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

Every move before 2026-08-20 was a definitional or data defect, none a market
event, and each now has a regression test: `test_entry_timing_eastern`,
`test_cumulative_signal_windows`, `test_published_returns`.

**This is the last planned move.** The sizing change is the one deliberate
parameter decision on the list, taken before the figures are marketed rather
than after. Anything that moves these numbers from here should be a new trade
or a decision made on purpose — not a discovery.

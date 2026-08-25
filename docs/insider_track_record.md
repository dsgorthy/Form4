# The insider Track Record block

*Rebuilt 2026-08-25. Prior to that date every number in this block was wrong,
in three independent ways at once.*

## What it is

The Buy Track Record and Sell Track Record cards on `/insider/[id]` — a 3×N
grid of Accuracy, Avg Move, Alpha and Scored across 7d / 30d / 90d windows.

## The one basis

Every figure in the block, both sides, all three windows, is computed the same
way. There is no second source.

1. **One row per FILING, never per execution lot.** A purchase filled in five
   tranches is one decision, not five. See the `filing_not_lot_grouping`
   memory — the same error cost A-List its published headline in August.
2. **Discretionary filings only.** Derived from `MEANINGFUL_CLASSES`, never
   typed out. A 10b5-1 plan sale, a tax withholding and an option exercise are
   not timing decisions and must not be scored as though someone chose the
   date.
3. **The same exclusions as the filing count rendered directly above it** —
   `superseded_by IS NULL`, `is_derivative = 0`, not a duplicate — so the
   header and the denominator describe one population.
4. **Suppressed below `MIN_SCORED_FILINGS`** (5). Below the floor the block
   publishes the count and nothing else.
5. **The denominator is always visible**, in the `Scored` row, at every n
   including zero.

It is computed on the fly in `api/routers/insiders.py`. Measured cost: 81 ms
for the heaviest insider in the database (Zuckerberg, 15,465 sell lots), ~9 ms
typical. `tests/unit/test_track_record_is_one_basis.py` fails the build if any
of the above drifts.

## What was wrong before

**Defect 1 — two denominators in one table row.** 7d came from a
filing-grouped API query; 30d and 90d came straight from
`insider_track_records`, which counts lots. Romano Gianluca (insider 27782)
rendered a header reading **"Filings 19"** directly above an accuracy computed
over **154 lots**.

**Defect 2 — mechanical trades scored as decisions.** No `signal_class` filter
existed anywhere in the block. 140 of Romano's 154 lots were 10b5-1 plan
sales. Across insiders with a scored sell record, **81.9%** contain mechanical
trades and **23.3%** are *entirely* mechanical — 9,474 pages whose whole sell
record is plan executions, withholding and exercises.

**Defect 3 — the columns froze in February 2026.** The daily writer
`pit_scoring.sync_to_track_records` (step 7 of `refresh_features_daily.sh`)
writes score, counts and dates and **never the win-rate columns**. The only
writer that touches them is `backfill.compute_track_records`, which is legacy
SQLite and not in the chain. Coverage of `sell_win_rate_30d` by when an
insider's record starts:

| record starts | has a 30d win rate |
|---|---|
| 2025-12 | 97.1% |
| 2026-01 | 92.1% |
| 2026-02 | 28.1% |
| 2026-03 | 2.2% |
| 2026-07 | **0.0%** |

## Why it mattered per page and not in aggregate

Across 31,165 comparable insiders the mean shifts only **−0.26pt**. But
**22.9% flip sign** and 43% move more than two points. This was never a
systematic bias to be corrected with an offset — it was per-page accuracy, and
the page is what people look at.

The direction is not random either. Plan-heavy sellers looked like *bad
timers*, because their mechanical sales during a rising stock were scored as
choices:

| insider | published before | correct basis |
|---|---|---|
| Romano Gianluca | 31% / **+6.6%** over 154 lots | 50% / **−0.6%** over 6 filings |
| Michael Saylor | 46% / **+25.0%** over 2,034 lots | 57% / **−0.5%** over 7 filings |
| Mark Zuckerberg | 34% / +2.1% over 15,465 lots | 35% / +1.3% over 732 filings |
| Patrizio Vinciarelli | 60% / −7.4% | 30% / +3.9% |

## Why there is a floor

Filtering to discretionary filings shrinks the basis hard, and an accuracy
rendered to the nearest point over one or two filings can only read 0%, 50% or
100%. Distribution of the corrected basis, for insiders with ≥5 lots:

| discretionary filings | sell records | buy records |
|---|---|---|
| 0 | 2.1% | 0.2% |
| 1 | 3.6% | 5.6% |
| 2 | 5.0% | 6.4% |
| 3–4 | 15.9% | 21.6% |
| 5–9 | 38.7% | 42.2% |
| 10–24 | 26.6% | 18.0% |
| 25+ | 8.2% | 6.0% |

A floor of 5 keeps the block on **73.5%** of sell records and **66.2%** of buy
records. Losing it entirely is informative in its own right: an insider with no
discretionary sells is someone who only ever sells on a plan.

Note the floor is applied per window, so a recent insider can show 7d and 30d
and correctly show nothing at 90d.

## Deliberately not changed

- **`best_window`** — still read from `insider_track_records`, still frozen on
  the same lot-based basis, and it drives both a StatBox and the `*` marker in
  this block. Redefining "best" is a product decision, not part of this fix.
- **`recent_win_rate_7d`** — nothing reads it.
- **Raw returns, not abnormal**, for Avg Move. Matches what the block always
  displayed; switching to market-adjusted is a separate change.

## Retired

`migrations/2026-08-25_retire_track_record_win_rates.sql` copies the 19 stored
win-rate columns to `insider_track_records_retired_win_rates`, then NULLs them.
The copy is not optional: `backfill.compute_track_records` takes a
`sqlite3.Connection` and cannot run against Postgres, so there is no other way
back.

# Admin data-health dashboard — spec

**Status: BACKLOG, not built.** Requested 2026-08-26.

## Why this exists

On 2026-08-26 we found that the Form 4 ingestion had been discarding roughly
one filing in eight since April, permanently, and that the historical record
held **48.6%** of what EDGAR published — 939,453 of 1,933,494 filings, uniformly
45–53% in every single year.

It ran undetected for five months. Not because monitoring was absent, but
because of what the monitoring asked:

- the job was green — it exited 0 every run
- the heartbeat was fresh — the loop turned every five minutes
- row counts looked normal — ~150k filings a year, as always
- `processed_filings` said the work was done — because the bug *wrote* that

It was found because a person opened Zillow and noticed there were no 2020
filings.

**Every check we had asked our own database whether it was happy.** That is the
gap this dashboard closes, and it dictates the one rule below.

## The one design rule

> **At least one panel must compare against a source we do not control, and the
> drift panels must show COMPOSITION over time, not totals.**

Totals looked correct throughout. What changed was *which* filings we held and
*what mix* of transaction codes survived — neither of which any total can show.
A dashboard of counts and green ticks would have rendered beautifully every day
of the outage.

## Access

Already solved — do not build new auth. `ADMIN_USER_IDS` (env, Clerk IDs) plus
the `require_admin` dependency in `api/auth.py` is exactly "only me". Every new
endpoint takes `user: UserContext = Depends(require_admin)`.

## What already exists — extend, do not duplicate

| surface | what it covers |
|---|---|
| `/admin` | landing |
| `/admin/jobs` | launchd job status — **legacy; see the Dagster note below** |
| `/admin/pipelines` | `pipeline_runs` — batch jobs wrapped in `pipeline_run()` |
| `/admin/strategies`, `/admin/strategies/[name]` | per-strategy evaluations, freshness, positions |
| `api/routers/admin_diagnostics.py` | 9 endpoints backing the above |

New endpoints belong in `admin_diagnostics.py`. New pages belong under
`/admin/`. Reuse the existing tables, chart theme (`lib/echarts-theme`) and dark
palette; check `reference_product_audit.md` before adding any component.

## Panels

Each is specified by **what it would have caught**, because a panel that cannot
name an incident it detects is decoration.

### A. Coverage vs EDGAR — the one that matters

Daily and monthly: Form 4 filings EDGAR published vs filings we hold, with the
shortfall called out.

*Catches:* the ~12% daily leak, and the 48.6% historical gap. Nothing else on
this page would have.

*Source:* `scripts/reconcile_form4_coverage.py` already computes this against
EDGAR's daily index. **Do not hit EDGAR on page load** — have the reconciliation
write to a small `coverage_daily` table and read that. Ground truth is
`/Archives/edgar/daily-index/…/form.{YYYYMMDD}.idx`, never EFTS: EFTS caps at
10,000 hits and reports the cap as the total, which is how this happened.
Count DISTINCT accessions; a Form 4 appears once per filer (~2.07x inflation).

*Needs:* new table, new endpoint, and the reconciliation on a schedule.

### B. Transaction-code mix over time

Stacked share of `trades.trans_code` by month — P, S, A, M, F, G, plus the
formerly-dropped C, J, D, L, I, U, W, O.

*Catches:* the April 2026 derivative-classifier change; the parser silently
dropping 6.29% of transactions; any future filter that quietly narrows what we
store. A composition chart shows this instantly. A row count never would.

*Source:* `trades` only. No new data.

### C. signal_class mix over time

Same treatment for `signal_class`. Watch `discretionary_buy` / `discretionary_sell`
(the meaningful 28.4%) against `compensation`, `option_exercise`,
`tax_withholding`, `planned_*`, `derivative`.

*Catches:* classification drift, and the tagger falling behind ingestion — the
condition that makes `NOT EXISTS` guards fail open and lets untagged rows pass
filters they should fail.

### D. Ingestion health

From `processed_filings`, now that it carries `status`:

- rows by status (`ok` / `empty` / `failed` / `abandoned`) over time
- **zero-trade rate by month** — the single number that went 0.0% through
  Feb 2026 to 21.5% in April and 25.5% in July
- current retry-queue depth, and anything `abandoned`

*Catches:* the original leak, on the day it started. This is the cheapest,
highest-value panel after A.

### E. Dagster runs — and what is NOT yet in Dagster

Run status, duration and failures per asset/job.

*Note:* genuinely new. `/admin/pipelines` reads `pipeline_runs`, which is the
launchd batch jobs — **not** Dagster. Dagster state lives in a **separate
database** (`dagster_runs`, its own PG database on Studio), so this needs a
second connection or a small sync. Do not assume `get_connection()` reaches it.

**Dagster owns scheduling. launchd is legacy and is being retired.** This panel
must therefore show BOTH, and make the remainder obvious: a count of scheduled
work still on launchd, with the list, so the migration debt is visible on the
page rather than in someone's memory. Audited 2026-08-26: 6 jobs on Dagster,
~20 units of scheduled work still on launchd. See
`reference_dagster_owns_scheduling` and the audit in
`project_2026-08-26_launchd_to_dagster_audit`.

### F. Strategy operational metadata

Extend `/admin/strategies/[name]` rather than starting a page:

- candidate funnel per run: evaluated → passed filters → conviction gate → alerted
- alert cadence, and **time since last alert per strategy**
- last successful run vs last *attempted* run

*Catches:* the failure in `feedback_liveness_is_not_health` — A-List's runner was
dead for five trading days while every check said green, because a fresh
heartbeat proves the loop turns, not that the cycle inside it did anything.
"Zero candidates for five days" is the signal; nothing surfaces it today.

### G. Freshness contracts

Already at `/admin/freshness`. Surface it on the overview rather than rebuilding.
Remember the contract is business-hours-aware (`business_hours_only`), so a
Monday morning is not staleness.

## Explicitly NOT in scope

- Anything user-facing. This is an operator tool.
- Recomputing published figures. The dashboard **reads**; it never writes to
  `trades`, `strategy_portfolio` or any grade.
- A second definition of anything already defined once. Ratings come from
  `api/ratings.py`, meaningful filings from `MEANINGFUL_CLASSES`, strategy labels
  from `api/public_fields.STRATEGIES`. A dashboard is a very easy place to
  accidentally fork a definition.

## Open questions for Derek

1. **Alerting, or just eyes?** A page only works if someone opens it. Panel A
   and Panel D are the two worth paging on — should coverage < 99% or a
   zero-trade-rate spike send email/Telegram, or is a dashboard enough?
2. **Retention.** Composition-over-time needs history. Compute nightly into a
   rollup table, or aggregate `trades` live per page load? The latter is simpler
   but gets slow at 7.2M rows.
3. **Dagster.** Cross-database read, or have Dagster write a summary row into
   `form4` at the end of each run (the `pipeline_run()` pattern already in use)?

## Related

`docs/data_flow_spec.md`, `docs/RUNBOOKS.md`,
memory: `project_2026-08-26_form4_ingestion_loss`, `feedback_liveness_is_not_health`,
`feedback_derived_tag_guards_fail_open`, `feedback_monitor_budgets_follow_schedules`.

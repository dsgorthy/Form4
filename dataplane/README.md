# dataplane — Pyrrho data plane

The data + signal foundation under Pyrrho. Modular alt-data ingestion +
PIT-correct signal pipelines, with the discipline locked in by code rather
than convention.

## Why this package exists

Build the data layer first. Nothing in this package depends on Dagster, dbt,
Claude, MCP, or any UI. It is the substrate everything else sits on.

Every output — raw price observation, insider quality score, sentiment z-score —
lands as a row in a single canonical table (`signal_observations`) keyed by
`(signal_id, ticker, as_of_date)`. Downstream consumers (Claude, strategies,
backtests) query this table only.

See `project_2026-06-10_pyrrho_data_plane.md` in Claude memory for the full
architecture decisions.

## Layout

```
dataplane/
├── README.md              ← this file
├── sqitch.conf            ← Sqitch config (Postgres target)
├── sqitch/
│   ├── sqitch.plan        ← migration manifest
│   ├── deploy/            ← per-change DDL
│   ├── verify/            ← per-change assertion (catches half-applied state)
│   └── revert/            ← per-change rollback
├── dataplane/             ← the Python package
│   ├── __init__.py        ← public exports
│   ├── observation.py     ← SignalObservation dataclass
│   ├── upstream.py        ← Upstream contract (with PIT lag)
│   ├── signal.py          ← Signal base class
│   ├── pit.py             ← PIT.strict, PITViolationError
│   ├── testing.py         ← PITTestCase, PITValidator, time-frozen view
│   ├── catalog.py         ← signal_definitions register/lookup
│   └── io.py              ← signal_observations write/read helpers
├── signals/               ← one file per signal definition
│   └── insider/
│       └── career_grade_v3.py    ← reference port from Form4
└── tests/                 ← pytest tests
```

## Quick start (operator workflow)

```bash
# Apply migrations to the dev database (Studio)
cd dataplane
sqitch deploy db:pg:pyrrho_data_dev
sqitch verify db:pg:pyrrho_data_dev

# Run the test suite
python3 -m pytest tests/ -v

# Compute a signal manually (one-off)
python3 -m signals.insider.career_grade_v3 --ticker AAPL --as-of 2026-06-01

# Register a signal into the catalog
python3 -m dataplane.catalog register signals/insider/career_grade_v3.py
```

## Adding a new signal

1. Create `signals/<class>/<name>_v<n>.py`. Subclass `Signal`.
2. Declare `signal_id`, `version`, `owner`, `sla_hours`, `upstream`,
   `output_schema`, and `business_hours_only`.
3. Implement `compute(ticker, as_of)`. Read upstream data exclusively via
   `self.read(...)`. Direct DB access raises `PITViolationError`.
4. Declare `test_cases` — list of `PITTestCase` tuples.
5. Run `python3 -m pytest tests/test_<name>.py`. `PITValidator` runs each
   test case twice (full DB vs time-frozen view) and fails on mismatch.
6. If you change schema: `sqitch add my_change --note "..."`, write
   deploy + verify + revert SQL, then `sqitch deploy db:pg:pyrrho_data_dev`.

## The PIT contract

Every signal declares its `upstream` list — each entry is a dependency
plus a `pit_lag` (timedelta). The framework guarantees:

- `self.read(signal_id, ticker, as_of)` returns rows where
  `as_of_date <= (current_as_of - upstream.pit_lag)`.
- In time-frozen test mode, additionally hides rows where
  `ingested_at > current_as_of`. This catches the class of bug where a
  signal works in production (because the data has been ingested by now)
  but would not have worked at the historical as_of (because the data
  hadn't landed yet).

Three enforcement layers:
1. **Compile-time** — `@PIT.strict` decorator + `Signal.read()` is the
   only data accessor. Direct DB imports are caught by `PITValidator`.
2. **Test-time** — every PR runs `PITValidator` on every signal. Diff
   between normal-mode and frozen-mode = PIT violation = CI fails.
3. **Runtime** — backfills walk chronologically; the framework refuses
   to read past `(current_as_of - upstream.pit_lag)`.

## Signal versioning

Signals are namespaced + versioned: `insider.career_grade.v3`,
`options.unusual_volume.v1`, etc. **Old versions stay alive.** When you
ship `v4`, both `v3` and `v4` write to `signal_observations` under
different `signal_id` strings. Strategies declare which version they
consume. After 90 days of clean operation, an explicit op archives the
old version's rows to the Parquet lake.

This is how you avoid the "we tuned the signal but forgot to invalidate
the backtest" class of bug.

## Storage model

| Where | What |
|---|---|
| Postgres on Studio internal SSD | `signal_definitions`, `signal_observations` (hot 90d), app tables. High IOPS path. |
| Parquet lake on 115 TB RAID-6 array (`/Volumes/data/lake/`, when attached) | Raw ingestion lands here. Monthly partitions. Older `signal_observations` partitions roll out here. |
| DuckDB | Reads the Parquet lake for backfills + analytics. Same SQL dialect mostly. |
| Backups | rsync to `/Volumes/data/backups/` on the same array (weekly + monthly retention). |

## Migration discipline (Sqitch)

Every schema change is three files in `sqitch/`:
- `deploy/<change>.sql` — the change itself
- `verify/<change>.sql` — assertion that the change landed correctly
- `revert/<change>.sql` — rollback

The verify step is the differentiator over plain SQL files: it catches
half-applied changes (DDL ran but data didn't backfill, index missing
after CREATE INDEX failed silently, etc).

To add a change:
```bash
sqitch add my_change --requires previous_change \
    --note "Human description"
# Edit the three generated files
sqitch deploy db:pg:pyrrho_data_dev
```

State per target is tracked in Sqitch's metadata schema on each database.
Dev can be ahead of prod; Sqitch knows.

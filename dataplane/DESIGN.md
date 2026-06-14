# Pyrrho Dataplane — Design Reference

Status: living document. Loosely fleshed by design — update as built.
Last updated: 2026-06-10. Owner: derek.

The four-layer vision: **data → signals → reasoning (MCP) → action**.
This doc covers layer 1 (the data layer) plus the layer-4 strategy
contract the data layer must serve. We build nothing above layer 1
until layer 1 is nailed; the strategy spec exists here so layer-1
priorities point at what the upper layers will actually need.

MVP consumer is Derek only. Strategies are trusted YAML in this repo —
no user-facing expression DSL until the product is multi-user.

## System diagram

```
                        ┌──────────────────────────────────────────────────┐
                        │                     SOURCES                      │
                        │  EDGAR Form4 (5-min)   Alpaca (1-min / EOD)      │
                        │  Earnings (quarterly)  Sentiment, flow, … (TBD)  │
                        └────────┬───────────────────┬─────────────────────┘
                                 │ ingestion signals (Dagster assets,
                                 │ auto-discovered from signals/<class>/*.py)
              high-frequency raw │                   │ decision-cadence rows
                                 ▼                   ▼
              ┌──────────────────────┐   ┌─────────────────────────────────┐
              │  Parquet lake        │   │  Postgres pyrrho_data_*         │
              │  /Volumes/data/lake/ │   │  signal_definitions  (catalog)  │
              │  115TB RAID-6 array  │   │  signal_observations            │
              │  minute bars, bulk   │   │  PK (signal_id,ticker,as_of)    │
              │  raw — NOT in PG     │   │  hot ~90d + all derived signals │
              └──────────┬───────────┘   └──────────┬──────────────────────┘
                         │ DuckDB reads             │ PIT read() ≤ as_of
                         └────────────┬─────────────┘
                                      ▼
                      derived / composite signals (same contract,
                      upstream declared, @PIT.strict enforced)
                                      │
                                      ▼
                  strategies (dataplane/strategies/*.yaml)
                  generic StrategySignal: triggers + gates + emit
                  every evaluation writes a strategy.* observation
                  (triggered OR suppressed, with reasons) ⇒ replay = backfill
                                      │ triggered=true, cooldown ok
                                      ▼
                              ntfy push → Derek's phone

  ── orchestration rail ──────────────────────────────────────────────
  Dagster: daily_signals job 04:30 UTC · dbt_marts 05:00 UTC ·
  run_failure_sensor → ntfy · daemon+webserver launchd on Studio
  (UI tailnet-only 100.78.9.66:3030) · run history in dagster_runs PG
```

## Storage schema

Sqitch-managed (`dataplane/sqitch/`), applied to `pyrrho_data_dev` on Studio.

**`signal_class` enum** — price, volume, insider, options_flow, fundamental,
analyst, sentiment, congress, earnings, macro, composite.

**`signal_definitions`** — the catalog. `(signal_id, version)` PK,
signal_class, description, owner, output_schema jsonb, upstream jsonb
(list of {signal_id, pit_lag_seconds}), sla_hours, business_hours_only,
status (active/deprecated), registered_at, last_modified_at.

**`signal_observations`** — every computation, raw or derived.
PK `(signal_id, ticker, as_of_date)`. Columns: value jsonb, confidence
[0,1] nullable, source_run_id uuid, ingested_at (when written — backs
frozen-mode PIT tests), metadata jsonb.
- `as_of_date` = when this fact was *knowable*, never when computed.
- Writes are idempotent upserts (`ON CONFLICT` overwrite + bump
  ingested_at) — re-running any partition is always safe.
- Multi-event days: per-event timestamps; same-second collisions
  disambiguated by `microsecond = trade_id % 1e6` (deterministic).

**Planned, not yet migrated:**
- `signal_definitions.cadence` (interval) — declared ingestion cadence.
- Monthly partitioning of signal_observations when volume demands.
- Roll-off of >90d rows to the Parquet lake (post array attach).

**Storage boundary rule:** high-frequency raw data (1-min bars, tick-ish
feeds) goes to the Parquet lake as files, NEVER into signal_observations.
Postgres holds decision-cadence signals: events, dailies, derived values.
Rough math that forces this: 1-min bars × 5,000 tickers ≈ 2M rows/day.

**Lake volume (confirmed 2026-06-10):** Areca hardware-RAID-6 enclosure on
Studio; macOS sees one 112 TB disk, APFS as shipped, 102 TiB usable.
Mounted at `/Volumes/data` (volume renamed from "Areca Array"). Spotlight
indexing disabled. Top level: `lake/` + `backups/` (PG rsync target).
Sequential write ≈ 1.0 GB/s (dd, 2 GiB).

## The signal contract (code: `dataplane/dataplane/`)

A signal = one Python file in `signals/<class>/` subclassing `Signal`:
declared metadata (id, version, owner, sla, upstream, output_schema) +
one compute body. Auto-discovered into a daily-partitioned Dagster asset —
**zero scheduling code per feed**.

Two materialization modes:
- `per_ticker_per_day` — `compute(ticker, as_of)` → one observation.
  For derived/score-like signals.
- `per_partition_events` — `materialize_partition(date)` → list of
  observations with precise event timestamps. For raw event streams
  (Form 4 filings, earnings, news).

PIT enforcement (three layers, non-negotiable):
1. Compile-time: `@PIT.strict` + `self.read()` is the only sanctioned
   accessor; raises on undeclared upstream; subtracts declared `pit_lag`.
2. Test-time: PITValidator runs each test case in normal vs frozen mode
   (frozen adds `ingested_at < as_of`); any material diff = violation.
3. Runtime: backfills walk partitions chronologically.

`read(signal_id, ticker, as_of)` returns all upstream rows with
`as_of_date <= as_of - pit_lag`, newest first — i.e. **"state of the
world at time T"**, which is what makes look-back gates and cadence
mixing work (see below).

Planned extension: `read(..., lookback=timedelta)` to bound window
queries server-side (today the caller filters the returned history).

## Cadence model

**Cadence is an ingestion property, not a storage property.** Every
observation is just a timestamped row; cadence only determines how often
a materializer runs. Declared per signal; mixed freely in strategies.

| Feed | Cadence | Mechanism | Destination |
|---|---|---|---|
| Ticker bars (1-min) | 1 min | Dagster sensor / minutely schedule (post-array) | Parquet lake |
| Insider filings | 5 min | sensor re-materializing *today's* partition (upsert makes this safe); matches existing insider-fetch | signal_observations |
| EOD prices, derived dailies | daily | `daily_signals` job 04:30 UTC | signal_observations |
| Earnings, fundamentals | quarterly/event | daily partitions, rows land only on event days (sparse is fine) | signal_observations |

Cadence mixing rule for strategies: a gate always reads **the latest
upstream value at or before the trigger's timestamp** (that's just the
`read()` query), plus a `max_staleness` bound — if the freshest value is
older than tolerance, the gate **fails closed** (no alert) rather than
passing vacuously. A 5-min trigger gated on a daily signal is therefore
well-defined: it sees yesterday's close, and says so.

## Strategy spec (layer-4 contract — design only, build in MVP)

A strategy is **a composite signal plus an emit policy**: data, not code.
YAML in `dataplane/strategies/`, interpreted by one generic
`StrategySignal` class, registered in the catalog as
`strategy.<name>.<version>` with `signal_class='composite'`.

```yaml
strategy: agrade_drawdown_buy        # → strategy.agrade_drawdown_buy.v1
version: v1
cadence: 5min                        # evaluation cadence = trigger feed's
universe: all
triggers:                            # EDGE: a new observation matching
  - signal: insider.trades.raw.v1
    when: value.trade_type == 'buy' and value.value >= 50000
gates:                               # LEVEL: checked as-of trigger ts
  - signal: insider.career_grade.v3
    when: value.grade in ['A+', 'A']
    max_staleness: 30h
  - signal: price.daily.close.v1
    window: 90d                      # LOOK-BACK: series (T-90d, T]
    when: latest.close / earliest.close - 1 <= -0.10
    max_staleness: 24h
emit:
  channel: ntfy
  cooldown: 5d per ticker
```

Semantics:
- **Trigger** = edge: a new upstream observation matching `when`.
- **Gate** = level: latest value (or `window:` series) at the trigger's
  timestamp, via PIT `read()` — never future data, fail closed on stale.
- **Every evaluation writes an observation** — triggered or suppressed,
  with per-gate reasons in value. Audit and backtest are the same rows:
  **replaying a strategy = backfilling its asset.** Replay depth =
  min(upstream backfill depth).
- Inputs are interchangeable by construction: swapping a momentum gate
  for a sentiment gate is editing two YAML lines.
- Cadence reality: ~5-min end-to-end reaction is the practical floor
  (sources are themselves minutes-delayed); HFT is out of scope by
  architecture and by intent — our alpha horizon is days-to-weeks.

## MVP — end-to-end vertical slice

Goal: demonstrate define → backfill → schedule → trigger+gates(+look-back)
→ push, all PIT-honest, using Derek's reference strategy: *"A-grade
insider buys a stock that's down >10% over 3 months → alert."*

Conveniently this needs **zero new raw feeds** — all three inputs exist.

- **M1 — daily loop (the demo):**
  1. `read(..., lookback=)` param (small framework change)
  2. Backfill CLI: `python3 -m dataplane backfill <signal> --from --to
     [--tickers]` (was already next on the list)
  3. Backfill `price.daily.close` ~120d × universe + `trades.raw` ~90d
  4. Generic `StrategySignal` + YAML loader + `agrade_drawdown_buy.v1`
  5. Backfill the strategy 90d → **the demo artifact: every alert it
     would have fired since March, each with its reasons** — then live
     daily evaluation → ntfy
- **M2 — 5-min loop:** trades.raw on a 5-min Dagster sensor; strategy
  evaluation chained via asset-sensor on new trades.raw materializations.
- **M3 — interchangeability proof:** `insider.cluster_buys.v1` derived
  from trades.raw; second strategy (cluster + momentum) reusing the same
  gate machinery with different inputs.

## Component inventory

| Component | Status |
|---|---|
| Sqitch schema (4 changes, verify scripts) | built, applied to dev |
| Signal contract + PIT machinery + 21 tests | built |
| Signals: trades.raw, career_grade (form4 bridge), price.daily.close | built |
| Dagster auto-discovery, daily partitions, Postgres instance | built |
| Schedules (04:30/05:00 UTC) + failure→ntfy + launchd services (UI 3030) | built, installed on Studio |
| dbt marts (latest_signal_per_ticker, signal_catalog_active) | built, thin |
| Backfill CLI (`python3 -m dataplane backfill/list`) | built |
| Status CLI (`python3 -m dataplane status`) + Pyrrho Desk dashboard (`:3031`) | built |
| Desk workbench: catalog/signal/strategy/ticker/pipeline pages | built (Phase A) |
| Desk composer: form → YAML → dry-run → save | built (Phase B) |
| Desk operate: backfill button + per-signal run history | built (Phase C) |
| Desk strategy edit/delete (CRUD over composed YAMLs) | built |
| M2: realtime 5-min sensor + strategy chain (DEFAULT STOPPED) | built, awaiting universe |
| Universe expansion script (batched Alpaca, insider-active tickers) | built (`scripts/expand_price_universe.py`) |
| **M1 demo on expanded universe: 355 real alerts in 90 days** | **done 2026-06-13** |
| Signal.auto_schedule opt-out + EDGAR retry-on-5xx | built |
| insider.filings.raw (direct EDGAR, parser reused from form4) | built |
| Parity CLI (`python3 -m dataplane parity`) | built |
| Parity baseline (1 day, v2 multi-pass at 95.4%/95.4%) | done |
| fundamental.facts.v1 (SEC XBRL companyfacts, third bronze source) | built |
| MCP read-only server (pyrrho-dataplane) — 7 tools | built |
| Strategy ntfy push (Dagster asset wrapper, cooldown + backfill guard) | built |
| Signal.read(..., lookback=) | built |
| StrategySignal (YAML-driven composite) + agrade_drawdown_buy.v1 | built |
| Report CLI (`python3 -m dataplane report`) — M1 demo artifact tool | built |
| Strategy replay 90d → eval tape with reasons (M1 acceptance bar) | done |
| Retire form4 bridge (insider.trades.raw.v1) once parity holds 30d | gated |
| Universe expansion (31 hardcoded → all ~5.7k priced tickers) | blocking real-world alerts |
| 5-min sensors, asset-sensor chaining | M2 |
| cluster_buys + second strategy | M3 |
| SLA probe → ntfy, /admin/signals, CI, Sqitch prod target | planned |
| Array attached + mounted (APFS, /Volumes/data, lake/+backups/) | done 2026-06-10 |
| Minute-bar ingestion, PG roll-off, backup rsync job | unblocked, not built |
| ticker_scores in-plane (drops form4 bridge; makes frozen-mode PIT real for career_grade) | planned |

## Open questions

- Universe management (31 hardcoded tickers → all ~5.7k priced tickers)
- Lake file layout (`lake/<source>/<symbol>/<yyyy-mm>/…` vs hive-style)
- PITTestCase shape for per_partition_events signals
- Restricted condition DSL — only when multi-user; YAML stays trusted
- Earnings/fundamentals source selection (free-first: SEC facts API?)

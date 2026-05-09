# Reliability Rebuild — Phase 2

**Status:** PROPOSAL — for review before any execution
**Authored:** 2026-05-08
**Triggered by:** the 9-day silent halt of all three live paper strategies (2026-04-30 → 2026-05-08), discovered while auditing infrastructure six days after Phase 1 reliability rebuild landed (`5610086`).

---

## TL;DR

Phase 1 (2026-05-02) was structurally correct but partially implemented. The `signal_freshness` table was created and the runner was wired to fail-closed against it, but **no compute pipeline writes to it**. The fallback path (`MAX(filing_date)`) measures the wrong thing — when the latest data row landed, not when the compute pipeline ran. Result: every morning since 2026-04-30, all three strategies have raised `STALE_INPUT_HALT` and exited with "0 candidates," because the fallback returns a date interpreted as midnight UTC and SLAs of 26-30h leave no margin for that structural lag.

This is the inverse failure of the original April outage. Same shape (silent zero-trading), opposite mechanism (false-positive halt vs. false-negative pass). And it confirms a meta-failure: the system has no way to detect when its own safety net is non-functional.

This document proposes Phase 2 — finishing what Phase 1 started, but reframed around an explicit layered architecture borrowed from production hedge-fund frameworks (NautilusTrader, QuantConnect LEAN). Goal: a system reliable enough to put real money behind, with extensibility on signal generation, decision handoff, and execution as first-class concerns.

The implementation is sequenced in six phases (P0–P5) that can land independently. P0 (4-6 hours) restores the safety net. P5 (over weeks) closes the live-launch readiness gate.

---

## 1. Context — what's working, what's broken, what's at stake

### What's working

- Postgres `form4` is the single source of truth. The 2026-04-07 SQLite→PG migration is solid; the schema and data are correct.
- The compute pipelines themselves are correct when they run. `compute_cw_indicators`, `build_pit_scores`, `backfill_pit_grades`, `update_daily_prices` produce correct outputs.
- The `refresh-features` daily chain (added Phase 1) bundles the compute pipelines into one launchd-scheduled job that runs reliably at 09:30 PT.
- The strategy logic is correct — when given fresh inputs, the conviction model produces good candidates. Verified via post-April backfill (16 quality_momentum candidates, 1 tenb51_surprise candidate during the April outage window were all valid).
- `cw_runner.py` correctly uses deterministic `client_order_id` for Alpaca-level order idempotency.
- The framework already supports event-driven backtest = paper = live symmetry (`framework/execution/{backtest,paper,live}.py`). This is the same architectural goal NautilusTrader hits.
- Schema for `signal_freshness`, `order_audit`, `trade_decision_audit`, `alpaca_reconciliation`, `is_live` columns all exist.

### What's broken

| # | Issue | Layer | Impact |
|---|---|---|---|
| 1 | `signal_freshness` table is empty (0 rows lifetime) | Data | Primary freshness path non-functional; fallback fires for every check |
| 2 | Fallback path measures wrong thing (`MAX(filing_date)` interpreted as midnight UTC) | Contract | False-positive halts every market morning since 2026-04-30 |
| 3 | Strategies halt with no detection that the *halt* is the bug | Safety/meta | No alert distinguishes "data legitimately stale" from "freshness measurement broken" |
| 4 | `order_audit` and `trade_decision_audit` tables exist but lifetime row count is 0 | Audit | No replay-from-audit story; no compliance trail; no "why didn't we trade X?" answer |
| 5 | `pit_cluster_size` orphaned for 37 days (same April pattern, missed) | Data | 827K trades NULL on this column; conviction scoring degraded |
| 6 | `backfill-returns` plist `PATH` excludes `/opt/homebrew/bin` | Schedule | 15-day gap in `trade_returns` (only 31.7% 90d returns filled when they should be 100%) |
| 7 | 18 files in repo had `/Users/openclaw/` legacy paths (now patched) | Schedule | Several plists ran successfully but logged into wrong directories; some scripts couldn't run at all on Studio |
| 8 | Alerts written to `logs/alerts.ndjson` are not delivered to a human | Observability | Candidate-count probe escalation (4 days × 0 candidates × 2 strategies) was never seen |
| 9 | `compute_cw_indicators.py` subprocess in `fetch_latest.py` times out at 300s every cycle (every 5 min) | Compute | Noisy logs; daily refresh chain still works; cosmetic but signals operator there's a problem |
| 10 | No pre-deploy gate (`studio --check`) for the bug classes above | Safety | Same bugs keep landing on Studio |

### What's at stake

The user's stated goal: a system reliable enough to put real money behind. `quality_momentum_live` is configured for $10k capital with tighter guardrails than paper. Plist exists in repo but isn't installed on Studio. The Day-14 readiness checklist in the postmortem has 19 action items; about half are done.

A live trade is a one-way operation. Once submitted, capital is committed. The asymmetry between false halts and false approvals dominates: a false halt costs missed opportunity (recoverable); a false approval costs real capital (not). Therefore the system's bias must be toward halting when uncertain — but only on actionable signals. False halts that operators learn to ignore are themselves a reliability failure (alarm fatigue → real alarms missed).

---

## 2. Reference architectures — what the field looks like

The web search for production-grade open-source frameworks surfaces two patterns the trading-framework should consciously adopt or reject:

### NautilusTrader

> "AI-first" event-driven Python platform addressing the parity challenge of keeping research/backtest consistent with live trading. Uses an event-driven engine where each market tick or bar triggers strategy logic which triggers orders.

**Take:** the explicit "event bus" abstraction. Today, `cw_runner.py` calls `scan_signals()` → applies filters → submits orders in one synchronous flow. The NautilusTrader pattern is: market events (bar, tick, fill, cancel) hit an event bus; strategy is one consumer; OMS is another. Same code path under backtest, paper, and live.

**Don't take:** their crypto-first / HFT-first orientation. We're trading EOD insider signals on US equities; we don't need microsecond precision.

### QuantConnect LEAN

> Open-source trading engine; brokerage-agnostic plugin pattern; powers 300+ hedge funds; supports Alpaca via plugin.

**Take:** the brokerage-agnostic plugin pattern. Our `framework/execution/{paper,live,backtest}.py` is the same shape but with weaker contract enforcement. LEAN's `IBrokerage` interface forces every plugin to implement the same surface (submit, cancel, get_position, get_account_value, on_order_event, on_account_event), making it impossible to call a broker-specific method in strategy code.

**Don't take:** the C# core. We're Python-native and that's fine.

### Plutus (Algorithmic Trading paper, May 2025)

> "Unified framework of tools, design patterns, and naming conventions so developers, researchers, and traders can speak the same language."

**Take:** the explicit naming-conventions discipline. The April postmortem named "silent exception swallowing" as contributing cause #1 — that's a naming/convention failure. Codifying conventions (e.g., "pipeline failure modes are typed exceptions, never logger.warning + continue") is cheap and prevents recurrence.

### What we're NOT building

- **HFT.** Our cadence is daily features + 5-min insider polling. Microseconds don't matter.
- **A new framework from scratch.** The trading-framework repo has 6+ months of investment. We're tightening the architecture, not replacing it.
- **A research platform.** Backtesting in this repo is good enough. Phase 2 is operational reliability, not research velocity.

### Sources

- [NautilusTrader](https://nautilustrader.io/) — event-driven Python platform
- [QuantConnect LEAN](https://github.com/QuantConnect/Lean) — open-source trading engine, 300+ hedge funds
- [Lean.Brokerages.Alpaca](https://github.com/QuantConnect/Lean.Brokerages.Alpaca) — brokerage plugin reference
- [PLUTUS open source paper (arXiv 2505.14050)](https://arxiv.org/html/2505.14050v1) — unified framework / design patterns
- [Best-of algorithmic trading list](https://github.com/merovinh/best-of-algorithmic-trading) — broader survey

---

## 3. Design principles

Six non-negotiable principles. Every architectural choice traces to one of these.

1. **Fail-closed by default.** When uncertain, halt. Bias toward missed trades over wrong trades. The cost of a missed trade is 0 ± expected_return; the cost of a wrong trade is unbounded.

2. **Typed everything.** Every error is a typed exception. Every event is a typed dataclass. Every config field has a schema. Phase 1 added this for `freshness.py`/`exceptions.py`; Phase 2 extends to compute pipelines, OMS, broker.

3. **Atomic compute writes.** Data write + freshness write + heartbeat write happen in the same transaction. Either all commit or none do. No partial states.

4. **Idempotent everywhere.** Every operation can re-run safely. Compute pipelines, order submission, reconciliation, backfills. Determinism via content-addressing (hash of inputs → output ID).

5. **Independent failure detection at every layer.** A failure in any layer must be detected by a *different* layer's monitor. The safety net itself must be monitored.

6. **Replay-from-audit.** Every state change persisted. The full system state at any historical point should be reproducible from the audit log. This subsumes compliance, debugging, drift detection.

---

## 4. Target architecture — layered design

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 9: Safety Gates                                               │
│   pre-deploy │ self-check │ probe │ preflight │ risk │ recon │ drift │ kill switch │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 8: Observability                                              │
│   heartbeats │ events │ alerts │ dashboard │ drift detection         │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 7: Reconciliation                                             │
│   broker↔DB diff (15-min) │ daily sweep │ order-state-machine completion │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 6: EMS / Broker Abstraction                                   │
│   IBroker interface │ {paper,live,backtest} implementations          │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 5: OMS / Order Management                                     │
│   OrderIntent → Order state machine → Fill                          │
│   pre-trade risk checks (composable)                                │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 4: Strategy / Decision                                        │
│   strategy = pure fn(signals, portfolio_state) → Decision[]         │
│   strategy versioning (yaml + git SHA + contract manifest)          │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 3: Signal Generation                                          │
│   raw events + features → signal stream                             │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 2: Compute / Feature Engineering                              │
│   compute pipelines: idempotent, atomic (data+freshness+heartbeat)  │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 1: Data Ingest                                                │
│   Form 4 fetch │ price feeds │ news │ congress │ EDGAR archive       │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 0: Foundation                                                 │
│   Postgres form4 │ launchd │ /opt/homebrew/bin Python │ env hygiene  │
└─────────────────────────────────────────────────────────────────────┘
```

### Layer 0 — Foundation

- Postgres `form4` on Studio is the system of record. **Single writer per table** going forward (today, multiple scripts write to `trades`; we'll keep that for now but centralize through a write-path module).
- launchd is acceptable for scheduling. (Re-evaluate at 5+ strategies or 50+ jobs.)
- Every plist's `EnvironmentVariables.PATH` includes `/opt/homebrew/bin`. Every Python invocation uses `/opt/homebrew/bin/python3` explicitly. Enforced by pre-deploy gate.
- No SQLite reads or writes anywhere in pipelines/. The `theta_cache.db` and `prices.db` SQLite holdovers are deprecated. Enforced by pre-deploy gate.

### Layer 1 — Data Ingest

- **Form 4 fetch** (`fetch_latest.py`, every 5 min) — writes to `trades`, emits heartbeat + signal_freshness for `trades.filing_date`.
- **Daily prices** (`update_daily_prices.py`, weekdays 17:30 PT) — writes to `prices.daily_prices`, emits freshness for that.
- **Insider scoring base** (`build_pit_scores.py`, daily 09:30 PT in refresh-features chain) — writes to `insider_ticker_scores`, emits freshness.
- **Congress, EDGAR archive, 8K, 13F, 144** — separate ingest pipelines, each with own freshness contract.

### Layer 2 — Compute / Feature Engineering

Every compute pipeline subclasses a new `framework.compute.ComputePipeline` base class that enforces:

```python
class ComputePipeline(ABC):
    name: str                          # for logging/freshness
    populates: list[ColumnSpec]        # which (table, column) tuples this writes
    sources: list[ColumnSpec]          # which inputs it reads (for dependency graph)

    def run(self, since: date, run_id: UUID) -> ComputeResult:
        with self._heartbeat_start(run_id):
            data = self._read_inputs(since)
            with self._db_transaction() as conn:
                rows_affected = self._compute_and_write(conn, data)
                self._write_freshness(conn, run_id, rows_affected)
        return ComputeResult(...)
```

The base class handles: heartbeat write, freshness write, transaction wrapping, structured-exception escalation, log line on every state.

Each existing compute pipeline gets migrated:
- `compute_cw_indicators.py` → `CWIndicatorsCompute(ComputePipeline)`
- `build_pit_scores.py` → `PITScoresCompute(ComputePipeline)`
- `backfill_pit_grades.py` → `PITGradesCompute(ComputePipeline)`
- `compute_pit_clusters.py` → `PITClustersCompute(ComputePipeline)` (already migrated to PG today)
- `compute_cohen_pit.py` → `CohenRoutineCompute(ComputePipeline)`

### Layer 3 — Signal Generation

**New first-class concept: the signal stream.**

Today, "signals" are implicit — they're WHERE clauses in the SQL inside `cw_runner.py`. Each strategy hand-builds a query against `trades` joined to derived columns. This makes adding a new strategy a Python edit; it makes signal-level analytics impossible.

Phase 2 introduces an explicit `signals` table:

```sql
CREATE TABLE signals (
    signal_id      uuid PRIMARY KEY,
    signal_type    text NOT NULL,        -- 'insider_buy' | 'cluster_buy' | 'tenb51_surprise' | ...
    signal_version text NOT NULL,        -- e.g. 'v1' — bumped on logic change
    ticker         text NOT NULL,
    direction      text NOT NULL,        -- 'long' | 'short'
    confidence     numeric(5,2),         -- 0-100
    inputs_hash    text NOT NULL,        -- sha256(canonical input snapshot)
    inputs_json    jsonb NOT NULL,       -- full snapshot for replay
    generated_at   timestamptz NOT NULL,
    source_event_id bigint,              -- → trades.trade_id (or other event source)
    UNIQUE (signal_type, signal_version, source_event_id)  -- idempotency
);
```

A `SignalGenerator` is a function: `(source_event) → Signal | None`. Examples:

- `InsiderBuySignalGenerator` — emits a signal for every P-code trade that meets minimum criteria (size, role).
- `ClusterBuySignalGenerator` — emits when `pit_cluster_size >= 2` etc.
- `TenB51SurpriseSignalGenerator` — emits when an established 10b5-1 seller breaks pattern with a buy.

Generators run nightly (or in real time for live). Each one knows its inputs and what freshness contracts apply.

**Strategies consume from `signals`, not `trades` directly.** This is the core extensibility win. Adding a new strategy is:
1. Pick (or create) a `signal_type`.
2. Write filters in YAML (already supported).
3. Declare exit rules in YAML (already supported).
4. Run.

No Python changes for vanilla strategies. New signal types still need Python (one generator class).

### Layer 4 — Strategy / Decision

Strategies become **pure functions**:

```python
def evaluate(signals: list[Signal], portfolio: PortfolioState) -> list[Decision]:
    """Return zero or more Decisions. Pure function — no side effects."""
```

Each `Decision` row goes to `strategy_decisions` (subsumes today's `trade_decision_audit`):

```
Decision:
  decision_id        uuid
  strategy           str
  strategy_version   str   # yaml sha + git SHA
  signal_id          uuid  # which signal triggered this
  decision           str   # 'enter' | 'reject' | 'exit'
  reason             str   # 'conviction:7.2' | 'capacity:full' | ...
  confidence         float
  portfolio_state_at jsonb # snapshot for replay
  decided_at         timestamptz
```

Per-stage decisions (today's `trade_decision_audit`) become per-row entries here with `decision='reject'` and granular `reason`. Replayability: given a frozen signals snapshot and strategy version, decisions are reproducible.

Strategy version = (yaml file hash) + (git SHA at decision time). Stored on every decision so we can later say "what would v2 have decided about this signal?"

### Layer 5 — OMS / Order Management

Today: cw_runner takes a candidate, sizes it, calls Alpaca. One conflated step.

Tomorrow: explicit state machine:

```
Decision (enter)  →  OrderIntent  →  Order  →  Fill | Reject | Cancel
                       ↑                ↑
                   risk check       broker submit
```

`OrderIntent` is the result of:
1. Sizing (Kelly / fixed-pct / unit) — based on portfolio state and confidence
2. Instrumenting (equity vs. option strike selection)
3. Risk checks (composable list — see below)

`Order` is post-submission state. State machine: `PENDING → SUBMITTED → ACCEPTED → PARTIALLY_FILLED → FILLED | REJECTED | CANCELLED`. Persisted in `orders` table (subsumes today's `order_audit`).

Idempotency: `client_order_id = sha256(decision_id || retry_count)`. Re-submitting the same decision twice produces the same `client_order_id`; Alpaca dedups server-side.

**Pre-trade risk checks** are a composable list, run in order, each returning `Pass | Reject(reason)`. Examples:

- `MaxPositionSizeCheck(pct=10)` — single position ≤ 10% of equity
- `MaxConcurrentCheck(n=10)` — ≤ N concurrent positions per strategy
- `DailyLossLimitCheck(pct=5)` — halt if today's P&L ≤ -5%
- `SectorConcentrationCheck(max_per_sector=30)` — ≤ 30% in any GICS sector
- `MarginCheck` — sufficient buying power available
- `SymbolBlocklistCheck` — symbol not in blocklist (delisted, halted, manipulated)

A failed risk check writes a `Decision(decision='reject', reason='risk:max_position_size')` row. The signal isn't lost; it's preserved for analysis ("we rejected 47 candidates last month for sector concentration — was that the right call?").

### Layer 6 — EMS / Broker Abstraction

Today: `framework/execution/{paper,live,backtest}.py` exists. Phase 2 codifies the interface:

```python
class IBroker(ABC):
    @abstractmethod
    def submit(self, intent: OrderIntent) -> Order: ...
    @abstractmethod
    def cancel(self, order_id: str) -> None: ...
    @abstractmethod
    def get_position(self, symbol: str) -> Position | None: ...
    @abstractmethod
    def get_account(self) -> Account: ...
    @abstractmethod
    def stream_events(self) -> Iterator[BrokerEvent]: ...
```

`PaperBroker`, `LiveBroker`, `BacktestBroker` all implement this. `LiveBroker` and `PaperBroker` differ only in API base URL (`api.alpaca.markets` vs `paper-api.alpaca.markets`) — already true today, just tighten the contract.

The strategy/OMS code path is **identical** under all three brokers. A bug in one is catchable by running the same strategy against all three with the same inputs and diffing outputs.

### Layer 7 — Reconciliation

Continuous: every 15 min during market hours, broker positions vs. DB positions:

```python
def reconcile():
    db_positions = fetch_db_positions(strategy)
    broker_positions = broker.list_positions()
    for diff in diff_positions(db_positions, broker_positions):
        if diff.size > tolerance:
            alert.critical("reconcile_drift", diff)
            write_drift_event(diff)
```

Tolerance: 0 for live; small for paper (Alpaca paper has known idiosyncrasies — fractional cash positions, etc.).

Daily: cash balance check, orders-in-terminal-state sweep (every order from yesterday must be `FILLED | REJECTED | CANCELLED` by EOD; anything still `SUBMITTED` is alerted).

### Layer 8 — Observability

Three signal types per pipeline:

| Signal | Frequency | Purpose | Storage |
|---|---|---|---|
| Heartbeat | Every cycle / every minute | "I'm alive and tried to run" | `pipeline_heartbeats` table + `logs/heartbeats.ndjson` |
| Event | Every state change | "X happened" | `events` table + `logs/events.ndjson` |
| Alert | Threshold breach | "human attention needed" | `logs/alerts.ndjson` + dispatcher |

**Alert dispatcher**: routes by severity.
- P0 (silent outage, broker drift, risk-check fail at submission, freshness-system-broken meta-alert): SMS + email, ack required within 4h
- P1 (compute pipeline failure, candidate-count anomaly): email
- P2 (cosmetic noise, recovery notifications): log only

The current `logs/alerts.ndjson`-only routing is the reason candidate-count probe escalations went unread. Phase 2 wires SMS and email properly. (Implementation: `framework/alerts/dispatch.py` reads severity, dispatches via Twilio + Resend or similar.)

**Operator dashboard**: `/admin/strategies` already exists. Extend to:
- Top: strategy health summary (heartbeat fresh? probe OK? open positions? today's activity?)
- Middle: recent decisions (per-stage filter outcomes for last N candidates)
- Bottom: alert feed with ack mechanism

### Layer 9 — Safety Gates

Eight independent gates. Each catches a different failure mode. None alone is sufficient.

| # | Gate | When | Catches |
|---|---|---|---|
| 1 | Pre-deploy gate (`studio --check`) | Before every deploy | Code/config bugs (legacy paths, missing PATH, missing deps, contract drift, SQLite reads) |
| 2 | Compute self-check | At end of every compute pipeline run | Pipeline ran but produced 0 rows / unexpected NULL fraction |
| 3 | Freshness probe | Every 15 min | Compute pipeline didn't run; data is stale |
| 4 | Strategy preflight (existing, fixed) | At start of every strategy scan | Required inputs are stale at decision time |
| 5 | Pre-trade risk check | At every order submission | Position size, daily loss, sector, margin, symbol blocklist |
| 6 | Post-fill reconciliation | Every 15 min, daily EOD | Broker state diverges from DB state |
| 7 | Daily burn-rate check | Daily | Today's order count >> historical mean (runaway loop, bug) |
| 8 | Backtest-vs-live drift | Weekly | Live trade count or P&L distribution diverges from expected |

Plus a **kill switch**: `STOP_TRADING` flag file in repo root. Read at every cw_runner cycle. If present, all strategies halt entries (exits still execute to flatten). Operator can flip without service restart.

---

## 5. Implementation phases

Six phases, sequenced by dependency. P0 unblocks the runners (this week). P5 is the live-money readiness gate.

### Phase 0 — Restore the safety net (4-6 hours focused work)

**Goal:** the runners trade again, on a correctly-functioning safety net, without any SLA bumps.

1. **Revert SLA bumps** in `freshness_contracts.yaml`. Restore 26-30h. They were correct; the lookup was broken.

2. **Add `framework/contracts/freshness_writer.py`** — single helper:
   ```python
   def write_freshness(conn, table, column, n_rows, run_id, populated_by):
       """Write one signal_freshness row in the current transaction."""
   ```

3. **Wire `write_freshness` into compute pipelines.** ~10 lines per pipeline:
   - `pipelines/insider_study/compute_cw_indicators.py` — for every column it populates
   - `strategies/insider_catalog/build_pit_scores.py` — `insider_ticker_scores.blended_score`
   - `pipelines/insider_study/backfill_pit_grades.py` — `trades.pit_grade`
   - `pipelines/insider_study/update_daily_prices.py` — `prices.daily_prices.date`
   - `strategies/insider_catalog/fetch_latest.py` — `trades.filing_date`
   - `pipelines/insider_study/compute_pit_clusters.py` — `trades.pit_cluster_size` (newly migrated to PG)
   - `pipelines/insider_study/compute_cohen_pit.py` — `trades.cohen_routine`

4. **Remove the broken fallback** in `framework/contracts/freshness.py`. Replace `_lookup_max_filing_date`, `_lookup_max_score_as_of`, `_lookup_max_price_date` with explicit `FreshnessUnknownError` (new exception class). The runner then knows the difference between "data is stale" (recoverable) and "the freshness system itself is non-functional" (meta-failure).

5. **Add meta-check in `cw_runner.assert_all_fresh_for_strategy`.** If `signal_freshness` has 0 rows for any of the strategy's contracted columns, raise `FreshnessSystemBrokenError` (P0, separate runbook). This is what would have caught Phase 1's incomplete deployment.

6. **One-shot backfill of `signal_freshness`** with current MAX timestamps from source tables. Avoids a warning storm when the new code rolls out.

7. **Add `compute_pit_clusters` to `refresh_features_daily.sh`** as step 5. Trigger one-shot backfill since 2026-04-01.

8. **Fix `backfill-returns` PATH on Studio.** ALREADY DONE (sed-edit + launchctl reload). Trigger manual run to fill 15-day forward-returns gap.

9. **Wire alert routing.** SMS for P0 via Twilio; email via Resend. Test by triggering a synthetic alert. Today, P0s go to `logs/alerts.ndjson` only.

10. **Trigger `refresh-features` chain manually.** Verify freshness probe shows OK across all contracts. Restart runners. Observe one full market day. Verify candidates flow.

**Exit criteria for P0:** all freshness contracts OK in probe output for 24h, all three strategies log non-zero candidate counts on a normal market day, alerts route to a phone.

### Phase 1 — Pre-deploy gate (1-2 days)

**Goal:** the bug classes from this audit can never land on Studio again.

1. **`scripts/preflight/infra_audit.py`** — Python script with these checks:
   - No `/Users/openclaw/` in repo (excluding allowed historical docs)
   - Every plist's `EnvironmentVariables.PATH` includes `/opt/homebrew/bin`
   - Every plist's first ProgramArgument is `/opt/homebrew/bin/python3*`
   - No `import sqlite3` in pipelines/ outside known-allowed (e.g., theta_cache reads)
   - No `DB_PATH = .*\.db` outside known-allowed
   - Every column in `cw_runner.py` SQL has a freshness contract
   - Every freshness contract's `populated_by` script exists
   - `python -m framework.contracts.freshness --check` exits 0 against current DB
   - All `tests/unit` pass
   - `scripts/preflight/e2e_smoke.py` passes

2. **`studio` CLI: add `--check`.** Calls `infra_audit.py`. `studio deploy` requires `--check` to pass first.

3. **CI hook.** `infra_audit.py` runs on every PR.

**Exit criteria for P1:** every check is enforced; running `studio --check` after a deliberate bug-introduction (e.g., add `/Users/openclaw/` to a plist) catches it.

### Phase 2 — OMS separation + audit population (3-5 days)

**Goal:** every order is fully audited; the order state machine is explicit.

1. Refactor `cw_runner._evaluate_candidate` to return a `Decision` dataclass (no Alpaca calls).
2. New `framework/oms/order_manager.py` consumes Decisions, produces OrderIntents (sizing + instrumentation), runs pre-trade risk, submits to broker.
3. Wire `order_audit` writes on every state transition. Backfill historical orders into the table from `strategy_portfolio` + Alpaca history.
4. Wire `trade_decision_audit` writes per filter stage. Pass + reject. Backfill from logs (best-effort).

**Exit criteria for P2:** every decision the runner makes is queryable from `trade_decision_audit`; every order is queryable from `order_audit`; "why didn't strategy X buy ticker Y on date Z" has a SQL answer.

### Phase 3 — Reconciliation + drift detection (3-5 days)

**Goal:** the system catches its own divergence from broker reality.

1. **Continuous broker↔DB reconciliation** (every 15 min market hours). Diff goes to `alpaca_reconciliation`. Alert P0 on drift > tolerance.
2. **Daily completeness sweep.** Every order from yesterday in terminal state. Every position in DB matches broker. Cash balance = expected.
3. **Drift detector.** Weekly: live trade count vs. backtest-expected; live fill price vs. close price; live P&L distribution vs. expected.

**Exit criteria for P3:** simulated drift (operator manually closes an Alpaca position) is detected within 15 min and alerts.

### Phase 4 — Signal stream + extensibility (1-2 weeks)

**Goal:** new strategies don't require Python changes.

1. Create `signals` table.
2. Migrate existing strategies to consume from `signals` (each strategy declares which `signal_type`s it consumes).
3. Build `SignalGenerator` interface and migrate hand-rolled SQL filters into named generators.
4. Document: "How to add a new strategy" — should be ≤ 30 minutes for a vanilla insider strategy.

**Exit criteria for P4:** add a fictional strategy "buy when 3+ insiders buy in 7 days" entirely in YAML, no Python changes, runs end to end.

### Phase 5 — Live launch readiness gate (gated on P0-P4 + 7-day burn-in)

**Goal:** all green for 7 consecutive days → live money eligible.

1. All Phase 0-3 checks green for 7 consecutive days.
2. SMS alert routing tested (operator manually triggers a P0).
3. Operator drill: simulated outage (e.g., kill `compute_cw_indicators` for one cycle); verify detection within probe interval; verify alert delivered.
4. Postmortem checklist 100%: items #9-#19 closed.
5. Live launch checklist sign-off (existing `docs/LIVE_LAUNCH.md` + Day-14 e2e_smoke green).

**Exit criteria for P5:** the live plist (`com.openclaw.quality-momentum-live`) is installed on Studio with `_LIVE` Alpaca creds in `.env`, runs alongside paper for 7 days with reconciliation green, then capital is committed.

---

## 6. Open questions for review

These need decisions from Derek before P0-P2 start.

### Q1 — alert routing details

Twilio for SMS or Apple iMessage relay (free, less reliable)? Resend for email or some existing provider? My default: Twilio + Resend, since both are commodity, well-instrumented, and `pipelines/notification_scanner.py` already has email infra.

### Q2 — strategy versioning granularity

Track strategy version as `(yaml_sha, git_sha)` per decision, or just `yaml_sha`? Git SHA covers Python behavior changes (e.g., conviction scoring update); yaml SHA covers config tuning. My default: both, store on every decision row.

### Q3 — signal stream rebuild policy

When `compute_cw_indicators` schema changes (new column added), do we recompute all historical signals? Or version the signals table by signal_version and let old signals coexist? My default: signal_version column, additive only. Recomputing 1.6M trade rows is cheap; backward compat matters for backtest vs live parity.

### Q4 — kill switch granularity

Single global `STOP_TRADING` flag, per-strategy flags, or per-strategy + per-direction (e.g., halt new entries but allow exits)? My default: per-strategy + per-direction. Operationally: most "halt" decisions want exits to keep working.

### Q5 — backfill scope for trade_decision_audit

Backfill from runner logs (best-effort, partial coverage) or only forward-fill from P2 onward? My default: forward-fill only. Logs are insufficient for historical reconstruction.

### Q6 — drift detector tolerance

What's the threshold for "live diverges from backtest"? E.g., quality_momentum backtest shows ~50 trades/yr; if live shows 30 in 12 months, alert? Or 20? My default: 1-sigma deviation per quarter, alert P1; 2-sigma alert P0.

### Q7 — should we adopt NautilusTrader or LEAN wholesale?

Risks of adoption: 6+ months of trading-framework investment, our event model is simpler than HFT. Risks of staying: reinventing wheels. My recommendation: stay on the trading-framework codebase, adopt NautilusTrader's *event-bus pattern* (Phase 4) without adopting the framework itself. If we hit 5+ strategies and the cw_runner approach starts to creak, revisit.

---

## 7. Risks

| # | Risk | Likelihood | Mitigation |
|---|---|---|---|
| 1 | P0 changes touch live writeback path; could break compute pipelines | Medium | Each compute pipeline change is independently testable. Roll out one at a time. Run with `--dry-run` flag first. |
| 2 | Removing the broken fallback hard-fails strategies if signal_freshness backfill misses a column | Low | Backfill script enumerates all contracted columns; CI test asserts every contract has a freshness row before runner starts. |
| 3 | OMS refactor (P2) introduces subtle behavior changes | Medium | Run paper-arm under both old and new code in parallel for 7 days; diff decisions. |
| 4 | Drift detector (P3, P5) over-alerts on legitimate drift (e.g., quiet markets) | Medium | Tolerance bands; transition-only alerting; weekly P1 not daily P0. |
| 5 | New signal stream (P4) creates duplicate signals | Low | Idempotency via `inputs_hash`; UNIQUE constraint on `(signal_type, signal_version, source_event_id)`. |
| 6 | Live launch gate is too lenient; we go live with subtle bug | Medium | 7-day burn-in is conservative; operator drill catches paths the burn-in doesn't. |
| 7 | This rebuild itself takes too long; meanwhile paper strategies underperform their backtest | High | P0 (4-6 hours) gets paper running clean. P5 (live readiness) is weeks away — that's correct; we're not rushing live launch. |
| 8 | Alert fatigue from too many P0s during P0-P3 ramp | Medium | Suppression rules; transition-only alerts; weekly review of alerts.ndjson to retire stale rules. |

---

## 8. Effort estimate

| Phase | Wall time | Focused-work hours |
|---|---|---|
| P0 — restore safety net | 1 day (with monitoring time) | 4-6 |
| P1 — pre-deploy gate | 1-2 days | 6-10 |
| P2 — OMS separation + audit | 3-5 days | 15-25 |
| P3 — reconciliation + drift | 3-5 days | 15-25 |
| P4 — signal stream | 1-2 weeks | 25-40 |
| P5 — live launch gate | 7-day burn-in + drill | 5-10 |
| **Total** | **~3-4 weeks** | **70-115** |

This matches the postmortem's Phase 2-3 effort estimate of "multi-week."

---

## 9. What this doesn't change

- Strategy logic (conviction scoring, exit rules) — those are correct.
- The PG schema for `trades`, `trade_returns`, `insider_ticker_scores`, etc. — those are correct.
- The Form 4 fetch pipeline — that's correct.
- The frontend — no changes.
- The board-of-personas review process for new strategies — that's correct.

This is a reliability and extensibility rebuild, not a strategy or product rebuild.

---

## 10. Decision needed

This document is a proposal. To move forward, Derek decides:

1. Approve / redline the architecture (Sections 4 + 5).
2. Answer Q1-Q7 (Section 6) — defaults provided where possible.
3. Authorize P0 execution. P1-P5 are gated on P0 success but should be queued.

Once P0 lands, the strategies trade again on a correctly-functioning safety net, with paper continuing to run while P1-P5 add the layers needed for live money.

---

## Appendix A — what would I take from the references

- **NautilusTrader event bus** — Phase 4 candidate. Today our cw_runner is a synchronous loop; if we hit 5+ strategies or want sub-minute latency, the event-bus pattern scales better.
- **QuantConnect LEAN brokerage interface** — Phase 6/EMS. Codify our `IBroker` interface to match LEAN's surface; makes our code portable to LEAN's broker plugins later if useful.
- **PLUTUS naming conventions** — Phase 0/typed exceptions. Already partially done (Phase 1 added typed exceptions for contracts); extend to compute pipelines, OMS, broker.
- **Don't take from anyone:** the C++/C# core. We're Python and that's the right choice for our cadence and team size.

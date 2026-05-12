# PIT-Honest Backtest Engine — Design

**Author:** Claude / Derek
**Date:** 2026-05-12
**Status:** Phase 1 + 2 shipped 2026-05-12. Phase 3+4 pending.
**Tracking:** This doc evolves as we ship `framework/pit/`. Update when behavior changes.

---

## 1. Why this exists

We just shipped a silent point-in-time (PIT) bug. The V3 career_grade scorer
(`compute_insider_ticker_score` in `strategies/insider_catalog/pit_scoring.py`)
filtered prior trades by `trade_date <= cutoff` but did **not** also require
`filing_date <= as_of_date`. Result: late-filed insider trades (5.14% of the
universe have filing lag > 10 days) leaked into earlier scores. The bug was
shipped on 2026-05-07, affected every QM entry decision since then, and would
have shipped into a live-money allocation if a routine V2-vs-V3 audit hadn't
caught it.

This is the third PIT bug in this codebase that I'm aware of:
1. `insider_track_records.score` used in PIT contexts (logged in
   `reference_signal_registry.md`)
2. `signal_quality.py` `sell_win_rate_7d` uses full track record
3. The 2026-05-07 V3 backfill bug, just patched

The root cause is the same each time: **PIT is enforced by convention, not by
construction**. Anyone writing a new score / signal / filter has to remember
to add `WHERE filing_date <= as_of_date` to every query. They will not.

The fix is to make lookahead structurally impossible to express. Patterns from
Zipline, LEAN, Compustat PIT, and López de Prado's CPCV converge on the same
five primitives — we apply them here.

## 2. Five primitives (from external research)

| # | Pattern | What it solves | Where this engine puts it |
|---|---------|----------------|---------------------------|
| 1 | Bitemporal facts | Confuses transaction-time vs knowledge-time | Every PIT query takes `as_of_date` and filters knowledge_time ≤ as_of_date |
| 2 | Engine-owned clock; data pushed | Strategy reaches around the clock | `PITDataView` is the *only* DB path; strategies receive a view, never a raw connection |
| 3 | Lookahead-safe typed accessors | `SELECT *` bypasses convention | All accessors return immutable dataclasses; `view.get_trades(...)` not raw SQL |
| 4 | Horizons are intervals (purge/embargo) | Forward-return features leak | `view.observable_returns(insider, as_of)` enforces lag (10/40/100d for 7d/30d/90d) |
| 5 | Restatement-aware ingestion | Amendments overwrite history | Out of scope here — Form 4 amendments tracked via `superseded_by` already |

## 3. Architecture

### 3.1 Layer cake

```
   ┌────────────────────────────────────────┐
   │ Strategies (QM, RD, 10b5, future)     │  ← evaluate(view, event) → Decision
   │ — pure functions of view + event       │
   └────────────────┬───────────────────────┘
                    │
   ┌────────────────▼───────────────────────┐
   │ PITDataView                            │  ← typed accessors only
   │ — get_price(t, on_or_before)           │
   │ — get_trades(insider, as_of)           │
   │ — get_insider_score(i, t, as_of)       │
   │ — observable_returns(i, as_of, window) │
   └────────────────┬───────────────────────┘
                    │
   ┌────────────────▼───────────────────────┐
   │ PITClock(as_of_date)                   │  ← single source of truth for "now"
   │ — assertions: data ≤ as_of_date        │
   │ — read-tape (audit trail)              │
   └────────────────┬───────────────────────┘
                    │
   ┌────────────────▼───────────────────────┐
   │ PostgreSQL form4 DB                    │  ← raw bitemporal data
   │ trades, insider_ticker_scores, prices  │
   └────────────────────────────────────────┘
```

### 3.2 Key types

```python
@dataclass(frozen=True)
class PITClock:
    """Immutable "now". The only legal way to ask "what could we know on date X."
    Carries an audit tape of every data read so we can prove non-violation."""
    as_of_date: str            # YYYY-MM-DD
    _read_tape: list  = field(default_factory=list)

    def assert_known(self, knowledge_date: str, source: str) -> None:
        if knowledge_date > self.as_of_date:
            raise LookaheadError(
                f"PIT violation: read {source} with knowledge_date={knowledge_date} "
                f"after as_of_date={self.as_of_date}"
            )
        self._read_tape.append((source, knowledge_date))


@dataclass(frozen=True)
class TradeEvent:
    """An insider trade as the engine sees it — only fields known at filing_date."""
    trade_id: int
    insider_id: int
    ticker: str
    trade_date: str
    filing_date: str           # the only knowledge_date that matters for entry
    trade_type: str            # 'buy' | 'sell'
    title: str | None
    is_csuite: bool
    # … other features pre-computed from PIT-safe inputs (dip_3mo, etc.)


@dataclass
class Decision:
    """A strategy's evaluation of an event. The engine consumes these."""
    action: str                # 'enter' | 'skip' | 'rotate'
    conviction: float
    reason: str
    snapshot: dict             # JSON-serializable; goes into trade_decision_audit


class PITDataView:
    """The *only* DB access surface visible to strategies."""
    def __init__(self, clock: PITClock, conn): ...

    def get_close(self, ticker: str, on_or_before: str) -> float | None: ...
    def get_insider_score(self, insider_id: int, ticker: str) -> InsiderScore: ...
    def get_prior_trades(self, insider_id: int, before: str) -> list[TradeEvent]: ...
    def observable_returns(self, insider_id: int, ticker: str | None,
                           window: str = "7d") -> list[tuple[str, float]]: ...
    def events_filed_on(self, date: str) -> list[TradeEvent]: ...
```

Every accessor:
1. Takes the clock implicitly (bound at view construction)
2. Builds WHERE clauses that include the bitemporal guard
3. Calls `clock.assert_known(row.knowledge_date, source)` for each row returned
4. Returns immutable dataclasses

### 3.3 Engine

```python
class PITBacktestEngine:
    def run(self, start: str, end: str, strategy: PITStrategy,
            config: dict) -> BacktestResult:
        portfolio = Portfolio(config)
        for d in trading_days(start, end):
            clock = PITClock(as_of_date=d)
            view = PITDataView(clock, self.conn)

            # 1) exits
            for pos in portfolio.open_positions:
                if strategy.should_exit(pos, view):
                    price = view.get_close(pos.ticker, d) or pos.entry_price
                    portfolio.close(pos, price, d)

            # 2) entries — process events filed today
            events = view.events_filed_on(d)
            decisions = [strategy.evaluate(view, e) for e in events]
            for d_ in sorted(decisions, key=lambda x: -x.conviction):
                portfolio.consider(d_, view, capacity_rule=config["at_capacity"])

        return portfolio.result()
```

The strategy never gets a raw `conn`. It cannot accidentally `SELECT *` past
the clock. If it tries to compute something the view doesn't expose, the
right answer is to add a method to `PITDataView` — and then by construction
every other strategy benefits.

## 4. How this composes with existing code

**Three existing PIT-sensitive code paths converge here:**

| Existing path | Replaced by |
|---|---|
| `pipelines/insider_study/simulate_decision_audit.py` (sim audit replay) | `PITBacktestEngine.run(...)` with `audit=True` |
| `strategies/cw_strategies/cw_runner.py:scan_signals + execute_entries` | Strategy is identical; engine becomes `PITBacktestEngine` with `live=True` (DB writes enabled, Alpaca calls enabled) |
| `pipelines/insider_study/grid_search_strategies.py` | `PITBacktestEngine.sweep(...)` over a parameter grid |

The strategy logic itself is **the same code**. The engine differs only in:
- Which "now" it presents to the strategy (today for live, replayed for backtest)
- What it does with decisions (submit Alpaca order vs write audit row vs nothing)

This is the same idea as LEAN's `IAlgorithm` interface — the strategy is
clock-agnostic; the engine owns the clock.

## 5. Test strategy

Five layers, in order of importance:

### 5.1 Clock invariants (`tests/unit/test_pit_clock.py`)
- `assert_known` raises when knowledge_date > as_of
- Read tape captures every assertion
- Multiple clocks with overlapping as_of don't share state

### 5.2 View enforcement (`tests/unit/test_pit_view.py`)
- `get_close` returns None for a price after as_of (mock fixture)
- `get_insider_score` filters by `as_of_date <= clock.as_of`
- `observable_returns` filters by `trade_date <= as_of - lag` AND `filing_date <= as_of`
- Each accessor calls `clock.assert_known` for every row

### 5.3 Contamination (`tests/integration/test_pit_contamination.py`)
- Insert a row with `filing_date = as_of + 1 day` into a sandbox DB
- Run the engine at `as_of`
- Assert the contaminated row appears nowhere in the result

### 5.4 Equivalence with live cw_runner (`tests/integration/test_pit_engine_equivalence.py`)
- Pick a historical event (e.g. KOS / Ogunlesi 2026-03-12)
- Run `PITBacktestEngine.evaluate_one(event, clock=PITClock(filing_date))`
- Compare to `simulate_decision_audit` row for the same (strategy, trade_id)
- Decisions must match exactly (action, conviction within 1e-6, pit_grade)

### 5.5 Determinism (`tests/integration/test_pit_engine_determinism.py`)
- Same inputs, two runs, byte-identical `BacktestResult`

The first three run in the standard `pytest tests/unit` cycle. (4) and (5)
need PG fixtures; they live in `tests/integration` and run on Studio.

## 6. Rollout plan

**Phase 1 — primitives** ✅ **SHIPPED 2026-05-12.**
`framework/pit/{clock,events,view,strategy,engine}.py` with 53 unit tests
in `tests/unit/test_pit_{clock,view,contamination}.py`. All green.

**Phase 2 — engine + migration** ✅ **SHIPPED 2026-05-12.**
Minimal `PITBacktestEngine` lives in `framework/pit/engine.py`.
`QualityMomentumStrategy` is the first reference `PITStrategy`. Integration
test `tests/integration/test_pit_engine_equivalence.py` runs the new engine
against PG and verifies decisions match the existing
`simulate_decision_audit.py` simulator byte-for-byte (action, pit_grade,
career_grade, conviction within 1e-4). Pass on Studio.

**Phase 3 — strategy + cw_runner refactor.** 🟢 MOSTLY DONE (2026-05-12).
- ✅ All 3 PITStrategy implementations (QM, RD, 10b5) pass byte-equivalence
  vs simulator for sampled filing_dates. 4/4 integration tests green.
- ✅ `PITLiveEngine` (`framework/pit/live.py`) — dry-run capacity+decision
  pipeline mirroring `cw_runner.scan_signals` + entry-side of
  `execute_entries`. Supports skip / replace_weakest / replace_oldest.
- ✅ `scripts/pit_shadow_run.py` — parallel validator that compares engine
  output to cw_runner's actual decisions in `trade_decision_audit`.
  Classifies diffs as: action (real bug), conviction (real bug), capacity
  (historical-state shadow limitation), or drift (post-PIT-fix rebuild).
  As of 2026-05-12: 0 real bugs across 7 days of recent decisions.
- ⏳ **Live submission path** NOT implemented (raises `NotImplementedError`
  on `dry_run=False`). This is the actual cutover step — mirroring the
  Alpaca order submission and persistence sections of cw_runner. Should
  land with parallel-run validation showing zero `action_diffs` over a
  longer window (e.g. 2+ weeks of nightly shadow runs).

**Phase 4 — grid search migration.** ⏳ NOT STARTED. Goal: replace
`pipelines/insider_study/grid_search_strategies.py` (1465 lines of partly
duplicated logic) with `PITBacktestEngine.sweep(...)` over a parameter
grid. Highest-leverage cleanup; eliminates the third copy of entry logic
in the repo.

Phase 1+2 unblock the immediate need (PIT-honest QM/RD validation, no
silent lookahead, single source of truth for new scoring features).
Phase 3+4 are repo-health cleanup that can lag.

## 7. Non-goals

- **Real-time tick-by-tick simulation.** Form 4 strategies are filing-event-driven
  with daily resolution. No need for sub-minute event handling.
- **Reproducing exact Alpaca fill prices.** Backtest uses close-on-filing-date
  as entry; live uses Alpaca's actual fill. The 30bp ± slippage is acknowledged
  drift, not a bug.
- **Restatement (Form 4/A) replay.** Existing `superseded_by` column handles
  this at the trade level. We do not currently model "what we believed about
  trade T at time t1 vs t2". Compustat-style snapshotting is overkill for now.
- **Vectorized speedup.** This engine is event-driven row-by-row. If profiling
  shows it's too slow for a 6-year backtest, we can add an array-mode optimization
  later — but only after the row-by-row path is proven byte-equivalent to live.

## 8. Open questions

1. **`prices.daily_prices.date` is a knowledge_date in disguise.** Close prices
   are usually known same-day, so `WHERE date <= as_of` is sufficient. But for
   after-hours filings, the close on filing_date isn't observable until next
   day's market open. Should we model this?
2. **What's the right behavior for `get_insider_score` when no row exists for
   `as_of_date <= filing_date`?** Today: returns None → `pit_grade = "C"` (default).
   Probably correct, but document explicitly.
3. **PITClock immutability.** Currently a `@dataclass(frozen=True)` with a
   `_read_tape: list` (mutable default). Should the tape live elsewhere?
   Probably yes — engine, not clock.
4. **How do we test the existing scorers under the new rails without rewriting
   them?** Wrapper: `compute_insider_ticker_score(clock=clock_at(filing_date),
   insider_id=..., ticker=...)` should produce the same result as the legacy
   function. Add equivalence test.

## 9. Appendix — Glossary

- **as_of_date** — the "what did we know at this moment" date. The clock's value.
- **knowledge_date** — when a fact became observable. For Form 4 trades, that's
  `filing_date`. For derived scores, that's `as_of_date` on the scoring row.
- **effective_date / valid_time** — when the underlying event happened. For
  Form 4 trades, that's `trade_date`. We do not currently filter by valid_time
  except to compute features (e.g., "consecutive sells before"); valid_time leak
  is less common than knowledge_date leak in this codebase.
- **PIT** — point-in-time. Shorthand for "no lookahead, no future contamination."
- **Lookahead bias** — any code path that uses information from after the as_of_date.

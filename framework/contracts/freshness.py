"""Freshness registry + assert_fresh() — fail-closed for stale data.

Reads `config/freshness_contracts.yaml` once at import and exposes:

    assert_fresh(table, column, strategy)
    get_freshness(table, column)        -> (last_computed_at, age_hours)

Lookup of `last_computed_at` has two modes:

  1. **Primary** — read from PG `signal_freshness` table
     (written by every compute pipeline on completion).
     Schema:
       (source text, table_name text, column_name text,
        last_computed_at timestamptz, n_rows_affected bigint, run_id uuid)

  2. **Fallback** — query the source table directly:
        SELECT MAX(filing_date) FROM trades WHERE <column> IS NOT NULL
     This works pre-migration (when `signal_freshness` doesn't exist yet)
     and serves as a sanity check post-migration.

The fallback is intentionally weaker (it tells you when the latest *row*
landed, not when the *compute* ran), but it's strictly better than no
freshness check at all. The migration to `signal_freshness` lands in
Phase 1.3.
"""
from __future__ import annotations

import logging
import os
import sys
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from framework.contracts.exceptions import (
    StaleSignalError,
    FreshnessUnknownError,
    FreshnessSystemBrokenError,
    WriterMismatchError,
)

logger = logging.getLogger(__name__)


CONTRACTS_PATH = Path(__file__).resolve().parents[2] / "config" / "freshness_contracts.yaml"
REGISTRY_PATH = Path(__file__).resolve().parents[2] / "config" / "writer_registry.yaml"


@dataclass(frozen=True)
class FreshnessContract:
    table: str
    column: str
    max_staleness_hours: float
    required_for: tuple[str, ...]   # strategies; '*' means all
    description: str
    populated_by: str
    # When True (default), staleness ignores weekend + US market holiday hours.
    # All current contracts are business-hours-driven (SEC publishes Mon-Fri;
    # daily-prices, refresh-features, etc. all run on the market calendar).
    # Set False only for a contract whose populator runs 7 days a week.
    business_hours_only: bool = True

    def applies_to(self, strategy: str) -> bool:
        return "*" in self.required_for or strategy in self.required_for


class FreshnessRegistry:
    """Loads freshness_contracts.yaml. Singleton-style — call get() to access."""

    _instance: Optional["FreshnessRegistry"] = None

    @classmethod
    def get(cls) -> "FreshnessRegistry":
        if cls._instance is None:
            cls._instance = cls(CONTRACTS_PATH)
        return cls._instance

    def __init__(self, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Freshness contracts file missing: {path}")
        raw = yaml.safe_load(path.read_text()) or {}
        self._contracts: dict[tuple[str, str], FreshnessContract] = {}
        for key, spec in raw.items():
            # key format: "table.column" or "schema.table.column"
            parts = key.split(".")
            if len(parts) == 2:
                table, column = parts
            elif len(parts) == 3:
                schema, table, column = parts
                table = f"{schema}.{table}"
            else:
                raise ValueError(f"Bad contract key: {key}")
            self._contracts[(table, column)] = FreshnessContract(
                table=table,
                column=column,
                max_staleness_hours=float(spec["max_staleness_hours"]),
                required_for=tuple(spec.get("required_for", ["*"])),
                description=spec.get("description", ""),
                populated_by=spec.get("populated_by", ""),
                business_hours_only=bool(spec.get("business_hours_only", True)),
            )

    def lookup(self, table: str, column: str) -> Optional[FreshnessContract]:
        return self._contracts.get((table, column))

    def for_strategy(self, strategy: str) -> list[FreshnessContract]:
        return [c for c in self._contracts.values() if c.applies_to(strategy)]

    def all(self) -> list[FreshnessContract]:
        return list(self._contracts.values())


# ── Business-hours staleness ────────────────────────────────────────────────

def business_age_hours(last_observed: datetime, now: Optional[datetime] = None) -> float:
    """Hours elapsed since `last_observed`, treating non-trading days
    (US weekends + NYSE holidays) as contributing zero hours.

    Used by the admin freshness panel to avoid showing weekend-only gaps
    as stale. A 50h gap that started Fri afternoon and ends Sun afternoon
    has ~10h of business time elapsed — well within a 26h SLA.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if last_observed >= now:
        return 0.0

    # Import lazily — calendar module is heavy and only needed for the
    # admin diagnostic endpoint, not for the runner halt path.
    from framework.data.calendar import MarketCalendar
    cal = MarketCalendar()

    # We use ET dates for the trading-day check. The exact offset doesn't
    # matter for whole-day weekend handling; small DST drift on the
    # boundary hour is acceptable for an SLA-display metric.
    ET = timezone(__import__("datetime").timedelta(hours=-4))
    start_et = last_observed.astimezone(ET)
    now_et = now.astimezone(ET)

    total_hours = (now - last_observed).total_seconds() / 3600.0
    if total_hours <= 0:
        return 0.0

    non_business_hours = 0.0
    from datetime import datetime as _dt, time as _time, timedelta as _td
    cursor_date = start_et.date()
    end_date = now_et.date()
    while cursor_date <= end_date:
        if not cal.is_trading_day(cursor_date):
            day_start = _dt.combine(cursor_date, _time.min, tzinfo=ET)
            day_end = day_start + _td(days=1)
            overlap_start = max(day_start, start_et)
            overlap_end = min(day_end, now_et)
            if overlap_end > overlap_start:
                non_business_hours += (overlap_end - overlap_start).total_seconds() / 3600.0
        cursor_date += _td(days=1)
    return max(0.0, total_hours - non_business_hours)


# ── Database lookup ─────────────────────────────────────────────────────────

def _lookup_signal_freshness(conn, table: str, column: str) -> Optional[datetime]:
    """Primary path: PG `signal_freshness` table written by compute pipelines."""
    with suppress(Exception):
        # Note: split table on '.' so "prices.daily_prices" maps to schema/table
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema, table_name = "public", table
        row = conn.execute(
            """
            SELECT last_computed_at FROM signal_freshness
             WHERE source = ? AND table_name = ? AND column_name = ?
             ORDER BY last_computed_at DESC LIMIT 1
            """,
            (schema, table_name, column),
        ).fetchone()
        if row and row[0]:
            ts = row[0]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
    return None


def get_freshness(
    conn,
    table: str,
    column: str,
) -> tuple[Optional[datetime], Optional[float]]:
    """Look up (last_computed_at, age_hours) from signal_freshness.

    Phase 2 P0 (2026-05-08) removed the structurally-broken fallback paths
    (MAX(filing_date) / MAX(as_of_date) / MAX(prices.date)) — they measured
    "when the latest row landed" not "when the compute pipeline ran," and
    that mismatch caused the false-positive halts the Phase 1 deploy left
    behind.

    Returns (None, None) if signal_freshness has no row for (table, column).
    Caller raises FreshnessUnknownError on (None, None) — this is distinct
    from StaleSignalError because it indicates the writer pipeline never
    ran, not that the data has aged out.
    """
    ts = _lookup_signal_freshness(conn, table, column)
    if ts is None:
        return None, None
    age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
    return ts, age_seconds / 3600.0


# ── Public API ──────────────────────────────────────────────────────────────

def assert_fresh(
    conn,
    *,
    table: str,
    column: str,
    strategy: Optional[str] = None,
) -> None:
    """Raise StaleSignalError if (table, column) is older than its contract.

    Caller passes a DB connection; we look up freshness from `signal_freshness`
    or the appropriate fallback.

    Strategy-aware: if `strategy` is provided, only check contracts that apply
    to that strategy (i.e., `required_for: [strategy]` or `['*']`).
    """
    registry = FreshnessRegistry.get()
    contract = registry.lookup(table, column)
    if contract is None:
        raise ValueError(
            f"No freshness contract registered for {table}.{column}. "
            f"Add an entry to config/freshness_contracts.yaml."
        )
    if strategy and not contract.applies_to(strategy):
        return  # contract doesn't apply to this strategy

    ts, age_hours = get_freshness(conn, table, column)
    if ts is None or age_hours is None:
        # No signal_freshness row exists — distinct from "data is stale."
        # This means the compute pipeline that should be writing this column
        # has never written. Fail closed with a different runbook (R-002).
        raise FreshnessUnknownError(
            table=table,
            column=column,
            strategy=strategy,
        )
    if age_hours > contract.max_staleness_hours:
        raise StaleSignalError(
            table=table,
            column=column,
            max_staleness_hours=contract.max_staleness_hours,
            observed_age_hours=age_hours,
            strategy=strategy,
        )


def assert_all_fresh_for_strategy(conn, strategy: str) -> None:
    """Raise on the first stale contract for `strategy`. Use at scan start."""
    registry = FreshnessRegistry.get()
    for contract in registry.for_strategy(strategy):
        assert_fresh(conn, table=contract.table, column=contract.column,
                     strategy=strategy)


# ── Writer registry (one source of truth for "who writes which column") ────


@dataclass(frozen=True)
class WriterRegistryEntry:
    column: str            # "table.column" format
    script: str            # path relative to repo root
    plists: tuple[str, ...]
    required_for: tuple[str, ...]
    sla_hours: Optional[float] = None
    notes: str = ""
    # When `recompute=False`, the column is set at INSERT-time (e.g. parsed
    # from XML) and never recomputed. There's no canonical writer-of-record
    # for signal_freshness rows in that case; the runtime writer-match check
    # skips these. Staleness check (assert_fresh) still runs — freshness
    # piggybacks on the ingest plist's filing_date row.
    recompute: bool = True
    # When `dynamic_columns=True`, the script writes signal_freshness rows
    # via a loop over many columns (e.g. compute_signals.py, 24 detectors).
    # The audit can't AST-extract literal column names, and the runtime
    # writer-match still works because populated_by IS uniformly the script.
    dynamic_columns: bool = False

    def applies_to(self, strategy: str) -> bool:
        return "*" in self.required_for or strategy in self.required_for


class WriterRegistry:
    """Loads writer_registry.yaml. Singleton-style."""

    _instance: Optional["WriterRegistry"] = None

    @classmethod
    def get(cls) -> "WriterRegistry":
        if cls._instance is None:
            cls._instance = cls(REGISTRY_PATH)
        return cls._instance

    def __init__(self, path: Path):
        self._by_column: dict[str, WriterRegistryEntry] = {}
        if not path.exists():
            # Registry is optional during the rollout window — log and continue
            logger.warning("Writer registry missing: %s (assert_writer_wired will no-op)", path)
            return
        raw = yaml.safe_load(path.read_text()) or {}
        for spec in raw.get("writers", []):
            col = spec.get("column")
            if not col:
                continue
            self._by_column[col] = WriterRegistryEntry(
                column=col,
                script=spec.get("script", ""),
                plists=tuple(spec.get("plists") or ()),
                required_for=tuple(spec.get("required_for") or ()),
                sla_hours=spec.get("sla_hours"),
                notes=spec.get("notes", "") or "",
                recompute=bool(spec.get("recompute", True)),
                dynamic_columns=bool(spec.get("dynamic_columns", False)),
            )

    def lookup(self, column_key: str) -> Optional[WriterRegistryEntry]:
        """`column_key` is the full 'table.column' (or 'schema.table.column') string."""
        return self._by_column.get(column_key)

    def for_strategy(self, strategy: str) -> list[WriterRegistryEntry]:
        return [e for e in self._by_column.values() if e.applies_to(strategy)]


def _lookup_populated_by(conn, table: str, column: str) -> Optional[str]:
    """Most-recent `populated_by` value in signal_freshness for (table, column)."""
    with suppress(Exception):
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema, table_name = "public", table
        row = conn.execute(
            """
            SELECT populated_by FROM signal_freshness
             WHERE source = ? AND table_name = ? AND column_name = ?
             ORDER BY last_computed_at DESC LIMIT 1
            """,
            (schema, table_name, column),
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    return None


def assert_writer_wired(
    conn,
    *,
    table: str,
    column: str,
    strategy: Optional[str] = None,
) -> None:
    """Raise WriterMismatchError if the most-recent signal_freshness row for
    `(table, column)` was written by a script different from the one declared
    in `config/writer_registry.yaml`.

    Catches the mislabel failure mode: a contract claims script_A is the
    writer, the column gets stamped fresh because script_A happens to call
    write_freshness for some unrelated work, but the REAL writer is script_B
    (which silently stopped running). is_rare_reversal was this exact case
    for 8 weeks pre-2026-05-16.

    If the registry has no entry for this column, no-op (during rollout we
    accept partial coverage). Once every contracted column has a registry
    entry, the writer_registry_audit check #4 will reject any contract
    without one.
    """
    registry = WriterRegistry.get()
    column_key = f"{table}.{column}"
    entry = registry.lookup(column_key)
    if entry is None or not entry.script:
        return  # not yet registered — skip
    if strategy and not entry.applies_to(strategy):
        return
    if not entry.recompute:
        # Parse-time column (set at INSERT, never re-stamped). No canonical
        # writer-of-record for signal_freshness; staleness check piggybacks
        # on the ingest plist's filing_date row.
        return

    observed = _lookup_populated_by(conn, table, column)
    if observed is None:
        # No signal_freshness row at all — that's FreshnessUnknownError territory
        # (handled by assert_fresh), not a mismatch. Skip here.
        return
    if observed != entry.script:
        raise WriterMismatchError(
            table=table,
            column=column,
            registered_script=entry.script,
            observed_populated_by=observed,
            strategy=strategy,
        )


def assert_all_writers_wired_for_strategy(conn, strategy: str) -> None:
    """Raise on the first writer mismatch for `strategy`. Use at scan start
    after `assert_freshness_system_healthy` and before `assert_all_fresh_for_strategy`."""
    registry = WriterRegistry.get()
    for entry in registry.for_strategy(strategy):
        if "." in entry.column:
            table, column = entry.column.rsplit(".", 1)
        else:
            continue
        assert_writer_wired(conn, table=table, column=column, strategy=strategy)


def assert_freshness_system_healthy(conn, strategy: str) -> None:
    """Meta-check: signal_freshness has at least one row for every contracted
    column the strategy depends on.

    Raised BEFORE per-column staleness checks. The distinction matters:

      - `assert_all_fresh_for_strategy` answers "are the inputs fresh enough
        for this scan?" Failure means a single nightly run lapsed (R-001).
      - `assert_freshness_system_healthy` answers "is the writer pipeline
        functional at all?" Failure means the writers have never run for
        one or more columns. Fundamentally a different problem (R-003)
        with a different remediation: run scripts/backfill_signal_freshness.py
        to seed initial values, then verify each compute pipeline calls
        write_freshness().

    This guard is what would have caught the Phase 1 incomplete deployment
    (signal_freshness schema landed, writes didn't): the meta-check would
    have raised FreshnessSystemBrokenError on Day 1, not Day 6.
    """
    registry = FreshnessRegistry.get()
    contracts = registry.for_strategy(strategy)
    missing: list[str] = []
    for c in contracts:
        ts = _lookup_signal_freshness(conn, c.table, c.column)
        if ts is None:
            missing.append(f"{c.table}.{c.column}")
    if missing:
        raise FreshnessSystemBrokenError(
            strategy=strategy,
            missing_columns=missing,
        )


# ── CLI for inspection ──────────────────────────────────────────────────────

if __name__ == "__main__":
    """Print the freshness status of every contracted column.

    Usage:  python3 -m framework.contracts.freshness
            python3 -m framework.contracts.freshness --strategy quality_momentum
    """
    import argparse
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from config.database import get_connection

    p = argparse.ArgumentParser()
    p.add_argument("--strategy", help="Show only contracts for this strategy")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    registry = FreshnessRegistry.get()
    contracts = (
        registry.for_strategy(args.strategy) if args.strategy else registry.all()
    )
    conn = get_connection(readonly=True)
    rows = []
    for c in contracts:
        try:
            ts, age = get_freshness(conn, c.table, c.column)
        except Exception as e:
            ts, age = None, None
        is_stale = age is None or age > c.max_staleness_hours
        rows.append({
            "table": c.table, "column": c.column,
            "max_h": c.max_staleness_hours,
            "age_h": round(age, 2) if age is not None else None,
            "stale": is_stale,
            "last_at": ts.isoformat() if ts else None,
        })
    conn.close()

    if args.json:
        import json
        print(json.dumps(rows, indent=2))
    else:
        print(f"{'table.column':<48} {'max_h':>7} {'age_h':>9} {'status':>8}")
        print("─" * 80)
        for r in rows:
            mark = "STALE" if r["stale"] else "ok"
            age_str = f"{r['age_h']:.1f}" if r['age_h'] is not None else "—"
            print(f"{r['table']+'.'+r['column']:<48} {r['max_h']:>7.1f} {age_str:>9} {mark:>8}")
        n_stale = sum(1 for r in rows if r["stale"])
        print(f"\n{n_stale} stale / {len(rows)} total")
        sys.exit(1 if n_stale else 0)

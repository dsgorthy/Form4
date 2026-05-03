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
from framework.contracts.exceptions import StaleSignalError

logger = logging.getLogger(__name__)


CONTRACTS_PATH = Path(__file__).resolve().parents[2] / "config" / "freshness_contracts.yaml"


@dataclass(frozen=True)
class FreshnessContract:
    table: str
    column: str
    max_staleness_hours: float
    required_for: tuple[str, ...]   # strategies; '*' means all
    description: str
    populated_by: str

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
            )

    def lookup(self, table: str, column: str) -> Optional[FreshnessContract]:
        return self._contracts.get((table, column))

    def for_strategy(self, strategy: str) -> list[FreshnessContract]:
        return [c for c in self._contracts.values() if c.applies_to(strategy)]

    def all(self) -> list[FreshnessContract]:
        return list(self._contracts.values())


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


def _lookup_max_filing_date(conn, table: str, column: str) -> Optional[datetime]:
    """Fallback: MAX(filing_date) WHERE column IS NOT NULL.

    Only valid for columns on `trades`. Other tables fall back to None
    (caller treats as "unknown freshness" → fails the contract).
    """
    if not table.endswith("trades"):
        return None
    with suppress(Exception):
        row = conn.execute(
            f"SELECT MAX(filing_date) FROM {table} WHERE {column} IS NOT NULL"
        ).fetchone()
        if row and row[0]:
            # filing_date is text 'YYYY-MM-DD'
            d = row[0]
            if isinstance(d, str):
                return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
            if isinstance(d, datetime):
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _lookup_max_score_as_of(conn) -> Optional[datetime]:
    """Fallback for insider_ticker_scores.blended_score — uses as_of_date."""
    with suppress(Exception):
        row = conn.execute(
            "SELECT MAX(as_of_date) FROM insider_ticker_scores"
        ).fetchone()
        if row and row[0]:
            d = row[0]
            if isinstance(d, str):
                return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
            if isinstance(d, datetime):
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _lookup_max_price_date(conn, ticker: str = "SPY") -> Optional[datetime]:
    """Fallback for prices.daily_prices — MAX(date) for SPY (proxy)."""
    with suppress(Exception):
        row = conn.execute(
            "SELECT MAX(date) FROM prices.daily_prices WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if row and row[0]:
            d = row[0]
            if isinstance(d, str):
                return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
            if isinstance(d, datetime):
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def get_freshness(
    conn,
    table: str,
    column: str,
) -> tuple[Optional[datetime], Optional[float]]:
    """Look up (last_computed_at, age_hours).

    Returns (None, None) if no freshness data is available — caller treats
    this as a contract violation.
    """
    ts = _lookup_signal_freshness(conn, table, column)
    if ts is None:
        # Fallback paths
        if table == "insider_ticker_scores" and column == "blended_score":
            ts = _lookup_max_score_as_of(conn)
        elif table.endswith("daily_prices"):
            ts = _lookup_max_price_date(conn)
        else:
            ts = _lookup_max_filing_date(conn, table, column)
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
        raise StaleSignalError(
            table=table,
            column=column,
            max_staleness_hours=contract.max_staleness_hours,
            observed_age_hours=float("inf"),
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

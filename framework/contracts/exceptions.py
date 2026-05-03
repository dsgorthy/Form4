"""Typed exceptions for fail-closed contract enforcement.

Why typed exceptions: the April 2026 outage was prolonged in part because
`backfill.py` caught `sqlite3.OperationalError` to handle ALTER TABLE
idempotency, and PG's equivalent `psycopg2.errors.DuplicateColumn` slipped
through the catch — silently bailing out of `migrate_schema`. Catching by
type, never by `str(e)` substring, eliminates that whole class of bug.

Convention: every contract violation is a distinct subclass so call sites
can choose to halt vs. degrade vs. skip per-violation. The runner halts
strategy-wide on `StaleSignalError` / `DataQualityHaltError` and rejects
individual candidates on `ConvictionInputMissing`.
"""
from __future__ import annotations

from typing import Optional


class ContractError(Exception):
    """Base class — never raised directly. Catch-all for the contract module."""


class StaleSignalError(ContractError):
    """A column the runner depends on is fresher than its freshness contract.

    Raised by `freshness.assert_fresh()`. Strategy runners that catch this
    must HALT entries strategy-wide and emit a P0 alert. Exits and
    reconciliation are unaffected — the halt is on the entry path only.
    """

    def __init__(
        self,
        *,
        table: str,
        column: str,
        max_staleness_hours: float,
        observed_age_hours: float,
        strategy: Optional[str] = None,
    ):
        self.table = table
        self.column = column
        self.max_staleness_hours = max_staleness_hours
        self.observed_age_hours = observed_age_hours
        self.strategy = strategy
        msg = (
            f"{table}.{column} is {observed_age_hours:.1f}h stale "
            f"(contract max: {max_staleness_hours:.1f}h)"
        )
        if strategy:
            msg = f"[{strategy}] {msg}"
        super().__init__(msg)


class DataQualityHaltError(ContractError):
    """Aggregate data-quality violation — too many NULL inputs in a single scan.

    Raised by the runner when per-stage NULL-rejection counters exceed the
    configured tolerance (default 10% of candidates). Distinct from
    `StaleSignalError` because the freshness contract is technically met
    (the data exists) but its quality is collapsed.
    """

    def __init__(
        self,
        *,
        strategy: str,
        stage: str,
        total: int,
        null_rejected: int,
        threshold_pct: float = 10.0,
    ):
        self.strategy = strategy
        self.stage = stage
        self.total = total
        self.null_rejected = null_rejected
        self.threshold_pct = threshold_pct
        actual_pct = (100.0 * null_rejected / total) if total else 0.0
        super().__init__(
            f"[{strategy}] stage={stage}: {null_rejected}/{total} "
            f"({actual_pct:.1f}%) candidates rejected for NULL inputs, "
            f"threshold {threshold_pct:.1f}%"
        )


class ConvictionInputMissing(ContractError):
    """A required input to compute_conviction() is NULL in strict mode.

    Raised by `compute_conviction(strict=True, ...)`. Caller (the runner)
    catches per-candidate and rejects with `reason='conviction_input_missing'`.
    NOT a strategy-wide halt — only this candidate is rejected.
    """

    def __init__(self, *, thesis: str, missing_fields: list[str]):
        self.thesis = thesis
        self.missing_fields = list(missing_fields)
        super().__init__(
            f"thesis={thesis}: required fields are NULL: {', '.join(self.missing_fields)}"
        )


class ReconciliationDriftError(ContractError):
    """Alpaca account state diverges from the runner's strategy_portfolio rows.

    Raised by `pipelines/probes/alpaca_reconcile.py` when:
      - A position exists in Alpaca but not in DB (orphan-in-broker)
      - A position exists in DB but not in Alpaca (orphan-in-DB)
      - Quantity / direction mismatch

    The runner reads a sentinel file (`data/{strategy}_reconcile_ack.json`)
    and refuses to enter new positions until a human acks the drift.
    """

    def __init__(self, *, strategy: str, kind: str, details: str):
        self.strategy = strategy
        self.kind = kind   # 'orphan_alpaca' | 'orphan_db' | 'size_mismatch'
        self.details = details
        super().__init__(f"[{strategy}] {kind}: {details}")

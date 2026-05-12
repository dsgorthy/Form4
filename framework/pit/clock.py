"""PITClock — the single source of truth for "what could we know at time X."

A `PITClock` is an immutable wrapper around an `as_of_date` (YYYY-MM-DD).
Every DB read in `PITDataView` asks the clock to verify that the row's
knowledge_date <= as_of_date. Violations raise `LookaheadError`.

The clock also accumulates a read tape — a list of (source, knowledge_date)
tuples — that makes PIT compliance debuggable. After a backtest run, the
tape can be diff'd against the row set the engine returned to prove no
contamination.

Design notes:
- Frozen dataclass: an as_of_date never mutates once a clock exists.
- The read tape lives on a separate object passed by reference so the
  frozen dataclass invariant holds even as the tape grows.
- `LookaheadError` is a hard error, not a warning. PIT bugs are silent by
  default — making this fail loudly is the whole point.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple


class LookaheadError(AssertionError):
    """Raised when a PIT-protected read returns data with a knowledge_date
    later than the clock's as_of_date. This is always a bug — either in the
    query (missing WHERE clause) or in the data (mislabeled knowledge_date).
    """


def _validate_iso_date(s: str, name: str) -> None:
    """Cheap shape check — ISO date strings should sort lexicographically."""
    if not isinstance(s, str) or len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"{name}={s!r} is not an ISO YYYY-MM-DD date string")
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"{name}={s!r} not a valid ISO date: {e}") from None


@dataclass
class ReadTape:
    """Mutable companion to PITClock. Records every PIT-protected read.

    Strategies/engines should NOT touch this directly; it's appended to by
    `PITClock.assert_known` and consumed by the engine for post-hoc audits.
    """
    entries: List[Tuple[str, str]] = field(default_factory=list)

    def record(self, source: str, knowledge_date: str) -> None:
        self.entries.append((source, knowledge_date))

    def __len__(self) -> int:
        return len(self.entries)

    def max_knowledge_date(self) -> str | None:
        return max((kd for _, kd in self.entries), default=None)


@dataclass(frozen=True)
class PITClock:
    """Immutable point-in-time clock.

    Example:
        clock = PITClock(as_of_date="2024-03-15")
        clock.assert_known("2024-03-10", source="trade.filing_date")  # OK
        clock.assert_known("2024-03-16", source="trade.filing_date")  # raises
    """
    as_of_date: str
    tape: ReadTape = field(default_factory=ReadTape, compare=False, hash=False)

    def __post_init__(self) -> None:
        _validate_iso_date(self.as_of_date, "as_of_date")

    def assert_known(self, knowledge_date: str, source: str) -> None:
        """Verify that a row with `knowledge_date` was observable on
        `as_of_date`. Always record the check on the tape (even if it
        passes) so we can prove the engine read only PIT-safe rows."""
        if knowledge_date is None:
            # Defensive: missing knowledge_date is itself a data quality bug.
            # We raise here because the engine cannot prove non-leakage.
            raise LookaheadError(
                f"PIT check failed: row from {source!r} has knowledge_date=None"
            )
        _validate_iso_date(knowledge_date, f"{source}.knowledge_date")
        if knowledge_date > self.as_of_date:
            raise LookaheadError(
                f"PIT violation in {source!r}: knowledge_date={knowledge_date} "
                f"> as_of_date={self.as_of_date}. Add `WHERE knowledge_date <= ?` "
                f"or fix the data."
            )
        self.tape.record(source, knowledge_date)

    def is_known(self, knowledge_date: str) -> bool:
        """Non-raising check. Use sparingly — prefer `assert_known` so
        violations surface loudly."""
        if knowledge_date is None:
            return False
        return knowledge_date <= self.as_of_date

    def cutoff(self, lag_days: int = 0) -> str:
        """Return `as_of_date - lag_days` as an ISO date. Used by accessors
        that need a forward-observability lag (e.g., 7d returns are only
        observable 7+ days after trade_date)."""
        if lag_days == 0:
            return self.as_of_date
        from datetime import timedelta
        return (
            datetime.strptime(self.as_of_date, "%Y-%m-%d")
            - timedelta(days=lag_days)
        ).strftime("%Y-%m-%d")

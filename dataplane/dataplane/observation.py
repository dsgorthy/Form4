"""SignalObservation — the universal row shape.

Every Signal.compute() returns one of these. They serialize to rows in
the signal_observations table. They are immutable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4


@dataclass(frozen=True)
class SignalObservation:
    """A single (signal_id, ticker, as_of_date) → value observation.

    Required invariants enforced in __post_init__:
      - as_of_date must be timezone-aware (UTC preferred)
      - confidence, if present, must be in [0.0, 1.0]
      - value must be JSON-serializable (validated at write time)
    """

    signal_id: str
    ticker: str
    as_of_date: datetime
    value: Any  # float | int | bool | str | dict
    source_run_id: UUID
    confidence: Optional[float] = None
    ingested_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.as_of_date.tzinfo is None:
            raise ValueError(
                f"as_of_date must be timezone-aware: {self.as_of_date!r}. "
                f"Use datetime.now(timezone.utc) or pin a UTC timestamp."
            )
        if self.confidence is not None and not (0.0 <= self.confidence <= 1.0):
            raise ValueError(
                f"confidence must be in [0.0, 1.0]; got {self.confidence}"
            )
        if not self.signal_id:
            raise ValueError("signal_id is required")
        if not self.ticker:
            raise ValueError("ticker is required")

    @property
    def signal_class(self) -> str:
        """Return the top-level class from signal_id ('insider', 'options', …).

        Used to route catalog UI grouping and validate against the
        signal_class enum at write time.
        """
        return self.signal_id.split(".", 1)[0]

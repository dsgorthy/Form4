"""Signal — the abstract base class for every dataplane signal.

A signal is a versioned, owned, contracted function from (ticker, as_of)
to a SignalObservation. The class attributes declare metadata; the
compute() method is the implementation; PIT.strict enforces the rules.

Subclass requirements:
  - signal_id      : str, e.g. "insider.career_grade"
  - version        : str, e.g. "v3.0.1"
  - owner          : str, e.g. "derek"
  - sla_hours      : float, max acceptable staleness
  - upstream       : list[Upstream], declared dependencies
  - output_schema  : dict, e.g. {"grade": "text", "score": "float"}
  - compute()      : implement; decorate with @PIT.strict

Optional:
  - business_hours_only : bool = True
  - test_cases    : classmethod returning list[PITTestCase]
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar, List, Optional
from uuid import UUID, uuid4

from dataplane.observation import SignalObservation
from dataplane.pit import PITViolationError
from dataplane.upstream import Upstream


class Signal(ABC):
    """Base class for all signals. See module docstring for the contract."""

    # Class-level metadata. Subclass MUST set; __init_subclass__ enforces.
    signal_id: ClassVar[Optional[str]] = None
    version: ClassVar[Optional[str]] = None
    owner: ClassVar[Optional[str]] = None
    sla_hours: ClassVar[Optional[float]] = None
    upstream: ClassVar[List[Upstream]] = []
    output_schema: ClassVar[dict] = {}
    business_hours_only: ClassVar[bool] = True
    description: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs):
        """Validate that subclass declared the required class attributes.

        Skipped for direct subclasses meant as further intermediate bases
        (set ``_dataplane_abstract = True`` on those).
        """
        super().__init_subclass__(**kwargs)
        if getattr(cls, "_dataplane_abstract", False):
            return
        required = ["signal_id", "version", "owner", "sla_hours"]
        missing = [a for a in required if getattr(cls, a, None) is None]
        if missing:
            raise TypeError(
                f"{cls.__name__} must define class attributes: {missing}. "
                f"See dataplane.Signal docstring."
            )
        if not isinstance(cls.upstream, (list, tuple)):
            raise TypeError(
                f"{cls.__name__}.upstream must be a list of Upstream; "
                f"got {type(cls.upstream).__name__}"
            )

    def __init__(self, conn=None):
        # Per-instance run state. A Signal is cheap to instantiate; one per
        # compute call is fine, but reuse is also fine (PITValidator does both).
        self._conn = conn
        self._pit_as_of: Optional[datetime] = None
        self._pit_mode: str = "normal"  # "normal" | "frozen"
        self._run_id: UUID = uuid4()

    # ── Subclass entry point ────────────────────────────────────────────

    @abstractmethod
    def compute(self, ticker: str, as_of: datetime) -> SignalObservation:
        """Compute the signal for one (ticker, as_of). Decorate with @PIT.strict."""
        ...

    @classmethod
    def test_cases(cls) -> list:
        """Override to return list[PITTestCase]. Defaults to empty.

        The empty default lets bare signals exist (for prototyping) but
        CI should require non-empty test_cases for any signal in production.
        """
        return []

    # ── Framework-provided data access ─────────────────────────────────

    def read(self, signal_id: str, ticker: str, as_of: Optional[datetime] = None):
        """The only sanctioned data accessor inside compute().

        Reads rows from signal_observations where:
          - signal_id matches the upstream declaration
          - ticker matches
          - as_of_date <= (current_as_of - upstream.pit_lag)
          - in frozen mode: additionally ingested_at < current_as_of

        Raises PITViolationError if:
          - signal_id is not in this signal's upstream list
          - called outside of a @PIT.strict compute() call
        """
        if self._pit_as_of is None:
            raise PITViolationError(
                "Signal.read() called outside of @PIT.strict compute(). "
                "Either decorate compute() with @PIT.strict or supply as_of."
            )
        current = as_of or self._pit_as_of

        # Look up the upstream declaration (must match exactly, version-agnostic)
        matches = [u for u in self.upstream if u.signal_id == signal_id]
        if not matches:
            declared = [u.signal_id for u in self.upstream]
            raise PITViolationError(
                f"{self.signal_id} reads '{signal_id}' but it is not in the "
                f"upstream list. Declared: {declared}. Add an Upstream(...) "
                f"entry to the class."
            )
        up = matches[0]
        max_as_of = current - up.pit_lag

        if self._conn is None:
            raise RuntimeError(
                f"Signal {self.signal_id} has no DB connection. "
                f"Pass conn= to the constructor or use the catalog runner."
            )

        if self._pit_mode == "frozen":
            sql = """
                SELECT signal_id, ticker, as_of_date, value, confidence,
                       source_run_id, ingested_at, metadata
                  FROM signal_observations
                 WHERE signal_id LIKE %s
                   AND ticker = %s
                   AND as_of_date <= %s
                   AND ingested_at < %s
                 ORDER BY as_of_date DESC
            """
            params = (
                f"{signal_id}%",   # allow version-suffixed reads (signal.v3)
                ticker,
                max_as_of,
                current,
            )
        else:
            sql = """
                SELECT signal_id, ticker, as_of_date, value, confidence,
                       source_run_id, ingested_at, metadata
                  FROM signal_observations
                 WHERE signal_id LIKE %s
                   AND ticker = %s
                   AND as_of_date <= %s
                 ORDER BY as_of_date DESC
            """
            params = (f"{signal_id}%", ticker, max_as_of)

        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    # ── Constructor sugar for the result of compute() ──────────────────

    def observation(
        self,
        ticker: str,
        as_of: datetime,
        value: Any,
        confidence: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> SignalObservation:
        """Build a SignalObservation tagged with this run's IDs."""
        return SignalObservation(
            signal_id=f"{self.signal_id}.{self.version}",
            ticker=ticker,
            as_of_date=as_of,
            value=value,
            source_run_id=self._run_id,
            confidence=confidence,
            metadata=metadata or {},
        )

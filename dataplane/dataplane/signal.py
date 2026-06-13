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

from datetime import datetime, timedelta
from typing import Any, ClassVar, List, Optional
from uuid import UUID, uuid4

from dataplane.observation import SignalObservation
from dataplane.pit import PITViolationError
from dataplane.upstream import Upstream


class Signal:
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

    # Materialization mode:
    #   "per_ticker_per_day" (default) → compute(ticker, as_of) → one obs
    #   "per_partition_events" → materialize_partition(partition_date) →
    #                            list[obs] with as_of_date = event timestamps
    # The per_partition_events mode handles raw event streams where many
    # observations land on the same calendar day for the same ticker (Form 4
    # filings, options ticks, news items). Each event gets a precise
    # timestamp so (signal_id, ticker, as_of_date) stays unique.
    materialization_mode: ClassVar[str] = "per_ticker_per_day"

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
        valid_modes = ("per_ticker_per_day", "per_partition_events")
        if cls.materialization_mode not in valid_modes:
            raise TypeError(
                f"{cls.__name__}.materialization_mode must be one of {valid_modes}; "
                f"got {cls.materialization_mode!r}"
            )

    def __init__(self, conn=None):
        # Per-instance run state. A Signal is cheap to instantiate; one per
        # compute call is fine, but reuse is also fine (PITValidator does both).
        self._conn = conn
        self._pit_as_of: Optional[datetime] = None
        self._pit_mode: str = "normal"  # "normal" | "frozen"
        self._run_id: UUID = uuid4()

    # ── Subclass entry point ────────────────────────────────────────────

    def compute(self, ticker: str, as_of: datetime) -> SignalObservation:
        """Per-ticker-per-day signals: implement this.

        Default raises NotImplementedError so per_partition_events signals
        don't accidentally get called via this path. Decorate with @PIT.strict.
        """
        raise NotImplementedError(
            f"{type(self).__name__} did not implement compute(). If this is "
            f"a per_partition_events signal, implement materialize_partition() "
            f"instead."
        )

    def materialize_partition(self, partition_date: datetime) -> list:
        """Per-partition-events signals: implement this. Returns
        list[SignalObservation], one per event landing on partition_date,
        each with as_of_date set to its own event timestamp.

        Per_ticker_per_day signals don't override this; the framework's
        asset wrapper routes to compute() instead.
        """
        raise NotImplementedError(
            f"{type(self).__name__} did not implement materialize_partition(). "
            f"If this is a per_ticker_per_day signal, implement compute() instead."
        )

    @classmethod
    def test_cases(cls) -> list:
        """Override to return list[PITTestCase]. Defaults to empty.

        The empty default lets bare signals exist (for prototyping) but
        CI should require non-empty test_cases for any signal in production.
        """
        return []

    # ── Framework-provided data access ─────────────────────────────────

    def read(
        self,
        signal_id: str,
        ticker: str,
        as_of: Optional[datetime] = None,
        lookback: Optional[timedelta] = None,
    ):
        """The only sanctioned data accessor inside compute().

        Reads rows from signal_observations where:
          - signal_id matches the upstream declaration
          - ticker matches
          - as_of_date <= (current_as_of - upstream.pit_lag)
          - if lookback is set: also as_of_date >= (max_as_of - lookback)
          - in frozen mode: additionally ingested_at < current_as_of

        Returns rows newest-first.

        ``lookback`` lets gates request a bounded series without filtering
        in Python — e.g. ``read(... lookback=timedelta(days=90))`` is the
        90-day window used by drawdown/momentum gates.

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
        min_as_of = max_as_of - lookback if lookback is not None else None

        if self._conn is None:
            raise RuntimeError(
                f"Signal {self.signal_id} has no DB connection. "
                f"Pass conn= to the constructor or use the catalog runner."
            )

        where = [
            "signal_id LIKE %s",
            "ticker = %s",
            "as_of_date <= %s",
        ]
        params: list = [f"{signal_id}%", ticker, max_as_of]
        if min_as_of is not None:
            where.append("as_of_date >= %s")
            params.append(min_as_of)
        if self._pit_mode == "frozen":
            where.append("ingested_at < %s")
            params.append(current)

        sql = (
            "SELECT signal_id, ticker, as_of_date, value, confidence, "
            "source_run_id, ingested_at, metadata "
            "FROM signal_observations "
            "WHERE " + " AND ".join(where) + " "
            "ORDER BY as_of_date DESC"
        )

        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(params))
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

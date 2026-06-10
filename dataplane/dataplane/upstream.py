"""Upstream — a declared data dependency with a PIT lag.

A signal that reads from another signal declares this. The framework's
self.read(upstream_signal_id, ticker, as_of) is the only sanctioned data
accessor inside compute(); it returns rows where as_of_date is no later
than (current_as_of - upstream.pit_lag).

Example: Form 4 filings legally have up to a 24-hour reporting lag. A signal
that reads insider.trades.raw at as_of=2024-06-01T12:00 should only see rows
whose as_of_date ≤ 2024-05-31T12:00. PIT lag enforces that.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Upstream:
    """One declared upstream dependency.

    Args:
        signal_id: Upstream signal_id (without version suffix), e.g. ``"insider.trades.raw"``
        pit_lag:   Wall-clock delay between the upstream observation becoming
                   true in the world and becoming queryable in our plane.
                   ``timedelta(0)`` is the right default for prices and any
                   feed where the data appears the instant it exists.
        version:   Optional explicit version pin. ``None`` = always read latest
                   active version.
    """

    signal_id: str
    pit_lag: timedelta = timedelta(0)
    version: str | None = None

    def to_json(self) -> dict:
        """For storage in signal_definitions.upstream JSONB column."""
        return {
            "signal_id": self.signal_id,
            "pit_lag_seconds": int(self.pit_lag.total_seconds()),
            "version": self.version,
        }

    @classmethod
    def from_json(cls, d: dict) -> "Upstream":
        return cls(
            signal_id=d["signal_id"],
            pit_lag=timedelta(seconds=d.get("pit_lag_seconds", 0)),
            version=d.get("version"),
        )

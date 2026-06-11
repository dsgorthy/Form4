"""Backfill — walk a signal's partitions chronologically and write each one.

Idempotent by construction: write_observation is ON CONFLICT DO UPDATE, so
re-running any window is always safe. Order matters for derived signals —
their read() must see the upstream rows that backfill writes earlier in the
walk, so always chronologically ascending.

Used by ``python3 -m dataplane backfill ...``.
"""
from __future__ import annotations

import os
import traceback
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, List, Optional, Sequence, Type

import psycopg2

from dataplane.catalog import register, write_observation
from dataplane.discovery import DEFAULT_TICKERS, find_signal
from dataplane.signal import Signal


# Connection helper — kept local so the CLI doesn't depend on dagster_project.
@contextmanager
def _dataplane_connection():
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    conn = psycopg2.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()


@dataclass
class PartitionResult:
    partition_date: str
    written: int = 0
    errors: int = 0
    error_samples: List[str] = field(default_factory=list)


@dataclass
class BackfillResult:
    signal_id: str
    version: str
    mode: str
    from_date: str
    to_date: str
    partitions: List[PartitionResult] = field(default_factory=list)

    @property
    def total_written(self) -> int:
        return sum(p.written for p in self.partitions)

    @property
    def total_errors(self) -> int:
        return sum(p.errors for p in self.partitions)


def _iter_dates(start: datetime, end: datetime) -> Iterable[datetime]:
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def _backfill_per_partition_events(
    signal: Signal,
    conn,
    dates: Sequence[datetime],
    progress: Optional[Callable[[PartitionResult], None]] = None,
) -> List[PartitionResult]:
    out: List[PartitionResult] = []
    for d in dates:
        pr = PartitionResult(partition_date=d.date().isoformat())
        try:
            observations = signal.materialize_partition(d)
        except Exception as exc:
            pr.errors += 1
            pr.error_samples.append(f"materialize_partition: {exc}")
            out.append(pr)
            if progress:
                progress(pr)
            continue
        for obs in observations:
            try:
                write_observation(conn, obs)
                pr.written += 1
            except Exception as exc:
                pr.errors += 1
                if len(pr.error_samples) < 5:
                    pr.error_samples.append(f"{obs.ticker}@{obs.as_of_date}: {exc}")
        out.append(pr)
        if progress:
            progress(pr)
    return out


def _backfill_per_ticker_per_day(
    signal: Signal,
    conn,
    dates: Sequence[datetime],
    tickers: Sequence[str],
    progress: Optional[Callable[[PartitionResult], None]] = None,
) -> List[PartitionResult]:
    out: List[PartitionResult] = []
    for d in dates:
        pr = PartitionResult(partition_date=d.date().isoformat())
        for ticker in tickers:
            try:
                obs = signal.compute(ticker, d)
                write_observation(conn, obs)
                pr.written += 1
            except Exception as exc:
                pr.errors += 1
                if len(pr.error_samples) < 5:
                    pr.error_samples.append(f"{ticker}: {exc}")
        out.append(pr)
        if progress:
            progress(pr)
    return out


def backfill(
    signal_ref: str,
    from_date: str,
    to_date: str,
    tickers: Optional[Sequence[str]] = None,
    progress: Optional[Callable[[PartitionResult], None]] = None,
    dry_run: bool = False,
) -> BackfillResult:
    """Run a backfill. Returns a result summary.

    Raises ValueError on unknown signal, invalid dates, or empty range.
    """
    sig_cls = find_signal(signal_ref)
    if sig_cls is None:
        raise ValueError(
            f"signal {signal_ref!r} not found in the signals/ package"
        )

    start = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc)
    if end < start:
        raise ValueError(f"to ({to_date}) is before from ({from_date})")

    dates = list(_iter_dates(start, end))

    universe = list(tickers) if tickers else DEFAULT_TICKERS

    result = BackfillResult(
        signal_id=sig_cls.signal_id,
        version=sig_cls.version,
        mode=sig_cls.materialization_mode,
        from_date=from_date,
        to_date=to_date,
    )

    if dry_run:
        # Resolve the signal class, count partitions + universe, return.
        for d in dates:
            result.partitions.append(
                PartitionResult(partition_date=d.date().isoformat())
            )
        return result

    with _dataplane_connection() as conn:
        register(conn, sig_cls)  # idempotent — ensures signal_definitions row exists
        signal = sig_cls(conn=conn)

        if sig_cls.materialization_mode == "per_partition_events":
            result.partitions = _backfill_per_partition_events(
                signal, conn, dates, progress=progress
            )
        else:
            result.partitions = _backfill_per_ticker_per_day(
                signal, conn, dates, universe, progress=progress
            )

    return result

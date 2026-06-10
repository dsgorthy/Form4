"""Auto-wrap Signal subclasses as Dagster assets.

For each Signal class discovered in the `signals/` package, this module
produces a Dagster asset that, when materialized, runs `compute()` for a
configured list of tickers at the partition's date and upserts the
resulting SignalObservations into `signal_observations` via the catalog.

Each asset is daily-partitioned. Backfills walk chronologically and respect
the per-signal upstream PIT lag.

Note: this module deliberately does NOT use `from __future__ import
annotations`. Dagster validates the asset compute function's `context`
parameter annotation at decoration time, and string-form annotations
break that validation.
"""
import importlib
import inspect
import pkgutil
from datetime import datetime, timezone
from typing import List, Type

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

import signals as signals_pkg
from dataplane import Signal
from dataplane.catalog import register, write_observation

from dagster_project.resources import PostgresResource


# Stable starting point for the dataplane's daily partitions. Plenty of
# room for historical backfill once the lake is online.
PARTITION_START = "2020-01-01"

# Conservative default ticker universe. Override per-asset via config or
# extend by editing this list. Backfill jobs can supply a custom list.
DEFAULT_TICKERS: List[str] = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
    "AMD", "AVGO", "ADBE", "NFLX", "INTC",
    "JPM", "BAC", "WFC", "GS",
    "JNJ", "PFE", "MRK", "LLY",
    "XOM", "CVX",
    "BRK.B", "V", "MA", "HD", "WMT", "PG",
]


# ── Discovery ────────────────────────────────────────────────────────

def discover_signal_classes() -> List[Type[Signal]]:
    """Walk the signals/ package and return every concrete Signal subclass."""
    found: List[Type[Signal]] = []
    for _, modname, _ in pkgutil.walk_packages(
        signals_pkg.__path__, prefix=f"{signals_pkg.__name__}."
    ):
        try:
            mod = importlib.import_module(modname)
        except Exception:  # pragma: no cover — import failures get surfaced as bad assets
            continue
        for _, cls in inspect.getmembers(mod, inspect.isclass):
            if cls is Signal:
                continue
            if not issubclass(cls, Signal):
                continue
            if getattr(cls, "_dataplane_abstract", False):
                continue
            # de-duplicate (re-imports happen via from-imports)
            if cls.signal_id and cls.version and cls not in found:
                found.append(cls)
    return found


# ── Asset factory ────────────────────────────────────────────────────

daily_partitions = DailyPartitionsDefinition(start_date=PARTITION_START)


def _signal_asset_key(cls: Type[Signal]) -> AssetKey:
    """Asset key is the namespaced signal_id, split on '.' so the
    Dagster lineage UI groups by class (insider/options/prices/…)."""
    return AssetKey([*cls.signal_id.split("."), cls.version])


def _make_signal_asset(cls: Type[Signal]):
    """Create a Dagster asset that materializes one Signal for one day's
    partition, computing every ticker in DEFAULT_TICKERS."""

    @asset(
        key=_signal_asset_key(cls),
        partitions_def=daily_partitions,
        description=cls.description or cls.__doc__ or cls.signal_id,
        compute_kind="python",
        group_name=cls.signal_id.split(".", 1)[0],
        metadata={
            "signal_id": cls.signal_id,
            "version": cls.version,
            "owner": cls.owner,
            "sla_hours": cls.sla_hours,
        },
    )
    def _materialize(
        context: AssetExecutionContext,
        dataplane_conn: PostgresResource,
    ) -> MaterializeResult:
        # Partition key like "2026-06-10" → as_of midnight UTC on that day.
        partition_date = context.partition_key
        as_of = datetime.fromisoformat(partition_date).replace(tzinfo=timezone.utc)

        with dataplane_conn.connection() as conn:
            # Register the signal in the catalog on every run — idempotent.
            register(conn, cls)

            signal = cls(conn=conn)
            n_written = 0
            n_errors = 0
            errors_sample = []
            for ticker in DEFAULT_TICKERS:
                try:
                    obs = signal.compute(ticker, as_of)
                    write_observation(conn, obs)
                    n_written += 1
                except Exception as exc:  # noqa: BLE001 — error tolerance per ticker
                    n_errors += 1
                    if len(errors_sample) < 5:
                        errors_sample.append(f"{ticker}: {exc}")
            return MaterializeResult(
                metadata={
                    "tickers": MetadataValue.int(len(DEFAULT_TICKERS)),
                    "written": MetadataValue.int(n_written),
                    "errors": MetadataValue.int(n_errors),
                    "error_sample": MetadataValue.md("\n".join(errors_sample) or "—"),
                    "partition": MetadataValue.text(partition_date),
                }
            )

    _materialize.__name__ = f"materialize_{cls.signal_id.replace('.', '_')}"
    return _materialize


def build_signal_assets():
    """Return a list of Dagster asset functions, one per discovered Signal."""
    return [_make_signal_asset(cls) for cls in discover_signal_classes()]

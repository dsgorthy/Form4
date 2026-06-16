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
from datetime import datetime, timezone
from typing import Type

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dataplane import Signal
from dataplane.catalog import register, write_observation
from dataplane.discovery import DEFAULT_TICKERS, discover_signal_classes
from dataplane.emit import emit_alerts

from dagster_project.resources import PostgresResource


# Stable starting point for the dataplane's daily partitions. Plenty of
# room for historical backfill once the lake is online.
PARTITION_START = "2020-01-01"


# ── Asset factory ────────────────────────────────────────────────────

daily_partitions = DailyPartitionsDefinition(start_date=PARTITION_START)


def _signal_asset_key(cls: Type[Signal]) -> AssetKey:
    """Asset key is the namespaced signal_id, split on '.' so the
    Dagster lineage UI groups by class (insider/options/prices/…)."""
    return AssetKey([*cls.signal_id.split("."), cls.version])


def _make_signal_asset(cls: Type[Signal]):
    """Create a Dagster asset that materializes one Signal for one day's
    partition. Dispatches on the Signal's materialization_mode:

      per_ticker_per_day (default) → iterate DEFAULT_TICKERS, call
                                     compute(ticker, as_of) per ticker
      per_partition_events          → call materialize_partition(date) once,
                                     write the returned list of observations
    """

    mode = cls.materialization_mode

    # Translate declared upstream signal_ids into Dagster asset deps so
    # the strategy/derived signal materializes AFTER its inputs in the
    # same job run. Without this, Dagster runs assets in parallel — the
    # strategy ends up reading a partially-populated trades.raw and
    # misses today's events (observed 2026-06-15: 12 evals vs 1305
    # expected).
    upstream_asset_keys = []
    for u in cls.upstream:
        # external.* upstreams aren't dataplane assets; skip them.
        if u.signal_id.startswith("external."):
            continue
        # Find the matching Signal class to get its asset key (which
        # encodes the version). Falls back gracefully if it isn't in
        # the discovered set.
        for other in discover_signal_classes():
            if other.signal_id == u.signal_id or u.signal_id.startswith(
                f"{other.signal_id}."
            ):
                upstream_asset_keys.append(_signal_asset_key(other))
                break

    common_kwargs = dict(
        key=_signal_asset_key(cls),
        partitions_def=daily_partitions,
        description=cls.description or cls.__doc__ or cls.signal_id,
        compute_kind="python",
        group_name=cls.signal_id.split(".", 1)[0],
        deps=upstream_asset_keys,
        metadata={
            "signal_id": cls.signal_id,
            "version": cls.version,
            "owner": cls.owner,
            "sla_hours": cls.sla_hours,
            "materialization_mode": mode,
        },
    )

    if mode == "per_partition_events":
        @asset(**common_kwargs)
        def _materialize(
            context: AssetExecutionContext,
            dataplane_conn: PostgresResource,
        ) -> MaterializeResult:
            partition_date = context.partition_key
            as_of = datetime.fromisoformat(partition_date).replace(tzinfo=timezone.utc)
            with dataplane_conn.connection() as conn:
                register(conn, cls)
                signal = cls(conn=conn)
                observations = signal.materialize_partition(as_of)
                n_written = 0
                n_errors = 0
                errors_sample = []
                tickers_seen = set()
                for obs in observations:
                    try:
                        write_observation(conn, obs)
                        n_written += 1
                        tickers_seen.add(obs.ticker)
                    except Exception as exc:  # noqa: BLE001
                        n_errors += 1
                        if len(errors_sample) < 5:
                            errors_sample.append(f"{obs.ticker}/{obs.as_of_date}: {exc}")
                # Strategy alert emission — only fires for live (recent)
                # partitions, only for triggered observations that haven't
                # alerted within the cooldown window. CLI backfills don't
                # reach this branch so replays stay silent.
                n_pushed = emit_alerts(conn, signal, observations, as_of)
            return MaterializeResult(
                metadata={
                    "events_returned": MetadataValue.int(len(observations)),
                    "written": MetadataValue.int(n_written),
                    "distinct_tickers": MetadataValue.int(len(tickers_seen)),
                    "errors": MetadataValue.int(n_errors),
                    "error_sample": MetadataValue.md("\n".join(errors_sample) or "—"),
                    "partition": MetadataValue.text(partition_date),
                    "alerts_pushed": MetadataValue.int(n_pushed),
                }
            )
        _materialize.__name__ = f"materialize_events_{cls.signal_id.replace('.', '_')}"
        return _materialize

    # Default: per_ticker_per_day
    @asset(**common_kwargs)
    def _materialize(
        context: AssetExecutionContext,
        dataplane_conn: PostgresResource,
    ) -> MaterializeResult:
        partition_date = context.partition_key
        as_of = datetime.fromisoformat(partition_date).replace(tzinfo=timezone.utc)
        with dataplane_conn.connection() as conn:
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
                except Exception as exc:  # noqa: BLE001
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


def scheduled_signal_asset_keys() -> list:
    """AssetKeys of signals whose nightly schedule we *do* want to run.

    Excludes Signal subclasses with auto_schedule=False (e.g. parity-mode
    feeds). The assets still exist in Dagster (and Definitions) so they're
    visible + manually triggerable; they just don't appear in the daily
    job's selection.
    """
    return [
        _signal_asset_key(cls)
        for cls in discover_signal_classes()
        if getattr(cls, "auto_schedule", True)
    ]

"""Top-level Dagster Definitions object.

This is the single entry point Dagster discovers (set in pyproject.toml
under [tool.dagster]). It collects all assets, jobs, schedules, sensors,
and resources.

Note: no `from __future__ import annotations` here — Dagster validates
decorated functions' context parameter annotations at decoration time,
and string-form annotations break that validation (same reason as
assets/signals.py).
"""
import os
import shutil
import time
from datetime import datetime, timezone

import requests
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunFailureSensorContext,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    build_schedule_from_partitioned_job,
    define_asset_job,
    run_failure_sensor,
    sensor,
)
from dagster_dbt import DbtCliResource

from dagster_project.assets.dbt import dataplane_dbt_assets, dbt_project
from dagster_project.assets.signals import (
    build_signal_assets,
    daily_partitions,
    scheduled_signal_asset_keys,
)
from dagster_project.resources import (
    dataplane_resource,
    form4_resource,
)

# dbt executable must be discoverable; fall through env, then venv.
_DBT_EXECUTABLE = (
    os.environ.get("DBT_EXECUTABLE")
    or shutil.which("dbt")
    or "/Users/derekg/dataplane_venv/bin/dbt"
)

signal_assets = build_signal_assets()


# ── Jobs + schedules ─────────────────────────────────────────────────
#
# One job covers every discovered signal asset, so a newly added Signal
# subclass is scheduled automatically — no scheduling code per feed.

daily_signals_job = define_asset_job(
    name="daily_signals",
    # Only signals with auto_schedule=True (parity-mode feeds opt out so
    # they don't crash the nightly job; they're still manually triggerable
    # from the Dagster UI and via the backfill CLI).
    selection=AssetSelection.keys(*scheduled_signal_asset_keys()),
    partitions_def=daily_partitions,
)

# 04:30 UTC = 21:30 PDT / 20:30 PST. After Alpaca EOD bars (~17:30 PT)
# and EDGAR's 22:00 ET acceptance cutoff, so the partition that just
# closed is complete when we materialize it.
daily_signals_schedule = build_schedule_from_partitioned_job(
    daily_signals_job,
    hour_of_day=4,
    minute_of_hour=30,
    default_status=DefaultScheduleStatus.RUNNING,
)

dbt_marts_job = define_asset_job(
    name="dbt_marts",
    selection=AssetSelection.assets(dataplane_dbt_assets),
)

# 05:00 UTC — right after the signals job normally finishes, so marts
# reflect the freshest observations.
dbt_marts_schedule = ScheduleDefinition(
    name="dbt_marts_daily",
    job=dbt_marts_job,
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)


# ── Failure alerting ─────────────────────────────────────────────────

@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING)
def ntfy_on_run_failure(context: RunFailureSensorContext):
    """Push any failed run to ntfy — same topic-as-secret convention as
    framework/alerts/ntfy.py (NTFY_ALERT_TOPIC env var, sourced from
    .env by the launchd wrapper). No topic → silently skip, so dev
    environments don't need it set.
    """
    topic = os.environ.get("NTFY_ALERT_TOPIC")
    if not topic:
        return
    run = context.dagster_run
    partition = run.tags.get("dagster/partition", "-")
    error = (context.failure_event.message or "")[:300]
    try:
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=f"{run.job_name} failed (partition {partition})\n{error}",
            headers={
                "Title": "Dataplane run failed",
                "Priority": "high",
                "Tags": "rotating_light",
            },
            timeout=10,
        )
    except Exception:
        pass  # alerting must never take down the daemon


# ── M2: realtime 5-min loop ──────────────────────────────────────────
#
# Re-materializes today's insider.trades.raw partition every 5 minutes
# (idempotent upsert; picks up new form4-bridge rows as they land) and
# chains the strategy partition right after. Combined with the existing
# ntfy emit in the asset wrapper, this is the live alerting loop.
#
# Default status STOPPED so it doesn't surprise — Derek toggles it from
# the Dagster UI when he wants live alerts on.

_REALTIME_KEYS = [
    AssetKey(["insider", "trades", "raw", "v1.0.0"]),
    AssetKey(["strategy", "agrade_drawdown_buy", "v1"]),
]

realtime_strategy_job = define_asset_job(
    name="realtime_strategy",
    selection=AssetSelection.keys(*_REALTIME_KEYS),
    partitions_def=daily_partitions,
)


@sensor(
    name="realtime_5min_loop",
    job=realtime_strategy_job,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.STOPPED,
)
def realtime_5min_loop(context: SensorEvaluationContext):
    """Every 5 min, refresh today's insider.trades.raw + strategy partitions.

    Idempotent at the signal-write layer, and the strategy's emit logic
    dedupes alerts via the cooldown window (so re-running doesn't spam).
    """
    today_utc = datetime.now(timezone.utc).date().isoformat()
    bucket = int(time.time() // 300)
    return SensorResult(
        run_requests=[RunRequest(
            run_key=f"realtime-{today_utc}-{bucket}",
            partition_key=today_utc,
        )],
    )


defs = Definitions(
    assets=[*signal_assets, dataplane_dbt_assets],
    jobs=[daily_signals_job, dbt_marts_job, realtime_strategy_job],
    schedules=[daily_signals_schedule, dbt_marts_schedule],
    sensors=[ntfy_on_run_failure, realtime_5min_loop],
    resources={
        "dataplane_conn": dataplane_resource(),
        "form4_conn":     form4_resource(),
        "dbt":            DbtCliResource(
            project_dir=str(dbt_project.project_dir),
            dbt_executable=_DBT_EXECUTABLE,
        ),
    },
)

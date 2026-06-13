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

import requests
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunFailureSensorContext,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    run_failure_sensor,
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


defs = Definitions(
    assets=[*signal_assets, dataplane_dbt_assets],
    jobs=[daily_signals_job, dbt_marts_job],
    schedules=[daily_signals_schedule, dbt_marts_schedule],
    sensors=[ntfy_on_run_failure],
    resources={
        "dataplane_conn": dataplane_resource(),
        "form4_conn":     form4_resource(),
        "dbt":            DbtCliResource(
            project_dir=str(dbt_project.project_dir),
            dbt_executable=_DBT_EXECUTABLE,
        ),
    },
)

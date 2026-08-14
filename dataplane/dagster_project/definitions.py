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
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from dagster import (
    AssetKey,
    AssetSelection,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunFailureSensorContext,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    build_schedule_from_partitioned_job,
    define_asset_job,
    run_failure_sensor,
    schedule,
    sensor,
)
from dagster_dbt import DbtCliResource

from dagster_project.assets.congress_sync import congress_trades_form4_sync
from dagster_project.assets.dbt import dataplane_dbt_assets, dbt_project
from dagster_project.assets.form4_pipeline import form4_pipeline_assets
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
    selection=AssetSelection.keys(*scheduled_signal_asset_keys())
    | AssetSelection.assets(congress_trades_form4_sync),
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

# ── form4 derived chain ───────────────────────────────────────────────
#
# Replaces four independent launchd crons that were ordered only by the
# clock: daily-prices 17:30, backfill-returns 05:00, refresh-features 06:00,
# compute-signals 17:45. Nothing connected them, so when prices went stale on
# 2026-07-28 all three downstream jobs still ran on stale input and exited 0
# for 18 days.
#
# As one job the ordering is structural, and each asset re-checks upstream
# freshness before computing, so a bad root fails the run instead of
# silently poisoning everything below it.
#
# 17:30 PT weekdays: same slot the prices cron used — after the Alpaca EOD
# bars land. The whole chain takes ~20min (prices ~35s, returns ~12min,
# features ~3min, signals ~3min), finishing well before the 03:15 backup.
form4_pipeline_job = define_asset_job(
    name="form4_pipeline",
    selection=AssetSelection.assets(*form4_pipeline_assets),
)

form4_pipeline_schedule = ScheduleDefinition(
    name="form4_pipeline_daily",
    job=form4_pipeline_job,
    cron_schedule="30 17 * * 1-5",
    execution_timezone="America/Los_Angeles",
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


# ── B2: insider ingest 5-min shadow ──────────────────────────────────
#
# Runs the dataplane-native EDGAR ingestor (insider.filings.raw.v1) on the
# same 5-minute cadence we promise users, alongside the form4 bridge, so
# parity can be measured under real conditions before the cutover in B4.
#
# Deliberately a SEPARATE sensor from realtime_5min_loop rather than another
# key in _REALTIME_KEYS: that job feeds the live strategy, and an unproven
# feed must not be able to delay or fail live alerting. Toggling one must not
# toggle the other.
#
# Writes to signal_observations under its own signal_id, so "shadow" needs no
# special plumbing — it is already isolated from form4.trades. Nothing reads
# it until B4.

_SHADOW_KEYS = [AssetKey(["insider", "filings", "raw", "v1.0.0"])]

insider_filings_shadow_job = define_asset_job(
    name="insider_filings_shadow",
    selection=AssetSelection.keys(*_SHADOW_KEYS),
    partitions_def=daily_partitions,
)


@sensor(
    name="insider_filings_shadow_5min",
    job=insider_filings_shadow_job,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
)
def insider_filings_shadow_5min(context: SensorEvaluationContext):
    """Re-materialize today's insider.filings.raw partition every 5 minutes.

    Partition key is the EASTERN date, not UTC. EDGAR dates a filing by ET,
    so after 20:00 ET a UTC-derived key asks for tomorrow's partition and
    ingests nothing — the bug realtime_5min_loop still has.

    Skips outside EDGAR's acceptance window (06:00-22:00 ET, weekdays);
    there is nothing new to fetch then, and the signal's own EFTS query is
    the expensive part of an empty run.
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:
        return SkipReason("weekend — EDGAR accepts no filings")
    if not (6 <= now_et.hour < 22):
        return SkipReason(f"{now_et:%H:%M} ET is outside EDGAR acceptance hours")

    # Never stack runs. The FIRST run of a day is a cold fetch of every filing
    # so far — minutes, not seconds — and it overruns the 5-minute tick. Left
    # unguarded the sensor queues a second run against the same partition doing
    # the same work, and they race on the same upsert. Observed 2026-08-13:
    # two runs stacked within 5 minutes of enabling the sensor.
    #
    # Skipping is free: the next tick is 5 minutes away and the partition is
    # re-materialized from scratch each time, so nothing is lost by waiting.
    in_flight = context.instance.get_runs(
        filters=RunsFilter(
            job_name="insider_filings_shadow",
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.NOT_STARTED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
            ],
        ),
        limit=1,
    )
    if in_flight:
        return SkipReason("previous shadow run still in flight")

    bucket = int(time.time() // 300)
    return SensorResult(
        run_requests=[RunRequest(
            run_key=f"insider-shadow-{now_et.date().isoformat()}-{bucket}",
            partition_key=now_et.date().isoformat(),
        )],
    )


# The 5-minute loop only ever touches TODAY. Anything EDGAR accepts after a
# day's last run is stranded permanently, because no later run revisits a
# closed partition. That single gap was the entire parity deficit: measured
# 2026-08-13, the ~20 filings/day missing from the candidate feed were all
# late-afternoon acceptances (16:05-21:13 ET), and re-materializing 08-11 and
# 08-12 took recall from 97.91%/98.30% to 100.00% on both days. Nothing was
# wrong with discovery or parsing.
#
# 23:30 ET, after EDGAR's 22:00 acceptance cutoff, so the days being settled
# are closed. Two days back covers same-day stragglers plus anything EFTS
# indexed a day late. Cheap: the incremental accession filter means a settled
# day re-fetches nothing, only the EFTS metadata query is paid again.
#
# Ordering matters — this runs before the 06:30 PT parity recording, so the
# gate scores a settled day rather than an unfinished one.

_SETTLE_DAYS = 2


@schedule(
    name="insider_filings_settle_nightly",
    job=insider_filings_shadow_job,
    cron_schedule="30 23 * * 1-5",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)
def insider_filings_settle_nightly(context: ScheduleEvaluationContext):
    """Re-materialize the last few closed partitions to catch late filings."""
    today_et = context.scheduled_execution_time.date()
    return [
        RunRequest(
            run_key=f"insider-settle-{today_et - timedelta(days=n)}",
            partition_key=(today_et - timedelta(days=n)).isoformat(),
        )
        for n in range(0, _SETTLE_DAYS + 1)
    ]


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
    assets=[*signal_assets, congress_trades_form4_sync,
            *form4_pipeline_assets, dataplane_dbt_assets],
    jobs=[daily_signals_job, dbt_marts_job, form4_pipeline_job,
          realtime_strategy_job, insider_filings_shadow_job],
    schedules=[daily_signals_schedule, dbt_marts_schedule,
               form4_pipeline_schedule, insider_filings_settle_nightly],
    sensors=[ntfy_on_run_failure, realtime_5min_loop,
             insider_filings_shadow_5min],
    resources={
        "dataplane_conn": dataplane_resource(),
        "form4_conn":     form4_resource(),
        "dbt":            DbtCliResource(
            project_dir=str(dbt_project.project_dir),
            dbt_executable=_DBT_EXECUTABLE,
        ),
    },
)

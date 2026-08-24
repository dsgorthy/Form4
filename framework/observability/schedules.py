"""When each scheduled job is next due, so monitors can stop guessing.

WHY THIS EXISTS

Three separate monitors independently expressed "is this fresh?" as a flat
number of hours, and all three were wrong the same way on 2026-08-24:

  offbox_watchdog.JOB_SUCCESS           30h budget  → paged every Monday
  monday_paper_monitor.refresh_chain     8h budget  → failed on every run
  (and the pattern nearly went into a third place)

The jobs they watch run on weekday-only crons. A budget in wall-clock hours
cannot describe "weekdays at 17:30": the Friday-to-Monday gap is ~62 hours and
no threshold short enough to catch a real miss survives it. A weekday guard on
the CHECK does not help — it silences Saturday and Sunday, when the job is
correctly idle, and leaves Monday armed, which is exactly when the gap peaks.

The question a monitor means to ask is "has this run since it was last DUE?".
That needs the schedule, so the schedule lives here, once.

Keep in step with dataplane/dagster_project/definitions.py.
tests/unit/test_watchdog_schedules.py diffs both against it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

WEEKDAYS = (0, 1, 2, 3, 4)
EVERY_DAY = (0, 1, 2, 3, 4, 5, 6)

# grace_h: how long after a scheduled fire before a missing result counts as a
# miss. One slow run plus a retry — form4_pipeline itself takes ~6 minutes.
JOB_SCHEDULES = {
    # cron_schedule="30 17 * * 1-5", execution_timezone America/Los_Angeles.
    # Writes the derived feature chain: career_grade, is_rare_reversal,
    # week52_proximity, pit_cluster_size, insider_track_records.score.
    "form4_pipeline": {"days": WEEKDAYS, "hour": 17, "minute": 30,
                       "tz": "America/Los_Angeles", "grace_h": 6},
    # build_schedule_from_partitioned_job(hour_of_day=4, minute_of_hour=30) UTC
    "daily_signals": {"days": EVERY_DAY, "hour": 4, "minute": 30,
                      "tz": "UTC", "grace_h": 6},
}


def last_expected_fire(spec: dict, now: datetime | None = None) -> datetime:
    """The most recent scheduled fire whose grace period has elapsed.

    Walks back day by day in the schedule's own timezone, so DST is handled by
    zoneinfo rather than arithmetic, and a weekday-only job skips the weekend
    instead of accumulating hours across it.
    """
    tz = ZoneInfo(spec["tz"])
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    for back in range(0, 11):
        day = (now - timedelta(days=back)).date()
        if day.weekday() not in spec["days"]:
            continue
        fire = datetime(day.year, day.month, day.day,
                        spec["hour"], spec["minute"], tzinfo=tz)
        if fire + timedelta(hours=spec["grace_h"]) <= now:
            return fire.astimezone(timezone.utc)
    return (now - timedelta(days=11)).astimezone(timezone.utc)


def is_overdue(job: str, last_seen: datetime, now: datetime | None = None) -> bool:
    """True when `job` has produced nothing since it was last due."""
    return last_seen < last_expected_fire(JOB_SCHEDULES[job], now)

"""The nightly-job watchdog must ask "did it run when it was due", not "how
long has it been".

WHAT WENT WRONG

    JOB_SUCCESS = [("form4_pipeline", 30)]   # max hours since last SUCCESS

form4_pipeline is `30 17 * * 1-5` — weekdays only. From midnight on Monday the
gap back to Friday's 17:30 run is 30.5 hours and rising, against a 30-hour
budget, so it failed continuously from 00:00 Monday until the 17:30 run cleared
it. The watchdog runs every 30 minutes and does not deduplicate: ~35 pushes a
week about a pipeline doing exactly what it was told. The log shows 18 alerts
on Monday 2026-08-17, 19 on Monday 2026-08-24, and none on any other day.

There WAS a weekend guard — `if date.today().weekday() < 5` — and it is what
made this look handled. It skipped Saturday and Sunday, when a weekday-only job
is correctly idle, and left Monday armed, which is exactly when the gap peaks.

A duration cannot express "weekdays at 17:30". The schedule can.

WHAT THESE TESTS PIN

  1. A Monday-morning check against Friday's run is quiet.
  2. A genuinely missed run is still caught — on a weekday AND on a weekend for
     the 7-day job. A fix that only silences the alarm is worse than the alarm.
  3. The watchdog's copy of each schedule matches the Dagster definition.
"""
from __future__ import annotations

import importlib.util
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

REPO = Path(__file__).resolve().parents[2]
DEFINITIONS = REPO / "dataplane/dagster_project/definitions.py"

_spec = importlib.util.spec_from_file_location(
    "offbox_watchdog", REPO / "scripts/offbox_watchdog.py")
wd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wd)

PT = ZoneInfo("America/Los_Angeles")
FORM4 = next(s for s in wd.JOB_SUCCESS if s["job"] == "form4_pipeline")
SIGNALS = next(s for s in wd.JOB_SUCCESS if s["job"] == "daily_signals")


def pt(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=PT)


# ── the bug ─────────────────────────────────────────────────────────────────


def test_monday_morning_does_not_look_back_past_the_weekend():
    """2026-08-24 09:23 PT, the exact moment Derek was being paged."""
    due = wd.last_expected_fire(FORM4, pt(2026, 8, 24, 9, 23))
    assert due == pt(2026, 8, 21, 17, 30).astimezone(timezone.utc), (
        "Monday's check must measure against FRIDAY's scheduled run, not "
        "against a flat 30-hour budget that the weekend always exceeds"
    )


def test_fridays_run_satisfies_mondays_check():
    """End to end: the healthy state that was paging."""
    now = pt(2026, 8, 24, 9, 23)
    due = wd.last_expected_fire(FORM4, now)
    last_success = pt(2026, 8, 21, 17, 30).astimezone(timezone.utc)
    assert last_success >= due, "a normal Friday run must leave Monday quiet"


@pytest.mark.parametrize("hour", [0, 3, 6, 9, 12, 15, 17])
def test_quiet_all_monday_until_the_run_is_due(hour):
    """The old budget failed from 00:00; every one of these was a push."""
    now = pt(2026, 8, 24, hour)
    due = wd.last_expected_fire(FORM4, now)
    last_success = pt(2026, 8, 21, 17, 30).astimezone(timezone.utc)
    assert last_success >= due


# ── it must still catch a real miss ─────────────────────────────────────────


def test_a_missed_weekday_run_is_caught():
    """Monday's run never happened; by Tuesday morning that is a problem."""
    now = pt(2026, 8, 25, 9, 0)
    due = wd.last_expected_fire(FORM4, now)
    assert due == pt(2026, 8, 24, 17, 30).astimezone(timezone.utc)
    stale = pt(2026, 8, 21, 17, 30).astimezone(timezone.utc)  # still Friday's
    assert stale < due, "a skipped Monday must still alert on Tuesday"


def test_the_grace_period_absorbs_a_slow_run_but_not_a_missing_one():
    fire = pt(2026, 8, 25, 17, 30)
    within = wd.last_expected_fire(FORM4, fire + timedelta(hours=5))
    assert within < fire.astimezone(timezone.utc), (
        "a run 5h late is inside the 6h grace and must not be due yet"
    )
    after = wd.last_expected_fire(FORM4, fire + timedelta(hours=7))
    assert after == fire.astimezone(timezone.utc), (
        "past the grace period the run is due and a miss must alert"
    )


def test_the_seven_day_job_is_still_checked_on_a_weekend():
    """The old weekday guard skipped Sat/Sun entirely, so a daily_signals
    failure on Friday night went unreported until Monday."""
    now = datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc)  # Saturday
    due = wd.last_expected_fire(SIGNALS, now)
    assert due == datetime(2026, 8, 22, 4, 30, tzinfo=timezone.utc)
    stale = datetime(2026, 8, 21, 4, 30, tzinfo=timezone.utc)
    assert stale < due, "a 7-day job that missed Saturday must alert on Saturday"


def test_dst_is_handled_by_the_timezone_not_by_arithmetic():
    """17:30 PT stays 17:30 PT across the change; the UTC offset moves."""
    before = wd.last_expected_fire(FORM4, pt(2026, 10, 30, 23, 59))
    after = wd.last_expected_fire(FORM4, pt(2026, 11, 6, 23, 59))
    assert before.astimezone(PT).hour == 17
    assert after.astimezone(PT).hour == 17


# ── the watchdog's copy must match Dagster ──────────────────────────────────


def test_form4_pipeline_schedule_matches_the_dagster_definition():
    src = DEFINITIONS.read_text()
    block = src[src.index("form4_pipeline_schedule = ScheduleDefinition("):]
    cron = re.search(r'cron_schedule="([^"]+)"', block).group(1)
    tz = re.search(r'execution_timezone="([^"]+)"', block).group(1)
    minute, hour, _, _, dow = cron.split()
    assert (int(hour), int(minute)) == (FORM4["hour"], FORM4["minute"]), (
        f"Dagster fires form4_pipeline at {hour}:{minute}, watchdog expects "
        f"{FORM4['hour']}:{FORM4['minute']}"
    )
    assert dow == "1-5" and FORM4["days"] == (0, 1, 2, 3, 4), (
        f"Dagster day-of-week is {dow!r}; watchdog has {FORM4['days']}"
    )
    assert tz == FORM4["tz"]


def test_daily_signals_schedule_matches_the_dagster_definition():
    """Built from a partitioned job rather than a cron literal, so the kwargs
    are the source of truth."""
    src = DEFINITIONS.read_text()
    block = src[src.index("daily_signals_schedule = build_schedule_from_partitioned_job("):]
    hour = int(re.search(r"hour_of_day=(\d+)", block).group(1))
    minute = int(re.search(r"minute_of_hour=(\d+)", block).group(1))
    assert (hour, minute) == (SIGNALS["hour"], SIGNALS["minute"])
    assert SIGNALS["days"] == (0, 1, 2, 3, 4, 5, 6), (
        "daily_signals is built from a DAILY partition — it fires every day"
    )
    assert SIGNALS["tz"] == "UTC"


def test_no_flat_hour_budget_survives_in_the_watchdog():
    """The shape that caused this: a bare (name, hours) tuple."""
    src = (REPO / "scripts/offbox_watchdog.py").read_text()
    block = src[src.index("JOB_SUCCESS = ["):src.index("JOB_SUCCESS_SQL")]
    assert not re.search(r'\(\s*"[a-z_]+"\s*,\s*\d+\s*\)', block), (
        "a flat hours budget is back in JOB_SUCCESS; it cannot express a "
        "weekday-only schedule and pages every Monday"
    )

"""The strategy scan window is counted in TRADING days, and a one-shot run
writes a heartbeat.

Measured 2026-09-14 over 30 days of trade_decision_audit, rows by the
FILING weekday: Mon 3,058 / Tue 3,553 / Wed 1,720 / Thu 439 / Fri 0. With
`filing_lookback_days: 2` applied as `filing_date >= date('now','-2 days')`,
Monday's window began on Saturday; Friday's filings were outside it, and the
runner scans only 06:00-13:00 PT on weekdays, so no later scan ever reached
them. Every Friday insider buy was silently never alerted, every Monday scan
returned "scanned: 0", and the Monday paper monitor failed on every Monday in
its history without anyone reading past its first line.
"""
import importlib.util
import re
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
RUNNER = ROOT / "strategies" / "cw_strategies" / "cw_runner.py"
PROBE = ROOT / "scripts" / "freshness_probe.py"


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("cw_runner_under_test", RUNNER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_monday_reaches_back_to_thursday_so_friday_is_inside(runner):
    """The regression. 2026-09-14 is a Monday."""
    assert runner._lookback_start(2, date(2026, 9, 14)) == date(2026, 9, 10)


def test_midweek_window_is_unchanged(runner):
    """Wednesday with 2 days still starts Monday, exactly as calendar days did."""
    assert runner._lookback_start(2, date(2026, 9, 16)) == date(2026, 9, 14)


def test_a_holiday_is_skipped_like_a_weekend(runner):
    """Tuesday 2026-09-08 follows Labor Day: two trading days back is
    Thursday 09-03, not Sunday."""
    assert runner._lookback_start(2, date(2026, 9, 8)) == date(2026, 9, 3)


def test_zero_lookback_is_today(runner):
    assert runner._lookback_start(0, date(2026, 9, 16)) == date(2026, 9, 16)


def test_the_sql_window_uses_the_trading_day_start():
    src = RUNNER.read_text()
    body = src[src.index("def _build_thesis_query"):src.index("def ", src.index("def _build_thesis_query") + 10)]
    assert "date('now'" not in body, "the calendar-day window is back"
    assert "_lookback_start(lookback_days)" in body


def test_the_engine_window_uses_the_same_start():
    src = RUNNER.read_text()
    assert "start = _lookback_start(lookback, today)" in src
    assert "(today - timedelta(days=i))" not in src, "the engine path still walks calendar days back from today"


def test_a_one_shot_run_writes_the_heartbeat():
    src = RUNNER.read_text()
    once = src[src.index("elif args.once or args.dry_run or args.catchup:"):src.index("else:\n        run_daemon(config)")]
    assert once.count("_write_heartbeat(") >= 2, "the --once path must write a heartbeat before and after the cycle"


def test_the_probe_judges_on_business_hours_like_the_preflight():
    src = PROBE.read_text()
    assert "business_age_hours" in src
    assert re.search(r"is_stale = effective is None or effective > c\.max_staleness_hours", src)

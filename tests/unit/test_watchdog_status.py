"""The service-heartbeat check must read pipeline_runs.status, not only recency.

WHAT WENT WRONG

From 2026-08-25 13:14 to 2026-09-10 23:10 notification_scanner started every
five minutes and wrote status='failed' on 3,811 of its 3,912 pipeline_runs rows
— the same TypeError every time. The SERVICE_HEARTBEAT check added on 09-10
asked one question, "when did it last start?", and on 2026-09-10 at 22:52:16
logs/offbox-watchdog.log reads:

    OK   notification_scanner last run: 2m ago (budget 45m)
    === all checks passed ===

The row it had just read, started 22:50:07.297518, was status='failed' with
error_message "TypeError: '<' not supported between instances of 'int' and
'NoneType'". A loop that turns and fails on every turn has the same heartbeat
as a healthy one.

WHAT THESE TESTS PIN

  1. That exact row, at that exact instant, produces exactly one problem and it
     says WHY: the status and the first line of the error.
  2. Stale and failing are independent verdicts — a 3-hour-old failure is two
     problems, a fresh 'abandoned' is one, a missing service is still "no run".
  3. 'running' has no verdict: an empty status (only in-flight rows so far) is
     not a failure, and the SQL excludes 'running' from the status column.
  4. A '|' inside an exception message cannot shift columns.
  5. The SQL survives ssh_psql's double-quote wrapping: no '"', no '$'.

Loaded by path like test_watchdog_parse.py: the script is not a package, and
it runs on /usr/bin/python3 (3.9), so nothing here may need a newer syntax.
"""
from __future__ import annotations

import importlib.util
import inspect
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_SPEC = importlib.util.spec_from_file_location(
    "offbox_watchdog",
    Path(__file__).resolve().parents[2] / "scripts" / "offbox_watchdog.py",
)
watchdog = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(watchdog)

PT = ZoneInfo("America/Los_Angeles")

# The instant of the "all checks passed" line in the log.
NOW = datetime(2026, 9, 10, 22, 52, 16, tzinfo=PT)

# The regression row, verbatim from pipeline_runs (error_message first line).
INCIDENT_ROW = (
    "notification_scanner|2026-09-10 22:50:07.297518|failed|"
    "TypeError: '<' not supported between instances of 'int' and 'NoneType'"
)

SERVICES = [s["service"] for s in watchdog.SERVICE_HEARTBEAT]


def _rows(overrides: "dict[str, str] | None" = None, now: datetime = NOW) -> str:
    """psql -tA text for all seven services, each a fresh 'ok' unless overridden.

    Fresh means started two minutes before `now`, which is inside every budget
    and matches the "2m ago" the log printed.
    """
    overrides = overrides or {}
    fresh = (now - timedelta(minutes=2)).astimezone(PT)
    lines = []
    for svc in SERVICES:
        if svc in overrides:
            lines.append(overrides[svc])
        else:
            lines.append(f"{svc}|{fresh:%Y-%m-%d %H:%M:%S.%f}|ok|")
    return "\n".join(lines)


def _status_problems(problems: "list[str]") -> "list[str]":
    return [p for p in problems if "has not run" not in p and "no run on record" not in p]


class TestTheIncidentRow:
    """Replay 2026-09-10 22:52:16 against the row that was on disk."""

    def test_a_fresh_failed_run_is_one_problem_that_names_the_error(self):
        rows = _rows({"notification_scanner": INCIDENT_ROW})
        problems, report = watchdog.evaluate_service_heartbeats(rows, NOW)

        assert len(problems) == 1, problems
        (p,) = problems
        assert "notification_scanner" in p
        assert "failed" in p
        assert "TypeError" in p
        # Recency was genuinely fine — 2 minutes — and must not be reported.
        assert "has not run" not in p

    def test_the_report_line_shows_the_status_not_only_the_age(self):
        rows = _rows({"notification_scanner": INCIDENT_ROW})
        _, report = watchdog.evaluate_service_heartbeats(rows, NOW)

        line = next(l for l in report if "notification_scanner" in l)
        assert line.startswith("  FAIL"), line
        assert "2m ago" in line
        assert "failed" in line and "TypeError" in line
        # And the other six say so too, in the same shape.
        assert len(report) == len(SERVICES)
        assert all("status ok" in l for l in report if "notification_scanner" not in l)


class TestVerdictsAreIndependent:

    def test_all_fresh_and_ok_is_quiet(self):
        problems, report = watchdog.evaluate_service_heartbeats(_rows(), NOW)
        assert problems == []
        assert all(l.startswith("  OK  ") for l in report)

    def test_a_stale_failure_is_two_problems(self):
        old = (NOW - timedelta(hours=3)).astimezone(PT)
        rows = _rows({"notification_scanner":
                      f"notification_scanner|{old:%Y-%m-%d %H:%M:%S.%f}|failed|TypeError: x"})
        problems, _ = watchdog.evaluate_service_heartbeats(rows, NOW)

        mine = [p for p in problems if p.startswith("notification_scanner")]
        assert len(mine) == 2, problems
        assert any("has not run" in p and "budget 45m" in p for p in mine)
        assert any("failed" in p and "TypeError" in p for p in mine)

    def test_a_fresh_abandoned_run_is_one_problem(self):
        # pipeline_runner reaps a 'running' row as 'abandoned' when the next
        # run starts — that is the previous run never finishing, not a
        # scheduler fault, so exactly the status verdict and not the stale one.
        fresh = (NOW - timedelta(minutes=2)).astimezone(PT)
        rows = _rows({"heartbeat_probe":
                      f"heartbeat_probe|{fresh:%Y-%m-%d %H:%M:%S.%f}|abandoned|"
                      "no terminal status recorded; reaped when a later run of this service started"})
        problems, _ = watchdog.evaluate_service_heartbeats(rows, NOW)
        assert len(problems) == 1, problems
        assert "heartbeat_probe" in problems[0] and "abandoned" in problems[0]

    def test_a_missing_service_is_still_no_run_on_record(self):
        rows = "\n".join(
            l for l in _rows().splitlines() if not l.startswith("freshness_probe|")
        )
        problems, _ = watchdog.evaluate_service_heartbeats(rows, NOW)
        assert len(problems) == 1, problems
        assert problems[0].startswith("freshness_probe")
        assert "no run on record" in problems[0]

    def test_only_running_rows_so_far_is_not_a_failure(self):
        # A service whose every row is still in flight has no completed run
        # to judge. The SQL renders that as an empty status column.
        fresh = (NOW - timedelta(minutes=2)).astimezone(PT)
        rows = _rows({"insider_fetch": f"insider_fetch|{fresh:%Y-%m-%d %H:%M:%S.%f}||"})
        problems, report = watchdog.evaluate_service_heartbeats(rows, NOW)
        assert problems == [], problems
        line = next(l for l in report if "insider_fetch" in l)
        assert line.startswith("  OK  ")


class TestParsing:

    def test_a_pipe_inside_the_error_stays_inside_the_error(self):
        fresh = (NOW - timedelta(minutes=2)).astimezone(PT)
        rows = _rows({"strategy_intraday":
                      f"strategy_intraday|{fresh:%Y-%m-%d %H:%M:%S.%f}|failed|"
                      "ValueError: bad row {'a': 1} | expected 3 columns | got 2"})
        problems, report = watchdog.evaluate_service_heartbeats(rows, NOW)

        assert len(problems) == 1, problems
        assert "expected 3 columns | got 2" in problems[0]
        # The other six were not disturbed by the extra separators.
        assert sum(l.startswith("  OK  ") for l in report) == len(SERVICES) - 1

    def test_an_unparseable_timestamp_is_reported_not_swallowed(self):
        rows = _rows({"form4_uptime": "form4_uptime|not a time|ok|"})
        problems, _ = watchdog.evaluate_service_heartbeats(rows, NOW)
        assert len(problems) == 1 and "form4_uptime" in problems[0]
        assert "unparseable" in problems[0]

    def test_main_is_wired_to_the_pure_function(self):
        # The function existing is not the fix; main() calling it is.
        assert "evaluate_service_heartbeats(rows" in inspect.getsource(watchdog.main)


class TestTheQuery:
    """What the SQL must and must not contain, since it is proven by hand on
    Studio and only pinned here."""

    def test_reads_status_and_excludes_running_from_it(self):
        sql = watchdog.SERVICE_HEARTBEAT_SQL
        assert "status" in sql
        assert "status <> 'running'" in sql

    def test_recency_is_over_all_rows_including_running(self):
        # A run that hangs in 'running' forever must age out, not pin "fresh".
        assert "max(started_at) AT TIME ZONE" in watchdog.SERVICE_HEARTBEAT_SQL

    def test_error_is_first_line_only_and_last_column(self):
        sql = watchdog.SERVICE_HEARTBEAT_SQL
        assert "split_part(error_message, chr(10), 1)" in sql
        assert sql.index("error_message") > sql.index("array_agg(status")
        assert "FROM pipeline_runs" in sql[sql.index("error_message"):]

    def test_survives_ssh_psql_double_quote_wrapping(self):
        sql = watchdog.SERVICE_HEARTBEAT_SQL.format(names="'a', 'b'")
        assert '"' not in sql
        assert "$" not in sql

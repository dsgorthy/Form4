"""A position entered TODAY must be able to alert today.

WHAT WAS WRONG

scan_portfolio_alerts bounded its query:

    AND entry_date >  watermark AND entry_date <= latest

`entry_date` is a text date, so both bounds are reduced to a date with
_as_date_str. The scanner runs every five minutes, so after its first run of
the day the watermark IS today — and `entry_date > today AND entry_date <=
today` is an empty range. The strategies write their entries at 09:31 ET, hours
after the watermark has passed them.

So a same-day entry could never fire. portfolio_alert has fired twelve times in
its entire history, all of them on 2026-03-31.

WHY >= IS SAFE

The dedup key is (strategy, ticker, entry_date), deliberately stable across the
nightly rebuild — the scanner is at-least-once and the dedup key is what makes
it exactly-once per subscriber. Re-reading today's rows on every tick costs a
suppressed INSERT, not a duplicate alert.

Found by scripts/alert_canary.py, which is the point of having one: every
component was individually healthy and the chain still did not work.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines/notification_scanner.py"


def _portfolio_alert_sql() -> str:
    src = SCANNER.read_text()
    body = src[src.index("def scan_portfolio_alerts("):]
    return body[:body.index("\ndef ")]


def test_a_date_watermark_is_inclusive():
    """Strict > on a date-granularity bound cannot match the current day."""
    sql = _portfolio_alert_sql()
    for col in ("entry_date", "exit_date"):
        assert re.search(rf"AND {col} >= \?", sql), (
            f"{col} is not compared with >=. With a date watermark that has "
            "already advanced to today, a strict > makes the range empty and "
            "the alert can never fire on the day it happens."
        )
        assert not re.search(rf"AND {col} > \?", sql), (
            f"{col} uses a strict > again"
        )


def test_the_string_comparison_that_broke_it():
    """Documents the mechanism, so nobody 'fixes' the date reduction instead."""
    assert not ("2026-08-24" > "2026-08-24 11:55:50.858377-07"), (
        "a bare date sorts BEFORE the same date with a time appended, which is "
        "why the timestamp watermark had to be reduced with _as_date_str"
    )
    assert "2026-08-24" >= "2026-08-24"


def test_dedup_is_what_makes_at_least_once_safe():
    """>= is only correct because the dedup key is stable per (user, strategy,
    ticker, date). If that changes, re-reading the day would duplicate."""
    sql = _portfolio_alert_sql()
    m = re.search(r'_dedup_key\("pfe",\s*user\["user_id"\],\s*r\["strategy"\],\s*'
                  r'\n?\s*r\["ticker"\],\s*r\["entry_date"\]\)', sql)
    assert m, (
        "the entry dedup key is no longer (user, strategy, ticker, entry_date). "
        ">= re-reads the current day on every 5-minute tick, so a key that is "
        "not stable across ticks would send a duplicate alert every tick."
    )


def test_the_notifier_reads_the_source_the_runner_writes():
    """The other half of the same break: cw_runner records alert-only entries
    with execution_source='alert', which this list omitted."""
    sql = _portfolio_alert_sql()
    m = re.search(r"live_sources = \(([^)]*)\)", sql)
    assert m, "live_sources is gone"
    assert "'alert'" in m.group(1) or '"alert"' in m.group(1), (
        "the notifier does not select execution_source='alert' — the only "
        "source an alert-only strategy ever writes"
    )

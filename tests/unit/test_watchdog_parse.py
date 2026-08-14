"""Timestamp parsing for the off-box watchdog.

This exists because of a false page on 2026-08-14: the watchdog reported a
7-hour insider-ingest stall while the feed was six minutes old, and 7 hours is
exactly the Pacific offset.

Two mistakes compounded. Postgres renders a whole-hour offset as "-07", which
datetime.fromisoformat only accepts from 3.11 — and the watchdog runs on
/usr/bin/python3, Apple's 3.9.6. The strptime fallback then sliced the input to
26 characters, discarding the very offset it was there to handle, and the naive
result was stamped UTC.

It passed local testing because the dev shell's python3 is 3.14, which parses
"-07" fine. So these cases must hold on the OLDEST interpreter that runs this
script, not just the newest one available.
"""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "offbox_watchdog",
    Path(__file__).resolve().parents[2] / "scripts" / "offbox_watchdog.py",
)
watchdog = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(watchdog)


class TestOffsetForms:
    """Every shape Postgres actually emits into this TEXT column."""

    @pytest.mark.parametrize("raw,expected_utc_hour", [
        # Bare whole-hour offset — the form that caused the false page.
        ("2026-08-14 10:01:43.680025-07", 17),
        ("2026-08-14 10:01:43-07", 17),
        # Fully-specified offset.
        ("2026-08-14 10:01:43.680025-07:00", 17),
        ("2026-08-14T10:01:43+00:00", 10),
        # Winter, when the offset is -08.
        ("2026-01-14 10:01:43-08", 18),
    ])
    def test_offset_is_honoured(self, raw, expected_utc_hour):
        ts = watchdog._parse_ts(raw)
        assert ts is not None
        assert ts.astimezone(timezone.utc).hour == expected_utc_hour

    def test_naive_is_database_local_not_utc(self):
        # created_at defaults to now(), which renders LOCAL time on Studio.
        # Reading a naive value as UTC backdates it by the offset and invents
        # staleness — the exact failure this module is named for.
        ts = watchdog._parse_ts("2026-08-14 10:01:43")
        assert ts is not None
        assert ts.utcoffset().total_seconds() != 0
        assert ts.astimezone(timezone.utc).hour == 17

    def test_date_only(self):
        ts = watchdog._parse_ts("2026-08-14")
        assert ts is not None and ts.year == 2026 and ts.month == 8

    def test_garbage_returns_none(self):
        assert watchdog._parse_ts("not a timestamp") is None
        assert watchdog._parse_ts("") is None


class TestAgeIsSane:
    """The number the alert is actually built on."""

    def test_a_fresh_row_is_not_stale(self):
        # A row written 6 minutes ago in Pacific must not read as 7h old.
        now_local = datetime.now(watchdog._DB_TZ)
        raw = now_local.strftime("%Y-%m-%d %H:%M:%S.%f")
        # Render the offset the way Postgres does: bare hours.
        raw += now_local.strftime("%z")[:3]
        ts = watchdog._parse_ts(raw)
        assert ts is not None
        age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
        assert abs(age_h) < 0.5, f"fresh row computed as {age_h:.1f}h old"

    def test_a_genuinely_stale_row_is_caught(self):
        old = datetime.now(watchdog._DB_TZ).replace(year=2026, month=1, day=2)
        ts = watchdog._parse_ts(old.strftime("%Y-%m-%d %H:%M:%S%z")[:-2])
        assert ts is not None
        age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
        assert age_h > 24


class TestAlertHeaderEncoding:
    """HTTP headers are latin-1; an unencodable Title kills the whole push.

    Found by sending a test alert whose title contained an em-dash: urllib
    raised, the handler swallowed it, and the notification vanished leaving
    only a line on stderr. A channel that silently drops messages is worse
    than no channel — it looks healthy until the one that mattered.
    """

    @pytest.mark.parametrize("title", [
        "Watchdog test — alert",      # em-dash: the exact character that failed
        "Watchdog ✅ 日本語",           # emoji + CJK
        "naïve café",                 # latin-1 representable, must survive intact
    ])
    def test_title_always_encodes_as_a_header(self, title):
        # The only thing that matters: urllib can put it in a header.
        watchdog._header_safe(title).encode("latin-1")

    def test_latin1_characters_are_preserved(self):
        assert watchdog._header_safe("naïve café") == "naïve café"

    def test_plain_title_is_untouched(self):
        assert watchdog._header_safe("Studio watchdog: 3 problem(s)") == \
            "Studio watchdog: 3 problem(s)"

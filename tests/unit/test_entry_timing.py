"""No path may open, price, or model a position before the filing was public.

Two halves. The first tests the shared rule. The second is the one that
matters: it fails the build if a simulator goes back to deciding this for
itself, which is how three of them ended up filling at a close the filing had
not reached yet.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from framework.decision.entry_timing import (
    filed_before_close,
    first_tradeable_index,
)

REPO = Path(__file__).resolve().parents[2]


class TestFiledBeforeClose:
    @pytest.mark.parametrize("utc,expected", [
        # EST (UTC-5): the bell is 21:00 UTC
        ("2026-01-15 20:59:00", True),
        ("2026-01-15 21:00:00", False),
        ("2026-01-15 21:01:00", False),
        # EDT (UTC-4): the bell is 20:00 UTC. A naive UTC hour test would call
        # 20:30 "before close" here and be wrong for half the year.
        ("2026-07-15 19:59:00", True),
        ("2026-07-15 20:00:00", False),
        ("2026-07-15 20:30:00", False),
        # Pre-open on the filing's own ET date: 12:00 UTC is 08:00 ET, which
        # is before that session's bell.
        ("2026-07-15 12:00:00", True),
        # Late-evening UTC belongs to the PREVIOUS ET date: 02:00 UTC on the
        # 15th is 22:00 ET on the 14th, and filing_date is the ET date, so
        # this is after its own session's close.
        ("2026-07-15 02:00:00", False),
        ("2026-07-16 01:00:00", False),
    ])
    def test_dst_correct_boundaries(self, utc, expected):
        assert filed_before_close(utc) is expected

    @pytest.mark.parametrize("bad", [
        None, "", "   ", "not-a-timestamp", "2026-07-15", "2026",
    ])
    def test_unusable_timestamps_are_after_close(self, bad):
        # Guessing "before" fabricates an edge; guessing "after" costs one
        # session of drift. Always guess after.
        assert filed_before_close(bad) is False

    def test_accepts_datetime_and_iso_t(self):
        assert filed_before_close("2026-07-15T19:59:00") is True
        aware = datetime(2026, 7, 15, 19, 59, tzinfo=ZoneInfo("UTC"))
        assert filed_before_close(aware) is True

    def test_naive_datetime_is_read_as_utc(self):
        assert filed_before_close(datetime(2026, 7, 15, 19, 59)) is True
        assert filed_before_close(datetime(2026, 7, 15, 20, 30)) is False


class TestFirstTradeableIndex:
    def test_same_session_when_it_beat_the_bell(self):
        assert first_tradeable_index(100, "2026-07-15 19:59:00") == 100

    def test_next_session_when_it_did_not(self):
        assert first_tradeable_index(100, "2026-07-15 20:30:00") == 101

    def test_next_session_when_unknown(self):
        assert first_tradeable_index(100, None) == 101

    def test_never_earlier_than_the_filing(self):
        for ts in ("2026-07-15 19:59:00", "2026-07-15 20:30:00", None, "junk"):
            assert first_tradeable_index(42, ts) >= 42


class TestNoSimulatorDecidesThisAlone:
    """The regression guard. This is the point of the file.

    Any module that opens positions must either import the shared rule or
    demonstrably not price entries off a bare filing date.
    """

    # Modules that create positions from filings and therefore must obey it.
    POSITION_OPENERS = [
        "pipelines/insider_study/simulate_strategy_portfolio.py",
        "pipelines/insider_study/simulate_portfolio_intraday.py",
        "pipelines/insider_study/backfill_qm_v3.py",
        "pipelines/portfolio_simulator.py",
        "pipelines/insider_study/backfill_cw_portfolio.py",
    ]

    ENTRY_AT_FILING_DATE = re.compile(
        r"(get_close_for|_get_close|find_first_price_on_or_after)\s*\([^)]*filing_date",
        re.IGNORECASE,
    )

    # Exempt only by explicit, reasoned entry — never by a phrase appearing
    # somewhere in the file. The first version of this guard skipped any module
    # containing "next trading day", and backfill_qm_v3.py has that phrase in a
    # comment about EXIT dates while still pricing entries at the filing date.
    # A heuristic escape hatch is how the thing being guarded gets through.
    EXEMPT = {
        "pipelines/portfolio_simulator.py":
            "enters on the next trading day's open, strictly later than any "
            "filing on that date",
    }

    @pytest.mark.parametrize("rel", POSITION_OPENERS)
    def test_module_respects_publication_time(self, rel):
        path = REPO / rel
        if not path.exists():
            pytest.skip(f"{rel} not present")
        src = path.read_text(errors="ignore")

        if rel in self.EXEMPT:
            return
        aware = (
            "entry_timing" in src                    # imports the shared rule
            or "filed_before_close" in src
            or "first_tradeable_index" in src
            or "tradeable_same_day" in src           # SQL-side equivalent
            or "_filed_during_market_hours" in src   # pre-existing local impl
        )
        assert aware, (
            f"{rel} opens positions but shows no sign of checking when the "
            f"filing became public. Use framework.decision.entry_timing — a "
            f"filing dated today may not have existed at today's close, and "
            f"filling there was worth 26 points of CAGR in the strategy sweep."
        )

    @pytest.mark.parametrize("rel", POSITION_OPENERS)
    def test_no_bare_filing_date_entry_pricing(self, rel):
        path = REPO / rel
        if not path.exists():
            pytest.skip(f"{rel} not present")
        if rel in self.EXEMPT:
            return
        src = path.read_text(errors="ignore")
        hits = [m.group(0) for m in self.ENTRY_AT_FILING_DATE.finditer(src)]
        assert not hits, (
            f"{rel} prices an entry directly off filing_date: {hits[:2]}. "
            f"Gate it on framework.decision.entry_timing.filed_before_close."
        )

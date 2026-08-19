"""filed_at is Eastern, we act on a polled pickup, and after-close fills at the open.

THE BUG THIS PINS

trades.filed_at held two timezones in one TEXT column. Pre-2026 rows came from
the bulk ingest in UTC; from 2026-01-01 backfill_live scrapes EDGAR's
"Accepted" field — published in Eastern — and stores it verbatim. Every reader
assumed UTC throughout, so a 2026 filing lost four hours.

Derek found it on CDNL: SEC accepted 2026-08-17 17:37:34 ET, after the bell.
Read as UTC that is 13:37 ET, so the guard said "before the close" and the
position booked against 2026-08-17's close of $39.34 — a price that had already
printed. The first price anyone could have paid was the next open, $41.74.

37 of 278 published positions were a day early for the same reason.
"""
from datetime import datetime

import pytest

from framework.decision.entry_timing import (
    MARKET_CLOSE_ET,
    POLL_MINUTES,
    entry_fill,
    filed_at_eastern,
    filed_before_close,
    first_tradeable_index,
    pickup_time,
)


# ── the column is Eastern ───────────────────────────────────────────────────

def test_filed_at_is_parsed_as_eastern_with_no_conversion():
    """The single most important assertion here. Any timezone shift
    reintroduces the CDNL bug."""
    dt = filed_at_eastern("2026-08-17 17:37:34")
    assert (dt.hour, dt.minute) == (17, 37), "filed_at must not be converted"
    assert dt.date().isoformat() == "2026-08-17"


def test_the_cdnl_filing_is_after_the_bell():
    assert filed_before_close("2026-08-17 17:37:34", 1812198) is False
    assert entry_fill("2026-08-17 17:37:34", 1812198) == (1, "open")


@pytest.mark.parametrize("bad", [None, "", "not-a-date", "2026-08-17"])
def test_unparseable_is_treated_as_after_close(bad):
    """Conservative direction: guessing 'before' fabricates an edge."""
    assert filed_at_eastern(bad) is None
    assert filed_before_close(bad) is False
    assert entry_fill(bad) == (1, "open")


# ── we act on pickup, not acceptance ────────────────────────────────────────

def test_pickup_rounds_up_to_the_next_poll():
    pu = pickup_time("2026-08-17 09:12:00", "k")
    assert (pu.hour, pu.minute) == (9, 15)


def test_a_filing_on_the_boundary_waits_for_the_next_poll():
    """15:55:00 exactly is not in the payload already requested."""
    pu = pickup_time("2026-08-17 15:55:00", "k")
    assert (pu.hour, pu.minute) == (16, 0)


def test_pickup_can_push_a_late_afternoon_filing_past_the_bell():
    """The point of modelling pickup at all. 15:57 acceptance is not 15:57
    knowledge — the next poll is at 16:00, after the close."""
    assert filed_before_close("2026-08-17 15:57:10", "k") is False
    assert filed_before_close("2026-08-17 15:48:00", "k") is True


def test_jitter_is_deterministic_per_filing():
    """A rerun must reproduce the same book, or no two backtests agree."""
    a = pickup_time("2026-08-17 09:12:00", 12345)
    b = pickup_time("2026-08-17 09:12:00", 12345)
    assert a == b
    assert a.second <= 90


def test_jitter_differs_between_filings():
    seconds = {pickup_time("2026-08-17 09:12:00", i).second for i in range(40)}
    assert len(seconds) > 5, "jitter is not varying across filings"


# ── what you can fill at ────────────────────────────────────────────────────

def test_before_the_bell_fills_at_that_session_close():
    assert entry_fill("2026-08-17 10:04:00", "k") == (0, "close")


def test_after_the_bell_fills_at_the_NEXT_OPEN_not_the_next_close():
    """Using the next close would hand the model a free session of drift in
    whichever direction the news pushed the stock."""
    offset, field = entry_fill("2026-08-17 17:37:34", "k")
    assert (offset, field) == (1, "open")


def test_first_tradeable_index_still_agrees():
    assert first_tradeable_index(10, "2026-08-17 10:00:00") == 10
    assert first_tradeable_index(10, "2026-08-17 17:37:34") == 11


def test_market_close_is_four_pm():
    assert MARKET_CLOSE_ET == (16, 0)
    assert POLL_MINUTES == 5


# ── the simulator must not reimplement any of it ────────────────────────────

def test_simulator_defers_to_this_module():
    """The SQL copy of this rule was corrected twice in two days before being
    deleted. It must not come back."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[2]
           / "pipelines/insider_study/simulate_strategy_portfolio.py").read_text()
    assert "entry_fill(" in src, "simulator no longer calls the canonical rule"
    assert "AT TIME ZONE 'UTC'" not in src, (
        "simulator is converting filed_at again — the column is Eastern"
    )
    assert "tradeable_same_day" not in src, (
        "the SQL-side tradeability decision is back"
    )

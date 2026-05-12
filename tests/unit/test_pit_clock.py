"""Unit tests for PITClock.

The clock is the foundation of the PIT engine; its assertions must be
unconditional, the read tape must capture every check, and date arithmetic
must be precise.
"""
from __future__ import annotations

import pytest

from framework.pit.clock import LookaheadError, PITClock, ReadTape


# ── Construction ────────────────────────────────────────────────────────

def test_clock_construction_valid_date():
    c = PITClock(as_of_date="2024-03-15")
    assert c.as_of_date == "2024-03-15"
    assert isinstance(c.tape, ReadTape)
    assert len(c.tape) == 0


@pytest.mark.parametrize("bad", ["2024-3-15", "20240315", "March 15, 2024",
                                 "2024-13-01", "", "yesterday"])
def test_clock_rejects_invalid_date(bad):
    with pytest.raises(ValueError):
        PITClock(as_of_date=bad)


def test_clock_is_frozen():
    c = PITClock(as_of_date="2024-03-15")
    with pytest.raises(Exception):
        c.as_of_date = "2024-03-16"  # frozen dataclass forbids


# ── assert_known semantics ──────────────────────────────────────────────

def test_assert_known_passes_when_equal():
    c = PITClock(as_of_date="2024-03-15")
    c.assert_known("2024-03-15", source="trades.filing_date")
    assert len(c.tape) == 1


def test_assert_known_passes_when_before():
    c = PITClock(as_of_date="2024-03-15")
    c.assert_known("2024-03-14", source="trades.filing_date")
    c.assert_known("2020-01-01", source="trades.filing_date")
    assert len(c.tape) == 2


def test_assert_known_raises_when_after():
    c = PITClock(as_of_date="2024-03-15")
    with pytest.raises(LookaheadError) as exc:
        c.assert_known("2024-03-16", source="trades.filing_date")
    msg = str(exc.value)
    assert "2024-03-16" in msg
    assert "2024-03-15" in msg
    # Failed checks do not pollute the tape.
    assert len(c.tape) == 0


def test_assert_known_rejects_none():
    c = PITClock(as_of_date="2024-03-15")
    with pytest.raises(LookaheadError):
        c.assert_known(None, source="trades.filing_date")


def test_assert_known_rejects_malformed():
    c = PITClock(as_of_date="2024-03-15")
    with pytest.raises(ValueError):
        c.assert_known("not-a-date", source="trades.filing_date")


# ── is_known semantics ──────────────────────────────────────────────────

def test_is_known_does_not_raise():
    c = PITClock(as_of_date="2024-03-15")
    assert c.is_known("2024-03-14") is True
    assert c.is_known("2024-03-15") is True
    assert c.is_known("2024-03-16") is False
    assert c.is_known(None) is False


def test_is_known_does_not_record_to_tape():
    c = PITClock(as_of_date="2024-03-15")
    c.is_known("2024-03-14")
    c.is_known("2024-03-16")
    assert len(c.tape) == 0


# ── cutoff arithmetic ───────────────────────────────────────────────────

def test_cutoff_zero_lag_is_as_of():
    c = PITClock(as_of_date="2024-03-15")
    assert c.cutoff(0) == "2024-03-15"
    assert c.cutoff() == "2024-03-15"


@pytest.mark.parametrize("lag,expected", [
    (1, "2024-03-14"),
    (10, "2024-03-05"),
    (40, "2024-02-04"),
    (100, "2023-12-06"),
])
def test_cutoff_subtracts_lag(lag, expected):
    c = PITClock(as_of_date="2024-03-15")
    assert c.cutoff(lag) == expected


def test_cutoff_handles_year_boundary():
    c = PITClock(as_of_date="2024-01-15")
    assert c.cutoff(30) == "2023-12-16"


# ── Read tape ───────────────────────────────────────────────────────────

def test_tape_records_in_order():
    c = PITClock(as_of_date="2024-03-15")
    c.assert_known("2024-01-01", source="trades.filing_date")
    c.assert_known("2024-02-01", source="prices.daily_prices.date")
    c.assert_known("2024-03-01", source="insider_ticker_scores.as_of_date")
    assert c.tape.entries == [
        ("trades.filing_date", "2024-01-01"),
        ("prices.daily_prices.date", "2024-02-01"),
        ("insider_ticker_scores.as_of_date", "2024-03-01"),
    ]


def test_tape_max_knowledge_date():
    c = PITClock(as_of_date="2024-03-15")
    assert c.tape.max_knowledge_date() is None
    c.assert_known("2024-01-01", source="x")
    c.assert_known("2024-02-15", source="x")
    c.assert_known("2024-01-31", source="x")
    assert c.tape.max_knowledge_date() == "2024-02-15"


# ── Multiple clocks isolated ────────────────────────────────────────────

def test_separate_clocks_have_separate_tapes():
    c1 = PITClock(as_of_date="2024-03-15")
    c2 = PITClock(as_of_date="2024-04-15")
    c1.assert_known("2024-01-01", source="x")
    c2.assert_known("2024-04-01", source="x")
    assert len(c1.tape) == 1
    assert len(c2.tape) == 1
    assert c1.tape.entries[0][1] == "2024-01-01"
    assert c2.tape.entries[0][1] == "2024-04-01"


# ── Equality on as_of_date only ─────────────────────────────────────────

def test_clocks_equal_by_as_of_only():
    c1 = PITClock(as_of_date="2024-03-15")
    c2 = PITClock(as_of_date="2024-03-15")
    # tapes diverge but clocks compare equal because tape has compare=False
    c1.assert_known("2024-01-01", source="x")
    assert c1 == c2

"""Capitol Trades date parsing — the Published column drifted to relative dates.

Some time after 2026-04 Capitol Trades started rendering the Published
column as a clock time plus a relative day ('13:05Today', '13:06Yesterday')
instead of an absolute date ('13 Mar2026'). parse_ct_date only understood
the absolute form, so it returned None for every recent row.

That is the worst shape of failure: the scrape still "succeeded", rows still
parsed, and only filing_date came back empty — while filing_date is the PIT
timestamp (when a trade became public). Nothing raised. Congress data simply
stopped being usable.

The Traded column stayed absolute throughout, which is why trade_date kept
parsing and made the breakage even easier to miss.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.congress_scraper.scrape_capitol_trades import parse_ct_date  # noqa: E402

NOW = datetime(2026, 8, 12, 15, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("text,expected", [
    # Relative — the shape that broke.
    ("13:05Today", "2026-08-12"),
    ("13:06Yesterday", "2026-08-11"),
    ("Today", "2026-08-12"),
    ("Yesterday", "2026-08-11"),
    ("09:30today", "2026-08-12"),      # case-insensitive
    ("23:59YESTERDAY", "2026-08-11"),
])
def test_relative_dates(text, expected):
    assert parse_ct_date(text, now=NOW) == expected


@pytest.mark.parametrize("text,expected", [
    # Absolute — must keep working; this is the Traded column's format.
    ("10 Aug2026", "2026-08-10"),
    ("13 Mar2026", "2026-03-13"),
    ("2 Feb2026", "2026-02-02"),
    ("1 Sept2026", "2026-09-01"),      # site writes "Sept", not "Sep"
])
def test_absolute_dates(text, expected):
    assert parse_ct_date(text, now=NOW) == expected


@pytest.mark.parametrize("text", ["", "   ", "garbage", "32 Xxx2026"])
def test_unparseable_returns_none(text):
    assert parse_ct_date(text, now=NOW) is None


def test_relative_is_resolved_against_supplied_now():
    """Relative labels must not silently bind to real wall-clock time."""
    other = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert parse_ct_date("Today", now=other) == "2026-01-01"
    assert parse_ct_date("Yesterday", now=other) == "2025-12-31"


def test_filing_date_never_silently_empty_for_known_shapes():
    """Guard the actual regression: every shape the site emits must parse.

    If Capitol Trades drifts again, this fails loudly instead of letting
    filing_date go quietly null across the whole feed.
    """
    observed_on_site = ["13:05Today", "13:06Yesterday", "10 Aug2026"]
    parsed = [parse_ct_date(t, now=NOW) for t in observed_on_site]
    assert all(p is not None for p in parsed), f"unparsed: {parsed}"

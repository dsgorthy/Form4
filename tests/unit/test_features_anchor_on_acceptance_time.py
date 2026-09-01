"""Features anchor on the acceptance TIMESTAMP, not the filing date.

WHAT WENT WRONG

compute_derived_features anchored every price window on "the first session at
or after filing_date". For a Form 4 accepted INTRADAY that is the same day's
CLOSE -- a price up to six hours in the future relative to the moment the
filing became public. Measured on discretionary buys 2016+:

    after the bell   196,442   61.8%   close is legitimately available
    INTRADAY          86,701   27.3%   close had NOT happened yet
    pre-open          29,194    9.2%   likewise
    no timestamp       5,769    1.8%

So the date-only anchor leaked on roughly a quarter of the corpus, and ~72,000
of those rows already had features computed from it.

THE RULE, which mirrors entry_timing.py

    accepted at/after 16:00 ET  ->  that session's close
    accepted before 16:00 ET    ->  the PREVIOUS session's close

Features describe the state the market was in when the filing landed. Entry
happens at the first price you could actually pay -- entry_timing fills an
after-bell filing at the NEXT session's open. The two anchors are mirror
images, and both read filed_at.

AND NO TIMEZONE CONVERSION. filed_at is TEXT holding naive Eastern wall time.
Casting it to timestamptz reinterprets it in the session timezone, which on a
UTC connection hands 71% of filings a session of look-ahead. That bug was live
in backfill_returns_from_filing until 2026-08-28.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "pipelines" / "insider_study" / "compute_derived_features.py"


def _ev_block() -> str:
    s = SRC.read_text(encoding="utf-8")
    body = s[s.index("), ev AS ("):]
    return body[:body.index(")\n", body.index("FROM trades t"))]


def test_the_anchor_reads_filed_at():
    ev = _ev_block()
    assert "filed_at" in ev, (
        "the feature anchor no longer consults filed_at, so an intraday filing "
        "reads a close that had not happened when it became public"
    )


def test_the_sixteen_hundred_boundary_is_applied():
    ev = _ev_block()
    assert "'16:00'" in ev, "the market-close boundary is gone from the anchor"


def test_intraday_filings_anchor_to_the_PREVIOUS_session():
    """The whole point. An after-bell filing may use that day's close; an
    intraday one must fall back a session."""
    ev = _ev_block()
    assert re.search(r"c\.date\s*<\s*t\.filing_date", ev), (
        "no strictly-earlier branch: intraday filings would still read the "
        "same session's close"
    )
    assert re.search(r"c\.date\s*<=\s*t\.filing_date", ev), (
        "no same-session branch: after-bell filings would needlessly lose a day"
    )


def test_no_timezone_conversion_on_filed_at():
    """filed_at is naive Eastern text. Casting it makes the result depend on
    whoever happens to be connected."""
    src = SRC.read_text(encoding="utf-8")
    assert "filed_at::timestamptz" not in src, (
        "filed_at is being cast. On a UTC session that shifts it five hours "
        "and grants look-ahead; entry_timing.py:42 forbids the conversion."
    )
    assert "AT TIME ZONE" not in src


def test_a_missing_timestamp_takes_the_conservative_branch():
    """1.8% of buys have no filed_at. Unknown must not silently become
    'intraday', which would move the anchor for no reason."""
    ev = _ev_block()
    assert "COALESCE" in ev and "TRUE" in ev, (
        "a NULL filed_at no longer resolves to a definite branch"
    )

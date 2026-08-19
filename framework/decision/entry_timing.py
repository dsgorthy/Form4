"""When could a filing first have been traded?

THE RULE

A position may never be opened, priced, or modelled at a moment before the
filing was public. There is exactly one input that answers "when did it become
public": `trades.filed_at`, the SEC acceptance timestamp. `filing_date` is a
date, and a date cannot answer the question — EDGAR accepts Form 4 until 22:00
ET, so a filing dated today may not have existed at today's 16:00 close.

WHY THIS MODULE EXISTS RATHER THAN THE RULE BEING RESTATED PER SIMULATOR

It was restated per simulator, and they disagreed:

    backfill_cw_portfolio.py        timestamp-aware      correct
    portfolio_simulator.py          next day's open      correct (conservative)
    simulate_strategy_portfolio.py  filing day's close   LOOK-AHEAD
    simulate_portfolio_intraday.py  filing day's close   LOOK-AHEAD
    backfill_qm_v3.py               filing day's close   LOOK-AHEAD

Measured on the strategy sweep, filling at the filing day's close when 43.5%
of A+/A filings post after the bell was worth 26 points of CAGR (59.9% ->
33.8% on the no-trend variant). That is not a rounding error, it is most of
the result.

So the rule lives here, once, and `tests/unit/test_entry_timing.py` fails the
build if a simulator reimplements it.

`filed_at` IS EASTERN

It was not always. Until 2026-08-19 the column held two timezones: pre-2026
rows in UTC from the bulk ingest, 2026-onward rows in Eastern because
backfill_live scrapes EDGAR's "Accepted" field, which SEC publishes in ET, and
stores the string verbatim. Every reader assumed UTC, so a 2026 filing lost
four hours and an after-bell acceptance read as before-bell.

Derek found it on CDNL: accepted 2026-08-17 17:37:34 ET, booked at that day's
$39.34 close. The first price anyone could have paid was the next open, $41.74.
37 of 278 published positions were a day early for the same reason.

migrations/2026-08-19_filed_at_normalize_eastern.sql converted the UTC era, so
the column is now Eastern throughout. DO NOT apply a timezone conversion when
reading it.

WE ACT ON PICKUP, NOT ON ACCEPTANCE

Knowing the instant EDGAR accepted a filing is not the same as knowing when we
could have acted on it. The scanner polls; a filing accepted at 15:57 is not in
our hands at 15:57. `pickup_time` models that: round the acceptance up to the
next five-minute poll, then add up to 90 seconds of jitter so the model does
not sit exactly on the boundary — a filing at 15:58 should not reliably beat
the bell just because our arithmetic is tidy.

The jitter is derived from the filing's own key, so a given filing always gets
the same pickup and a rerun reproduces the same book.

WHAT YOU CAN ACTUALLY FILL AT

  picked up before 16:00 ET  ->  that session's CLOSE
  picked up at or after      ->  the NEXT session's OPEN

Not the next close. A filing that lands after the bell is actionable at the
next open, and pretending otherwise hands the model a free session of drift in
whichever direction the news pushed the stock.

CONSERVATIVE DIRECTION

A missing or unparseable `filed_at` is treated as after-close. Roughly 0% of
recent rows lack it, but the failure mode of guessing "before" is a fabricated
edge, and the failure mode of guessing "after" is one session of lost drift.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

__all__ = [
    "MARKET_CLOSE_ET",
    "POLL_MINUTES",
    "filed_at_eastern",
    "pickup_time",
    "filed_before_close",
    "first_tradeable_index",
    "entry_fill",
]

#: How often the scanner polls EDGAR.
POLL_MINUTES = 5

#: Upper bound on the jitter added after rounding up to a poll boundary.
_JITTER_SECONDS = 90

#: US equity regular-session close, Eastern. Filings accepted at or after this
#: instant cannot be traded until the following session.
MARKET_CLOSE_ET = (16, 0)


def filed_at_eastern(filed_at: str | datetime | None) -> datetime | None:
    """Parse `filed_at` as a naive Eastern datetime. No conversion applied.

    The column is Eastern. Converting it would reintroduce the bug this module
    documents.
    """
    if filed_at is None:
        return None
    if isinstance(filed_at, datetime):
        return filed_at.replace(tzinfo=None)
    text = str(filed_at).strip().replace("T", " ")
    if len(text) < 19:
        return None
    try:
        return datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def pickup_time(filed_at: str | datetime | None, key: str | int = "") -> datetime | None:
    """When the scanner would have had this filing in hand, Eastern.

    Acceptance rounded UP to the next `POLL_MINUTES` boundary, plus deterministic
    jitter of 0-90s keyed on the filing so reruns reproduce the same book.
    A filing landing exactly on a boundary waits for the following poll — it was
    not in the payload that had already been requested.
    """
    dt = filed_at_eastern(filed_at)
    if dt is None:
        return None
    bump = (POLL_MINUTES - dt.minute % POLL_MINUTES) % POLL_MINUTES or POLL_MINUTES
    slot = (dt + timedelta(minutes=bump)).replace(second=0, microsecond=0)
    jitter = int(hashlib.sha256(str(key).encode()).hexdigest()[:4], 16) % (_JITTER_SECONDS + 1)
    return slot + timedelta(seconds=jitter)


def filed_before_close(filed_at: str | datetime | None, key: str | int = "") -> bool:
    """True when we would have had the filing before that day's closing bell.

    Measured at PICKUP, not acceptance: a filing accepted at 15:57 is not in
    our hands until the next poll, which may be after the bell.
    """
    pu = pickup_time(filed_at, key)
    if pu is None:
        return False
    return (pu.hour, pu.minute) < MARKET_CLOSE_ET


def entry_fill(filed_at: str | datetime | None, key: str | int = "") -> tuple[int, str]:
    """(session offset, price field) for the first fill we could have got.

    (0, "close")  picked up before the bell — that session's close
    (1, "open")   picked up after — the NEXT session's open, not its close
    """
    return (0, "close") if filed_before_close(filed_at, key) else (1, "open")


def first_tradeable_index(filing_index: int, filed_at: str | datetime | None) -> int:
    """Index of the first session whose CLOSE could have been traded on.

    `filing_index` is the filing date's position in a trading calendar. Returns
    the same index when the filing beat the bell, otherwise the next session.

    Deliberately index-based rather than date-based: every simulator here walks
    a precomputed calendar, and returning a date would make each of them
    re-derive "what is the next trading day", which is the second place this
    logic used to fork.
    """
    return filing_index if filed_before_close(filed_at) else filing_index + 1

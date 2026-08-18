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

CONSERVATIVE DIRECTION

A missing or unparseable `filed_at` is treated as after-close. Roughly 0% of
recent rows lack it, but the failure mode of guessing "before" is a fabricated
edge, and the failure mode of guessing "after" is one session of lost drift.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

__all__ = [
    "MARKET_CLOSE_ET",
    "filed_before_close",
    "first_tradeable_index",
]

_ET = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")

#: US equity regular-session close, Eastern. Filings accepted at or after this
#: instant cannot be traded until the following session.
MARKET_CLOSE_ET = (16, 0)


def filed_before_close(filed_at: str | datetime | None) -> bool:
    """True when the filing was public before that day's closing bell.

    `filed_at` is stored as a UTC timestamp (text or datetime). Comparison is
    done in Eastern so it stays correct across DST — a naive UTC hour test is
    wrong for half the year, which is the bug this function exists to avoid.

    Returns False for anything missing or unparseable. See module docstring on
    why the conservative direction is the safe one.
    """
    if filed_at is None:
        return False

    if isinstance(filed_at, datetime):
        dt = filed_at
    else:
        text = str(filed_at).strip().replace("T", " ")
        if len(text) < 19:
            return False
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return False

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)

    et = dt.astimezone(_ET)
    close_h, close_m = MARKET_CLOSE_ET
    return (et.hour, et.minute) < (close_h, close_m)


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

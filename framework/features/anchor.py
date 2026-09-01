"""The one definition of "which session could this filing have been acted on".

WHY THIS EXISTS

Three separate look-aheads in one week, each because a different script derived
this for itself:

  1. `filed_at` read as UTC when it holds naive Eastern -> 37 positions entered
     a session early (2026-08-19).
  2. `(filed_at::timestamptz AT TIME ZONE 'America/New_York')` in the label
     generator -> the result depended on the SESSION timezone: +3h on Studio,
     but -5h on any UTC connection, which handed 71.3% of filings a same-session
     close they did not exist for (2026-08-28).
  3. Derived features anchored on `filing_date` rather than `filed_at` -> for
     the 27.3% of buys accepted intraday, every price window ended on a close
     up to six hours in the future (2026-08-31).

Same mistake three times: TREATING A DATE AS IF IT WERE A MOMENT. Every feature
and every label now resolves its session here, and nowhere else.

THE TWO ANCHORS, AND WHY THEY DIFFER

    OBSERVATION  the last session that had CLOSED when the filing appeared.
                 What the market looked like as the news landed. Backward
                 windows -- momentum, 52-week high, average volume -- end here.

    EXECUTION    the first session you could actually pay for. An after-bell
                 filing fills at the NEXT session's open.

They are mirror images across the same 16:00 ET boundary, and they are NOT
interchangeable. A feature that uses the execution anchor is reading a price
that had not printed; a label that uses the observation anchor is claiming a
fill nobody could get.

    filed 10:00 ET on D   observation = D-1 close   execution = D
    filed 18:59 ET on D   observation = D   close   execution = D+1

filed_at IS EASTERN, AS TEXT, AND MUST NOT BE CONVERTED. See
framework/decision/entry_timing.py; migrations/2026-08-19_filed_at_normalize_eastern.sql
converted the historical UTC era, so the column is Eastern throughout. A
missing or unparseable timestamp resolves to AFTER THE BELL, which is the
conservative direction for both anchors.
"""
from __future__ import annotations

#: US equity regular-session close, Eastern. A filing accepted at or after this
#: cannot be traded until the next session.
MARKET_CLOSE_ET = "16:00"

#: Positions 12..16 of "YYYY-MM-DD HH:MM:SS" are "HH:MM". A lexicographic
#: compare on that substring is correct for 24-hour times and needs no cast --
#: which is the entire point, since casting is what broke it twice.
_HHMM = "substring({col} from 12 for 5)"


def after_bell_sql(col: str = "t.filed_at") -> str:
    """SQL boolean: did this filing arrive at or after the close?

    NULL or short timestamps resolve TRUE (after the bell), which pushes the
    observation anchor later and the execution anchor later -- conservative on
    both sides.
    """
    return f"COALESCE({_HHMM.format(col=col)} >= '{MARKET_CLOSE_ET}', TRUE)"


def observation_session_sql(cal: str = "cal",
                            filed_at: str = "t.filed_at",
                            filing_date: str = "t.filing_date") -> str:
    """Calendar index of the last session CLOSED when the filing appeared.

    `cal` must be a relation with (date, d), d ascending -- conventionally the
    SPY calendar, so a long weekend cannot shorten a window.
    """
    return (
        f"CASE WHEN {after_bell_sql(filed_at)} "
        f"THEN (SELECT MAX(c.d) FROM {cal} c WHERE c.date <= {filing_date}) "
        f"ELSE (SELECT MAX(c.d) FROM {cal} c WHERE c.date <  {filing_date}) END"
    )


def execution_session_sql(cal: str = "cal",
                          filed_at: str = "t.filed_at",
                          filing_date: str = "t.filing_date") -> str:
    """Calendar index of the first session the filing could be traded in.

    Mirror of the observation anchor: before the bell fills the same session,
    at or after it fills the next.
    """
    return (
        f"CASE WHEN {after_bell_sql(filed_at)} "
        f"THEN (SELECT MIN(c.d) FROM {cal} c WHERE c.date >  {filing_date}) "
        f"ELSE (SELECT MIN(c.d) FROM {cal} c WHERE c.date >= {filing_date}) END"
    )


def after_bell(filed_at: str | None) -> bool:
    """Python mirror of after_bell_sql, for non-SQL callers."""
    if not filed_at or len(filed_at) < 16:
        return True
    return filed_at[11:16] >= MARKET_CLOSE_ET

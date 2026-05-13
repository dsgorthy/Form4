"""Stop rules sourced from the May 13 thesis PDFs.

Hard-coded for now — small enough that a config file would be overkill,
and these are the contract we wrote in the research docs.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

from pipelines.thesis_monitor.macro import MacroPoint
from pipelines.thesis_monitor.prices import Quote


@dataclass
class StopHit:
    thesis: str
    label: str
    action: str
    detail: str


# Each stop: (label, action, predicate(quotes, macro, today) -> str|None)
# Predicate returns the detail string when tripped, else None.

def _eq_below(ticker: str, level: float) -> Callable:
    def f(quotes: dict[str, Quote], macro, today) -> Optional[str]:
        q = quotes.get(ticker)
        if q and q.last < level:
            return f"{ticker} closed at ${q.last:.2f}, below ${level:.2f}"
        return None
    return f


def _macro_below(key: str, level: float) -> Callable:
    def f(quotes, macro: dict[str, MacroPoint], today) -> Optional[str]:
        m = macro.get(key)
        if m and m.value < level:
            return f"{key.upper()} at {m.value:.2f}, below {level:.2f} ({m.date})"
        return None
    return f


def _time_stop(end: date) -> Callable:
    def f(quotes, macro, today: date) -> Optional[str]:
        if today >= end:
            return f"Time stop reached ({end})"
        return None
    return f


STOPS = [
    # Oil thesis
    ("oil", "FRO < $30",
     "Exit equity leg of oil thesis",
     _eq_below("FRO", 30.0)),
    ("oil", "Brent < $90 (single-day)",
     "Watch — close options leg if Brent < $90 for 10 consecutive sessions",
     _macro_below("brent", 90.0)),
    ("oil", "Time stop Feb 13 2027",
     "Mechanical exit of all oil-thesis positions",
     _time_stop(date(2027, 2, 13))),

    # Data center thesis
    ("data_center", "MP < $49 (-25%)",
     "Exit MP shares",
     _eq_below("MP", 49.0)),
    ("data_center", "COPX < $74 (-20%)",
     "Exit COPX shares",
     _eq_below("COPX", 74.0)),
]


def check_stops(
    quotes: dict[str, Quote],
    macro: dict[str, MacroPoint],
    today: Optional[date] = None,
) -> list[StopHit]:
    today = today or date.today()
    hits: list[StopHit] = []
    for thesis, label, action, predicate in STOPS:
        detail = predicate(quotes, macro, today)
        if detail:
            hits.append(StopHit(thesis=thesis, label=label, action=action, detail=detail))
    return hits

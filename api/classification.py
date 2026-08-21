"""What KIND of filing is this? One definition, for every surface.

Dependency-free on purpose — no FastAPI, no DB — so Studio's host Python and
the test suite can both import it, same as `api.public_fields`, `api.ratings`
and `api.titles`.

────────────────────────────────────────────────────────────────────────────
WHY THIS FILE EXISTS
────────────────────────────────────────────────────────────────────────────

"Routine" meant five different things, measured 2026-08-21 over the 126,521
filings since 2026-01-01:

    signal_class (canonical, trigger-maintained)     68,394 routine
    api/narrative.py _is_routine                     45,371
    feed-list.tsx + insider-trades-table.tsx         35,060
    the Stocktwits generator                         30,630
    trades-table.tsx (/explore)                      11,477

Every pair disagreed on tens of thousands of rows. The visible symptom:
**22,937 10b5-1 planned sells showed "SELL · Routine" in the feed and no label
at all on /explore.**

The cause is that four surfaces each assembled their own answer out of raw
flags with wildly different coverage — `is_routine` is 16% populated,
`cohen_routine` 100%, `is_10b5_1` 96% — so a definition resting on
`is_routine` alone silently misses most of what it means to catch.

────────────────────────────────────────────────────────────────────────────
THE SOURCE IS signal_class, AND ONLY signal_class
────────────────────────────────────────────────────────────────────────────

`trades.signal_class` is maintained by a database trigger from
`form4_signal_class()` (migrations/2026-08-17_trades_signal_class.sql). It is
100% populated and it already encodes every distinction the flags were being
combined to recover:

    discretionary_sell   33,566   0 are 10b5-1  <- by construction
    compensation         26,284
    planned_sell         23,885   ALL 23,885 are 10b5-1
    tax_withholding      16,213
    option_exercise      14,249
    discretionary_buy     9,502   0 are 10b5-1
    gift                  1,728
    derivative              717
    planned_buy             284   all 284 are 10b5-1
    inconsistent             93

Note what this means for `is_10b5_1`: it is NOT dead, despite reading as zero
on every discretionary sell. It is zero there because a 10b5-1 sale is not a
discretionary sale and the trigger routes it to `planned_sell`. Deriving the
published label from signal_class therefore captures 10b5-1 automatically, and
no surface needs to know the flag exists.

────────────────────────────────────────────────────────────────────────────
KIND IS 1-TO-1. RECURRENCE IS A TAG.
────────────────────────────────────────────────────────────────────────────

`cohen_routine` cuts ACROSS signal_class — 5,831 discretionary sells carry it —
so folding it into the kind would make one filing two kinds at once, which is
the error api/ratings.py exists to prevent, arriving through a different door.

A filing has exactly one KIND (what the insider did) and may separately be
RECURRING (a pattern in how they do it). Those are different questions and get
different fields.
"""
from __future__ import annotations

from typing import Any, Optional

__all__ = [
    "FILING_KINDS",
    "filing_kind",
    "is_discretionary",
    "is_scheduled",
    "is_recurring_pattern",
    "kind_meta",
    "attach_classification",
    "DISCRETIONARY_CLASSES",
]

#: Published vocabulary, most-signal first. Deliberately five, not the ten
#: values of signal_class: `gift`, `derivative` and `inconsistent` name
#: internal plumbing rather than anything a subscriber recognises, and they are
#: 2% of the corpus between them. They map to None and render no chip.
FILING_KINDS = ("Discretionary", "Scheduled", "Compensation", "Tax", "Exercise")

#: signal_class -> published kind. None means "render nothing".
_KIND_OF: dict[str, Optional[str]] = {
    "discretionary_buy": "Discretionary",
    "discretionary_sell": "Discretionary",
    "planned_sell": "Scheduled",
    "planned_buy": "Scheduled",
    "compensation": "Compensation",
    "tax_withholding": "Tax",
    "option_exercise": "Exercise",
    "gift": None,
    "derivative": None,
    "inconsistent": None,
}

#: The classes that represent an actual decision to buy or sell on the open
#: market. This is the set every "is this a real signal?" filter should use —
#: strategy entry, the Stocktwits generator, the notable-trade gate.
DISCRETIONARY_CLASSES = ("discretionary_buy", "discretionary_sell")

KIND_META: dict[str, dict[str, Any]] = {
    "Discretionary": {
        "blurb": "An open-market decision to buy or sell.",
        "signal": True,
    },
    "Scheduled": {
        "blurb": "Executed under a 10b5-1 plan set up in advance.",
        "signal": False,
    },
    "Compensation": {
        "blurb": "Shares received as pay, not bought.",
        "signal": False,
    },
    "Tax": {
        "blurb": "Shares withheld to cover tax on a vesting award.",
        "signal": False,
    },
    "Exercise": {
        "blurb": "Options converted into shares.",
        "signal": False,
    },
}


def filing_kind(signal_class: Optional[str]) -> Optional[str]:
    """Published kind for a filing. None means render no label.

    None in, None out — an unclassified row gets no claim made about it.
    """
    if not signal_class:
        return None
    return _KIND_OF.get(str(signal_class).strip().lower())


def is_discretionary(signal_class: Optional[str]) -> bool:
    """Did the insider make a decision, as opposed to receiving or scheduling?

    This replaces the five hand-rolled `_is_routine` variants. Note the
    polarity flip: the old checks asked "is this routine", each with different
    coverage; this asks the positive question against the one column that is
    always populated.
    """
    return (signal_class or "").strip().lower() in DISCRETIONARY_CLASSES


def is_scheduled(signal_class: Optional[str]) -> bool:
    """Pre-arranged under a 10b5-1 plan. Equivalent to is_10b5_1 on sells,
    but derived from the column that is 100% populated rather than 96%."""
    return filing_kind(signal_class) == "Scheduled"


def is_recurring_pattern(item: dict) -> bool:
    """A behavioural tag, NOT a kind — see the module docstring.

    `cohen_routine` is 100% populated and cuts across signal_class;
    `is_recurring` is a much narrower marker (197 rows since January). Either
    one means the insider does this on a rhythm, which is worth saying beside
    the kind and must never replace it.
    """
    return bool(item.get("cohen_routine") or item.get("is_recurring"))


def kind_meta(kind: Optional[str]) -> Optional[dict[str, Any]]:
    return KIND_META.get(kind) if kind else None


def attach_classification(items: Optional[list[dict]]) -> None:
    """Stamp `filing_kind` and `is_recurring` onto API rows, in place.

    Call once per response, beside attach_ratings(). Consumers then read one
    field instead of recombining flags — which is how five definitions of the
    same word came to exist.
    """
    for item in items or []:
        item["filing_kind"] = filing_kind(item.get("signal_class"))
        item["is_discretionary"] = is_discretionary(item.get("signal_class"))
        item["recurring_pattern"] = is_recurring_pattern(item)

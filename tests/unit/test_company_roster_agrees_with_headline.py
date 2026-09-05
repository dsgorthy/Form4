"""The roster and the headline on a company page must count the same thing.

WHAT WENT WRONG

The "one trade population" change unified the summary sentence and the insider
count, and left the ROSTER reading insider_companies.trade_count — every row
the insider ever filed on that ticker, all transaction codes, one row per
EXECUTION LOT. Live on 2026-09-05, /company/AAPL served:

    headline   261 open-market insider trades by 31 insiders
    roster     Timothy D. Cook   547 trades   $3,230,021,710
    roster sum 3,084 trades

A 12x contradiction on one screen — the exact defect that change claimed to
have resolved, one component below the fix. Cook's real discretionary filing
count is 18.

AND pit_grade WAS BEING PUBLISHED for every roster insider, 31 occurrences on
AAPL. api/ratings.py is explicit that pit_grade may never render as a
user-facing rating: it is an internal point-in-time score, not a published
scale, and it inverts relative to career_grade. The insider page had this
removed on 2026-09-03; the company roster kept doing it.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SRC = (REPO / "api" / "routers" / "companies.py").read_text(encoding="utf-8")


def _roster_query() -> str:
    """The SELECT that builds overview.insiders."""
    i = SRC.index("FROM insider_companies ic")
    return SRC[SRC.rindex("SELECT", 0, i):SRC.index(").fetchall()", i)]


def test_the_roster_counts_filings_not_lots():
    q = _roster_query()
    assert "ic.trade_count" not in q, (
        "the roster is back to insider_companies.trade_count, which counts "
        "execution lots across every transaction code. AAPL summed to 3,084 "
        "under a headline of 261."
    )
    assert "count(DISTINCT COALESCE(t.filing_key, t.accession))" in q, (
        "the roster no longer counts distinct filings"
    )


def test_the_roster_uses_the_same_population_as_the_headline():
    """Both must exclude non-discretionary, duplicate, superseded, derivative
    and value-suspect rows, or the two numbers diverge again."""
    q = _roster_query()
    for guard in ("signal_class IN", "superseded_by IS NULL",
                  "is_duplicate", "is_derivative = 0", "value_suspect"):
        assert guard in q, f"the roster no longer filters on {guard!r}"


def test_the_roster_does_not_publish_pit_grade():
    assert 'ins["pit_grade"] =' not in SRC, (
        "pit_grade is being attached to roster insiders again. It is not a "
        "published scale — see api/ratings.py."
    )
    assert 'ins.pop("pit_grade", None)' in SRC, (
        "the defensive pop is gone, so a pit_grade arriving from any future "
        "query would be served"
    )


def test_the_meaningful_classes_are_not_typed_here():
    """The router builds its IN-list from MEANINGFUL_CLASSES via _meaningful().
    A hand-typed list is the drift that constant exists to prevent."""
    assert "'discretionary_buy', 'discretionary_sell'" not in SRC, (
        "companies.py types the class names again"
    )
    assert "{MEANINGFUL_IN}" in _roster_query(), (
        "the roster's class filter is not going through _meaningful()"
    )

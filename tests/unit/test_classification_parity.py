"""api/classification.py and frontend/src/lib/classification.ts must agree.

Same reasoning as test_ratings_parity: the rule has to live in two languages,
and two copies of a rule is how five definitions of "routine" came to exist in
the first place. Parses the TypeScript with regexes rather than running node,
so the suite stays runnable on Studio's host Python.

The mapping table is the part that matters. If Python routes `planned_sell` to
"Scheduled" and TypeScript routes it to null, 23,885 filings render one way
from the API and another in the component — which is exactly the class of bug
this module was written to end.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from api.classification import (
    DISCRETIONARY_CLASSES,
    FILING_KINDS,
    KIND_META,
    _KIND_OF,
    filing_kind,
    is_discretionary,
    is_recurring_pattern,
    is_scheduled,
)

REPO = Path(__file__).resolve().parents[2]
TS = REPO / "frontend/src/lib/classification.ts"


@pytest.fixture(scope="module")
def ts() -> str:
    assert TS.exists(), "frontend/src/lib/classification.ts is missing"
    return TS.read_text()


def _string_array(src: str, name: str) -> list[str]:
    m = re.search(rf"export const {name} = \[(.*?)\] as const;", src, re.S)
    assert m, f"could not find {name} in the TypeScript"
    return re.findall(r'"([^"]+)"', m.group(1))


def _kind_of(src: str) -> dict[str, str | None]:
    m = re.search(r"const KIND_OF: Record<string, FilingKind \| null> = \{(.*?)\n\};",
                  src, re.S)
    assert m, "could not find KIND_OF in the TypeScript"
    out: dict[str, str | None] = {}
    for key, val in re.findall(r"(\w+):\s*(null|\"[^\"]+\")", m.group(1)):
        out[key] = None if val == "null" else val.strip('"')
    return out


# ── parity ────────────────────────────────────────────────────────────────

def test_published_vocabulary_matches(ts):
    assert _string_array(ts, "FILING_KINDS") == list(FILING_KINDS)


def test_discretionary_classes_match(ts):
    assert _string_array(ts, "DISCRETIONARY_CLASSES") == list(DISCRETIONARY_CLASSES)


def test_every_signal_class_maps_the_same_way(ts):
    """The table that decides what 126,521 filings a quarter get labelled."""
    assert _kind_of(ts) == _KIND_OF


def test_blurbs_match(ts):
    for kind, meta in KIND_META.items():
        m = re.search(rf'{kind}:\s*\{{[^}}]*blurb:\s*\n?\s*"([^"]+)"', ts, re.S)
        assert m, f"no blurb for {kind} in the TypeScript"
        assert m.group(1) == meta["blurb"], kind


def test_signal_flags_match(ts):
    """Only Discretionary is a signal; the rest are things that happened to
    the insider rather than decisions they made."""
    for kind, meta in KIND_META.items():
        m = re.search(rf"{kind}:\s*\{{[^}}]*signal:\s*(true|false)", ts, re.S)
        assert m, f"no signal flag for {kind}"
        assert (m.group(1) == "true") == meta["signal"], kind


# ── behaviour ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "signal_class,expected",
    [
        ("discretionary_buy", "Discretionary"),
        ("discretionary_sell", "Discretionary"),
        ("planned_sell", "Scheduled"),
        ("planned_buy", "Scheduled"),
        ("compensation", "Compensation"),
        ("tax_withholding", "Tax"),
        ("option_exercise", "Exercise"),
        ("gift", None),
        ("derivative", None),
        ("inconsistent", None),
        (None, None),
        ("", None),
        ("  Planned_Sell  ", "Scheduled"),   # tolerant of whitespace and case
        ("something_new", None),             # an unmapped class claims nothing
    ],
)
def test_filing_kind(signal_class, expected):
    assert filing_kind(signal_class) == expected


def test_the_headline_case_10b5_1_sells_are_scheduled():
    """23,885 planned sells, all of them 10b5-1, previously rendered as
    "SELL · Routine" in the feed and with no label at all on /explore."""
    assert filing_kind("planned_sell") == "Scheduled"
    assert is_scheduled("planned_sell")
    assert not is_discretionary("planned_sell")


def test_discretionary_sells_are_not_scheduled():
    """0 of 33,566 carry is_10b5_1 — the trigger routes 10b5-1 sales away
    from this class, which is why deriving from signal_class is sufficient."""
    assert is_discretionary("discretionary_sell")
    assert not is_scheduled("discretionary_sell")


def test_recurrence_is_a_tag_not_a_kind():
    """cohen_routine cuts across signal_class — 5,831 discretionary sells
    carry it. Folding it into the kind would make one filing two kinds."""
    item = {"signal_class": "discretionary_sell", "cohen_routine": 1}
    assert filing_kind(item["signal_class"]) == "Discretionary"
    assert is_discretionary(item["signal_class"])
    assert is_recurring_pattern(item)


@pytest.mark.parametrize(
    "item,expected",
    [
        ({"cohen_routine": 1}, True),
        ({"is_recurring": 1}, True),
        ({"cohen_routine": 0, "is_recurring": 0}, False),
        ({"cohen_routine": None, "is_recurring": None}, False),
        ({}, False),
    ],
)
def test_is_recurring_pattern(item, expected):
    assert is_recurring_pattern(item) is expected


def test_every_kind_has_metadata():
    assert set(KIND_META) == set(FILING_KINDS)


def test_every_mapped_kind_is_published():
    for kind in _KIND_OF.values():
        assert kind is None or kind in FILING_KINDS, kind

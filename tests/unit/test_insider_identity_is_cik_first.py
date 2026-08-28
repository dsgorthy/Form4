"""One CIK is one person, whatever the zero-padding.

WHAT WENT WRONG

`get_or_create_insider` matched on

    name_normalized = ? AND (cik = ? OR cik IS NULL OR ? IS NULL)

so it required the NAME to agree and compared the CIK as a raw string. EDGAR
renders a CIK both zero-padded to ten digits ("0002014440") and bare
("2014440") depending on the endpoint, and those two are not string-equal --
so the same filer arriving down the historical path and the live path was
minted TWICE.

Woodrow D. Anderson became insider_id 128655 (cik 0002014440) and 213762 (cik
2014440). On 2026-08-27 the Stocktwits generator produced:

    $INDV -- Chief Accounting Officer Anderson Woodrow D bought $54K
    - Chief Accounting Officer Woodrow D. Anderson bought $54K alongside them.
    - 2 insiders have bought $108K here in the last 23 days.

He bought alongside himself, and one $54,420 purchase became $108K from two
people. Across the table: 1,557 CIKs split over 4,007 insider rows carrying
244,300 trades, of which only 1.1% were resolved by effective_insider_id.

It is not cosmetic. A split identity halves an insider's filing history, so
career_grade -- the primary gate on all three published books -- is computed
on partial evidence and can fall below MIN_SCORED_FILINGS entirely.

THE PROPERTIES

  1. CIK matches on its NORMALIZED form, so padding cannot split a person.
  2. CIK is checked FIRST. It is the SEC's identifier and the only stable one;
     names change and titles change.
  3. The name fallback only matches rows with NO cik. Otherwise a namesake who
     has a known, different CIK would absorb an unrelated filer -- the
     opposite failure, and the one that lives in backfill_from_sec_datasets.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "strategies" / "insider_catalog" / "backfill.py"


def _normalize_cik():
    """Load just the function, without importing a module that wants a DB."""
    import ast
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "normalize_cik":
            ns: dict = {}
            exec(compile(ast.Module([node], []), "<n>", "exec"), ns)
            return ns["normalize_cik"]
    raise AssertionError("normalize_cik is gone from backfill.py")


@pytest.mark.parametrize("a,b", [
    ("0002014440", "2014440"),     # the exact INDV case
    ("0000320193", "320193"),      # Apple
    ("0000000123", "123"),
    ("00001", "1"),
])
def test_padded_and_bare_are_the_same_filer(a, b):
    n = _normalize_cik()
    assert n(a) == n(b), (
        f"{a!r} and {b!r} normalize differently, so the same person would be "
        "minted twice again"
    )


def test_distinct_ciks_stay_distinct():
    """Normalization must not collapse genuinely different filers."""
    n = _normalize_cik()
    assert n("0002014440") != n("0002014441")
    assert n("123") != n("1230")


@pytest.mark.parametrize("val,expected", [
    (None, None),
    ("", None),
    ("   ", None),
    ("0000000000", "0"),      # all zeros must not become the empty string
])
def test_edge_values(val, expected):
    assert _normalize_cik()(val) == expected


def test_non_numeric_cik_is_passed_through_not_stripped():
    """Never lstrip a value that is not a number -- that would corrupt it."""
    n = _normalize_cik()
    assert n("0ABC") == "0ABC"


def test_lookup_matches_cik_first_and_normalized():
    src = SRC.read_text(encoding="utf-8")
    fn = src[src.index("def get_or_create_insider"):]
    fn = fn[:fn.index("\ndef ", 1)] if "\ndef " in fn[1:] else fn

    assert "ltrim(cik" in fn, (
        "the CIK lookup no longer normalizes padding; 0002014440 and 2014440 "
        "will split one filer into two again"
    )
    cik_at = fn.index("ltrim(cik")
    name_at = fn.index("name_normalized = ?")
    assert cik_at < name_at, (
        "CIK must be matched BEFORE name. It is the authoritative identifier; "
        "names change and titles change."
    )


def test_name_fallback_cannot_absorb_a_filer_with_a_different_cik():
    src = SRC.read_text(encoding="utf-8")
    fn = src[src.index("def get_or_create_insider"):]
    fn = fn[:fn.index("\ndef ", 1)] if "\ndef " in fn[1:] else fn
    tail = fn[fn.index("name_normalized = ?"):]
    assert "cik IS NULL" in tail, (
        "the name fallback must only match rows with no CIK, or a namesake "
        "with a known different CIK absorbs an unrelated person"
    )

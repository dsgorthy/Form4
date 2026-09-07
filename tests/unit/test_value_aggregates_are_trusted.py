"""Every published dollar figure excludes prices we do not believe.

WHAT WENT WRONG (2026-09-06)

`price_quality = 'implausible'` was added to Silver and then wired into the
serving layer "next to every existing value_suspect guard". That pairing rule
silently skipped every query that had NO guard to sit next to — which included
the company page's own headline. IHT served a total of $7,207,876,940 for a
hotel REIT trading at $1.50, directly above a roster that summed to
$2,325,931. One page, two numbers, both computed from the same rows.

The first version of this test could not have caught it: it looked only at
routers that already filtered `value_suspect`, and SKIPPED the rest. A guard
that only inspects the places already guarded is not a guard.

THE RULE, AND WHY IT IS A FILTER AND NOT A WHERE CLAUSE

A bad price is not a bad filing. The flag lands on 28.9% of IHT's rows and 50%
of AMMA's, so moving the rule into WHERE would erase a quarter of a company's
real filing history in order to fix its dollar total. The insider did file and
did trade; only the price they typed is unusable. So:

    value aggregates   FILTERed
    counts and dates   untouched

which is why `SUM(...) FILTER (WHERE ...)` is the required shape and a
WHERE-level guard on a query that also COUNTs is a defect.
"""
from __future__ import annotations

import ast
import pathlib
import re

import pytest

from api.filters import trusted_value_filter

ROUTERS = sorted(pathlib.Path("api/routers").glob("*.py"))
IDS = [p.name for p in ROUTERS]


def _sum_spans(src: str):
    """(start, end) of every SUM(...) with balanced parentheses."""
    for m in re.finditer(r"\bSUM\s*\(", src):
        i, depth = m.end(), 1
        while i < len(src) and depth:
            depth += (src[i] == "(") - (src[i] == ")")
            i += 1
        if depth == 0:
            yield m.start(), i


def _value_sums(src: str):
    for a, b in _sum_spans(src):
        if re.search(r"\bvalue\b", src[a:b]):
            yield a, b, src[a:b]


@pytest.mark.parametrize("path", ROUTERS, ids=IDS)
def test_every_value_aggregate_is_filtered(path: pathlib.Path):
    """No SKIP branch. A router with no value aggregate simply has none."""
    src = path.read_text(encoding="utf-8")
    unfiltered = [
        re.sub(r"\s+", " ", body)[:60]
        for _, b, body in _value_sums(src)
        if not src[b:b + 80].lstrip().upper().startswith("FILTER")
    ]
    assert not unfiltered, (
        f"{path.name} sums `value` without FILTER:\n  "
        + "\n  ".join(unfiltered)
        + "\n\nUse api.filters.trusted_value_filter(alias). A dollar figure "
          "that includes a filer's typo is not a dollar figure."
    )


@pytest.mark.parametrize("path", ROUTERS, ids=IDS)
def test_the_filter_text_is_never_hand_typed(path: pathlib.Path):
    """One definition. A hand-edited copy drifts the day a third flag lands."""
    src = path.read_text(encoding="utf-8")
    for alias in ("t", ""):
        canonical = trusted_value_filter(alias)
        stray = re.findall(
            rf"FILTER \(WHERE NOT COALESCE\({alias + '.' if alias else ''}"
            rf"value_suspect[^)]*\)[^)]*\)", src)
        for found in stray:
            assert found == canonical, (
                f"{path.name} carries a FILTER that is not byte-identical to "
                f"trusted_value_filter({alias!r}):\n  got      {found}\n"
                f"  expected {canonical}"
            )


@pytest.mark.parametrize("path", ROUTERS, ids=IDS)
def test_descending_rankings_put_unknowns_last(path: pathlib.Path):
    """A group whose every lot is implausible sums to NULL, and Postgres sorts
    NULL FIRST under DESC — the rows we trust least would lead the ranking."""
    src = path.read_text(encoding="utf-8")
    for m in re.finditer(r"SUM\([^)]*value[^)]*\)\s*FILTER \(WHERE[^)]*\)[^)]*\)\s*DESC(\s*NULLS\s+LAST)?", src):
        assert m.group(1), (
            f"{path.name}: `{re.sub(r'FILTER .*', 'FILTER(...)', m.group(0))}` "
            "needs NULLS LAST. Without it an all-implausible group ranks first."
        )


@pytest.mark.parametrize("path", ROUTERS, ids=IDS)
def test_a_bad_price_never_deletes_a_real_filing(path: pathlib.Path):
    """The guard must not sit in a WHERE that also feeds a COUNT.

    This is the regression that would silently drop 28.9% of IHT's filings.
    """
    src = path.read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(src)):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        sql = node.value
        if "price_quality IS DISTINCT FROM" not in sql:
            continue
        # Strip FILTER (WHERE ...) FIRST — it contains the word WHERE, so a
        # naive split treats the filter itself as a WHERE clause and every
        # correctly-written query looks like a violation.
        bare = re.sub(r"FILTER \(WHERE[^)]*\)[^)]*\)", "", sql)
        where = re.split(r"\bWHERE\b", bare, flags=re.I)[1:]
        in_where = any("price_quality IS DISTINCT FROM" in seg for seg in where)
        counts = re.search(r"\bCOUNT\s*\(", sql, re.I)
        assert not (in_where and counts), (
            f"{path.name}:{node.lineno} filters price_quality in WHERE while "
            "also COUNTing. That removes real filings to fix a dollar total. "
            "Move the rule into FILTER on the value aggregate."
        )

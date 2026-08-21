"""One trade, one grade — whichever endpoint you ask.

WHAT WENT WRONG

compute_trade_grade() reads its inputs off a dict with item.get(), so a field
the query forgot to select is not an error. It is silently None, the factor
never fires, and the trade scores lower — but only on that endpoint.

MED trade xm7gfj on 2026-08-21 scored 58 in /filings and 59 in /filings/{id}.
Four scoring inputs were missing from the list queries and present in the
detail one: is_largest_ever, dip_1mo, dip_3mo and the cluster count. Together
those are worth up to 13 points (Dip up to 10, Largest Trade 3, Cluster up to
12) against rating bands that are 10 points wide.
So the feed and the filing page could publish two different ratings for the
same filing — the exact contradiction api/ratings.py exists to prevent.

WHY A STATIC TEST

The failure is a missing SELECT column, which no amount of exercising the
scorer will catch: you have to compare what the scorer READS against what each
query PROVIDES. This parses both and diffs them, so a new factor added to
trade_grade.py fails the build until every query feeding it is updated too.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TRADE_GRADE = REPO / "api" / "trade_grade.py"
FILINGS = REPO / "api" / "routers" / "filings.py"

#: Inputs the scorer reads that are NOT expected to come from the trades query.
#: Keep this list short and justified — it is the exemption, not the rule.
_NOT_FROM_QUERY: set[str] = {
    # Computed per query as the AS-OF cluster count (everyone on the ticker
    # that day). Distinct from cluster_size_pit, which is the point-in-time
    # count the scorer actually uses — see the note in trade_grade.py.
    "n_filers",
}


def _scorer_inputs() -> set[str]:
    """Every column name compute_trade_grade pulls off its input dict.

    Comment lines are skipped: the module documents the fields it USED to read
    and why it stopped, and a test that treats prose as code would demand the
    queries keep selecting a retired column.
    """
    code = "\n".join(
        line for line in TRADE_GRADE.read_text().splitlines()
        if not line.lstrip().startswith("#")
    )
    found = set(re.findall(r"""item\.get\(\s*["']([a-z0-9_]+)["']""", code))
    assert found, "parsed no inputs from trade_grade.py — did item.get() change shape?"
    return found - _NOT_FROM_QUERY


def _query_blocks() -> dict[str, str]:
    """The SQL that feeds each endpoint that grades a filing.

    Split on the endpoint decorators so a column present in one block does not
    excuse its absence from another — which is precisely how this bug hid.
    """
    src = FILINGS.read_text()
    parts = re.split(r"\n@router\.get\(", src)
    blocks = {}
    for part in parts[1:]:
        name = part.split("\n", 1)[0].strip().strip('"\'(),')
        if "SELECT" in part and "trade_grade" not in name:
            blocks[name or "unnamed"] = part
    assert blocks, "found no router blocks with SQL in filings.py"
    return blocks


def _grading_blocks() -> dict[str, str]:
    """Only the blocks that actually attach a grade to their rows."""
    return {
        name: sql for name, sql in _query_blocks().items()
        if "trade_grade" in sql or "attach_ratings" in sql
    }


def test_the_scorer_has_inputs_to_parse():
    got = _scorer_inputs()
    assert "is_largest_ever" in got and "dip_3mo" in got, sorted(got)


@pytest.mark.parametrize("field", sorted(_scorer_inputs()))
def test_every_scoring_input_is_selected_by_every_grading_query(field):
    """A field the scorer reads must be available wherever a grade is computed.

    item.get() turns a missing column into None rather than an error, so this
    is the only place the omission surfaces.
    """
    missing = [
        name for name, sql in _grading_blocks().items()
        if not re.search(rf"\b{re.escape(field)}\b", sql)
    ]
    assert not missing, (
        f"compute_trade_grade reads {field!r}, but these grading endpoints do "
        f"not select it: {missing}. item.get() will return None, the factor "
        f"will not fire, and the same trade will score differently there than "
        f"on the endpoints that do select it."
    )


def test_the_four_fields_that_caused_this_are_present():
    """Explicit regression guard, independent of the parsing above."""
    sql = FILINGS.read_text()
    for field in ("is_largest_ever", "dip_1mo", "dip_3mo", "cluster_size_pit"):
        # Once in each list aggregate plus the outer select, or the detail
        # query. Two occurrences is the minimum that means "more than the
        # detail endpoint alone".
        assert sql.count(field) >= 2, (
            f"{field} appears {sql.count(field)}x in filings.py. It was added "
            "to the list queries on 2026-08-21 because having it only on the "
            "detail endpoint made the feed and the filing page disagree."
        )

"""api/ratings.py and frontend/src/lib/ratings.ts must agree, value for value.

The taxonomy has to live in two languages: the API stamps ratings onto the
payload, and the frontend renders and colours them. Two copies of a rule is
how the last set of scales rotted — trade_grade.py said 2 stars was "Weak"
while the methodology page said "Below Average", and the page claimed +4.78%
for the top band where the code's own docstring said +3.0%.

So the copies are compared here rather than trusted. This parses the
TypeScript with regexes instead of running node: the suite must stay runnable
on Studio's host Python, and the shapes involved are small literal tables.
"""
import ast
import re
from pathlib import Path

import pytest

from api.ratings import (
    INSIDER_RATINGS,
    INSIDER_RATING_META,
    PUBLISHED_TAG_KINDS,
    TAG_KINDS,
    TRADE_RATINGS,
    TRADE_RATING_BANDS,
    TRADE_RATING_META,
)

REPO = Path(__file__).resolve().parents[2]
TS = REPO / "frontend/src/lib/ratings.ts"


@pytest.fixture(scope="module")
def ts() -> str:
    assert TS.exists(), "frontend/src/lib/ratings.ts is missing"
    return TS.read_text()


def _string_array(src: str, name: str) -> list[str]:
    m = re.search(rf"export const {name} = \[(.*?)\] as const;", src, re.S)
    assert m, f"{name} not found in ratings.ts"
    return re.findall(r'"([^"]+)"', m.group(1))


def _record(src: str, name: str) -> dict[str, str]:
    m = re.search(rf"const {name}: Record<[^>]+> = \{{(.*?)\n\}};", src, re.S)
    if not m:
        m = re.search(rf"export const {name}: Record<[^>]+> = \{{(.*?)\n\}};", src, re.S)
    assert m, f"{name} not found in ratings.ts"
    return dict(re.findall(r'"?([A-Za-z_+][A-Za-z0-9_+]*)"?:\s*"([^"]+)"', m.group(1)))


def test_insider_scale_matches(ts):
    assert _string_array(ts, "INSIDER_RATINGS") == list(INSIDER_RATINGS)


def test_trade_scale_matches(ts):
    assert _string_array(ts, "TRADE_RATINGS") == list(TRADE_RATINGS)


def test_grade_display_mapping_matches(ts):
    """Including D -> C. If one side merges and the other does not, the same
    insider reads C in a table and D on their profile."""
    from api.ratings import _GRADE_DISPLAY
    assert _record(ts, "GRADE_DISPLAY") == _GRADE_DISPLAY


def test_trade_band_thresholds_match(ts):
    m = re.search(r"const TRADE_BANDS:.*?\[(.*?)\n\];", ts, re.S)
    assert m, "TRADE_BANDS not found"
    pairs = [(int(lo), name) for lo, name in
             re.findall(r"\[(\d+),\s*\"([^\"]+)\"\]", m.group(1))]
    assert pairs == [(lo, name) for lo, name in TRADE_RATING_BANDS]


def test_segment_counts_match(ts):
    m = re.search(r"export const TRADE_RATING_SEGMENTS: Record<[^>]+> = \{(.*?)\};",
                  ts, re.S)
    assert m, "TRADE_RATING_SEGMENTS not found"
    got = {k: int(v) for k, v in re.findall(r"(\w+):\s*(\d+)", m.group(1))}
    want = {name: TRADE_RATING_META[name]["segments"] for name in TRADE_RATINGS}
    assert got == want


def test_tag_kinds_match(ts):
    m = re.search(r"const TAG_KINDS: Record<string, TagKind> = \{(.*?)\n\};", ts, re.S)
    assert m, "TAG_KINDS not found"
    got = dict(re.findall(r"(\w+):\s*\"(\w+)\"", m.group(1)))
    assert got == TAG_KINDS, (
        "tag vocabulary differs; a tag classed verdict on one side and pattern "
        "on the other is shown on some surfaces and hidden on others"
    )


def test_published_kinds_match(ts):
    m = re.search(r"export const PUBLISHED_TAG_KINDS: readonly TagKind\[\] = \[(.*?)\];", ts)
    assert m, "PUBLISHED_TAG_KINDS not found"
    assert re.findall(r'"(\w+)"', m.group(1)) == list(PUBLISHED_TAG_KINDS)


def test_blurbs_match(ts):
    """The words a reader sees. Different copy on two surfaces for the same
    rating is the confusion this whole change is removing."""
    for const, meta, keys in (
        ("INSIDER_RATING_BLURB", INSIDER_RATING_META, INSIDER_RATINGS),
        ("TRADE_RATING_BLURB", TRADE_RATING_META, TRADE_RATINGS),
    ):
        m = re.search(rf"export const {const}: Record<[^>]+> = \{{(.*?)\n\}};", ts, re.S)
        assert m, f"{const} not found"
        body = m.group(1)
        for key in keys:
            want = meta[key]["blurb"]
            # Normalise whitespace: the TS copy wraps at a different width.
            norm = " ".join(want.split())
            found = " ".join(body.split())
            assert norm in found, f"{const}[{key}] differs from Python:\n  {want}"


def test_no_extra_ratings_in_typescript(ts):
    """A rating that exists only in the frontend is a rating with no data
    behind it."""
    for name in _string_array(ts, "INSIDER_RATINGS"):
        assert name in INSIDER_RATINGS
    for name in _string_array(ts, "TRADE_RATINGS"):
        assert name in TRADE_RATINGS


def test_python_module_parses_as_the_source_of_truth():
    """Cheap guard that ratings.py stays importable without the app."""
    tree = ast.parse((REPO / "api/ratings.py").read_text())
    imported = {
        n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module
    }
    assert imported <= {"__future__", "typing"}, f"ratings.py imports {imported}"


# ── the docs must name the bands the code actually has ─────────────────────


def test_claude_md_names_the_real_bands():
    """CLAUDE.md called the 50-59 band "Routine" for three days after it was
    renamed to "Modest". That is not a cosmetic error: `is_routine`,
    `cohen_routine` and the "SELL · Routine" chip already use that word for a
    different axis, the two disagreed on the same filing, and the rename on
    2026-08-21 exists specifically to end the collision. A stale name in the
    file every session reads first puts it straight back.
    """
    import re
    from pathlib import Path

    from api.ratings import TRADE_RATINGS

    doc = (Path(__file__).resolve().parents[2] / "CLAUDE.md").read_text()
    m = re.search(r"Trade Rating \(([^,]+),", doc)
    assert m, "CLAUDE.md no longer describes the Trade Rating bands"
    named = [x.strip("* ") for x in m.group(1).split("/")]
    assert named == list(TRADE_RATINGS), (
        f"CLAUDE.md names the bands {named}; the code has {list(TRADE_RATINGS)}"
    )
    assert "Routine" not in named, (
        "'Routine' is back as a rating band name. It already means "
        "'this insider does this on a schedule' elsewhere in the product."
    )

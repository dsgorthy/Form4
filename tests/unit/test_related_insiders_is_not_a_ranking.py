"""The related-insiders list is similarity, and must never become a ranking.

WHY THIS FILE EXISTS

The neighbour list is built from the same behavioural clustering as
scripts/insider_archetypes.py, and that clustering was tested against forward
returns and FAILED: observed between-archetype spread 2.33pp against a
permutation null whose median was 1.75pp, p=0.208.

So the list groups like with like, which is all it claims. The failure mode is
not that it breaks -- it is that someone later adds a grade to the card, or
sorts by score, or renames the heading to "Top Related", and the product starts
making a claim three experiments this month could not support. That change
would look harmless in review. These tests make it fail the build instead.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROUTER = (REPO / "api" / "routers" / "insiders.py").read_text(encoding="utf-8")
COMPONENT = (REPO / "frontend" / "src" / "components" / "related-insiders.tsx").read_text(encoding="utf-8")

# The comment block in that file NAMES the words it is avoiding ("not 'Best' or
# 'Recommended'"), so a naive substring search flags the documentation for the
# rule as a violation of it. Strip comments and test what actually renders.
RENDERED = re.sub(r"/\*.*?\*/", "", COMPONENT, flags=re.S)
RENDERED = re.sub(r"^\s*//.*$", "", RENDERED, flags=re.M)
SCRIPT = (REPO / "scripts" / "insider_similarity.py").read_text(encoding="utf-8")


def _endpoint() -> str:
    i = ROUTER.index("def get_related_insiders(")
    j = ROUTER.index("\n@router.", i)
    return ROUTER[i:j]


# ── the payload carries no quality signal ──────────────────────────────────

@pytest.mark.parametrize("banned", [
    "career_grade", "best_career_grade", "pit_grade", "score_tier",
    "percentile", "conviction",
])
def test_no_rating_field_reaches_the_card(banned: str):
    """A rating beside the word 'related' reads as a ranking."""
    body = _endpoint()
    assert banned not in body, (
        f"get_related_insiders now returns {banned!r}. A grade on a similarity "
        "card tells the reader these people are BETTER, which is exactly what "
        "the clustering underneath does not support (p=0.208). The grade lives "
        "on the insider's own page, where it has its context."
    )
    assert banned not in COMPONENT, f"the card renders {banned!r}"


def test_the_raw_score_is_stripped_before_the_response():
    """The blend and its components are for auditing, not for a card. Shipping
    `score` invites a UI that sorts or displays it."""
    body = _endpoint()
    m = re.search(r'for k in \(([^)]*)\)', body, re.S)
    assert m, "the strip-list is gone from get_related_insiders"
    stripped = m.group(1)
    for field in ("score", "co_investment", "sector_overlap", "profile_sim"):
        assert f'"{field}"' in stripped, f"{field} is no longer stripped"


# ── the copy does not promise quality ──────────────────────────────────────

@pytest.mark.parametrize("word", ["Top ", "Best ", "Recommended", "Strongest"])
def test_the_heading_makes_no_quality_claim(word: str):
    assert word not in RENDERED, (
        f"the related-insiders card says {word!r}. The list is ordered by "
        "similarity, so superlative language misdescribes it."
    )


def test_the_disclaimer_is_present():
    """One plain sentence, because 'this is a leaderboard' is the single most
    likely misreading of a list of people on a page about a person."""
    assert "not a ranking" in COMPONENT, (
        "the visible disclaimer is gone; a reader has nothing telling them "
        "this is similarity rather than a leaderboard"
    )


# ── the two relations stay distinguishable ─────────────────────────────────

def test_both_reasons_exist_and_are_the_only_ones():
    body = _endpoint()
    assert '"co_investment"' in body and '"similar_profile"' in body
    assert 'r.reason === "co_investment"' in COMPONENT, (
        "the card no longer distinguishes a shared-company match from a "
        "behavioural one. They are not equally strong and the reader is "
        "entitled to know which they are looking at."
    )


# ── the compute keeps the properties the list depends on ───────────────────

def test_the_behavioural_half_is_not_padded():
    """Padding to fill TOP_K is what put a bank director on a biotech page."""
    assert "NO FALLBACK" in SCRIPT, (
        "scripts/insider_similarity.py no longer documents the no-padding "
        "rule; check whether the fallback came back"
    )
    assert re.search(r"scored_pf = \[r for r in scored_pf if pa and", SCRIPT), (
        "the same-primary-sector filter on behavioural neighbours is gone"
    )


def test_same_name_pairs_are_dropped():
    """insiders still holds unconsolidated duplicates; without this the top
    'related insider' is frequently the same human being."""
    assert re.search(r"names\.get\(a\) == names\.get\(b\)", SCRIPT), (
        "duplicate-name pairs are no longer filtered"
    )


def test_the_insert_is_parameterised():
    """Tickers in `trades` carry stray quote characters from EDGAR parsing
    ('LTRX, ''NHPR'', \"AFNG\"). Interpolating them into VALUES both breaks and
    is an injection shape."""
    i = SCRIPT.index("INSERT INTO sim_new")
    stmt = SCRIPT[i - 400:i + 200]
    assert 'row_ph' in stmt or '?' in stmt, "the insert went back to interpolation"
    assert "def lit(" not in SCRIPT, "the hand-rolled SQL literal escaper is back"

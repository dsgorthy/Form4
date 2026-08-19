"""The rating taxonomy: two scales, one tag vocabulary, no fifth verdict.

Before 2026-08-18 a single filing reached the reader carrying pit_grade D,
career_grade C, a 0-100 score of 61, 3 stars and the word "Average" — five
verdicts on three scales, none of which said how it related to the others. A
portfolio row added conviction 1.5/10 beside "Grade A". The leaderboard added
legacy_score, score_tier and percentile, and offered to rank by any of them.

These tests pin the collapse of that into: one insider rating, one trade
rating, and tags that state facts instead of opinions.
"""
import re
from pathlib import Path

import pytest

from api.ratings import (
    INSIDER_RATINGS,
    INSIDER_RATING_META,
    INTERNAL_ONLY_FIELDS,
    PIT_VIOLATING_FIELDS,
    PUBLISHED_TAG_KINDS,
    RETIRED_SORT_KEYS,
    TAG_KINDS,
    TRADE_RATINGS,
    TRADE_RATING_BANDS,
    TRADE_RATING_META,
    UNRATED,
    attach_ratings,
    insider_rating,
    is_published_tag,
    tag_kind,
    trade_rating,
    trade_rating_segments,
    visible_tags,
)

REPO = Path(__file__).resolve().parents[2]


# ── Insider rating ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("stored,expected", [
    ("A+", "A+"), ("A", "A"), ("B", "B"), ("C", "C"),
    ("D", "C"),          # merged: D measured -0.18%, C -0.38% — they cross
    ("a+", "A+"),        # case and whitespace tolerated
    (" B ", "B"),
])
def test_stored_grade_maps_to_published_rating(stored, expected):
    assert insider_rating(stored) == expected


@pytest.mark.parametrize("absent", [None, "", "   ", "Z", "New"])
def test_absent_grade_is_unrated_not_a_failing_one(absent):
    """A null career_grade is how compute_career_grades records 'not enough
    history'. Those buys average +1.41% at 30d against -0.25% for a measured
    C, so rendering them as blank or as a low grade inverts the meaning."""
    assert insider_rating(absent) == UNRATED


def test_pit_grade_is_never_substituted_for_a_missing_career_grade():
    """The whole point of Unrated. Falling back to pit relabels an unrated
    insider as C or D and reintroduces the bug."""
    rows = [{"career_grade": None, "pit_grade": "D", "trade_grade": None}]
    attach_ratings(rows)
    assert rows[0]["insider_rating"] == UNRATED


def test_sufficient_data_false_forces_unrated():
    assert insider_rating("A", 0) == UNRATED
    assert insider_rating("A", False) == UNRATED
    assert insider_rating("A", 1) == "A"
    assert insider_rating("A", None) == "A"   # unknown flag: trust the letter


def test_insider_scale_is_ordered_and_documented():
    assert INSIDER_RATINGS == ("A+", "A", "B", "C", UNRATED)
    assert set(INSIDER_RATING_META) == set(INSIDER_RATINGS)
    for name, meta in INSIDER_RATING_META.items():
        assert meta["blurb"].strip(), f"{name} has no explanation"


def test_measured_grades_are_monotonic_on_the_evidence():
    """The reason career_grade is the source and D is merged into C. If someone
    edits these numbers, the ordering claim has to still hold."""
    graded = ["A+", "A", "B", "C"]
    means = [INSIDER_RATING_META[g]["mean_abnormal_30d"] for g in graded]
    wins = [INSIDER_RATING_META[g]["win_rate"] for g in graded]
    assert means == sorted(means, reverse=True), f"not monotonic: {means}"
    assert wins == sorted(wins, reverse=True), f"not monotonic: {wins}"


def test_unrated_is_not_the_worst_bucket():
    """Documents the finding, so nobody 'tidies' Unrated back into D."""
    assert (INSIDER_RATING_META[UNRATED]["mean_abnormal_30d"]
            > INSIDER_RATING_META["C"]["mean_abnormal_30d"])


# ── Trade rating ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("score,expected", [
    (100, "Exceptional"), (80, "Exceptional"), (79.9, "Strong"),
    (70, "Strong"), (69, "Notable"), (60, "Notable"),
    (59, "Routine"), (50, "Routine"), (49, "Weak"), (0, "Weak"),
])
def test_trade_bands(score, expected):
    assert trade_rating(score) == expected


def test_no_score_is_no_rating():
    assert trade_rating(None) is None
    assert trade_rating_segments(None) == 0


def test_bands_are_contiguous_and_descending():
    mins = [m for m, _ in TRADE_RATING_BANDS]
    assert mins == sorted(mins, reverse=True)
    assert mins[-1] == 0, "the bottom band must catch every score"
    assert len(set(mins)) == len(mins), "duplicate thresholds"


def test_trade_bands_separate_on_the_evidence():
    """The defect in the shipped 73/63/55/45 cut: the top band returned +1.51%
    against the second's +1.28% but had the LOWER win rate, so the best rating
    we could give carried no information the second-best did not."""
    means = [TRADE_RATING_META[b]["mean_abnormal_30d"] for b in TRADE_RATINGS]
    wins = [TRADE_RATING_META[b]["win_rate"] for b in TRADE_RATINGS]
    assert means == sorted(means, reverse=True), f"not monotonic: {means}"
    assert wins == sorted(wins, reverse=True), f"not monotonic: {wins}"


def test_segments_are_one_through_five():
    seg = [TRADE_RATING_META[b]["segments"] for b in TRADE_RATINGS]
    assert seg == [5, 4, 3, 2, 1]


def test_trade_grade_derives_its_bands_from_here():
    """trade_grade.py kept its own 73/63/55/45 copy with its own labels, which
    drifted from the methodology page. One definition now."""
    from api.trade_grade import STAR_LABELS, compute_trade_grade, score_to_stars
    assert STAR_LABELS[5] == "Exceptional"
    assert STAR_LABELS[3] == "Notable", "stale label — 3 was 'Average'"
    assert score_to_stars(80) == 5 and score_to_stars(79) == 4
    out = compute_trade_grade({"trade_type": "buy", "pit_grade": "A+", "title": "CFO"})
    assert out["rating"] == trade_rating(out["score"])
    assert out["label"] == out["rating"], "label must not be a second scale"


# ── Tags ────────────────────────────────────────────────────────────────────

def test_every_known_tag_has_a_kind():
    for name, kind in TAG_KINDS.items():
        assert kind in (*PUBLISHED_TAG_KINDS, "verdict"), f"{name}: {kind}"


def test_verdict_tags_are_not_published():
    """top_trade is on 495,478 trades and high_signal on 12,724. Both are an
    opinion about quality, which is what the Trade Rating is for — showing
    'Top Trade' beside a rating of Routine is a contradiction."""
    for name in ("top_trade", "high_signal", "insider_returns"):
        assert tag_kind(name) == "verdict"
        assert not is_published_tag(name)


def test_descriptive_tags_are_published():
    for name in ("buying_the_dip", "size_anomaly", "quality_momentum_buy"):
        assert is_published_tag(name)


def test_unknown_tags_default_to_visible():
    """A new descriptive tag is the common case; defaulting to verdict would
    silently hide it from every surface."""
    assert tag_kind("some_new_signal") == "pattern"
    assert is_published_tag("some_new_signal")


def test_visible_tags_filters_and_preserves_order():
    sig = [{"signal_type": "top_trade"}, {"signal_type": "buying_the_dip"},
           {"signal_type": "high_signal"}, {"signal_type": "size_anomaly"}]
    assert [s["signal_type"] for s in visible_tags(sig)] == [
        "buying_the_dip", "size_anomaly"]
    assert visible_tags(None) == []
    assert visible_tags([]) == []


def test_tags_are_one_to_many_and_ratings_are_one_to_one():
    """The shape claim itself: a trade carries exactly one of each rating and
    any number of tags."""
    row = {
        "career_grade": "B",
        "trade_grade": {"score": 72, "rating": "Strong"},
        "signals": [{"signal_type": "buying_the_dip"},
                    {"signal_type": "size_anomaly"},
                    {"signal_type": "top_trade"}],
        "signal_types": "buying_the_dip,size_anomaly,top_trade",
    }
    attach_ratings([row])
    assert isinstance(row["insider_rating"], str)
    assert isinstance(row["trade_rating"], str)
    assert len(row["signals"]) == 2
    assert row["signal_types"] == "buying_the_dip,size_anomaly"


# ── Things that are not ratings ─────────────────────────────────────────────

def test_leaderboard_offers_only_the_pit_correct_ranking():
    src = (REPO / "api/routers/leaderboard.py").read_text()
    block = src.split("SORT_COLUMNS = {", 1)[1].split("}", 1)[0]
    keys = re.findall(r'"([a-z_]+)":', block)
    assert keys == ["score"], f"leaderboard sorts on {keys}"
    for retired in RETIRED_SORT_KEYS:
        assert f'"{retired}":' not in block, f"{retired} ranks on a PIT violation"


def test_pit_violating_columns_never_reach_a_sort_column():
    src = (REPO / "api/routers/leaderboard.py").read_text()
    block = src.split("SORT_COLUMNS = {", 1)[1].split("}", 1)[0]
    for field in PIT_VIOLATING_FIELDS:
        assert f"itr.{field}" not in block, f"ranking by itr.{field}"


def test_conviction_is_declared_internal():
    assert "conviction" in INTERNAL_ONLY_FIELDS
    assert "signal_quality" in INTERNAL_ONLY_FIELDS


def test_ratings_module_has_no_heavy_imports():
    """Runs on Studio's host Python, same constraint as public_fields."""
    src = (REPO / "api/ratings.py").read_text()
    for banned in ("import fastapi", "from fastapi", "import psycopg2",
                   "from config.database"):
        assert banned not in src, f"api/ratings.py imports {banned}"

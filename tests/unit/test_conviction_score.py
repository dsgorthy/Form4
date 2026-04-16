"""Tests for conviction scoring — thesis routing, score ranges, edge cases.

These tests exist because a thesis name mismatch silently blocked all live
strategy entries for 10 days in April 2026.  The critical invariant: every
strategy_name in configs/*.yaml must be recognized by compute_conviction(),
and typical signals must exceed that config's min_conviction.
"""

import pytest
import yaml
from pathlib import Path

from pipelines.insider_study.conviction_score import (
    compute_conviction,
    _categorize_insider,
    pit_score_to_grade,
    VALID_THESES,
    REVERSAL_THESES,
    COMPOSITE_THESES,
)

CONFIGS_DIR = Path(__file__).resolve().parents[2] / "strategies" / "cw_strategies" / "configs"


# ---------------------------------------------------------------------------
# Thesis name coverage — every YAML config must be recognized
# ---------------------------------------------------------------------------

def _load_active_configs():
    """Load all active YAML configs and yield (path, config) pairs."""
    for p in sorted(CONFIGS_DIR.glob("*.yaml")):
        with open(p) as f:
            cfg = yaml.safe_load(f)
        if isinstance(cfg, dict) and "strategy_name" in cfg:
            yield p.name, cfg


@pytest.mark.parametrize("filename,cfg", list(_load_active_configs()), ids=lambda x: x if isinstance(x, str) else "")
def test_strategy_name_in_valid_theses(filename, cfg):
    """Every strategy_name used by cw_runner must be in VALID_THESES."""
    name = cfg["strategy_name"]
    assert name in VALID_THESES, (
        f"{filename}: strategy_name={name!r} is not in VALID_THESES. "
        f"Add it to conviction_score.py to prevent silent routing failures."
    )


def test_unknown_thesis_raises():
    """Unknown thesis names must raise ValueError, not silently fall through."""
    with pytest.raises(ValueError, match="Unknown thesis"):
        compute_conviction(thesis="nonexistent", insider_title="CEO")


# ---------------------------------------------------------------------------
# Routing correctness
# ---------------------------------------------------------------------------

def test_reversal_dip_uses_reversal_scorer():
    """reversal_dip must route to _score_reversal, which uses consecutive_sells."""
    score_with_sells = compute_conviction(
        thesis="reversal_dip", signal_grade="D",
        consecutive_sells=20, dip_3mo=-0.50, insider_title="CEO",
    )
    score_without_sells = compute_conviction(
        thesis="reversal_dip", signal_grade="D",
        consecutive_sells=0, dip_3mo=-0.50, insider_title="CEO",
    )
    assert score_with_sells > score_without_sells, (
        "reversal_dip should score higher with more consecutive sells (reversal path)"
    )


def test_quality_momentum_uses_composite_scorer():
    """quality_momentum must route to _score_composite, which uses role."""
    vp_score = compute_conviction(
        thesis="quality_momentum", signal_grade="A+",
        above_sma50=True, above_sma200=True, insider_title="Vice President",
    )
    other_score = compute_conviction(
        thesis="quality_momentum", signal_grade="A+",
        above_sma50=True, above_sma200=True, insider_title="Analyst",
    )
    assert vp_score > other_score, (
        "quality_momentum composite scorer should give VP higher score than 'other'"
    )


# ---------------------------------------------------------------------------
# Score reachability — typical signals must exceed min_conviction
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("filename,cfg", list(_load_active_configs()), ids=lambda x: x if isinstance(x, str) else "")
def test_typical_signal_exceeds_min_conviction(filename, cfg):
    """A typical good signal for each strategy must pass its min_conviction threshold."""
    name = cfg["strategy_name"]
    min_conv = cfg.get("min_conviction", 5.0)

    typical_signals = {
        "quality_momentum": dict(
            signal_grade="A+", above_sma50=True, above_sma200=True,
            insider_title="Vice President",
        ),
        "reversal_dip": dict(
            signal_grade="D", consecutive_sells=15, dip_3mo=-0.35,
            insider_title="CEO",
        ),
        "tenb51_surprise": dict(
            signal_grade="C", insider_title="Chief Financial Officer",
        ),
    }
    if name not in typical_signals:
        pytest.skip(f"No typical signal defined for {name}")

    score = compute_conviction(thesis=name, **typical_signals[name])
    assert score >= min_conv, (
        f"{name}: typical signal scores {score}, below min_conviction={min_conv}. "
        f"Either raise scoring weights or lower min_conviction."
    )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestCategorizeInsider:
    def test_dir_abbreviation(self):
        assert _categorize_insider("Dir") == "director"

    def test_dir_with_period(self):
        assert _categorize_insider("Dir.") == "director"

    def test_full_director(self):
        assert _categorize_insider("Director") == "director"

    def test_ceo(self):
        assert _categorize_insider("Chief Executive Officer") == "ceo"

    def test_cfo(self):
        assert _categorize_insider("Chief Financial Officer") == "cfo"

    def test_vp(self):
        assert _categorize_insider("Vice President") == "vp"

    def test_svp(self):
        assert _categorize_insider("SVP, Engineering") == "vp"

    def test_president_solo(self):
        assert _categorize_insider("President") == "president"

    def test_president_with_ceo(self):
        """President+CEO should be CEO, not president."""
        assert _categorize_insider("President and CEO") == "ceo"

    def test_10pct_owner(self):
        assert _categorize_insider("10% Owner") == "10pct_owner"

    def test_none_title(self):
        assert _categorize_insider(None) == "other"

    def test_empty_string(self):
        assert _categorize_insider("") == "other"


class TestGradeHandling:
    def test_a_plus_gets_bonus_in_composite(self):
        """A+ must get at least as much bonus as A in composite scoring."""
        a_plus = compute_conviction(thesis="quality_momentum", signal_grade="A+", insider_title="CEO")
        a_only = compute_conviction(thesis="quality_momentum", signal_grade="A", insider_title="CEO")
        assert a_plus >= a_only

    def test_a_plus_gets_bonus_in_reversal(self):
        """A+ should score higher than A in reversal scoring."""
        a_plus = compute_conviction(thesis="reversal_dip", signal_grade="A+", consecutive_sells=10, insider_title="CEO")
        a_only = compute_conviction(thesis="reversal_dip", signal_grade="A", consecutive_sells=10, insider_title="CEO")
        assert a_plus > a_only


class TestRoleFiltering:
    def test_10pct_owner_excluded(self):
        """10% owners must always score 0."""
        score = compute_conviction(thesis="quality_momentum", signal_grade="A+", insider_title="10% Owner")
        assert score == 0.0

    def test_president_penalized_not_excluded(self):
        """Presidents should be heavily penalized but not hard-excluded."""
        score = compute_conviction(
            thesis="reversal_dip", signal_grade="A+",
            consecutive_sells=50, dip_3mo=-0.60, insider_title="President",
        )
        assert score > 0.0, "President with exceptional signal should still score > 0"

    def test_president_weak_signal_blocked(self):
        """President with a weak signal should score 0 (clamped by max(0, ...))."""
        score = compute_conviction(
            thesis="quality_momentum", signal_grade="B", insider_title="President",
        )
        assert score == 0.0


class TestPitScoreToGrade:
    def test_a_plus(self):
        assert pit_score_to_grade(3.0) == "A+"

    def test_a(self):
        assert pit_score_to_grade(2.0) == "A"

    def test_b(self):
        assert pit_score_to_grade(1.5) == "B"

    def test_c(self):
        assert pit_score_to_grade(0.8) == "C"

    def test_d(self):
        assert pit_score_to_grade(0.1) == "D"

    def test_none(self):
        assert pit_score_to_grade(None) is None

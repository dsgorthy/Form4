"""Unit tests for framework.oms.decision."""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.oms.decision import Decision, STAGES


# ── Factory: reject ─────────────────────────────────────────────────────────


class TestDecisionReject:

    def test_reject_basic(self):
        d = Decision.reject(
            run_id="run-1",
            strategy="quality_momentum",
            strategy_version="abc:def",
            trade_id=42,
            ticker="AAPL",
            filing_date="2026-05-08",
            stage="pit_lookup",
            reason="pit_grade=C not in [A,A+]",
        )
        assert d.action == "reject"
        assert d.stage == "pit_lookup"
        assert d.reason == "pit_grade=C not in [A,A+]"
        assert d.ticker == "AAPL"
        assert d.trade_id == 42
        assert d.confidence is None
        assert d.feature_snapshot == {}
        assert d.decision_id  # uuid auto-generated

    def test_reject_requires_reason(self):
        # Going through __init__ directly with no reason should raise
        with pytest.raises(ValueError, match="reject Decision requires a reason"):
            Decision(
                decision_id="x", run_id="r", strategy="qm", strategy_version="v",
                trade_id=1, ticker="A", filing_date="2026-05-08",
                action="reject", stage="dedup", reason=None,
                confidence=None, pit_grade=None, conviction=None,
                feature_snapshot={},
            )

    def test_reject_passes_through_pit_grade_and_conviction(self):
        d = Decision.reject(
            run_id="run-1", strategy="rd", strategy_version="v",
            trade_id=1, ticker="X", filing_date="2026-05-08",
            stage="conviction", reason="conviction:3.2 < 5.0",
            pit_grade="B", conviction=3.2,
        )
        assert d.pit_grade == "B"
        assert d.conviction == 3.2


# ── Factory: enter ──────────────────────────────────────────────────────────


class TestDecisionEnter:

    def test_enter_basic(self):
        d = Decision.enter(
            run_id="run-1",
            strategy="quality_momentum",
            strategy_version="abc:def",
            trade_id=42,
            ticker="AAPL",
            filing_date="2026-05-08",
            confidence=0.83,
            feature_snapshot={"pit_grade": "A", "conviction": 7.2, "above_sma50": True},
            pit_grade="A",
            conviction=7.2,
        )
        assert d.action == "enter"
        assert d.stage == "final"
        assert d.confidence == 0.83
        assert d.feature_snapshot["pit_grade"] == "A"
        assert d.pit_grade == "A"


# ── Factory: exit ───────────────────────────────────────────────────────────


class TestDecisionExit:

    def test_exit_basic(self):
        d = Decision.exit(
            run_id="run-1",
            strategy="qm",
            strategy_version="v",
            trade_id=42,
            ticker="AAPL",
            reason="trailing_stop_hit:-15%",
        )
        assert d.action == "exit"
        assert d.stage == "exit"
        # exit isn't in STAGES; should warn (we test that elsewhere)


# ── Validation ──────────────────────────────────────────────────────────────


class TestDecisionValidation:

    def test_invalid_action_raises(self):
        with pytest.raises(ValueError, match="must be enter|reject|exit"):
            Decision(
                decision_id="x", run_id="r", strategy="qm", strategy_version="v",
                trade_id=1, ticker="A", filing_date=None,
                action="cancel",  # invalid
                stage="dedup", reason="test",
                confidence=None, pit_grade=None, conviction=None,
                feature_snapshot={},
            )

    def test_unknown_stage_warns(self):
        """Unknown stages produce a UserWarning so contributors add them."""
        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter("always")
            Decision(
                decision_id="x", run_id="r", strategy="qm", strategy_version="v",
                trade_id=1, ticker="A", filing_date="2026-05-08",
                action="reject",
                stage="bogus_unknown_stage",  # not in STAGES
                reason="testing", confidence=None, pit_grade=None,
                conviction=None, feature_snapshot={},
            )
            assert any("not in STAGES" in str(w.message) for w in ws)

    def test_known_stage_no_warning(self):
        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter("always")
            Decision.reject(
                run_id="r", strategy="qm", strategy_version="v",
                trade_id=1, ticker="A", filing_date="2026-05-08",
                stage="pit_lookup", reason="missing pit_grade",
            )
            assert not any("not in STAGES" in str(w.message) for w in ws)


# ── Frozen ──────────────────────────────────────────────────────────────────


class TestDecisionFrozen:

    def test_frozen_cannot_mutate(self):
        d = Decision.reject(
            run_id="r", strategy="qm", strategy_version="v",
            trade_id=1, ticker="A", filing_date="2026-05-08",
            stage="dedup", reason="duplicate",
        )
        with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
            d.action = "enter"


# ── STAGES list completeness ────────────────────────────────────────────────


class TestStages:

    def test_stages_unique(self):
        assert len(STAGES) == len(set(STAGES))

    def test_stages_are_lowercase_underscore(self):
        for s in STAGES:
            assert s.islower() or "_" in s
            assert " " not in s

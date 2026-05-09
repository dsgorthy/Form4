"""Unit tests for framework.oms.audit — DB writers (mock-based, no DB)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.oms.audit import write_decision, write_decisions, write_order
from framework.oms.decision import Decision
from framework.oms.order_manager import Order, OrderIntent


# ── Mock connection ─────────────────────────────────────────────────────────


class MockConn:
    """Minimal connection mock — captures execute() calls."""

    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()):
        self.calls.append((sql, params))
        return self

    def fetchone(self):
        return None

    def commit(self):
        pass


# ── Fixtures ────────────────────────────────────────────────────────────────


def _make_decision(action: str = "reject", stage: str = "pit_lookup") -> Decision:
    return Decision(
        decision_id="dec-1",
        run_id="run-1",
        strategy="quality_momentum",
        strategy_version="abc:def",
        trade_id=42,
        ticker="AAPL",
        filing_date="2026-05-08",
        action=action,
        stage=stage,
        reason="pit_grade=C not in [A,A+]" if action == "reject" else None,
        confidence=0.83 if action == "enter" else None,
        pit_grade="A" if action == "enter" else "C",
        conviction=7.2 if action == "enter" else 3.1,
        feature_snapshot={"above_sma50": True, "dip_3mo": -0.05},
        thesis="thesis_a",
    )


def _make_order(*, decision_id: str = "dec-1") -> Order:
    intent = OrderIntent(
        intent_id="int-1",
        decision_id=decision_id,
        strategy="quality_momentum",
        strategy_version="abc:def",
        ticker="AAPL",
        side="buy",
        qty=100.0,
        order_type="market",
        is_live=False,
        estimated_value_usd=15_000,
        pit_grade="A",
        conviction_score=7.2,
        decision_rationale="A-grade insider, 3mo dip",
    )
    return Order.from_intent(intent)


# ── write_decision ──────────────────────────────────────────────────────────


class TestWriteDecision:

    def test_inserts_into_trade_decision_audit(self):
        conn = MockConn()
        d = _make_decision()
        write_decision(conn, d)
        assert len(conn.calls) == 1
        sql, params = conn.calls[0]
        assert "INSERT INTO trade_decision_audit" in sql
        # 13 params per the schema
        assert len(params) == 13

    def test_passed_false_for_reject(self):
        conn = MockConn()
        write_decision(conn, _make_decision(action="reject"))
        sql, params = conn.calls[0]
        # passed is the 9th column (after ts, run_id, strategy, ticker,
        # trade_id, filing_date, thesis, stage)
        # → param index 8
        assert params[8] is False  # passed

    def test_passed_true_for_enter(self):
        conn = MockConn()
        write_decision(conn, _make_decision(action="enter", stage="final"))
        sql, params = conn.calls[0]
        assert params[8] is True

    def test_passed_true_for_exit(self):
        conn = MockConn()
        d = Decision.exit(
            run_id="r", strategy="qm", strategy_version="v",
            trade_id=1, ticker="AAPL", reason="trailing_stop",
        )
        write_decision(conn, d)
        sql, params = conn.calls[0]
        assert params[8] is True  # exit decisions are "passed"

    def test_feature_snapshot_serialized_as_json(self):
        conn = MockConn()
        d = _make_decision()
        write_decision(conn, d)
        sql, params = conn.calls[0]
        # feature_snapshot is the last param (index 12)
        feat_json = params[12]
        assert isinstance(feat_json, str)
        parsed = json.loads(feat_json)
        assert parsed["above_sma50"] is True
        assert parsed["dip_3mo"] == -0.05

    def test_empty_feature_snapshot_is_none(self):
        conn = MockConn()
        d = Decision.reject(
            run_id="r", strategy="qm", strategy_version="v",
            trade_id=1, ticker="A", filing_date="2026-05-08",
            stage="dedup", reason="duplicate",
            feature_snapshot=None,
        )
        write_decision(conn, d)
        sql, params = conn.calls[0]
        assert params[12] is None

    def test_pit_grade_and_conviction_passed_through(self):
        conn = MockConn()
        d = _make_decision(action="enter")
        write_decision(conn, d)
        sql, params = conn.calls[0]
        # pit_grade is column 11 (index 10), conviction is column 12 (index 11)
        assert params[10] == "A"
        assert params[11] == 7.2


# ── write_decisions (bulk) ──────────────────────────────────────────────────


class TestWriteDecisions:

    def test_bulk_writes_each_decision(self):
        conn = MockConn()
        ds = [_make_decision(stage="dedup"), _make_decision(stage="filter")]
        n = write_decisions(conn, ds)
        assert n == 2
        assert len(conn.calls) == 2

    def test_empty_list_is_zero(self):
        conn = MockConn()
        n = write_decisions(conn, [])
        assert n == 0
        assert conn.calls == []


# ── write_order ─────────────────────────────────────────────────────────────


class TestWriteOrder:

    def test_inserts_with_upsert(self):
        conn = MockConn()
        order = _make_order()
        write_order(conn, order, config_version_sha="git-abc", config_yaml_sha="yaml-123")
        assert len(conn.calls) == 1
        sql, params = conn.calls[0]
        assert "INSERT INTO order_audit" in sql
        assert "ON CONFLICT (order_id) DO UPDATE" in sql

    def test_initial_state_is_pending(self):
        conn = MockConn()
        order = _make_order()
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        sql, params = conn.calls[0]
        # fill_status is the 16th column — let's just verify "pending" is in params
        assert "pending" in params

    def test_state_transitions_persist(self):
        conn = MockConn()
        order = _make_order()
        order.mark_submitted("alpaca-99")
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        sql, params = conn.calls[0]
        assert "alpaca-99" in params
        assert "submitted" in params

    def test_filled_state(self):
        conn = MockConn()
        order = _make_order()
        order.mark_submitted("a-1")
        order.mark_accepted()
        order.mark_filled(fill_qty=100, avg_price=150.50)
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        sql, params = conn.calls[0]
        assert "filled" in params
        assert 150.50 in params
        assert 100.0 in params

    def test_rejected_with_reason(self):
        conn = MockConn()
        order = _make_order()
        order.mark_rejected("insufficient buying power")
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        sql, params = conn.calls[0]
        assert "rejected" in params
        assert "insufficient buying power" in params

    def test_signal_inputs_serialized_as_json(self):
        conn = MockConn()
        order = _make_order()
        write_order(
            conn, order,
            config_version_sha="g",
            config_yaml_sha="y",
            signal_inputs={"pit_grade": "A", "conviction": 7.2},
        )
        sql, params = conn.calls[0]
        # find the JSON string
        json_strs = [p for p in params if isinstance(p, str) and p.startswith("{")]
        assert len(json_strs) >= 1
        parsed = json.loads(json_strs[0])
        assert parsed["pit_grade"] == "A"

    def test_no_signal_inputs_writes_empty_object(self):
        conn = MockConn()
        order = _make_order()
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        sql, params = conn.calls[0]
        # signal_inputs_json should be "{}" (NOT NULL per schema)
        assert "{}" in params

    def test_is_live_flag_passes_through(self):
        conn = MockConn()
        intent = OrderIntent(
            intent_id="i", decision_id="d", strategy="qm", strategy_version="v",
            ticker="AAPL", side="buy", qty=100, is_live=True,
        )
        order = Order.from_intent(intent)
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        sql, params = conn.calls[0]
        assert True in params  # is_live=True


# ── Order.order_id determinism via deterministic_client_order_id ────────────


class TestOrderIdDeterminism:

    def test_same_decision_same_order_id(self):
        order_a = _make_order(decision_id="dec-XYZ")
        order_b = _make_order(decision_id="dec-XYZ")
        assert order_a.order_id == order_b.order_id

    def test_writing_same_order_twice_uses_upsert(self):
        # The DB-side guarantee is upsert; the audit module relies on it.
        # We can't actually exercise the upsert without a real DB, but we
        # CAN verify our SQL is structured to use ON CONFLICT.
        conn = MockConn()
        order = _make_order(decision_id="dec-XYZ")
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        order.mark_submitted("alpaca-1")
        write_order(conn, order, config_version_sha="g", config_yaml_sha="y")
        # Both writes used upsert SQL
        for sql, _ in conn.calls:
            assert "ON CONFLICT (order_id) DO UPDATE" in sql

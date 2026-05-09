"""Unit tests for framework.oms.order_manager — state machine + ID."""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.oms.order_manager import (
    InvalidStateTransition,
    Order,
    OrderIntent,
    OrderState,
    _TRANSITIONS,
    deterministic_client_order_id,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


def make_intent(
    *,
    decision_id: str = "decision-1",
    ticker: str = "AAPL",
    side: str = "buy",
    qty: float = 100.0,
    order_type: str = "market",
    limit_price=None,
    is_live: bool = False,
) -> OrderIntent:
    return OrderIntent(
        intent_id="intent-1",
        decision_id=decision_id,
        strategy="quality_momentum",
        strategy_version="abc:def",
        ticker=ticker,
        side=side,
        qty=qty,
        order_type=order_type,
        limit_price=limit_price,
        is_live=is_live,
    )


# ── OrderIntent validation ──────────────────────────────────────────────────


class TestOrderIntent:

    def test_valid_market_buy(self):
        intent = make_intent()
        assert intent.qty == 100.0
        assert intent.side == "buy"
        assert intent.order_type == "market"
        assert intent.limit_price is None

    def test_invalid_qty_zero(self):
        with pytest.raises(ValueError, match="qty must be > 0"):
            make_intent(qty=0)

    def test_invalid_qty_negative(self):
        with pytest.raises(ValueError, match="qty must be > 0"):
            make_intent(qty=-5)

    def test_invalid_side(self):
        with pytest.raises(ValueError, match="side must be buy"):
            make_intent(side="short")  # type: ignore[arg-type]

    def test_limit_order_requires_price(self):
        with pytest.raises(ValueError, match="limit order requires limit_price"):
            make_intent(order_type="limit", limit_price=None)

    def test_market_order_must_not_set_price(self):
        with pytest.raises(ValueError, match="market order must not set limit_price"):
            make_intent(order_type="market", limit_price=100.0)

    def test_intent_is_frozen(self):
        intent = make_intent()
        with pytest.raises(Exception):
            intent.qty = 999  # frozen dataclass


# ── Deterministic client_order_id ───────────────────────────────────────────


class TestDeterministicClientOrderId:

    def test_same_inputs_same_id(self):
        a = deterministic_client_order_id("d1")
        b = deterministic_client_order_id("d1")
        assert a == b

    def test_different_decision_different_id(self):
        a = deterministic_client_order_id("d1")
        b = deterministic_client_order_id("d2")
        assert a != b

    def test_different_retry_different_id(self):
        a = deterministic_client_order_id("d1", retry=0)
        b = deterministic_client_order_id("d1", retry=1)
        assert a != b

    def test_id_format(self):
        a = deterministic_client_order_id("d1")
        assert a.startswith("f4_")
        # f4_ + 24 hex chars = 27 chars total
        assert len(a) == 27
        # remaining is hex
        hex_part = a[3:]
        int(hex_part, 16)  # raises if not hex


# ── State machine: valid transitions ────────────────────────────────────────


class TestValidTransitions:

    @pytest.fixture
    def order(self):
        return Order.from_intent(make_intent())

    def test_initial_state_is_pending(self, order):
        assert order.state == OrderState.PENDING
        assert order.state_history == []

    def test_pending_to_submitted(self, order):
        order.transition(OrderState.SUBMITTED)
        assert order.state == OrderState.SUBMITTED
        assert len(order.state_history) == 1
        assert order.state_history[0][0] == OrderState.PENDING

    def test_full_happy_path(self, order):
        order.mark_submitted("alpaca-123")
        assert order.state == OrderState.SUBMITTED
        assert order.alpaca_order_id == "alpaca-123"
        assert order.submitted_at is not None

        order.mark_accepted()
        assert order.state == OrderState.ACCEPTED

        order.mark_filled(fill_qty=100.0, avg_price=150.50)
        assert order.state == OrderState.FILLED
        assert order.fill_qty == 100.0
        assert order.fill_price == 150.50
        assert order.filled_at is not None
        assert order.is_terminal

    def test_partial_fill_then_complete(self, order):
        order.mark_submitted("a-1")
        order.mark_accepted()
        order.mark_partial_fill(fill_qty=40.0, avg_price=150.0)
        assert order.state == OrderState.PARTIALLY_FILLED
        assert order.fill_qty == 40.0

        # Another partial — stays in PARTIALLY_FILLED
        order.mark_partial_fill(fill_qty=70.0, avg_price=150.25)
        assert order.state == OrderState.PARTIALLY_FILLED
        assert order.fill_qty == 70.0

        order.mark_filled(fill_qty=100.0, avg_price=150.50)
        assert order.state == OrderState.FILLED
        assert order.is_terminal

    def test_rejected_from_submitted(self, order):
        order.mark_submitted("a-1")
        order.mark_rejected("insufficient buying power")
        assert order.state == OrderState.REJECTED
        assert order.rejection_reason == "insufficient buying power"
        assert order.is_terminal

    def test_cancelled_from_accepted(self, order):
        order.mark_submitted("a-1")
        order.mark_accepted()
        order.mark_cancelled()
        assert order.state == OrderState.CANCELLED
        assert order.is_terminal


# ── State machine: invalid transitions ──────────────────────────────────────


class TestInvalidTransitions:

    def test_filled_terminal(self):
        o = Order.from_intent(make_intent())
        o.mark_submitted("a-1")
        o.mark_accepted()
        o.mark_filled(fill_qty=100, avg_price=100)
        with pytest.raises(InvalidStateTransition):
            o.transition(OrderState.SUBMITTED)
        with pytest.raises(InvalidStateTransition):
            o.transition(OrderState.CANCELLED)

    def test_rejected_terminal(self):
        o = Order.from_intent(make_intent())
        o.mark_rejected("blocked by risk")
        with pytest.raises(InvalidStateTransition):
            o.transition(OrderState.SUBMITTED)
        with pytest.raises(InvalidStateTransition):
            o.transition(OrderState.FILLED)

    def test_cancelled_terminal(self):
        o = Order.from_intent(make_intent())
        o.mark_cancelled()
        with pytest.raises(InvalidStateTransition):
            o.mark_submitted("a-1")

    def test_cannot_skip_submitted_to_filled(self):
        o = Order.from_intent(make_intent())
        with pytest.raises(InvalidStateTransition):
            o.transition(OrderState.FILLED)  # PENDING → FILLED not allowed

    def test_cannot_skip_accepted(self):
        o = Order.from_intent(make_intent())
        o.mark_submitted("a-1")
        with pytest.raises(InvalidStateTransition):
            o.transition(OrderState.PARTIALLY_FILLED)  # SUBMITTED → PARTIALLY_FILLED not allowed

    def test_partial_fill_from_pending_invalid(self):
        o = Order.from_intent(make_intent())
        with pytest.raises(InvalidStateTransition, match="partial fill requires"):
            o.mark_partial_fill(fill_qty=10, avg_price=100)


# ── Idempotent transitions ──────────────────────────────────────────────────


class TestIdempotency:

    def test_same_state_transition_is_noop(self):
        o = Order.from_intent(make_intent())
        o.mark_submitted("a-1")
        # Same state — no error, no history append
        before = list(o.state_history)
        o.transition(OrderState.SUBMITTED)
        assert o.state_history == before

    def test_state_history_records_each_transition(self):
        o = Order.from_intent(make_intent())
        o.mark_submitted("a-1")
        o.mark_accepted()
        o.mark_filled(fill_qty=100, avg_price=100)
        # 3 transitions → 3 history entries (each records the OLD state)
        assert len(o.state_history) == 3
        assert o.state_history[0][0] == OrderState.PENDING
        assert o.state_history[1][0] == OrderState.SUBMITTED
        assert o.state_history[2][0] == OrderState.ACCEPTED


# ── Property: every state has defined transitions ───────────────────────────


class TestStateMachineCompleteness:

    def test_every_state_in_transitions_table(self):
        """Every OrderState must have an entry in _TRANSITIONS (even if empty)."""
        for state in OrderState:
            assert state in _TRANSITIONS, f"Missing transitions for {state}"

    def test_terminal_states_have_no_outgoing(self):
        for state in OrderState.terminal_states():
            assert _TRANSITIONS[state] == frozenset(), (
                f"Terminal state {state} should have no outgoing transitions"
            )

    def test_transitions_target_valid_states(self):
        all_states = set(OrderState)
        for src, targets in _TRANSITIONS.items():
            for t in targets:
                assert t in all_states, f"{src} → {t} but {t} not a valid OrderState"


# ── Construction: Order.from_intent ─────────────────────────────────────────


class TestOrderFromIntent:

    def test_from_intent_uses_deterministic_id(self):
        intent = make_intent(decision_id="dec-xyz")
        o1 = Order.from_intent(intent)
        o2 = Order.from_intent(intent)
        # Both come from same decision_id → same client_order_id
        assert o1.order_id == o2.order_id

    def test_retry_changes_order_id(self):
        intent = make_intent(decision_id="dec-xyz")
        o1 = Order.from_intent(intent, retry=0)
        o2 = Order.from_intent(intent, retry=1)
        assert o1.order_id != o2.order_id

    def test_initial_fill_state(self):
        o = Order.from_intent(make_intent())
        assert o.fill_qty == 0.0
        assert o.fill_price is None
        assert o.filled_at is None
        assert o.alpaca_order_id is None
        assert o.submitted_at is None
        assert o.rejection_reason is None

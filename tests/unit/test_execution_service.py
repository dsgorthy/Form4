"""Scaffold tests for ExecutionService.

Focused on the contract that matters most for live-money safety:
deterministic client_order_id (so retries don't double-fill) and
correct fan-out from TradeIntent → broker call.
"""
from framework.decision.types import ExitIntent, TradeIntent
from framework.execution.base import ExecutionBackend, OrderResult
from framework.execution.service import ExecutionService


class FakeBackend(ExecutionBackend):
    """Records every call so we can assert on routing without hitting Alpaca."""
    def __init__(self, mode="PaperBackend"):
        self._mode = mode
        self.submitted = []
        self.closed = []

    def mode(self):
        return self._mode

    def submit_order(self, symbol, qty, side, order_type="market",
                     limit_price=None, time_in_force="day"):
        self.submitted.append({
            "symbol": symbol, "qty": qty, "side": side,
            "order_type": order_type, "limit_price": limit_price,
            "time_in_force": time_in_force,
        })
        return OrderResult(
            order_id=f"FAKE_{len(self.submitted)}",
            status="filled",
            symbol=symbol, qty=qty, side=side,
            filled_price=100.0,
            filled_qty=qty,
        )

    def close_position(self, symbol):
        self.closed.append(symbol)
        return OrderResult(
            order_id=f"FAKE_CLOSE_{len(self.closed)}",
            status="filled",
            symbol=symbol, qty=10, side="sell",
            filled_price=110.0, filled_qty=10,
        )

    def get_position(self, symbol):
        return None

    def get_account(self):
        return {"equity": 100000.0, "cash": 50000.0, "buying_power": 100000.0}

    def cancel_order(self, order_id):
        return True

    def get_open_orders(self):
        return []


def make_intent(**overrides):
    base = dict(
        strategy="quality_momentum",
        ticker="AAPL",
        trade_id=12345,
        side="buy",
        qty_dollars=10_000.0,
        conviction=2.5,
        reason="entry",
        metadata={"date": "2026-05-22"},
    )
    base.update(overrides)
    return TradeIntent(**base)


def test_sizes_qty_from_dollars_and_quote():
    svc = ExecutionService(FakeBackend(), "quality_momentum", is_live=False)
    fill = svc.execute_intent(make_intent(qty_dollars=10000), quote=200.0)
    assert fill.qty == 50
    assert fill.filled is True
    assert fill.side == "buy"
    assert svc.backend.submitted[0]["qty"] == 50


def test_client_order_id_is_deterministic_per_intent():
    svc = ExecutionService(FakeBackend(), "quality_momentum", is_live=False)
    intent = make_intent()
    coid_1 = svc._client_order_id(intent)
    coid_2 = svc._client_order_id(intent)
    assert coid_1 == coid_2

    # Different trade_id → different client_order_id
    other = make_intent(trade_id=99999)
    assert svc._client_order_id(other) != coid_1


def test_rejects_non_buy_intent():
    svc = ExecutionService(FakeBackend(), "quality_momentum", is_live=False)
    bad = make_intent(side="sell")
    try:
        svc.execute_intent(bad, quote=100.0)
        assert False, "should have raised"
    except ValueError:
        pass


def test_zero_quote_returns_unfilled_no_submission():
    backend = FakeBackend()
    svc = ExecutionService(backend, "quality_momentum", is_live=False)
    fill = svc.execute_intent(make_intent(), quote=0.0)
    assert fill.filled is False
    assert "non-positive quote" in fill.error
    assert backend.submitted == []


def test_exit_intent_closes_position():
    backend = FakeBackend()
    svc = ExecutionService(backend, "quality_momentum", is_live=False)
    exit_intent = ExitIntent(
        strategy="quality_momentum",
        ticker="AAPL",
        trade_id=1,
        side="sell",
        reason="time",
        exit_price=110.0,
        pnl_pct=0.10,
        pnl_dollar=1000.0,
        hold_days=42,
    )
    fill = svc.execute_exit(exit_intent)
    assert fill.filled is True
    assert backend.closed == ["AAPL"]

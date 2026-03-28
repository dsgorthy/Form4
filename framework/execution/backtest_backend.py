"""
BacktestFillBackend — simulated fills for the backtest engine.

Fills at next bar open + configurable slippage. Implements the same
ExecutionBackend interface as paper/live backends.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from framework.execution.base import ExecutionBackend, OrderResult

logger = logging.getLogger(__name__)


class BacktestFillBackend(ExecutionBackend):
    """
    Simulated execution for backtesting.

    Fill model: next-bar open + slippage.
    No partial fills. Assumes unlimited liquidity.

    Parameters
    ----------
    slippage_pct : float
        Slippage as a fraction of price. E.g. 0.001 = 0.1%.
        Applied in the adverse direction (buy fills above, sell below).
    starting_cash : float
        Initial cash balance.
    """

    def __init__(
        self,
        slippage_pct: float = 0.001,
        starting_cash: float = 30_000.0,
    ) -> None:
        self.slippage_pct = slippage_pct
        self._cash = starting_cash
        self._starting_cash = starting_cash
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._orders: Dict[str, OrderResult] = {}
        self._trade_history: List[Dict[str, Any]] = []

    def submit_order(
        self,
        symbol: str,
        qty: int,
        side: str,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        time_in_force: str = "day",
        fill_price: Optional[float] = None,  # Direct fill price for backtest
    ) -> OrderResult:
        """
        Simulate an order fill.

        Parameters
        ----------
        fill_price : float, optional
            If provided, fills at this price (e.g. the bar open price).
            Otherwise returns a pending order that needs to be filled via fill_pending().
        """
        order_id = str(uuid.uuid4())[:8]

        if fill_price is None:
            # Pending — needs explicit fill
            result = OrderResult(
                order_id=order_id, status="pending",
                symbol=symbol, qty=qty, side=side,
                submitted_price=limit_price,
            )
            self._orders[order_id] = result
            logger.debug("Order %s pending: %s %d %s", order_id, side, qty, symbol)
            return result

        # Apply slippage
        if side == "buy":
            actual_fill = fill_price * (1.0 + self.slippage_pct)
        else:
            actual_fill = fill_price * (1.0 - self.slippage_pct)

        # Update cash
        trade_value = actual_fill * qty * 100  # options multiplier (or 1 for equity)
        if symbol.upper() in ("SPY", "QQQ", "IWM") and len(symbol) <= 4:
            trade_value = actual_fill * qty  # equity: no multiplier

        if side == "buy":
            self._cash -= trade_value
        else:
            self._cash += trade_value

        # Update position
        self._update_position(symbol, qty if side == "buy" else -qty, actual_fill)

        result = OrderResult(
            order_id=order_id, status="filled",
            symbol=symbol, qty=qty, side=side,
            filled_price=actual_fill, filled_qty=qty,
        )
        self._orders[order_id] = result
        self._trade_history.append({"order_id": order_id, "symbol": symbol, "qty": qty,
                                     "side": side, "fill_price": actual_fill})

        logger.debug("Order %s filled: %s %d %s @ %.4f", order_id, side, qty, symbol, actual_fill)
        return result

    def fill_pending(self, order_id: str, fill_price: float) -> OrderResult:
        """Fill a pending order at the given price (e.g. next bar open)."""
        if order_id not in self._orders:
            return OrderResult(order_id=order_id, status="rejected", symbol="", qty=0, side="",
                               error="Order not found")

        order = self._orders[order_id]
        if order.status != "pending":
            return order

        return self.submit_order(
            symbol=order.symbol, qty=order.qty, side=order.side,
            fill_price=fill_price,
        )

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._positions.get(symbol.upper())

    def get_account(self) -> Dict[str, Any]:
        portfolio_value = sum(
            abs(pos.get("qty", 0)) * pos.get("current_price", pos.get("avg_entry_price", 0))
            for pos in self._positions.values()
        )
        return {
            "equity": self._cash + portfolio_value,
            "cash": self._cash,
            "buying_power": self._cash,
            "portfolio_value": portfolio_value,
        }

    def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders and self._orders[order_id].status == "pending":
            self._orders[order_id].status = "cancelled"
            return True
        return False

    def get_open_orders(self) -> List[Dict[str, Any]]:
        return [
            {"order_id": oid, "symbol": o.symbol, "qty": o.qty, "side": o.side}
            for oid, o in self._orders.items() if o.status == "pending"
        ]

    def reset(self, starting_cash: Optional[float] = None) -> None:
        """Reset the backend to initial state (useful for walk-forward testing)."""
        self._cash = starting_cash if starting_cash is not None else self._starting_cash
        self._positions.clear()
        self._orders.clear()
        self._trade_history.clear()

    def _update_position(self, symbol: str, qty_delta: int, fill_price: float) -> None:
        symbol = symbol.upper()
        if symbol not in self._positions:
            self._positions[symbol] = {"symbol": symbol, "qty": 0, "avg_entry_price": 0.0}

        pos = self._positions[symbol]
        existing_qty = pos["qty"]
        existing_avg = pos["avg_entry_price"]

        new_qty = existing_qty + qty_delta

        if new_qty == 0:
            del self._positions[symbol]
            return

        # Weighted average entry price (for same-direction adds)
        if existing_qty == 0 or (existing_qty > 0) == (qty_delta > 0):
            pos["avg_entry_price"] = (
                (abs(existing_qty) * existing_avg + abs(qty_delta) * fill_price)
                / abs(new_qty)
            )
        # Reducing position: avg doesn't change for the remaining portion
        pos["qty"] = new_qty
        pos["current_price"] = fill_price

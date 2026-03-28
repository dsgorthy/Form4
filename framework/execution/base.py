"""
ExecutionBackend abstract base class.

All execution backends (backtest, paper, live) implement this interface.
Strategy and pipeline code never imports PaperBackend or LiveBackend directly —
they always use this ABC so the same code runs in all three contexts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class OrderResult:
    """Result of a submitted order."""

    order_id: str
    status: str                 # "filled" | "pending" | "rejected" | "cancelled"
    symbol: str
    qty: int
    side: str                   # "buy" | "sell"
    filled_price: Optional[float] = None
    filled_qty: int = 0
    submitted_price: Optional[float] = None  # limit price if applicable
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_filled(self) -> bool:
        return self.status == "filled" and self.filled_qty > 0

    @property
    def is_error(self) -> bool:
        return self.status == "rejected" or self.error is not None


class ExecutionBackend(ABC):
    """
    Abstract execution backend.

    Implementations:
      - BacktestFillBackend   (in backtest_backend.py)
      - PaperBackend          (in paper.py) — Alpaca paper API
      - LiveBackend           (in live.py)  — Alpaca live API

    All three expose the same interface so strategy/pipeline code is unchanged
    when switching execution mode.
    """

    @abstractmethod
    def submit_order(
        self,
        symbol: str,
        qty: int,
        side: str,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        time_in_force: str = "day",
    ) -> OrderResult:
        """
        Submit a buy or sell order.

        Parameters
        ----------
        symbol : str
            Ticker or OCC option symbol.
        qty : int
            Number of shares or contracts.
        side : str
            "buy" or "sell".
        order_type : str
            "market" or "limit".
        limit_price : float, optional
            Required when order_type="limit".
        time_in_force : str
            "day", "gtc", "ioc", "fok".

        Returns
        -------
        OrderResult
        """
        ...

    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the current position for a symbol.

        Returns None if no position exists.
        Returns dict with at least: symbol, qty, avg_entry_price, market_value.
        """
        ...

    @abstractmethod
    def get_account(self) -> Dict[str, Any]:
        """
        Get current account state.

        Returns dict with at least: equity, cash, buying_power, portfolio_value.
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Returns True if successfully cancelled, False otherwise.
        """
        ...

    @abstractmethod
    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Return list of open (unfilled) orders."""
        ...

    def close_position(self, symbol: str) -> OrderResult:
        """
        Convenience method: close an open position at market.

        Default implementation looks up position and submits opposing market order.
        Can be overridden for more efficient close implementations.
        """
        position = self.get_position(symbol)
        if position is None:
            return OrderResult(
                order_id="no_position",
                status="rejected",
                symbol=symbol,
                qty=0,
                side="sell",
                error="No position to close",
            )
        qty = abs(int(position.get("qty", 0)))
        side = "sell" if float(position.get("qty", 0)) > 0 else "buy"
        return self.submit_order(symbol=symbol, qty=qty, side=side)

    def mode(self) -> str:
        """Return the execution mode name."""
        return self.__class__.__name__

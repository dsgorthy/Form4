"""
PaperBackend — Alpaca paper trading API execution backend.

Connects to paper-api.alpaca.markets/v2 for order management.
Uses the same ExecutionBackend interface as all other backends.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from framework.execution.base import ExecutionBackend, OrderResult

logger = logging.getLogger(__name__)

PAPER_API_BASE = "https://paper-api.alpaca.markets/v2"
MAX_RETRIES = 3
BACKOFF_BASE = 1.0


class PaperBackend(ExecutionBackend):
    """
    Alpaca paper trading execution backend.

    Parameters
    ----------
    api_key : str
        Alpaca API key ID.
    api_secret : str
        Alpaca API secret key.
    base_url : str
        API base URL. Defaults to paper API endpoint.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = PAPER_API_BASE,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError("Alpaca credentials required: api_key and api_secret")
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.request(method, url, json=json_body, params=params, timeout=30)
            except requests.RequestException as exc:
                logger.warning("Request error attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(BACKOFF_BASE * (2 ** (attempt - 1)))
                continue

            if resp.status_code in (200, 201, 207):
                return resp.json() if resp.content else {}

            logger.warning("HTTP %d on %s (attempt %d): %s",
                           resp.status_code, path, attempt, resp.text[:300])

            if resp.status_code in (429, 500, 502, 503) and attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE * (2 ** (attempt - 1)))
                continue

            resp.raise_for_status()

        raise RuntimeError("Exhausted retries")

    def submit_order(
        self,
        symbol: str,
        qty: int,
        side: str,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        time_in_force: str = "day",
    ) -> OrderResult:
        body: Dict[str, Any] = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force,
        }
        if order_type == "limit" and limit_price is not None:
            body["limit_price"] = str(round(limit_price, 2))

        try:
            data = self._request("POST", "/orders", json_body=body)
        except Exception as exc:
            logger.error("Order submission failed: %s", exc)
            return OrderResult(
                order_id="error", status="rejected",
                symbol=symbol, qty=qty, side=side,
                error=str(exc),
            )

        order_id = data.get("id", "unknown")
        status_raw = data.get("status", "new")

        # Map Alpaca status to our status
        if status_raw in ("filled", "partially_filled"):
            status = "filled"
            filled_price = float(data.get("filled_avg_price") or 0) or None
            filled_qty = int(data.get("filled_qty") or 0)
        elif status_raw in ("new", "accepted", "pending_new"):
            status = "pending"
            filled_price = None
            filled_qty = 0
        elif status_raw in ("canceled", "cancelled", "expired", "rejected"):
            status = "rejected"
            filled_price = None
            filled_qty = 0
        else:
            status = "pending"
            filled_price = None
            filled_qty = 0

        return OrderResult(
            order_id=order_id, status=status,
            symbol=symbol, qty=qty, side=side,
            filled_price=filled_price, filled_qty=filled_qty,
            submitted_price=limit_price, raw=data,
        )

    def wait_for_fill(self, order_id: str, timeout: int = 30, poll_interval: float = 0.5) -> OrderResult:
        """
        Poll until the order is filled or timeout is reached.

        Parameters
        ----------
        order_id : str
            The order ID from submit_order().
        timeout : int
            Maximum seconds to wait.
        poll_interval : float
            Seconds between polls.

        Returns
        -------
        OrderResult
            Final order state.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = self._request("GET", f"/orders/{order_id}")
            except Exception as exc:
                logger.warning("Error polling order %s: %s", order_id, exc)
                time.sleep(poll_interval)
                continue

            status_raw = data.get("status", "")
            if status_raw == "filled":
                return OrderResult(
                    order_id=order_id, status="filled",
                    symbol=data.get("symbol", ""),
                    qty=int(data.get("qty", 0)),
                    side=data.get("side", ""),
                    filled_price=float(data.get("filled_avg_price") or 0),
                    filled_qty=int(data.get("filled_qty") or 0),
                    raw=data,
                )
            elif status_raw in ("canceled", "cancelled", "expired", "rejected"):
                return OrderResult(
                    order_id=order_id, status="rejected",
                    symbol=data.get("symbol", ""),
                    qty=int(data.get("qty", 0)),
                    side=data.get("side", ""),
                    error=f"Order {status_raw}",
                    raw=data,
                )

            time.sleep(poll_interval)

        logger.warning("Order %s not filled within %ds timeout", order_id, timeout)
        return OrderResult(
            order_id=order_id, status="pending",
            symbol="", qty=0, side="",
            error=f"Timeout waiting for fill after {timeout}s",
        )

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            data = self._request("GET", f"/positions/{symbol}")
            return {
                "symbol": data.get("symbol"),
                "qty": float(data.get("qty", 0)),
                "avg_entry_price": float(data.get("avg_entry_price", 0)),
                "market_value": float(data.get("market_value", 0)),
                "current_price": float(data.get("current_price", 0)),
                "unrealized_pl": float(data.get("unrealized_pl", 0)),
            }
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise
        except Exception:
            return None

    def get_account(self) -> Dict[str, Any]:
        data = self._request("GET", "/account")
        return {
            "equity": float(data.get("equity", 0)),
            "cash": float(data.get("cash", 0)),
            "buying_power": float(data.get("buying_power", 0)),
            "portfolio_value": float(data.get("portfolio_value", 0)),
            "pattern_day_trader": data.get("pattern_day_trader", False),
            "raw": data,
        }

    def cancel_order(self, order_id: str) -> bool:
        try:
            self._request("DELETE", f"/orders/{order_id}")
            return True
        except Exception as exc:
            logger.warning("Failed to cancel order %s: %s", order_id, exc)
            return False

    def get_open_orders(self) -> List[Dict[str, Any]]:
        try:
            orders = self._request("GET", "/orders", params={"status": "open"})
            return [
                {
                    "order_id": o.get("id"),
                    "symbol": o.get("symbol"),
                    "qty": int(o.get("qty", 0)),
                    "side": o.get("side"),
                    "status": o.get("status"),
                    "submitted_at": o.get("submitted_at"),
                }
                for o in (orders if isinstance(orders, list) else [])
            ]
        except Exception:
            return []

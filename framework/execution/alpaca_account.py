"""
AlpacaAccount — read-only wrapper for Alpaca's account/positions endpoints.

Used by the API layer to surface live paper trading state in the
/paper-trading dashboard. Each strategy has its own Alpaca paper account
with separate API keys, so this is instantiated per-strategy.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

PAPER_API_BASE = "https://paper-api.alpaca.markets/v2"
LIVE_API_BASE = "https://api.alpaca.markets/v2"
TIMEOUT = 5.0


class AlpacaAccount:
    """Read-only Alpaca account wrapper. Hits /v2/account and /v2/positions."""

    def __init__(self, api_key: str, api_secret: str, paper: bool = True):
        self.base_url = PAPER_API_BASE if paper else LIVE_API_BASE
        self.headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        }

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self.headers, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def get_account(self) -> dict:
        """Returns equity, cash, buying_power, last_equity, status, etc."""
        return self._get("/account")

    def get_positions(self) -> list:
        """Returns list of open positions with symbol, qty, prices, P&L."""
        return self._get("/positions")

    def get_snapshot(self) -> dict:
        """Combined snapshot: account + positions + day_change_pct.

        Returns:
            {
                "equity": float,
                "last_equity": float,            # equity at start of trading day
                "cash": float,
                "buying_power": float,
                "status": str,
                "day_change": float,
                "day_change_pct": float,
                "positions": [
                    {symbol, qty, avg_entry_price, current_price,
                     market_value, unrealized_pl, unrealized_plpc}
                ],
                "position_count": int,
            }
        """
        account = self.get_account()
        positions = self.get_positions()

        equity = float(account.get("equity", 0) or 0)
        last_equity = float(account.get("last_equity", 0) or 0)
        day_change = equity - last_equity
        day_change_pct = (day_change / last_equity * 100) if last_equity > 0 else 0.0

        # Normalize positions to a small dict per ticker
        normalized_positions = []
        for p in positions:
            normalized_positions.append({
                "symbol": p.get("symbol"),
                "qty": float(p.get("qty", 0) or 0),
                "avg_entry_price": float(p.get("avg_entry_price", 0) or 0),
                "current_price": float(p.get("current_price", 0) or 0),
                "market_value": float(p.get("market_value", 0) or 0),
                "unrealized_pl": float(p.get("unrealized_pl", 0) or 0),
                "unrealized_plpc": float(p.get("unrealized_plpc", 0) or 0) * 100,  # Alpaca returns as decimal
            })

        return {
            "equity": equity,
            "last_equity": last_equity,
            "cash": float(account.get("cash", 0) or 0),
            "buying_power": float(account.get("buying_power", 0) or 0),
            "status": account.get("status", "unknown"),
            "day_change": day_change,
            "day_change_pct": day_change_pct,
            "positions": normalized_positions,
            "position_count": len(normalized_positions),
        }

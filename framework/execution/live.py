"""
LiveBackend — Alpaca live trading execution backend.

This is a URL-swap subclass of PaperBackend that connects to the live API.
DISABLED BY DEFAULT — must be explicitly enabled by passing enable_live=True.

WARNING: This submits REAL orders with REAL money. Use with extreme care.
"""

from __future__ import annotations

import logging
import os

from framework.execution.paper import PaperBackend

logger = logging.getLogger(__name__)

LIVE_API_BASE = "https://api.alpaca.markets/v2"


class LiveBackend(PaperBackend):
    """
    Alpaca live trading backend.

    Identical to PaperBackend except it uses the live API URL.

    Parameters
    ----------
    api_key : str
        Alpaca live API key.
    api_secret : str
        Alpaca live API secret.
    enable_live : bool
        Safety flag — must be True to use live trading. Default False.
        This prevents accidental live order submission.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        enable_live: bool = False,
    ) -> None:
        if not enable_live:
            raise RuntimeError(
                "LiveBackend requires enable_live=True. "
                "This submits REAL orders with REAL money. "
                "Set enable_live=True only when you are ready for live trading."
            )

        logger.warning(
            "⚠️  LIVE TRADING ENABLED — orders will be submitted with REAL money."
        )

        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            base_url=LIVE_API_BASE,
        )

    def mode(self) -> str:
        return "LIVE"


def make_execution_backend(mode: str, api_key: str, api_secret: str, **kwargs):
    """
    Factory function to create the appropriate execution backend.

    Parameters
    ----------
    mode : str
        "backtest", "paper", or "live".
    api_key, api_secret : str
        Alpaca credentials (not needed for backtest).
    **kwargs :
        Additional arguments passed to the backend constructor.

    Returns
    -------
    ExecutionBackend
    """
    if mode == "backtest":
        from framework.execution.backtest_backend import BacktestFillBackend
        return BacktestFillBackend(
            starting_cash=kwargs.get("starting_cash", 30_000.0),
            slippage_pct=kwargs.get("slippage_pct", 0.001),
        )
    elif mode == "paper":
        return PaperBackend(api_key=api_key, api_secret=api_secret)
    elif mode == "live":
        return LiveBackend(
            api_key=api_key,
            api_secret=api_secret,
            enable_live=kwargs.get("enable_live", False),
        )
    else:
        raise ValueError(f"Unknown execution mode: {mode!r}. Use 'backtest', 'paper', or 'live'.")

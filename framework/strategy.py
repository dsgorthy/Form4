"""
Base strategy interface for the generalized trading framework.

All strategies must subclass BaseStrategy and implement the four core methods.
The framework handles position sizing, risk filters, and execution — the strategy
handles signal generation and instrument selection.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd


@dataclass
class Signal:
    """Output from BaseStrategy.generate_signal()."""

    direction: Optional[str]    # "long" | "short" | None (no trade)
    confidence: float           # [0.0, 1.0]
    instrument: Optional[dict]  # strategy hint for instrument selection
    metadata: dict = field(default_factory=dict)  # arbitrary extra context

    def is_valid(self) -> bool:
        """True if the signal indicates a trade should be taken."""
        return self.direction in ("long", "short")


@dataclass
class DataRequirements:
    """Declares what data a strategy needs."""

    symbols: List[str]           # e.g. ["SPY", "VIXY"]
    timeframes: List[str]        # e.g. ["1Min", "5Min"]
    lookback_days: int           # how many calendar days of history to pre-load
    requires_options: bool = False

    def primary_symbol(self) -> str:
        """Return the first symbol (conventionally the traded instrument)."""
        return self.symbols[0] if self.symbols else ""


class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.

    Subclasses implement signal generation and instrument selection.
    The framework owns: position sizing, trade filters, execution, P&L tracking.

    bars dict convention
    --------------------
    ``bars`` is a dict keyed by "SYMBOL_TIMEFRAME", e.g.:
        bars["SPY_1Min"]   -> DataFrame of 1-minute bars for SPY
        bars["VIXY_1Min"]  -> DataFrame of 1-minute bars for VIXY
        bars["SPY_5Min"]   -> DataFrame of 5-minute bars for SPY (resampled)

    The DataLoader produces this dict from a single 1-minute parquet read;
    higher timeframes are resampled in-process.
    """

    def __init__(self, config: dict) -> None:
        self.config = config

    @abstractmethod
    def data_requirements(self) -> DataRequirements:
        """
        Declare what data this strategy needs.

        Called once at engine initialization to know which symbols/timeframes
        to load and how much history to pre-fetch.
        """
        ...

    @abstractmethod
    def generate_signal(
        self, bars: Dict[str, pd.DataFrame], date: str
    ) -> Signal:
        """
        Evaluate the current market state and produce a trading signal.

        Parameters
        ----------
        bars : dict
            Keyed "SYMBOL_TIMEFRAME". Contains bars up to (and including)
            the strategy's entry time for the given date.
        date : str
            Trading date as "YYYY-MM-DD".

        Returns
        -------
        Signal
            direction=None means no trade. Otherwise "long" or "short".
        """
        ...

    @abstractmethod
    def select_instrument(
        self,
        signal: Signal,
        bars: Dict[str, pd.DataFrame],
        date: str,
    ) -> dict:
        """
        Given a valid signal, choose the specific instrument to trade.

        For options strategies: returns {"symbol", "option_type", "strike",
        "expiry", "entry_price", "iv", "greeks"}.
        For equity strategies: returns {"symbol", "entry_price"}.

        Parameters
        ----------
        signal : Signal
            The validated signal from generate_signal().
        bars : dict
            Same bars dict passed to generate_signal().
        date : str
            Trading date as "YYYY-MM-DD".

        Returns
        -------
        dict
            Instrument specification. Must contain at least "entry_price".
            Returns empty dict if instrument selection fails.
        """
        ...

    @abstractmethod
    def should_exit(
        self,
        position: dict,
        bars: Dict[str, pd.DataFrame],
    ) -> Optional[str]:
        """
        Bar-by-bar exit check. Called for every bar after entry.

        Parameters
        ----------
        position : dict
            Current open position. Keys depend on instrument type but always
            include: "entry_price", "entry_time", "direction", "instrument".
        bars : dict
            Current bars (up to the bar being evaluated).

        Returns
        -------
        str or None
            Exit reason string ("target", "stop", "time_stop", etc.) if the
            position should be closed NOW, or None to hold.
        """
        ...

    def strategy_name(self) -> str:
        """Return a human-readable strategy name (default: class name)."""
        return self.__class__.__name__

"""
Shared pytest fixtures for the trading framework test suite.
"""

from __future__ import annotations

from typing import Dict, Optional
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from framework.strategy import BaseStrategy, DataRequirements, Signal


# ---------------------------------------------------------------------------
# Fixture: sample_ohlcv_df
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_ohlcv_df() -> pd.DataFrame:
    """
    Return a minimal 1-minute OHLCV DataFrame for one trading day.

    390 bars from 09:30 to 16:00 ET with realistic SPY-like prices
    (~$500, small random walk). Uses a fixed random seed for reproducibility.
    """
    rng = np.random.default_rng(seed=42)
    n_bars = 390  # 6.5 hours * 60 minutes

    # Generate index: 09:30 through 15:59 (390 one-minute bars)
    date_str = "2025-06-16"
    index = pd.date_range(
        start=f"{date_str} 09:30:00",
        periods=n_bars,
        freq="1min",
        tz="US/Eastern",
    )

    # Random walk for close prices starting around $500
    price = 500.0
    closes = np.empty(n_bars)
    for i in range(n_bars):
        price += rng.normal(0, 0.05)  # ~$0.05 stdev per bar
        closes[i] = round(price, 2)

    # Derive open/high/low from close with small perturbations
    opens = closes + rng.normal(0, 0.02, n_bars)
    highs = np.maximum(closes, opens) + np.abs(rng.normal(0, 0.03, n_bars))
    lows = np.minimum(closes, opens) - np.abs(rng.normal(0, 0.03, n_bars))
    volumes = rng.integers(10_000, 500_000, size=n_bars)

    df = pd.DataFrame(
        {
            "open": np.round(opens, 2),
            "high": np.round(highs, 2),
            "low": np.round(lows, 2),
            "close": np.round(closes, 2),
            "volume": volumes,
        },
        index=index,
    )
    return df


# ---------------------------------------------------------------------------
# Fixture: mock_storage
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_storage(sample_ohlcv_df):
    """
    A mock DataStorage that returns sample_ohlcv_df for any symbol/date.

    mock_storage.load_minute_bars(symbol, date) -> sample_ohlcv_df
    mock_storage.has_date(symbol, date) -> True
    mock_storage.get_available_dates(symbol) -> ["2025-06-16"]
    """
    storage = MagicMock()
    storage.load_minute_bars.return_value = sample_ohlcv_df
    storage.has_date.return_value = True
    storage.get_available_dates.return_value = ["2025-06-16"]
    return storage


# ---------------------------------------------------------------------------
# Fixture: simple_strategy
# ---------------------------------------------------------------------------

class _AlwaysLongStubStrategy(BaseStrategy):
    """
    A stub strategy that always goes long at 10:00 and exits at 15:30.

    Used for integration-style tests that need a concrete strategy instance
    without complex signal logic.
    """

    def __init__(self):
        config = {
            "entry": {"time": "10:00"},
            "exit": {"time": "15:30"},
        }
        super().__init__(config)

    def data_requirements(self) -> DataRequirements:
        return DataRequirements(
            symbols=["SPY"],
            timeframes=["1Min"],
            lookback_days=0,
        )

    def generate_signal(
        self, bars: Dict[str, pd.DataFrame], date: str
    ) -> Signal:
        return Signal(
            direction="long",
            confidence=0.8,
            instrument=None,
            metadata={"reason": "stub_always_long"},
        )

    def select_instrument(
        self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str
    ) -> dict:
        # Use the last close price from the primary 1Min bars as entry price
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return {}
        entry_price = float(spy_bars.iloc[-1]["close"])
        return {
            "symbol": "SPY",
            "type": "equity",
            "entry_price": entry_price,
        }

    def should_exit(
        self, position: dict, bars: Dict[str, pd.DataFrame]
    ) -> Optional[str]:
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return None
        last_ts = spy_bars.index[-1]
        # Exit at or after 15:30
        exit_hour, exit_min = 15, 30
        if last_ts.hour > exit_hour or (
            last_ts.hour == exit_hour and last_ts.minute >= exit_min
        ):
            return "time_stop"
        return None

    def strategy_name(self) -> str:
        return "AlwaysLongStub"


@pytest.fixture
def simple_strategy() -> BaseStrategy:
    """A stub BaseStrategy that always goes long at 10:00 and exits at 15:30."""
    return _AlwaysLongStubStrategy()

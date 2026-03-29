"""
DataLoader — fetches and resamples market data for strategy consumption.

Single source of truth: load 1-minute bars from storage, then resample
in-process to higher timeframes. No separate Alpaca calls per timeframe.

bars dict convention:
    bars["SPY_1Min"]  -> 1-minute DataFrame
    bars["SPY_5Min"]  -> 5-minute resampled DataFrame
    bars["VIXY_1Min"] -> 1-minute DataFrame
    etc.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

from framework.data.storage import DataStorage
from framework.strategy import DataRequirements

logger = logging.getLogger(__name__)

_OHLCV_AGG = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}


def _resample_timeframe(df_1min: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample a 1-minute DataFrame to the given timeframe."""
    if timeframe == "1Min":
        return df_1min

    # Map common timeframe strings to pandas offset aliases
    alias_map = {
        "5Min": "5min",
        "15Min": "15min",
        "30Min": "30min",
        "1Hour": "1h",
        "1H": "1h",
        "1Day": "1D",
    }
    alias = alias_map.get(timeframe, timeframe.lower())

    agg = {col: _OHLCV_AGG[col] for col in _OHLCV_AGG if col in df_1min.columns}
    resampled = df_1min.resample(alias).agg(agg).dropna(subset=["close"])
    return resampled


class DataLoader:
    """
    Loads market data for one or more symbols and resamples to requested timeframes.

    Usage
    -----
    loader = DataLoader(storage)
    bars = loader.load_bars_for_date("2025-06-15", requirements)
    # bars["SPY_1Min"] -> pd.DataFrame
    # bars["SPY_5Min"] -> pd.DataFrame (resampled from 1-min)
    """

    def __init__(self, storage: Optional[DataStorage] = None):
        self.storage = storage or DataStorage()

    def load_bars_for_date(
        self,
        date: str,
        requirements: DataRequirements,
        up_to_time: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Load bars for all symbols/timeframes required by a strategy.

        Parameters
        ----------
        date : str
            Trading date "YYYY-MM-DD".
        requirements : DataRequirements
            Declares which symbols and timeframes are needed.
        up_to_time : str, optional
            If provided ("HH:MM"), slice all bars to be <= this time.
            Used to prevent look-ahead bias at the signal-generation step.

        Returns
        -------
        dict
            Keyed "SYMBOL_TIMEFRAME", values are DataFrames.
            Missing data returns empty DataFrames (never raises).
        """
        bars: Dict[str, pd.DataFrame] = {}

        for symbol in requirements.symbols:
            df_1min = self.storage.load_minute_bars(symbol, date)

            if df_1min is None or df_1min.empty:
                logger.debug("No 1-min data for %s on %s", symbol, date)
                for tf in requirements.timeframes:
                    bars[f"{symbol}_{tf}"] = pd.DataFrame()
                continue

            # Apply time slice if requested
            if up_to_time is not None:
                df_1min = self._slice_up_to(df_1min, date, up_to_time)

            for tf in requirements.timeframes:
                key = f"{symbol}_{tf}"
                bars[key] = _resample_timeframe(df_1min, tf)

        return bars

    def load_bars_range(
        self,
        start_date: str,
        end_date: str,
        requirements: DataRequirements,
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Load bars for a date range. Returns dict keyed by date.

        Parameters
        ----------
        start_date, end_date : str
            Date range inclusive, "YYYY-MM-DD".
        requirements : DataRequirements
            What data is needed.

        Returns
        -------
        dict
            { "YYYY-MM-DD": { "SYMBOL_TIMEFRAME": DataFrame } }
        """
        from framework.data.calendar import MarketCalendar
        calendar = MarketCalendar()
        trading_days = calendar.get_trading_days(start_date, end_date)

        result = {}
        for date_str in trading_days:
            result[date_str] = self.load_bars_for_date(date_str, requirements)

        return result

    @staticmethod
    def _slice_up_to(df: pd.DataFrame, date: str, time_str: str) -> pd.DataFrame:
        """Return bars from market open up to and including time_str."""
        hour, minute = map(int, time_str.split(":"))
        if df.index.tz is not None:
            cutoff = pd.Timestamp(f"{date} {hour:02d}:{minute:02d}:00").tz_localize(
                str(df.index.tz)
            )
        else:
            cutoff = pd.Timestamp(f"{date} {hour:02d}:{minute:02d}:00")
        return df.loc[df.index <= cutoff]

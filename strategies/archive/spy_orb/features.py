"""
Feature engineering for the SPY Opening Range Breakout strategy.
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from framework.signals.indicators import rsi, vwap, atr

logger = logging.getLogger(__name__)


class ORBFeatureEngine:
    """
    Computes features for the Opening Range Breakout strategy.

    Features center on the opening range (9:30–10:00 AM ET):
      - opening_range_high / opening_range_low / opening_range_width
      - gap_size
      - breakout direction and magnitude
      - pre-entry volume ratio
      - VWAP position
    """

    def __init__(self, storage, spy_symbol: str = "SPY", vixy_symbol: str = "VIXY"):
        self.storage = storage
        self.spy_symbol = spy_symbol
        self.vixy_symbol = vixy_symbol

    def compute_features(
        self,
        date: str,
        range_start: str = "09:30",
        range_end: str = "10:00",
        check_time: str = "10:05",
    ) -> dict:
        """
        Compute features for a single trading day.

        Parameters
        ----------
        date : str
            Trading date "YYYY-MM-DD".
        range_start : str
            Opening range start time (HH:MM).
        range_end : str
            Opening range end time (HH:MM).
        check_time : str
            Time to evaluate breakout condition.

        Returns
        -------
        dict
            Feature name -> value.
        """
        spy_bars = self.storage.load_minute_bars(self.spy_symbol, date)
        vixy_bars = self.storage.load_minute_bars(self.vixy_symbol, date)
        features: dict = {"date": date}

        if spy_bars is None or spy_bars.empty:
            return {**features, **self._nan_features()}

        day_open = float(spy_bars.iloc[0]["open"])

        # Opening range bars
        range_bars = self._bars_between(spy_bars, date, range_start, range_end)
        if range_bars.empty:
            return {**features, **self._nan_features()}

        or_high = float(range_bars["high"].max())
        or_low = float(range_bars["low"].min())
        or_width = (or_high - or_low) / day_open

        features["opening_range_high"] = or_high
        features["opening_range_low"] = or_low
        features["opening_range_width_pct"] = or_width * 100.0

        # Bars up to check_time
        bars_to_check = self._bars_up_to(spy_bars, date, check_time)
        if bars_to_check.empty:
            return {**features, **self._nan_features()}

        check_price = float(bars_to_check.iloc[-1]["close"])

        # Breakout direction
        if check_price > or_high:
            features["breakout_direction"] = 1    # bullish breakout
            features["breakout_magnitude_pct"] = (check_price - or_high) / or_high * 100
        elif check_price < or_low:
            features["breakout_direction"] = -1   # bearish breakout
            features["breakout_magnitude_pct"] = (or_low - check_price) / or_low * 100
        else:
            features["breakout_direction"] = 0    # inside range
            features["breakout_magnitude_pct"] = 0.0

        # Gap size
        all_dates = self.storage.get_available_dates(self.spy_symbol)
        try:
            idx = all_dates.index(date)
            if idx > 0:
                prev_bars = self.storage.load_minute_bars(self.spy_symbol, all_dates[idx - 1])
                if prev_bars is not None and not prev_bars.empty:
                    prev_close = float(prev_bars.iloc[-1]["close"])
                    features["gap_pct"] = (day_open - prev_close) / prev_close * 100
                else:
                    features["gap_pct"] = np.nan
            else:
                features["gap_pct"] = np.nan
        except ValueError:
            features["gap_pct"] = np.nan

        # VWAP position
        vwap_series = vwap(bars_to_check)
        if not vwap_series.empty:
            current_vwap = float(vwap_series.iloc[-1])
            features["distance_from_vwap_pct"] = (check_price - current_vwap) / current_vwap * 100
        else:
            features["distance_from_vwap_pct"] = np.nan

        # VIX proxy
        if vixy_bars is not None and not vixy_bars.empty:
            vixy_up_to = self._bars_up_to(vixy_bars, date, check_time)
            features["vix_level"] = float(vixy_up_to.iloc[-1]["close"]) if not vixy_up_to.empty else np.nan
        else:
            features["vix_level"] = np.nan

        # Volume ratio (opening range vs 20-day average)
        or_volume = float(range_bars["volume"].sum())
        features["or_volume_ratio"] = self._volume_ratio(or_volume, date, range_start, range_end)

        return features

    @staticmethod
    def _bars_up_to(df: pd.DataFrame, date: str, time_str: str) -> pd.DataFrame:
        h, m = map(int, time_str.split(":"))
        if df.index.tz is not None:
            cutoff = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00").tz_localize(str(df.index.tz))
        else:
            cutoff = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00")
        return df.loc[df.index <= cutoff]

    @staticmethod
    def _bars_between(df: pd.DataFrame, date: str, start: str, end: str) -> pd.DataFrame:
        sh, sm = map(int, start.split(":"))
        eh, em = map(int, end.split(":"))
        if df.index.tz is not None:
            start_ts = pd.Timestamp(f"{date} {sh:02d}:{sm:02d}:00").tz_localize(str(df.index.tz))
            end_ts = pd.Timestamp(f"{date} {eh:02d}:{em:02d}:00").tz_localize(str(df.index.tz))
        else:
            start_ts = pd.Timestamp(f"{date} {sh:02d}:{sm:02d}:00")
            end_ts = pd.Timestamp(f"{date} {eh:02d}:{em:02d}:00")
        return df.loc[(df.index >= start_ts) & (df.index <= end_ts)]

    def _volume_ratio(self, today_vol: float, date: str, range_start: str, range_end: str) -> float:
        all_dates = self.storage.get_available_dates(self.spy_symbol)
        prior = [d for d in all_dates if d < date][-20:]
        if not prior:
            return np.nan
        prior_vols = []
        for d in prior:
            bars = self.storage.load_minute_bars(self.spy_symbol, d)
            if bars is None or bars.empty:
                continue
            rb = self._bars_between(bars, d, range_start, range_end)
            if not rb.empty:
                prior_vols.append(float(rb["volume"].sum()))
        if not prior_vols:
            return np.nan
        avg = np.mean(prior_vols)
        return np.nan if avg == 0 else today_vol / avg

    @staticmethod
    def _nan_features() -> dict:
        return {
            "opening_range_high": np.nan, "opening_range_low": np.nan,
            "opening_range_width_pct": np.nan, "breakout_direction": np.nan,
            "breakout_magnitude_pct": np.nan, "gap_pct": np.nan,
            "distance_from_vwap_pct": np.nan, "vix_level": np.nan,
            "or_volume_ratio": np.nan,
        }

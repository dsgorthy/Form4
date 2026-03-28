"""
Feature engineering for SPY Noon Session Range Break strategy.

Computes:
- session_high / session_low from the 9AM-noon window
- session_range_width_pct
- vixy_level
- first breakout bar and magnitude post-noon
- breakout RVOL (relative volume at breakout bar vs. 14-day average)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import numpy as np
import pandas as pd

from framework.data.storage import DataStorage

logger = logging.getLogger(__name__)


class NoonBreakFeatureEngine:
    """Computes session range and breakout features for the Noon Break strategy."""

    def __init__(self, storage: Optional[DataStorage] = None):
        self.storage = storage or DataStorage()

    def compute_session_range(
        self,
        date: str,
        range_start: str = "09:00",
        range_end: str = "12:00",
        spy_bars: Optional[pd.DataFrame] = None,
    ) -> Dict:
        """
        Compute the 9AM-noon session range features.

        Parameters
        ----------
        date : str  YYYY-MM-DD
        range_start : str  e.g. "09:00"
        range_end : str    e.g. "12:00"
        spy_bars : DataFrame, optional  Pre-loaded 1Min bars (SPY). If None, loads from storage.

        Returns
        -------
        dict with keys: session_high, session_low, session_range_width_pct,
                        session_open, first_bar_close, vixy_level
        """
        features: Dict = {}

        if spy_bars is None:
            try:
                spy_bars = self.storage.load_minute_bars("SPY", date)
            except Exception as e:
                logger.warning("Could not load SPY bars for %s: %s", date, e)
                return features

        if spy_bars is None or spy_bars.empty:
            return features

        # Parse range bounds
        rsh, rsm = map(int, range_start.split(":"))
        reh, rem = map(int, range_end.split(":"))

        tz = str(spy_bars.index.tz) if spy_bars.index.tz is not None else None

        def ts(h, m):
            t = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00")
            return t.tz_localize(tz) if tz else t

        range_bars = spy_bars.loc[
            (spy_bars.index >= ts(rsh, rsm)) & (spy_bars.index <= ts(reh, rem))
        ]

        if range_bars.empty:
            return features

        features["session_high"] = float(range_bars["high"].max())
        features["session_low"] = float(range_bars["low"].min())
        features["session_open"] = float(range_bars.iloc[0]["open"])
        features["first_bar_close"] = float(range_bars.iloc[0]["close"])

        mid = (features["session_high"] + features["session_low"]) / 2.0
        if mid > 0:
            features["session_range_width_pct"] = (
                (features["session_high"] - features["session_low"]) / mid * 100
            )
        else:
            features["session_range_width_pct"] = np.nan

        # VWAP from range_start to range_end — use Alpaca's pre-computed vwap if available
        # Note: SPY data from Alpaca includes a 'vwap' column (volume-weighted avg price for each bar)
        # We use the last bar's running VWAP as the session VWAP
        vwap_at_end = np.nan
        if "vwap" in range_bars.columns:
            last_vwap = range_bars["vwap"].dropna()
            if not last_vwap.empty:
                vwap_at_end = float(last_vwap.iloc[-1])
        features["session_vwap"] = vwap_at_end
        # 1 = VWAP above session open (bullish), -1 = below (bearish), 0 = flat/unknown
        if not np.isnan(vwap_at_end) and features["session_open"] > 0:
            vwap_vs_open = (vwap_at_end - features["session_open"]) / features["session_open"] * 100
            features["vwap_trend_pct"] = vwap_vs_open
            features["vwap_direction"] = 1 if vwap_vs_open > 0.02 else (-1 if vwap_vs_open < -0.02 else 0)
        else:
            features["vwap_trend_pct"] = np.nan
            features["vwap_direction"] = 0

        # VIXY level (proxy for VIX) — use 12:00 value
        try:
            vixy_bars = self.storage.load_minute_bars("VIXY", date)
            if vixy_bars is not None and not vixy_bars.empty:
                vixy_at_noon = vixy_bars.loc[vixy_bars.index <= ts(reh, rem)]
                features["vixy_level"] = float(vixy_at_noon.iloc[-1]["close"]) if not vixy_at_noon.empty else np.nan
            else:
                features["vixy_level"] = np.nan
        except Exception:
            features["vixy_level"] = np.nan

        return features

    def scan_for_breakout(
        self,
        spy_bars: pd.DataFrame,
        date: str,
        session_high: float,
        session_low: float,
        window_start: str = "12:01",
        window_end: str = "15:00",
        min_break_pct: float = 0.15,
        confirm_bars: int = 2,
        rvol_lookback_days: int = 14,
    ) -> Dict:
        """
        Scan post-noon bars for the first confirmed breakout.

        A breakout is "confirmed" when `confirm_bars` consecutive closes are
        outside the session range by at least `min_break_pct`.

        Also computes RVOL (relative volume) at the confirmation bar by comparing
        to the same-minute average volume over the past `rvol_lookback_days` days.

        Returns dict with:
            breakout_direction: 1 (up), -1 (down), 0 (none)
            breakout_bar_ts: Timestamp of confirmation bar
            breakout_entry_price: Close of confirmation bar
            breakout_magnitude_pct: How far beyond the range
            breakout_rvol: Relative volume vs historical average (1.0 = average)
        """
        wsh, wsm = map(int, window_start.split(":"))
        weh, wem = map(int, window_end.split(":"))

        tz = str(spy_bars.index.tz) if spy_bars.index.tz is not None else None

        def ts(h, m):
            t = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00")
            return t.tz_localize(tz) if tz else t

        window_bars = spy_bars.loc[
            (spy_bars.index >= ts(wsh, wsm)) & (spy_bars.index <= ts(weh, wem))
        ]

        if window_bars.empty:
            return {"breakout_direction": 0, "breakout_entry_price": None,
                    "breakout_bar_ts": None, "breakout_rvol": 1.0}

        upper_threshold = session_high * (1.0 + min_break_pct / 100.0)
        lower_threshold = session_low * (1.0 - min_break_pct / 100.0)

        consecutive_up = 0
        consecutive_down = 0

        for ts_bar, row in window_bars.iterrows():
            close = float(row["close"])
            vol = float(row.get("volume", 0))

            if close > upper_threshold:
                consecutive_up += 1
                consecutive_down = 0
            elif close < lower_threshold:
                consecutive_down += 1
                consecutive_up = 0
            else:
                consecutive_up = 0
                consecutive_down = 0

            if consecutive_up >= confirm_bars:
                magnitude = (close - session_high) / session_high * 100
                rvol = self._compute_rvol(date, ts_bar, vol, rvol_lookback_days)
                return {
                    "breakout_direction": 1,
                    "breakout_bar_ts": ts_bar,
                    "breakout_entry_price": close,
                    "breakout_magnitude_pct": magnitude,
                    "breakout_rvol": rvol,
                }
            if consecutive_down >= confirm_bars:
                magnitude = (session_low - close) / session_low * 100
                rvol = self._compute_rvol(date, ts_bar, vol, rvol_lookback_days)
                return {
                    "breakout_direction": -1,
                    "breakout_bar_ts": ts_bar,
                    "breakout_entry_price": close,
                    "breakout_magnitude_pct": magnitude,
                    "breakout_rvol": rvol,
                }

        return {
            "breakout_direction": 0,
            "breakout_entry_price": None,
            "breakout_bar_ts": None,
            "breakout_magnitude_pct": 0.0,
            "breakout_rvol": 1.0,
        }

    def _compute_rvol(
        self,
        date: str,
        breakout_ts: pd.Timestamp,
        current_volume: float,
        lookback_days: int,
    ) -> float:
        """
        Relative volume at the breakout bar vs. the same-minute historical average.

        Loads the past `lookback_days` trading days from storage, finds the bar at
        the same clock time as the breakout, and returns current_volume / mean_historical.
        Returns 1.0 if historical data is insufficient.
        """
        if current_volume <= 0:
            return 1.0

        # Collect past trading day dates before `date`
        from framework.data.calendar import MarketCalendar
        cal = MarketCalendar()
        # Look back far enough to find lookback_days trading days
        lookback_start = (
            datetime.strptime(date, "%Y-%m-%d") - timedelta(days=lookback_days * 2)
        ).strftime("%Y-%m-%d")
        all_days = cal.get_trading_days(lookback_start, date)
        # Exclude today
        past_days = [d for d in all_days if d < date][-lookback_days:]

        if not past_days:
            return 1.0

        breakout_h = breakout_ts.hour
        breakout_m = breakout_ts.minute
        tz = str(breakout_ts.tz) if breakout_ts.tz is not None else None

        historical_volumes = []
        for hist_date in past_days:
            try:
                hist_bars = self.storage.load_minute_bars("SPY", hist_date)
                if hist_bars is None or hist_bars.empty:
                    continue
                bar_tz = str(hist_bars.index.tz) if hist_bars.index.tz is not None else None
                t = pd.Timestamp(f"{hist_date} {breakout_h:02d}:{breakout_m:02d}:00")
                t = t.tz_localize(bar_tz) if bar_tz else t
                bar_vol = hist_bars.loc[hist_bars.index == t]
                if not bar_vol.empty:
                    v = float(bar_vol.iloc[0].get("volume", 0))
                    if v > 0:
                        historical_volumes.append(v)
            except Exception:
                continue

        if not historical_volumes:
            return 1.0

        avg_vol = float(np.mean(historical_volumes))
        if avg_vol <= 0:
            return 1.0

        return current_volume / avg_vol

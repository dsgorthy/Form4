"""
Feature engineering for the SPY 0DTE end-of-day reversal strategy.
Migrated from spy-0dte; no settings import.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from framework.signals.indicators import rsi, vwap, rolling_percentile

logger = logging.getLogger(__name__)


class FeatureEngine:
    """
    Computes features at a given intraday entry time for each trading day.

    Parameters
    ----------
    storage : DataStorage
        Data storage instance.
    spy_symbol : str
        Primary symbol (default "SPY").
    vixy_symbol : str
        Volatility proxy symbol (default "VIXY").
    """

    def __init__(
        self,
        storage,
        spy_symbol: str = "SPY",
        vixy_symbol: str = "VIXY",
    ):
        self.storage = storage
        self.spy_symbol = spy_symbol
        self.vixy_symbol = vixy_symbol

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_features(self, date: str, entry_time: str = "15:29") -> dict:
        """Compute all features for a single trading day at the given entry time."""
        spy_bars = self.storage.load_minute_bars(self.spy_symbol, date)
        vixy_bars = self.storage.load_minute_bars(self.vixy_symbol, date)

        features: dict = {"date": date}

        if spy_bars is None or spy_bars.empty:
            logger.warning("No SPY data for %s", date)
            features.update(self._nan_features())
            return features

        spy_up_to = self._bars_up_to(spy_bars, date, entry_time)

        if spy_up_to.empty:
            features.update(self._nan_features())
            return features

        entry_bar = spy_up_to.iloc[-1]
        price_at_entry = float(entry_bar["close"])
        day_open = float(spy_bars.iloc[0]["open"])
        day_high = float(spy_up_to["high"].max())
        day_low = float(spy_up_to["low"].min())

        features["intraday_return"] = (price_at_entry - day_open) / day_open

        vixy_price = self._vixy_price_at(vixy_bars, date, entry_time)
        features["vix_level"] = vixy_price
        features["vix_percentile_20d"] = self._vix_percentile(date, entry_time)

        vwap_at_entry = self._vwap_at_entry(spy_up_to)
        if np.isnan(vwap_at_entry) or vwap_at_entry == 0.0:
            features["distance_from_vwap"] = np.nan
        else:
            features["distance_from_vwap"] = (price_at_entry - vwap_at_entry) / vwap_at_entry

        features["intraday_range_pct"] = (day_high - day_low) / day_open

        range_size = day_high - day_low
        features["price_position_in_range"] = (
            np.nan if range_size == 0.0 else (price_at_entry - day_low) / range_size
        )

        features["rsi_14_5min"] = self._rsi_5min(spy_up_to)
        features["volume_ratio"] = self._volume_ratio(spy_up_to, date, entry_time)
        features["gap_size"] = self._gap_size(date)
        features["trend_30min"] = self._trend(spy_up_to, minutes=30)
        features["trend_60min"] = self._trend(spy_up_to, minutes=60)

        dt = datetime.strptime(date, "%Y-%m-%d")
        features["day_of_week"] = dt.weekday()

        hour, minute = map(int, entry_time.split(":"))
        features["hour_minute"] = hour + minute / 60.0

        return features

    def compute_features_range(
        self, start_date: str, end_date: str, entry_time: str = "15:29"
    ) -> pd.DataFrame:
        available_dates = self.storage.get_available_dates(self.spy_symbol)
        dates_in_range = [d for d in available_dates if start_date <= d <= end_date]
        if not dates_in_range:
            return pd.DataFrame()
        rows = []
        for d in dates_in_range:
            features = self.compute_features(d, entry_time)
            targets = self.compute_reversal_metrics(d, entry_time)
            rows.append({**features, **targets})
        df = pd.DataFrame(rows)
        if "date" in df.columns:
            df.set_index("date", inplace=True)
        return df

    def compute_reversal_metrics(self, date: str, entry_time: str = "15:29") -> dict:
        spy_bars = self.storage.load_minute_bars(self.spy_symbol, date)
        if spy_bars is None or spy_bars.empty:
            return self._nan_targets()
        spy_up_to = self._bars_up_to(spy_bars, date, entry_time)
        if spy_up_to.empty:
            return self._nan_targets()

        entry_bar = spy_up_to.iloc[-1]
        price_at_entry = float(entry_bar["close"])
        day_open = float(spy_bars.iloc[0]["open"])
        day_close = float(spy_bars.iloc[-1]["close"])

        spy_after = self._bars_after(spy_bars, date, entry_time)

        reversal_magnitude = (day_close - price_at_entry) / price_at_entry
        dist_at_entry = abs(price_at_entry - day_open)
        dist_at_close = abs(day_close - day_open)
        reversal_toward_open = dist_at_entry - dist_at_close
        did_reverse = dist_at_close < dist_at_entry

        if not spy_after.empty:
            post_highs = spy_after["high"]
            post_lows = spy_after["low"]
            if price_at_entry > day_open:
                mfe = (price_at_entry - float(post_lows.min())) / price_at_entry
                mae = (float(post_highs.max()) - price_at_entry) / price_at_entry
            else:
                mfe = (float(post_highs.max()) - price_at_entry) / price_at_entry
                mae = (price_at_entry - float(post_lows.min())) / price_at_entry
        else:
            mfe = mae = np.nan

        return {
            "reversal_magnitude": reversal_magnitude,
            "reversal_toward_open": reversal_toward_open,
            "mfe": mfe, "mae": mae, "did_reverse": did_reverse,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _bars_up_to(df: pd.DataFrame, date: str, entry_time: str) -> pd.DataFrame:
        hour, minute = map(int, entry_time.split(":"))
        if df.index.tz is not None:
            cutoff = pd.Timestamp(f"{date} {hour:02d}:{minute:02d}:00").tz_localize(str(df.index.tz))
        else:
            cutoff = pd.Timestamp(f"{date} {hour:02d}:{minute:02d}:00")
        return df.loc[df.index <= cutoff]

    @staticmethod
    def _bars_after(df: pd.DataFrame, date: str, entry_time: str) -> pd.DataFrame:
        hour, minute = map(int, entry_time.split(":"))
        if df.index.tz is not None:
            cutoff = pd.Timestamp(f"{date} {hour:02d}:{minute:02d}:00").tz_localize(str(df.index.tz))
        else:
            cutoff = pd.Timestamp(f"{date} {hour:02d}:{minute:02d}:00")
        return df.loc[df.index > cutoff]

    def _vixy_price_at(self, vixy_bars, date, entry_time):
        if vixy_bars is None or vixy_bars.empty:
            return np.nan
        vixy_up_to = self._bars_up_to(vixy_bars, date, entry_time)
        if vixy_up_to.empty:
            return np.nan
        return float(vixy_up_to.iloc[-1]["close"])

    def _vix_percentile(self, date, entry_time):
        today_vixy = self.storage.load_minute_bars(self.vixy_symbol, date)
        if today_vixy is None or today_vixy.empty:
            return np.nan
        today_price = self._vixy_price_at(today_vixy, date, entry_time)
        if np.isnan(today_price):
            return np.nan
        all_dates = self.storage.get_available_dates(self.vixy_symbol)
        prior_dates = [d for d in all_dates if d < date][-20:]
        if len(prior_dates) < 20:
            return np.nan
        prior_prices = []
        for d in prior_dates:
            bars = self.storage.load_minute_bars(self.vixy_symbol, d)
            price = self._vixy_price_at(bars, d, entry_time)
            if not np.isnan(price):
                prior_prices.append(price)
        if len(prior_prices) < 20:
            return np.nan
        return rolling_percentile(pd.Series(prior_prices), today_price, window=20)

    @staticmethod
    def _vwap_at_entry(spy_up_to):
        vwap_series = vwap(spy_up_to)
        if vwap_series.empty or vwap_series.isna().all():
            return np.nan
        return float(vwap_series.iloc[-1])

    @staticmethod
    def _rsi_5min(spy_up_to):
        if spy_up_to.empty:
            return np.nan
        resampled = spy_up_to.resample("5min").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
        }).dropna(subset=["close"])
        if resampled.empty or len(resampled) < 15:
            return np.nan
        rsi_values = rsi(resampled["close"], period=14)
        last_valid = rsi_values.dropna()
        return float(last_valid.iloc[-1]) if not last_valid.empty else np.nan

    def _volume_ratio(self, spy_up_to, date, entry_time):
        if spy_up_to.empty:
            return np.nan
        today_vol = float(spy_up_to["volume"].sum())
        all_dates = self.storage.get_available_dates(self.spy_symbol)
        prior_dates = [d for d in all_dates if d < date][-20:]
        if not prior_dates:
            return np.nan
        prior_vols = []
        for d in prior_dates:
            bars = self.storage.load_minute_bars(self.spy_symbol, d)
            if bars is None or bars.empty:
                continue
            up_to = self._bars_up_to(bars, d, entry_time)
            if not up_to.empty:
                prior_vols.append(float(up_to["volume"].sum()))
        if not prior_vols:
            return np.nan
        avg_vol = np.mean(prior_vols)
        return np.nan if avg_vol == 0.0 else today_vol / avg_vol

    def _gap_size(self, date):
        all_dates = self.storage.get_available_dates(self.spy_symbol)
        try:
            idx = all_dates.index(date)
        except ValueError:
            return np.nan
        if idx == 0:
            return np.nan
        prev_date = all_dates[idx - 1]
        today_bars = self.storage.load_minute_bars(self.spy_symbol, date)
        prev_bars = self.storage.load_minute_bars(self.spy_symbol, prev_date)
        if not today_bars is not None and not today_bars.empty:
            return np.nan
        if not prev_bars is not None and not prev_bars.empty:
            return np.nan
        today_open = float(today_bars.iloc[0]["open"])
        prev_close = float(prev_bars.iloc[-1]["close"])
        return np.nan if prev_close == 0.0 else (today_open - prev_close) / prev_close

    @staticmethod
    def _trend(spy_up_to, minutes):
        if spy_up_to.empty or len(spy_up_to) < 2:
            return np.nan
        entry_price = float(spy_up_to.iloc[-1]["close"])
        entry_ts = spy_up_to.index[-1]
        lookback_ts = entry_ts - pd.Timedelta(minutes=minutes)
        earlier_bars = spy_up_to.loc[spy_up_to.index <= lookback_ts]
        earlier_price = float(spy_up_to.iloc[0]["close"]) if earlier_bars.empty else float(earlier_bars.iloc[-1]["close"])
        return np.nan if earlier_price == 0.0 else (entry_price - earlier_price) / earlier_price

    @staticmethod
    def _nan_features():
        return {k: np.nan for k in [
            "intraday_return", "vix_level", "vix_percentile_20d", "distance_from_vwap",
            "intraday_range_pct", "price_position_in_range", "rsi_14_5min",
            "volume_ratio", "gap_size", "trend_30min", "trend_60min",
            "day_of_week", "hour_minute",
        ]}

    @staticmethod
    def _nan_targets():
        return {k: np.nan for k in [
            "reversal_magnitude", "reversal_toward_open", "mfe", "mae", "did_reverse"
        ]}

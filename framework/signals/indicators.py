"""
Technical indicator calculations for the trading framework.

Pure functions — each takes a pandas DataFrame or Series and returns
computed values.  No TA-Lib dependency; uses numpy/pandas only.
"""

import numpy as np
import pandas as pd


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index using Wilder's EMA (alpha = 1/period)."""
    if series.empty or len(series) < period + 1:
        return pd.Series(np.nan, index=series.index, dtype=float)
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    alpha = 1.0 / period
    avg_gain = gain.ewm(alpha=alpha, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    result = 100.0 - (100.0 / (1.0 + rs))
    result[avg_loss == 0] = 100.0
    result[avg_gain == 0] = 0.0
    return result


def vwap(df: pd.DataFrame) -> pd.Series:
    """Cumulative intraday VWAP."""
    required = {"open", "high", "low", "close", "volume"}
    if df.empty or not required.issubset(df.columns):
        return pd.Series(np.nan, index=df.index, dtype=float)
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    cumulative_tp_vol = (typical_price * df["volume"]).cumsum()
    cumulative_vol = df["volume"].cumsum()
    result = cumulative_tp_vol / cumulative_vol
    result[cumulative_vol == 0] = np.nan
    return result


def sma(series: pd.Series, period: int) -> pd.Series:
    """Simple moving average."""
    if series.empty or len(series) < period:
        return pd.Series(np.nan, index=series.index, dtype=float)
    return series.rolling(window=period, min_periods=period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential moving average (span formula, alpha = 2/(period+1))."""
    if series.empty or len(series) < period:
        return pd.Series(np.nan, index=series.index, dtype=float)
    return series.ewm(span=period, min_periods=period, adjust=False).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range using Wilder smoothing."""
    required = {"high", "low", "close"}
    if df.empty or not required.issubset(df.columns) or len(df) < period + 1:
        return pd.Series(np.nan, index=df.index, dtype=float)
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    alpha = 1.0 / period
    return tr.ewm(alpha=alpha, min_periods=period, adjust=False).mean()


def rolling_percentile(series: pd.Series, value: float, window: int = 20) -> float:
    """Where value falls in the last window values of series."""
    if series.empty or len(series) < window:
        return np.nan
    recent = series.iloc[-window:].values
    return float(np.sum(recent <= value) / len(recent))

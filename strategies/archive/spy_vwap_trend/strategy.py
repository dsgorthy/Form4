"""
SPY VWAP Mean Reversion Strategy — BaseStrategy implementation.

Hypothesis:
    Price deviating significantly from VWAP by 10:30 AM tends to revert toward
    VWAP during the remainder of the session, not continue trending away from it.

    Mechanism: VWAP acts as a gravitational anchor throughout the trading day.
    Institutional VWAP-benchmarked algos (passive, execution-oriented) continuously
    trade to achieve VWAP prices. A significant 10:30 AM deviation represents an
    overshoot driven by directional flow in the opening hour; as that order flow
    exhausts itself, VWAP-seeking institutional volume pulls price back toward the
    benchmark. Price seldom "escapes" VWAP in a single session.

    Academic anchors:
    - Madhavan (2002): VWAP as the dominant institutional execution benchmark;
      VWAP-benchmarked orders create persistent intraday mean reversion around VWAP.
    - Berkowitz et al. (1988): intraday mean reversion in equity returns is stronger
      after the first trading hour (post-10:00 AM) when opening imbalances clear.
    - Almgren & Chriss (2001) optimal execution: VWAP-anchored strategies imply
      counter-trend order flow that resists large deviations.

    Empirical observation: In our 2025 and 2026 YTD data, the original VWAP
    trend-following approach (above VWAP → long) produced 28.6% win rate — meaning
    71.4% of the time, price did NOT trend in the deviation direction after 10:30 AM.
    This is the signal; the trend assumption was inverted.

Signal:
    At 10:30 AM, compute VWAP from 9:30 AM.
    If SPY close is >= 0.15% ABOVE VWAP and has been above for >= 15 bars:
      → go SHORT (expect reversion back toward VWAP, target 0.20%)
    If SPY close is <= -0.15% BELOW VWAP and has been below for >= 15 bars:
      → go LONG (expect reversion back toward VWAP, target 0.20%)

    Hold to 3:30 PM. Stop on 0.20% adverse (deviation extends further) or 0.20% target.
    The 15-bar sustained requirement filters noise crossings while confirming the
    deviation has been real and persistent (but not so long it signals a breakout trend).

Filters:
    - VIXY 12–50: normal volatility regime (extremes create unpredictable flow)
    - Reject FOMC days
    - Min opening move 0.05%: flat opens produce no meaningful VWAP deviation

Note on short exposure:
    Short SPY requires a locate (margin account). Paper/live trading: ensure short
    selling is enabled. Backtesting: engine handles short P&L via direction_mult.

Version history:
    v1.0: VWAP trend-following (above VWAP → long). Results 2024: Sharpe 1.24 (anomaly).
    v1.1: Lowered min_deviation to 0.10%. Still trend-following.
    v2.0: Signal flipped to mean reversion. Reduced target/sustained_bars for MR fit.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar, EconomicCalendar

logger = logging.getLogger(__name__)


def compute_vwap(bars: pd.DataFrame) -> pd.Series:
    """Compute cumulative VWAP from the first bar of the DataFrame."""
    typical_price = (bars["high"] + bars["low"] + bars["close"]) / 3.0
    volume = bars["volume"].replace(0, 1)
    return (typical_price * volume).cumsum() / volume.cumsum()


class SpyVwapTrendStrategy(BaseStrategy):
    """
    SPY VWAP Mean Reversion.

    At 10:30 AM, price significantly above VWAP → SHORT (expect reversion).
    Price significantly below VWAP → LONG (expect reversion).
    Target 0.20% reversion, stop 0.20% extension, time stop 3:30 PM.
    """

    def __init__(self, config: dict, storage: Optional[DataStorage] = None):
        super().__init__(config)
        self._storage = storage or DataStorage()
        self._calendar = MarketCalendar()
        self._econ_calendar = EconomicCalendar()

        class _FeatureEngineStub:
            def __init__(self, storage):
                self.storage = storage
        self._feature_engine = _FeatureEngineStub(self._storage)

        entry_cfg = config.get("entry", {})
        exit_cfg = config.get("exit", {})
        filter_cfg = config.get("filters", {})

        self._entry_time = entry_cfg.get("time", "10:30")
        self._min_deviation_pct = float(entry_cfg.get("min_vwap_deviation_pct", 0.15))
        self._max_deviation_pct = float(entry_cfg.get("max_vwap_deviation_pct", 0.0))  # 0 = no cap
        self._min_sustained_bars = int(entry_cfg.get("min_sustained_bars", 15))

        self._time_stop = exit_cfg.get("time_stop", "15:30")
        self._target_pct = float(exit_cfg.get("target_pct", 0.20))
        self._stop_pct = float(exit_cfg.get("stop_pct", 0.20))

        self._max_vixy = float(filter_cfg.get("max_vixy", 50.0))
        self._min_vixy = float(filter_cfg.get("min_vixy", 12.0))
        self._reject_on_fomc = bool(filter_cfg.get("reject_on_fomc", True))
        self._min_opening_move = float(filter_cfg.get("min_opening_move_pct", 0.05))

    def strategy_name(self) -> str:
        return "spy_vwap_trend"

    def data_requirements(self) -> DataRequirements:
        data_cfg = self.config.get("data", {})
        return DataRequirements(
            symbols=data_cfg.get("symbols", ["SPY", "VIXY"]),
            timeframes=data_cfg.get("timeframes", ["1Min"]),
            lookback_days=data_cfg.get("lookback_days", 30),
            requires_options=False,
        )

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        """
        Compute VWAP from 9:30 AM. At 10:30 AM, price significantly above VWAP
        → SHORT (mean reversion); price significantly below VWAP → LONG.
        """
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None)

        tz = str(spy_bars.index.tz) if spy_bars.index.tz is not None else None

        def ts(time_str):
            h, m = map(int, time_str.split(":"))
            t = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00")
            return t.tz_localize(tz) if tz else t

        # VIXY filter
        vixy_bars = bars.get("VIXY_1Min", pd.DataFrame())
        vixy_level = np.nan
        if not vixy_bars.empty:
            entry_vixy = vixy_bars.loc[vixy_bars.index <= ts(self._entry_time)]
            if not entry_vixy.empty:
                vixy_level = float(entry_vixy.iloc[-1]["close"])

        if not np.isnan(vixy_level):
            if vixy_level > self._max_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vixy_too_high:{vixy_level:.1f}"})
            if vixy_level < self._min_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vixy_too_low:{vixy_level:.1f}"})

        if self._reject_on_fomc and self._econ_calendar.is_fomc_day(date):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "fomc_day"})

        # Compute VWAP from 9:30 AM on bars up to entry time
        session_bars = spy_bars.loc[
            (spy_bars.index >= ts("09:30")) & (spy_bars.index <= ts(self._entry_time))
        ]
        if len(session_bars) < 30:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "insufficient_vwap_bars"})

        vwap_series = compute_vwap(session_bars)
        session_bars = session_bars.copy()
        session_bars["vwap"] = vwap_series

        # Opening move filter: skip flat opens
        open_price = float(session_bars.iloc[0]["open"])
        current_price = float(session_bars.iloc[-1]["close"])
        current_vwap = float(vwap_series.iloc[-1])

        if open_price > 0:
            opening_move_pct = abs((current_price - open_price) / open_price * 100)
            if opening_move_pct < self._min_opening_move:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"opening_too_flat:{opening_move_pct:.3f}%"})

        # Check VWAP deviation
        vwap_deviation = (current_price - current_vwap) / current_vwap * 100.0

        if abs(vwap_deviation) < self._min_deviation_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": f"deviation_too_small:{vwap_deviation:.3f}%"})

        if self._max_deviation_pct > 0 and abs(vwap_deviation) > self._max_deviation_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": f"deviation_too_large:{vwap_deviation:.3f}%_likely_trend"})

        # Check sustained bars on same side of VWAP
        closes = session_bars["close"].values
        vwaps = session_bars["vwap"].values
        n = min(self._min_sustained_bars, len(closes))

        recent_closes = closes[-n:]
        recent_vwaps = vwaps[-n:]

        if vwap_deviation > 0:
            # Price above VWAP → SHORT (mean reversion: expect price to fall back)
            above_vwap = np.sum(recent_closes > recent_vwaps)
            if above_vwap < int(n * 0.75):
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"not_sustained_above_vwap:{above_vwap}/{n}"})
            direction = "short"
        else:
            # Price below VWAP → LONG (mean reversion: expect price to rise back)
            below_vwap = np.sum(recent_closes < recent_vwaps)
            if below_vwap < int(n * 0.75):
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"not_sustained_below_vwap:{below_vwap}/{n}"})
            direction = "long"

        features = {
            "vwap_deviation_pct": vwap_deviation,
            "current_vwap": current_vwap,
            "current_price": current_price,
            "sustained_bars": n,
            "vixy_level": vixy_level,
        }

        # Higher confidence with larger deviations — more rubber band tension
        confidence = min(0.9, 0.5 + abs(vwap_deviation) / 2.0)

        return Signal(
            direction=direction,
            confidence=confidence,
            instrument={"entry_price": current_price},
            metadata=features,
        )

    def select_instrument(
        self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str
    ) -> dict:
        instrument_info = signal.instrument or {}
        entry_price = float(instrument_info.get("entry_price", 0))

        if entry_price <= 0:
            spy_bars = bars.get("SPY_1Min", pd.DataFrame())
            if spy_bars.empty:
                return {}
            entry_price = float(spy_bars.iloc[-1]["close"])

        if signal.direction == "long":
            target_price = entry_price * (1.0 + self._target_pct / 100.0)
            stop_price = entry_price * (1.0 - self._stop_pct / 100.0)
        else:
            target_price = entry_price * (1.0 - self._target_pct / 100.0)
            stop_price = entry_price * (1.0 + self._stop_pct / 100.0)

        time_stop_mins = self._time_to_minutes(self._time_stop)

        def _get_current_price(current_bars, ts_val):
            sb = current_bars.get("SPY_1Min", pd.DataFrame())
            return float(sb.iloc[-1]["close"]) if not sb.empty else entry_price

        return {
            "type": "equity",
            "symbol": "SPY",
            "entry_price": entry_price,
            "_direction": signal.direction.upper(),
            "_target_price": target_price,
            "_stop_price": stop_price,
            "_time_stop_mins": time_stop_mins,
            "_get_current_price": _get_current_price,
            "current_price": entry_price,
        }

    def should_exit(
        self, position: dict, bars: Dict[str, pd.DataFrame]
    ) -> Optional[str]:
        instrument = position.get("instrument", {})
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())

        if spy_bars.empty:
            return None

        current_spy = float(spy_bars.iloc[-1]["close"])
        current_ts = spy_bars.index[-1]
        current_mins = current_ts.hour * 60 + current_ts.minute

        direction = instrument.get("_direction", "LONG")
        target_price = instrument.get("_target_price")
        stop_price = instrument.get("_stop_price")
        time_stop_mins = instrument.get("_time_stop_mins", self._time_to_minutes(self._time_stop))

        if current_mins >= time_stop_mins:
            return "time_stop"

        if target_price is not None and stop_price is not None:
            if direction == "LONG":
                if current_spy >= target_price:
                    return "target"
                if current_spy <= stop_price:
                    return "stop"
            else:
                if current_spy <= target_price:
                    return "target"
                if current_spy >= stop_price:
                    return "stop"

        return None

    @staticmethod
    def _time_to_minutes(t: str) -> int:
        h, m = map(int, t.split(":"))
        return h * 60 + m

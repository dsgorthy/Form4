"""
SPY First Half-Hour Momentum Strategy — BaseStrategy implementation.

Academic basis:
    Gao, Han, Li & Zhou (2018). "Intraday Momentum: The First Half-Hour Return
    Predicts the Last Half-Hour Return." NBER Working Paper 25120.

    Key finding: For the S&P 500 index (and SPY ETF), the return over the first
    30 minutes (9:30–10:00) is a statistically significant predictor of the return
    over the last 30 minutes (15:30–16:00) and the full day direction.
    Effect is strongest on high-volume, high-VIX days.

    Additional support:
    - Guo et al. (2023): Out-of-sample validation across global ETFs
    - Quantpedia Strategy #181: empirical replication
    - Market microstructure: institutional informed order flow concentrated at open;
      end-of-day rebalancing amplifies the same direction

Entry:
    3:30 PM — trade executes at the START of the last half-hour.
    Signal = sign of 9:30–10:00 AM return (measured at 10:00 AM, held until 3:30 PM
    to confirm the first-half trend, then execute in that direction).

    This is the canonical Gao et al. implementation:
      "The first half-hour return positively predicts the last half-hour return."
    The 6-hour gap between signal and execution is intentional — the signal (at 10:00)
    is used to pre-commit direction for the last 30 minutes.

Exit:
    - Target: +0.25% from entry (achievable in 30-min hold)
    - Stop:   -0.12% from entry (tight — short hold)
    - Time:   3:59 PM — last minute before close, no overnight holds
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


class SpyFirst30MomentumStrategy(BaseStrategy):
    """
    SPY First 30-Minute Return Momentum.

    Long (short) SPY at 10:01 when the 9:30-10:00 return is strongly positive (negative).
    Holds until 3:30 PM, target, or stop.
    """

    def __init__(self, config: dict, storage: Optional[DataStorage] = None):
        super().__init__(config)
        self._storage = storage or DataStorage()
        self._calendar = MarketCalendar()
        self._econ_calendar = EconomicCalendar()

        # Create stub feature_engine for storage injection compatibility
        class _FeatureEngineStub:
            def __init__(self, storage):
                self.storage = storage
        self._feature_engine = _FeatureEngineStub(self._storage)

        entry_cfg = config.get("entry", {})
        exit_cfg = config.get("exit", {})
        filter_cfg = config.get("filters", {})

        self._signal_window_start = entry_cfg.get("signal_window_start", "09:30")
        self._signal_window_end = entry_cfg.get("signal_window_end", "10:00")
        self._entry_time = entry_cfg.get("time", "15:30")
        self._min_signal_pct = float(entry_cfg.get("min_signal_pct", 0.15))

        self._time_stop = exit_cfg.get("time_stop", "15:59")
        self._target_pct = float(exit_cfg.get("target_pct", 0.25))
        self._stop_pct = float(exit_cfg.get("stop_pct", 0.12))

        self._max_vixy = float(filter_cfg.get("max_vixy", 45.0))
        self._min_vixy = float(filter_cfg.get("min_vixy", 10.0))
        self._reject_on_fomc = bool(filter_cfg.get("reject_on_fomc", True))

    def strategy_name(self) -> str:
        return "spy_first30_momentum"

    def data_requirements(self) -> DataRequirements:
        data_cfg = self.config.get("data", {})
        return DataRequirements(
            symbols=data_cfg.get("symbols", ["SPY", "VIXY"]),
            timeframes=data_cfg.get("timeframes", ["1Min", "5Min"]),
            lookback_days=data_cfg.get("lookback_days", 30),
            requires_options=False,
        )

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        """
        Compute 9:30–10:00 SPY return. Signal if magnitude >= min_signal_pct.
        """
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None)

        tz = str(spy_bars.index.tz) if spy_bars.index.tz is not None else None

        def ts(time_str):
            h, m = map(int, time_str.split(":"))
            t = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00")
            return t.tz_localize(tz) if tz else t

        # Get window open (9:30 first bar) and close (10:00 last bar)
        window_bars = spy_bars.loc[
            (spy_bars.index >= ts(self._signal_window_start)) &
            (spy_bars.index <= ts(self._signal_window_end))
        ]

        if len(window_bars) < 10:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "insufficient_window_bars"})

        open_price = float(window_bars.iloc[0]["open"])
        close_price = float(window_bars.iloc[-1]["close"])

        if open_price <= 0:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "zero_open_price"})

        first30_return_pct = (close_price - open_price) / open_price * 100.0

        features = {
            "first30_return_pct": first30_return_pct,
            "first30_open": open_price,
            "first30_close": close_price,
        }

        # VIXY filter
        vixy_bars = bars.get("VIXY_1Min", pd.DataFrame())
        vixy_level = np.nan
        if not vixy_bars.empty:
            vixy_at_entry = vixy_bars.loc[vixy_bars.index <= ts(self._signal_window_end)]
            if not vixy_at_entry.empty:
                vixy_level = float(vixy_at_entry.iloc[-1]["close"])
        features["vixy_level"] = vixy_level

        if not np.isnan(vixy_level):
            if vixy_level > self._max_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={**features, "skip_reason": f"vixy_too_high:{vixy_level:.1f}"})
            if vixy_level < self._min_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={**features, "skip_reason": f"vixy_too_low:{vixy_level:.1f}"})

        if self._reject_on_fomc and self._econ_calendar.is_fomc_day(date):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={**features, "skip_reason": "fomc_day"})

        # Signal threshold
        if abs(first30_return_pct) < self._min_signal_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={**features, "skip_reason": f"signal_too_small:{first30_return_pct:.3f}%"})

        direction = "long" if first30_return_pct > 0 else "short"
        confidence = min(0.9, 0.5 + abs(first30_return_pct) / 2.0)

        return Signal(
            direction=direction,
            confidence=confidence,
            instrument={"first30_close": close_price},
            metadata=features,
        )

    def select_instrument(
        self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str
    ) -> dict:
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

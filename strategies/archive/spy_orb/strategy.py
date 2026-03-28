"""
SPY Opening Range Breakout Strategy — BaseStrategy implementation.

Hypothesis: When SPY breaks above/below the 9:30–10:00 AM opening range,
the breakout direction tends to continue through at least 10:30 AM.
This is a momentum (not reversal) strategy — validates framework generality.

Entry: 10:00–10:30 AM window, on confirmed breakout
Exit: Target (0.50% SPY move) or Stop (0.25% adverse) or 3:30 PM time stop
Asset: SPY equity (can be swapped to options via config)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar, EconomicCalendar
from strategies.archive.spy_orb.features import ORBFeatureEngine

logger = logging.getLogger(__name__)


class SpyOrbStrategy(BaseStrategy):
    """
    SPY Opening Range Breakout.

    Buy SPY (or SPY options) when price breaks above the opening range.
    Short SPY when price breaks below the opening range.
    """

    def __init__(self, config: dict, storage: Optional[DataStorage] = None):
        super().__init__(config)
        self._storage = storage or DataStorage()
        self._calendar = MarketCalendar()
        self._econ_calendar = EconomicCalendar()
        self._feature_engine = ORBFeatureEngine(storage=self._storage)

        # Config
        entry_cfg = config.get("entry", {})
        exit_cfg = config.get("exit", {})
        filter_cfg = config.get("filters", {})
        or_cfg = config.get("opening_range", {})
        instrument_cfg = config.get("instrument", {})

        self._range_start = or_cfg.get("start_time", "09:30")
        self._range_end = or_cfg.get("end_time", "10:00")
        self._entry_window_start = entry_cfg.get("time_window_start", "10:00")
        self._entry_window_end = entry_cfg.get("time_window_end", "10:30")
        self._time_stop = exit_cfg.get("time_stop", "15:30")
        self._stop_mode = exit_cfg.get("stop_mode", "underlying")
        self._stop_pct = float(exit_cfg.get("stop_underlying_pct", 0.25))
        self._target_pct = float(exit_cfg.get("target_underlying_pct", 0.50))
        self._instrument_type = instrument_cfg.get("type", "equity")

        self._min_range_width = float(filter_cfg.get("min_range_width_pct", 0.20))
        self._max_range_width = float(filter_cfg.get("max_range_width_pct", 2.0))
        self._min_vix = float(filter_cfg.get("min_vix", 12.0))
        self._max_vix = float(filter_cfg.get("max_vix", 60.0))
        self._reject_on_fomc = bool(filter_cfg.get("reject_on_fomc", True))

    def strategy_name(self) -> str:
        return "spy_orb"

    def data_requirements(self) -> DataRequirements:
        data_cfg = self.config.get("data", {})
        return DataRequirements(
            symbols=data_cfg.get("symbols", ["SPY", "VIXY"]),
            timeframes=data_cfg.get("timeframes", ["1Min", "5Min", "15Min"]),
            lookback_days=data_cfg.get("lookback_days", 30),
            requires_options=data_cfg.get("requires_options", False),
        )

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        """
        Check for opening range breakout anywhere in the entry window (10:01–10:30).
        Scans each minute bar and takes the first confirmed breakout.
        """
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None)

        # Compute ORB features using range definition (single-bar check just for range/vix)
        features = self._feature_engine.compute_features(
            date,
            range_start=self._range_start,
            range_end=self._range_end,
            check_time=self._entry_window_start,  # initial check at 10:01
        )

        or_high = features.get("opening_range_high", np.nan)
        or_low = features.get("opening_range_low", np.nan)
        range_width = features.get("opening_range_width_pct", np.nan)
        vix_level = features.get("vix_level", np.nan)

        # If range isn't valid, skip
        if np.isnan(or_high) or np.isnan(or_low):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "no_opening_range"})

        # Check the LAST bar in the entry window (10:30) for confirmed breakout.
        # Using the final bar ensures entry_price == signal_price == same bar close,
        # so there's no "already moved" entry lag.
        weh, wem = map(int, self._entry_window_end.split(":"))
        if spy_bars.index.tz is not None:
            ts_end = pd.Timestamp(f"{date} {weh:02d}:{wem:02d}:00").tz_localize(str(spy_bars.index.tz))
        else:
            ts_end = pd.Timestamp(f"{date} {weh:02d}:{wem:02d}:00")

        window_bars = spy_bars.loc[spy_bars.index <= ts_end]
        if window_bars.empty:
            features["breakout_direction"] = 0
            features["breakout_magnitude_pct"] = 0.0
        else:
            check_price = float(window_bars.iloc[-1]["close"])
            if check_price > or_high:
                breakout_dir = 1
                breakout_magnitude = (check_price - or_high) / or_high * 100
            elif check_price < or_low:
                breakout_dir = -1
                breakout_magnitude = (or_low - check_price) / or_low * 100
            else:
                breakout_dir = 0
                breakout_magnitude = 0.0
            features["breakout_direction"] = breakout_dir
            features["breakout_magnitude_pct"] = breakout_magnitude

        # No breakout
        if breakout_dir == 0 or np.isnan(breakout_dir):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "no_breakout"})

        # Filter: range width
        if not np.isnan(range_width):
            if range_width < self._min_range_width:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"range_too_narrow:{range_width:.3f}%"})
            if range_width > self._max_range_width:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"range_too_wide:{range_width:.3f}%"})

        # Filter: VIX
        if not np.isnan(vix_level):
            if vix_level > self._max_vix:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vix_too_high:{vix_level:.1f}"})
            if vix_level < self._min_vix:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vix_too_low:{vix_level:.1f}"})

        # Filter: FOMC
        if self._reject_on_fomc and self._econ_calendar.is_fomc_day(date):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "fomc_day"})

        direction = "long" if breakout_dir > 0 else "short"

        return Signal(
            direction=direction,
            confidence=0.6,
            instrument={"features": features},
            metadata=features,
        )

    def select_instrument(
        self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str
    ) -> dict:
        """Select instrument: SPY equity or SPY options."""
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return {}

        entry_price = float(spy_bars.iloc[-1]["close"])
        features = signal.instrument.get("features", {})

        # Compute stop and target levels
        if signal.direction == "long":
            target_spy = entry_price * (1.0 + self._target_pct / 100.0)
            stop_spy = entry_price * (1.0 - self._stop_pct / 100.0)
        else:
            target_spy = entry_price * (1.0 - self._target_pct / 100.0)
            stop_spy = entry_price * (1.0 + self._stop_pct / 100.0)

        time_stop_mins = self._time_to_minutes(self._time_stop)

        def _get_current_price(current_bars, ts):
            sb = current_bars.get("SPY_1Min", pd.DataFrame())
            return float(sb.iloc[-1]["close"]) if not sb.empty else entry_price

        return {
            "type": "equity",
            "symbol": "SPY",
            "entry_price": entry_price,
            "_direction": signal.direction.upper(),
            "_target_spy": target_spy,
            "_stop_spy": stop_spy,
            "_time_stop_mins": time_stop_mins,
            "_get_current_price": _get_current_price,
            "current_price": entry_price,
        }

    def should_exit(
        self, position: dict, bars: Dict[str, pd.DataFrame]
    ) -> Optional[str]:
        """Bar-by-bar exit check for ORB."""
        instrument = position.get("instrument", {})
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())

        if spy_bars.empty:
            return None

        current_spy = float(spy_bars.iloc[-1]["close"])
        current_ts = spy_bars.index[-1]
        current_mins = current_ts.hour * 60 + current_ts.minute

        direction = instrument.get("_direction", "LONG")
        target_spy = instrument.get("_target_spy")
        stop_spy = instrument.get("_stop_spy")
        time_stop_mins = instrument.get("_time_stop_mins", self._time_to_minutes(self._time_stop))

        # Time stop
        if current_mins >= time_stop_mins:
            return "time_stop"

        # Target / stop
        if target_spy is not None and stop_spy is not None:
            if direction == "LONG":
                if current_spy >= target_spy:
                    return "target"
                if current_spy <= stop_spy:
                    return "stop"
            else:
                if current_spy <= target_spy:
                    return "target"
                if current_spy >= stop_spy:
                    return "stop"

        return None

    @staticmethod
    def _time_to_minutes(t: str) -> int:
        h, m = map(int, t.split(":"))
        return h * 60 + m

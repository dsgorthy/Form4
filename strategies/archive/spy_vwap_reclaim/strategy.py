"""
SPY VWAP Reclaim Bounce Strategy — BaseStrategy implementation.

Academic / practitioner basis:
    VWAP (Volume Weighted Average Price) is the dominant execution benchmark for
    institutional equity trading. Institutional algorithms that benchmark to VWAP
    are programmed to buy below VWAP and sell above VWAP. This creates a structural
    mean-reversion tendency around the daily VWAP line.

    When SPY deviates significantly below VWAP (retail selling, momentum flush) and
    then "reclaims" VWAP (price crosses back above with continuation), it signals:
    1. The deviation was temporary (absorbed by institutional VWAP buyers)
    2. The reclaim triggers more systematic VWAP buyers to re-engage
    3. Short-sellers who sold below VWAP begin covering

    Key references:
    - Kissell & Glantz (2003), "Optimal Trading Strategies" — VWAP execution algorithms
    - Almgren & Chriss (2001) — optimal liquidation anchored to VWAP
    - Practitioner documentation: VWAP as intraday support/resistance extensively
      documented in institutional trading desk research (JPM, Goldman, Citadel)

Entry:
    - Scan 10:30 AM – 1:30 PM for VWAP deviation events
    - "Deviation": SPY closes >= 0.25% below VWAP for >= 15 consecutive bars
    - "Reclaim": SPY then closes above VWAP for >= 3 consecutive bars
    - Enter at close of 3rd confirmation bar

Exit:
    - Target: +0.30% from entry
    - Stop:   -0.15% from entry (if reclaim fails, price drops back below VWAP)
    - Time:   3:30 PM hard stop
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar, EconomicCalendar

logger = logging.getLogger(__name__)


def compute_vwap(bars: pd.DataFrame) -> pd.Series:
    """Compute cumulative VWAP from the start of the bars DataFrame."""
    typical_price = (bars["high"] + bars["low"] + bars["close"]) / 3.0
    volume = bars["volume"].replace(0, 1)
    cumulative_tp_vol = (typical_price * volume).cumsum()
    cumulative_vol = volume.cumsum()
    return cumulative_tp_vol / cumulative_vol


class SpyVwapReclaimStrategy(BaseStrategy):
    """
    SPY VWAP Reclaim Bounce.

    Finds days where SPY dips significantly below VWAP, then reclaims it
    with consecutive closes above. Enters on confirmation; exits via target,
    stop, or 3:30 PM.
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

        self._scan_start = entry_cfg.get("scan_start", "10:30")
        self._scan_end = entry_cfg.get("scan_end", "13:30")
        self._min_deviation_pct = float(entry_cfg.get("min_deviation_pct", 0.25))
        self._deviation_lookback_bars = int(entry_cfg.get("deviation_lookback_bars", 15))
        self._reclaim_confirmation_bars = int(entry_cfg.get("reclaim_confirmation_bars", 3))

        self._time_stop = exit_cfg.get("time_stop", "15:30")
        self._target_pct = float(exit_cfg.get("target_pct", 0.30))
        self._stop_pct = float(exit_cfg.get("stop_pct", 0.15))

        self._max_vixy = float(filter_cfg.get("max_vixy", 50.0))
        self._min_vixy = float(filter_cfg.get("min_vixy", 10.0))
        self._reject_on_fomc = bool(filter_cfg.get("reject_on_fomc", True))
        self._require_vwap_slope = bool(filter_cfg.get("require_vwap_slope_positive", True))

    def strategy_name(self) -> str:
        return "spy_vwap_reclaim"

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
        1. Compute full-session VWAP from 9:30 AM.
        2. Apply VIXY/FOMC filters.
        3. Scan 10:30-1:30 for deviation+reclaim pattern.
        4. Return signal at first confirmed reclaim.
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
            vixy_at_scan = vixy_bars.loc[vixy_bars.index <= ts(self._scan_start)]
            if not vixy_at_scan.empty:
                vixy_level = float(vixy_at_scan.iloc[-1]["close"])

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

        # Compute session VWAP from 9:30 AM
        session_bars = spy_bars.loc[spy_bars.index >= ts("09:30")]
        if len(session_bars) < 30:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "insufficient_session_bars"})

        vwap_series = compute_vwap(session_bars)

        # Align vwap with spy_bars
        spy_bars = spy_bars.copy()
        spy_bars["vwap"] = vwap_series

        # Scan window
        scan_bars = spy_bars.loc[
            (spy_bars.index >= ts(self._scan_start)) &
            (spy_bars.index <= ts(self._scan_end))
        ].dropna(subset=["vwap"])

        if scan_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "no_scan_bars"})

        result = self._find_reclaim(scan_bars, date)

        if result is None:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "no_vwap_reclaim"})

        direction, entry_price, entry_ts, deviation_pct, vwap_at_entry = result

        # VWAP slope filter: VWAP must be rising for longs, falling for shorts
        if self._require_vwap_slope:
            # Use last 10 bars of VWAP before entry
            vwap_before = vwap_series.loc[vwap_series.index <= entry_ts]
            if len(vwap_before) >= 10:
                vwap_slope = vwap_before.iloc[-1] - vwap_before.iloc[-10]
                if direction == "long" and vwap_slope < 0:
                    return Signal(direction=None, confidence=0.0, instrument=None,
                                  metadata={"skip_reason": "vwap_slope_against_direction"})
                if direction == "short" and vwap_slope > 0:
                    return Signal(direction=None, confidence=0.0, instrument=None,
                                  metadata={"skip_reason": "vwap_slope_against_direction"})

        features = {
            "deviation_pct": deviation_pct,
            "vwap_at_entry": vwap_at_entry,
            "entry_ts": str(entry_ts),
            "vixy_level": vixy_level,
        }

        confidence = min(0.9, 0.5 + abs(deviation_pct) / 2.0)

        return Signal(
            direction=direction,
            confidence=confidence,
            instrument={"entry_price": entry_price, "entry_ts": entry_ts},
            metadata=features,
        )

    def _find_reclaim(
        self, scan_bars: pd.DataFrame, date: str
    ) -> Optional[Tuple[str, float, object, float, float]]:
        """
        Scan bars for deviation + reclaim pattern.

        Returns (direction, entry_price, entry_ts, deviation_pct, vwap_at_entry)
        or None if no pattern found.

        Looks for both:
          - Downward deviation then upward reclaim (long trade)
          - Upward deviation then downward reclaim (short trade)
        """
        closes = scan_bars["close"].values
        vwaps = scan_bars["vwap"].values
        idx = scan_bars.index

        min_dev = self._min_deviation_pct / 100.0
        dev_bars = self._deviation_lookback_bars
        confirm_bars = self._reclaim_confirmation_bars

        # We need at least dev_bars + confirm_bars in the window
        if len(closes) < dev_bars + confirm_bars:
            return None

        for i in range(dev_bars, len(closes) - confirm_bars + 1):
            # Check if the prior dev_bars had sustained deviation
            prior_window = slice(i - dev_bars, i)
            prior_closes = closes[prior_window]
            prior_vwaps = vwaps[prior_window]

            prior_below = np.all(prior_closes < prior_vwaps * (1.0 - min_dev))
            prior_above = np.all(prior_closes > prior_vwaps * (1.0 + min_dev))

            if not prior_below and not prior_above:
                continue

            # Check if the next confirm_bars show a reclaim
            confirm_window = slice(i, i + confirm_bars)
            confirm_closes = closes[confirm_window]
            confirm_vwaps = vwaps[confirm_window]

            if prior_below:
                # Looking for upward reclaim (price crosses above VWAP)
                reclaimed = np.all(confirm_closes >= confirm_vwaps)
                if reclaimed:
                    entry_i = i + confirm_bars - 1
                    entry_price = float(closes[entry_i])
                    entry_ts = idx[entry_i]
                    avg_prior_dev = float(np.mean((prior_vwaps - prior_closes) / prior_vwaps) * 100)
                    vwap_at_entry = float(vwaps[entry_i])
                    return ("long", entry_price, entry_ts, avg_prior_dev, vwap_at_entry)

            elif prior_above:
                # Looking for downward reclaim (price crosses below VWAP)
                reclaimed = np.all(confirm_closes <= confirm_vwaps)
                if reclaimed:
                    entry_i = i + confirm_bars - 1
                    entry_price = float(closes[entry_i])
                    entry_ts = idx[entry_i]
                    avg_prior_dev = float(np.mean((prior_closes - prior_vwaps) / prior_vwaps) * 100)
                    vwap_at_entry = float(vwaps[entry_i])
                    return ("short", entry_price, entry_ts, avg_prior_dev, vwap_at_entry)

        return None

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

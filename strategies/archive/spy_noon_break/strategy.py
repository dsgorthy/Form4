"""
SPY Noon Session Range Break Strategy — BaseStrategy implementation.

Derek's observation:
    "Looking at SPY, I will wait from 9AM to noon to establish a session high
    and session low. My thesis is that when the session breaks in one direction
    with strength, that will indicate a strong move in that direction."

Entry:
    - Session range established from 9:00 AM to 12:00 PM (noon)
    - After noon: scan for first confirmed breakout beyond range
    - "Strength" = price closes outside range by >= min_break_pct for >= confirm_bars
      consecutive bars
    - Enter at the close of the confirmation bar

Flat Day Filter (Derek: "doesn't work on flat days"):
    - Opening range width (9:30–10:00) must be >= min_opening_range_width_pct
    - Total morning move must exceed min_morning_move_pct
    - These identify "energy" / directional conviction in the session

Exit:
    Equity mode:
        - Target: +0.40% from entry price
        - Stop:   -0.20% from entry (back toward / into the range)
        - Time:   3:30 PM hard stop — no overnight holds
    Options mode (0DTE):
        - Target: +75% of entry premium
        - Stop:   -50% of entry premium
        - Time:   3:30 PM hard stop (close before expiry)

Supports both equity (shares) and same-day 0DTE options via config.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Dict, Optional

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar, EconomicCalendar
from framework.pricing.black_scholes import BlackScholes
from framework.pricing.vol_engine import VolEngine
from strategies.archive.spy_noon_break.features import NoonBreakFeatureEngine

logger = logging.getLogger(__name__)

_TRADING_DAY_HOURS = 6.5
_MARKET_CLOSE_HOUR = 16.0  # 4:00 PM ET


class SpyNoonBreakStrategy(BaseStrategy):
    """
    SPY Noon Session Range Breakout.

    Waits for a full morning range (9AM–noon), then enters on a confirmed
    directional break with strength filter and flat-day filter. Exits via
    target, stop, or EOD. No overnight holds.

    Supports equity (shares) and 0DTE options (calls/puts).
    """

    def __init__(self, config: dict, storage: Optional[DataStorage] = None):
        super().__init__(config)
        self._storage = storage or DataStorage()
        self._calendar = MarketCalendar()
        self._econ_calendar = EconomicCalendar()
        self._feature_engine = NoonBreakFeatureEngine(storage=self._storage)
        self._vol_engine = VolEngine()
        self._bs = BlackScholes()

        sr_cfg = config.get("session_range", {})
        entry_cfg = config.get("entry", {})
        exit_cfg = config.get("exit", {})
        filter_cfg = config.get("filters", {})
        instrument_cfg = config.get("instrument", {})

        self._range_start = sr_cfg.get("start_time", "09:00")
        self._range_end = sr_cfg.get("end_time", "12:00")
        self._entry_window_start = entry_cfg.get("window_start", "12:01")
        self._entry_window_end = entry_cfg.get("window_end", "15:00")
        self._time_stop = exit_cfg.get("time_stop", "15:30")
        self._target_pct = float(exit_cfg.get("target_pct", 0.40))
        self._stop_pct = float(exit_cfg.get("stop_pct", 0.20))

        self._min_break_pct = float(entry_cfg.get("min_break_pct", 0.15))
        self._confirm_bars = int(entry_cfg.get("confirm_bars", 2))
        self._instrument_type = instrument_cfg.get("type", "equity")

        # Options params
        self._strike_offset = float(instrument_cfg.get("strike_offset", 0))
        self._stop_premium_pct = float(instrument_cfg.get("stop_premium_pct", 50)) / 100.0
        self._target_premium_pct = float(instrument_cfg.get("target_premium_pct", 75)) / 100.0

        # Filters
        self._min_range_width = float(filter_cfg.get("min_range_width_pct", 0.10))
        self._max_range_width = float(filter_cfg.get("max_range_width_pct", 2.5))
        self._max_vixy = float(filter_cfg.get("max_vixy", 45.0))
        self._min_vixy = float(filter_cfg.get("min_vixy", 10.0))
        self._reject_on_fomc = bool(filter_cfg.get("reject_on_fomc", True))
        # Flat day filters
        self._min_opening_range_width = float(filter_cfg.get("min_opening_range_width_pct", 0.12))
        self._min_morning_move = float(filter_cfg.get("min_morning_move_pct", 0.10))
        # RVOL filter (0 = disabled)
        self._min_rvol = float(filter_cfg.get("rvol_min", 0.0))
        self._rvol_lookback_days = int(filter_cfg.get("rvol_lookback_days", 14))

        # Leverage multiplier (1=SPY, 2=SSO-equivalent, 3=UPRO-equivalent)
        self._leverage_multiplier = float(instrument_cfg.get("leverage_multiplier", 1.0))

        # DTE for options (0=same day, 1=next day, etc.)
        self._expiry_dte = int(instrument_cfg.get("expiry_dte", 0))

    def strategy_name(self) -> str:
        return "spy_noon_break"

    def data_requirements(self) -> DataRequirements:
        data_cfg = self.config.get("data", {})
        return DataRequirements(
            symbols=data_cfg.get("symbols", ["SPY", "VIXY"]),
            timeframes=data_cfg.get("timeframes", ["1Min", "5Min"]),
            lookback_days=data_cfg.get("lookback_days", 30),
            requires_options=data_cfg.get("requires_options", False),
        )

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        """
        1. Compute session range from 9AM-noon bars.
        2. Apply filters (range width, VIXY, FOMC, flat-day).
        3. Scan post-noon bars for confirmed breakout.
        4. Return Signal with direction and entry price embedded.
        """
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None)

        tz = str(spy_bars.index.tz) if spy_bars.index.tz is not None else None

        def ts(time_str):
            h, m = map(int, time_str.split(":"))
            t = pd.Timestamp(f"{date} {h:02d}:{m:02d}:00")
            return t.tz_localize(tz) if tz else t

        # Step 1: session range features
        features = self._feature_engine.compute_session_range(
            date,
            range_start=self._range_start,
            range_end=self._range_end,
            spy_bars=spy_bars,
        )

        session_high = features.get("session_high", np.nan)
        session_low = features.get("session_low", np.nan)
        range_width = features.get("session_range_width_pct", np.nan)
        vixy_level = features.get("vixy_level", np.nan)

        if np.isnan(session_high) or np.isnan(session_low):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "no_session_range"})

        # Step 2a: session range width filter
        if not np.isnan(range_width):
            if range_width < self._min_range_width:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"range_too_narrow:{range_width:.3f}%"})
            if range_width > self._max_range_width:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"range_too_wide:{range_width:.3f}%"})

        # Step 2b: VIXY filter
        if not np.isnan(vixy_level):
            if vixy_level > self._max_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vixy_too_high:{vixy_level:.1f}"})
            if vixy_level < self._min_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vixy_too_low:{vixy_level:.1f}"})

        # Step 2c: FOMC filter
        if self._reject_on_fomc and self._econ_calendar.is_fomc_day(date):
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": "fomc_day"})

        # Step 2d: Flat day filter — opening range energy check
        opening_range_bars = spy_bars.loc[
            (spy_bars.index >= ts("09:30")) & (spy_bars.index <= ts("10:00"))
        ]
        if not opening_range_bars.empty and len(opening_range_bars) >= 5:
            or_high = float(opening_range_bars["high"].max())
            or_low = float(opening_range_bars["low"].min())
            or_open = float(opening_range_bars.iloc[0]["open"])
            or_width_pct = (or_high - or_low) / or_open * 100.0 if or_open > 0 else 0.0
            features["opening_range_width_pct"] = or_width_pct

            if or_width_pct < self._min_opening_range_width:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={**features, "skip_reason": f"flat_open:{or_width_pct:.3f}%"})

            # Morning move (open to session high or low)
            session_open = features.get("session_open", or_open)
            if session_open > 0:
                morning_move = max(
                    abs(session_high - session_open),
                    abs(session_low - session_open),
                ) / session_open * 100.0
                features["morning_move_pct"] = morning_move
                if morning_move < self._min_morning_move:
                    return Signal(direction=None, confidence=0.0, instrument=None,
                                  metadata={**features, "skip_reason": f"flat_morning:{morning_move:.3f}%"})

        # Step 3: scan for breakout
        breakout = self._feature_engine.scan_for_breakout(
            spy_bars=spy_bars,
            date=date,
            session_high=session_high,
            session_low=session_low,
            window_start=self._entry_window_start,
            window_end=self._entry_window_end,
            min_break_pct=self._min_break_pct,
            confirm_bars=self._confirm_bars,
            rvol_lookback_days=self._rvol_lookback_days,
        )

        features.update(breakout)
        breakout_dir = breakout.get("breakout_direction", 0)

        if breakout_dir == 0:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={**features, "skip_reason": "no_breakout"})

        # Step 3b: RVOL filter — require high-volume confirmation
        if self._min_rvol > 0:
            rvol = breakout.get("breakout_rvol", 1.0)
            if rvol < self._min_rvol:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={**features, "skip_reason": f"low_rvol:{rvol:.2f}x"})

        # Step 3c: VWAP filter
        # Backtest evidence: breakouts that ALIGN with morning VWAP direction fail (0% WR).
        # The noon break is a REVERSAL strategy — SPY overshoots its morning range,
        # then the afternoon trade works when it fades back. Filter for COUNTER-VWAP breakouts.
        entry_cfg = self.config.get("entry", {})
        vwap_filter = entry_cfg.get("vwap_filter", "none")  # "aligned" | "counter" | "none"
        if vwap_filter != "none":
            vwap_dir = features.get("vwap_direction", 0)
            if vwap_dir != 0:
                if vwap_filter == "aligned" and vwap_dir != breakout_dir:
                    return Signal(direction=None, confidence=0.0, instrument=None,
                                  metadata={**features, "skip_reason": f"vwap_misalign:vwap={vwap_dir},break={breakout_dir}"})
                elif vwap_filter == "counter" and vwap_dir == breakout_dir:
                    return Signal(direction=None, confidence=0.0, instrument=None,
                                  metadata={**features, "skip_reason": f"vwap_same_dir:vwap={vwap_dir},break={breakout_dir}"})

        direction = "long" if breakout_dir > 0 else "short"
        magnitude = breakout.get("breakout_magnitude_pct", 0.0)
        confidence = min(0.9, 0.5 + magnitude / 2.0)

        return Signal(
            direction=direction,
            confidence=confidence,
            instrument={"features": features, "breakout": breakout},
            metadata=features,
        )

    # ------------------------------------------------------------------
    # Instrument selection: equity or 0DTE options
    # ------------------------------------------------------------------

    def select_instrument(
        self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str
    ) -> dict:
        """Select SPY equity or 0DTE ATM options based on config."""
        breakout = signal.instrument.get("breakout", {})
        entry_price = breakout.get("breakout_entry_price")
        breakout_ts = breakout.get("breakout_bar_ts")

        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if entry_price is None:
            if spy_bars.empty:
                return {}
            entry_price = float(spy_bars.iloc[-1]["close"])
        entry_price = float(entry_price)

        if self._instrument_type == "options":
            return self._select_options_instrument(signal, bars, date, entry_price, breakout_ts)
        else:
            return self._select_equity_instrument(signal, entry_price, entry_bar_ts=breakout_ts)

    def _select_equity_instrument(self, signal: Signal, entry_price: float,
                                   entry_bar_ts=None) -> dict:
        if signal.direction == "long":
            target_price = entry_price * (1.0 + self._target_pct / 100.0)
            stop_price = entry_price * (1.0 - self._stop_pct / 100.0)
        else:
            target_price = entry_price * (1.0 - self._target_pct / 100.0)
            stop_price = entry_price * (1.0 + self._stop_pct / 100.0)

        time_stop_mins = self._time_to_minutes(self._time_stop)
        leverage = self._leverage_multiplier

        # For leverage > 1 (SSO=2x, UPRO=3x simulation), divide the effective
        # entry price so the engine can afford more units. P&L is then scaled
        # by _leverage so actual dollar return = SPY_move × num_units × leverage.
        virtual_entry = entry_price / leverage if leverage > 1 else entry_price

        # Entry bar time as HH:MM string for engine exit-monitoring cutoff
        entry_bar_time_str = None
        if entry_bar_ts is not None:
            try:
                ts = pd.Timestamp(entry_bar_ts)
                entry_bar_time_str = f"{ts.hour:02d}:{ts.minute:02d}"
            except Exception:
                pass

        def _get_current_price(current_bars, ts_val):
            sb = current_bars.get("SPY_1Min", pd.DataFrame())
            return float(sb.iloc[-1]["close"]) if not sb.empty else entry_price

        lev_label = f"SPY×{int(leverage)}" if leverage > 1 else "SPY"

        return {
            "type": "equity",
            "symbol": lev_label,
            "entry_price": virtual_entry,       # used for position sizing (num_units)
            "_spy_entry_price": entry_price,    # actual SPY price for P&L calculation
            "_leverage": leverage,
            "_direction": signal.direction.upper(),
            "_target_price": target_price,      # compared vs SPY close in should_exit
            "_stop_price": stop_price,
            "_time_stop_mins": time_stop_mins,
            "_entry_bar_time": entry_bar_time_str,  # actual breakout bar time for exit monitoring
            "_get_current_price": _get_current_price,
            "current_price": virtual_entry,
        }

    def _select_options_instrument(
        self,
        signal: Signal,
        bars: Dict[str, pd.DataFrame],
        date: str,
        spy_price: float,
        entry_ts=None,
    ) -> dict:
        """
        Select a 0DTE ATM call (long) or put (short) and price it via Black-Scholes.

        Returns instrument dict compatible with the backtest engine's should_exit
        and _get_current_instrument_price calls.
        """
        option_type = "call" if signal.direction == "long" else "put"
        strike = round(spy_price + self._strike_offset * (1 if option_type == "call" else -1))

        # Get VIXY level at entry for IV estimation
        vixy_bars = bars.get("VIXY_1Min", pd.DataFrame())
        vixy_level = 15.0  # default
        if not vixy_bars.empty:
            vixy_level = float(vixy_bars.iloc[-1]["close"])

        iv = self._vol_engine.get_iv_for_strike(
            vixy_price=vixy_level,
            strike=strike,
            spot=spy_price,
            option_type=option_type,
        )

        # Time to expiry: hours remaining today + full DTE extra days
        # 0DTE at noon = 4hrs; 1DTE at noon = 4hrs + 6.5hrs = 10.5hrs
        if entry_ts is not None:
            try:
                ts_obj = pd.Timestamp(entry_ts)
                hours_today = _MARKET_CLOSE_HOUR - (ts_obj.hour + ts_obj.minute / 60.0)
                hours_today = max(0.1, hours_today)
            except Exception:
                hours_today = 2.0
        else:
            hours_today = 2.0

        total_hours = hours_today + self._expiry_dte * _TRADING_DAY_HOURS
        time_to_expiry = total_hours / (_TRADING_DAY_HOURS * 252.0)

        # Price the option
        try:
            entry_premium = self._bs.price(
                S=spy_price,
                K=strike,
                T=time_to_expiry,
                r=0.05,
                sigma=iv,
                option_type=option_type,
            )
        except Exception as e:
            logger.warning("BS pricing failed: %s", e)
            entry_premium = spy_price * 0.003  # ~0.3% fallback

        if entry_premium <= 0.01:
            entry_premium = 0.01

        stop_premium = entry_premium * (1.0 - self._stop_premium_pct)
        target_premium = entry_premium * (1.0 + self._target_premium_pct)
        time_stop_mins = self._time_to_minutes(self._time_stop)

        # Capture for pricing function
        _iv = iv
        _strike = strike
        _option_type = option_type
        _spy_price_at_entry = spy_price
        _bs = self._bs
        _vol_engine = self._vol_engine
        _expiry_dte = self._expiry_dte

        def _get_current_price(current_bars, ts_val):
            """Reprice option at each bar using BS + remaining time."""
            sb = current_bars.get("SPY_1Min", pd.DataFrame())
            if sb.empty:
                return entry_premium
            current_spy = float(sb.iloc[-1]["close"])
            try:
                bar_ts = pd.Timestamp(ts_val)
                hours_left_today = _MARKET_CLOSE_HOUR - (bar_ts.hour + bar_ts.minute / 60.0)
                hours_left_today = max(0.01, hours_left_today)
            except Exception:
                hours_left_today = 0.5
            # For 1DTE: still have extra day of time value
            total_hours_left = hours_left_today + _expiry_dte * _TRADING_DAY_HOURS
            t_remaining = total_hours_left / (_TRADING_DAY_HOURS * 252.0)

            # Recalibrate IV from current VIXY if available
            vb = current_bars.get("VIXY_1Min", pd.DataFrame())
            cur_vixy = vixy_level
            if not vb.empty:
                cur_vixy = float(vb.iloc[-1]["close"])
            cur_iv = _vol_engine.get_iv_for_strike(cur_vixy, _strike, current_spy, _option_type)

            try:
                p = _bs.price(
                    S=current_spy,
                    K=_strike,
                    T=t_remaining,
                    r=0.05,
                    sigma=cur_iv,
                    option_type=_option_type,
                )
                return max(0.01, p)
            except Exception:
                return entry_premium

        return {
            "type": "option",
            "symbol": "SPY",
            "option_type": option_type,
            "strike": strike,
            "expiry": date,
            "entry_price": entry_premium,
            "_direction": signal.direction.upper(),
            "_stop_premium": stop_premium,
            "_target_premium": target_premium,
            "_time_stop_mins": time_stop_mins,
            "_get_current_price": _get_current_price,
            "current_price": entry_premium,
            "greeks": {"iv": _iv},
        }

    # ------------------------------------------------------------------
    # Exit logic: handles both equity and options modes
    # ------------------------------------------------------------------

    def should_exit(
        self, position: dict, bars: Dict[str, pd.DataFrame]
    ) -> Optional[str]:
        """Bar-by-bar exit: target, stop, or time stop."""
        instrument = position.get("instrument", {})
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())

        if spy_bars.empty:
            return None

        current_ts = spy_bars.index[-1]
        current_mins = current_ts.hour * 60 + current_ts.minute
        time_stop_mins = instrument.get("_time_stop_mins", self._time_to_minutes(self._time_stop))

        if current_mins >= time_stop_mins:
            return "time_stop"

        instrument_type = instrument.get("type", "equity")

        if instrument_type == "option":
            # Get repriced option value
            get_price_fn = instrument.get("_get_current_price")
            current_premium = (
                get_price_fn(bars, current_ts)
                if callable(get_price_fn)
                else instrument.get("current_price", 0)
            )
            stop_prem = instrument.get("_stop_premium")
            target_prem = instrument.get("_target_premium")
            if stop_prem is not None and current_premium <= stop_prem:
                return "stop"
            if target_prem is not None and current_premium >= target_prem:
                return "target"
        else:
            current_spy = float(spy_bars.iloc[-1]["close"])
            direction = instrument.get("_direction", "LONG")
            target_price = instrument.get("_target_price")
            stop_price = instrument.get("_stop_price")
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

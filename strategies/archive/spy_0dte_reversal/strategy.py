"""
SPY 0DTE End-of-Day Reversal Strategy — BaseStrategy implementation.

Wraps the original spy-0dte logic into the generic framework interface.
  - generate_signal(): compute features, apply filters, determine direction
  - select_instrument(): select strike, price option via BS or real data
  - should_exit(): bar-by-bar stop/target/time-stop check
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, date as date_type
from typing import Dict, Optional

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar, EconomicCalendar
from framework.data.options_collector import build_occ_symbol, expiry_for_dte
from framework.pricing.black_scholes import BlackScholes
from framework.pricing.vol_engine import VolEngine
from strategies.archive.spy_0dte_reversal.features import FeatureEngine

logger = logging.getLogger(__name__)

MINUTES_PER_YEAR = 252 * 6.5 * 60
TRADING_MINUTES_PER_DAY = 390
OPTION_MULTIPLIER = 100
DEFAULT_RISK_FREE_RATE = 0.0525
DEFAULT_DIVIDEND_YIELD = 0.013


class SPY0DTEReversalStrategy(BaseStrategy):
    """
    SPY 0DTE End-of-Day Reversal.

    At 3:29 PM:
      - If SPY is above open -> buy put (expect reversal down)
      - If SPY is below open -> buy call (expect reversal up)
    Exit at take-profit, stop-loss, or 3:55 PM.
    """

    def __init__(self, config: dict, storage: Optional[DataStorage] = None):
        super().__init__(config)

        self._storage = storage or DataStorage()
        self._calendar = MarketCalendar()
        self._econ_calendar = EconomicCalendar()
        self._bs = BlackScholes()
        self._vol_engine = VolEngine(
            vol_proxy_symbol=self.config.get("data", {}).get("symbols", ["SPY", "VIXY"])[1]
            if len(self.config.get("data", {}).get("symbols", [])) > 1 else "VIXY"
        )
        self._feature_engine = FeatureEngine(storage=self._storage)

        # Parse config sections
        self._entry_cfg = config.get("entry", {})
        self._exit_cfg = config.get("exit", {})
        self._instrument_cfg = config.get("instrument", {})
        self._filter_cfg = config.get("filters", {})

        self._entry_time = self._entry_cfg.get("time", "15:29")
        self._time_stop = self._exit_cfg.get("time_stop", "15:55")
        self._stop_mode = self._exit_cfg.get("stop_mode", "underlying")
        self._stop_underlying_pct = float(self._exit_cfg.get("stop_underlying_pct", 0.15))
        self._target_underlying_pct = float(self._exit_cfg.get("target_underlying_pct", 0.20))
        self._stop_loss_pct = float(self._exit_cfg.get("stop_loss_pct", 0.40))
        self._take_profit_pct = float(self._exit_cfg.get("take_profit_pct", 0.50))
        self._expiry_dte = int(self._instrument_cfg.get("expiry_dte", 0))
        self._use_spot_strikes = bool(self._instrument_cfg.get("use_spot_strikes", True))
        self._use_real_options = bool(self._instrument_cfg.get("use_real_options", True))
        self._max_strike_offset = float(self._instrument_cfg.get("max_strike_offset", 1.0))

        self._min_move_pct = float(self._filter_cfg.get("min_intraday_move_pct", 0.10))
        self._max_move_pct = float(self._filter_cfg.get("max_intraday_move_pct", 3.0))
        self._min_vix = float(self._filter_cfg.get("min_vix", 15.0))
        self._max_vix = float(self._filter_cfg.get("max_vix", 70.0))

        # Options data cache
        self._options_cache: dict = {}

    def strategy_name(self) -> str:
        return "spy_0dte_reversal"

    def data_requirements(self) -> DataRequirements:
        data_cfg = self.config.get("data", {})
        return DataRequirements(
            symbols=data_cfg.get("symbols", ["SPY", "VIXY"]),
            timeframes=data_cfg.get("timeframes", ["1Min", "5Min"]),
            lookback_days=data_cfg.get("lookback_days", 30),
            requires_options=data_cfg.get("requires_options", True),
        )

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        """
        At entry time: check filters, determine direction.
        Returns Signal(direction=None) if filters block the trade.
        """
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        if spy_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None)

        # Compute features using FeatureEngine (needs storage access for history)
        features = self._feature_engine.compute_features(date, self._entry_time)

        intraday_return = features.get("intraday_return", np.nan)
        vix_level = features.get("vix_level", np.nan)

        if np.isnan(intraday_return):
            return Signal(direction=None, confidence=0.0, instrument=None, metadata={"skip_reason": "nan_return"})

        # Filter: minimum intraday move
        abs_move_pct = abs(intraday_return) * 100.0
        if abs_move_pct < self._min_move_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": f"move_too_small:{abs_move_pct:.3f}%"})

        # Filter: maximum intraday move
        if abs_move_pct > self._max_move_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip_reason": f"move_too_large:{abs_move_pct:.3f}%"})

        # Filter: VIX range
        if not np.isnan(vix_level):
            if vix_level > self._max_vix:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vix_too_high:{vix_level:.1f}"})
            if vix_level < self._min_vix:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip_reason": f"vix_too_low:{vix_level:.1f}"})

        # Determine direction: above open -> put (short), below open -> call (long)
        direction = "short" if intraday_return > 0 else "long"
        option_type = "put" if direction == "short" else "call"

        return Signal(
            direction=direction,
            confidence=0.5,  # Fixed confidence; could be model-driven
            instrument={"option_type": option_type, "features": features},
            metadata=features,
        )

    def select_instrument(
        self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str
    ) -> dict:
        """Select strike, price option using BS or real options data."""
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())
        vixy_bars = bars.get("VIXY_1Min", pd.DataFrame())

        if spy_bars.empty:
            return {}

        entry_spy_price = float(spy_bars.iloc[-1]["close"])
        day_open = float(spy_bars.iloc[0]["open"])
        vixy_price = float(vixy_bars.iloc[-1]["close"]) if not vixy_bars.empty else 22.0

        option_type = signal.instrument.get("option_type", "call")
        direction = signal.direction.upper()  # "LONG" or "SHORT"

        # Compute time to expiry
        close_time = self._calendar.market_close_time(date)
        close_mins = self._time_to_minutes(close_time)
        entry_mins = self._time_to_minutes(self._entry_time)
        remaining_today = close_mins - entry_mins
        extra_dte_mins = self._expiry_dte * TRADING_MINUTES_PER_DAY
        time_to_expiry_years = (remaining_today + extra_dte_mins) / MINUTES_PER_YEAR

        # Select strike: ATM relative to spot (use_spot_strikes=True) or open price
        strike_anchor = entry_spy_price if self._use_spot_strikes else day_open
        strike = round(strike_anchor)  # Round to nearest integer

        # Price option via Black-Scholes
        iv = self._vol_engine.get_iv_for_strike(
            vixy_price=vixy_price,
            strike=strike,
            spot=entry_spy_price,
            option_type=option_type,
            time_to_expiry_years=time_to_expiry_years,
        )

        bs_price = self._bs.price(
            S=entry_spy_price, K=strike, T=time_to_expiry_years,
            r=DEFAULT_RISK_FREE_RATE, sigma=iv,
            option_type=option_type,
        )

        entry_price = bs_price

        # Try real options data
        if self._use_real_options:
            real_price = self._get_real_options_price(
                date, entry_spy_price, option_type, strike
            )
            if real_price and real_price > 0:
                entry_price = real_price

        if entry_price <= 0:
            return {}

        greeks = self._bs.all_greeks(
            S=entry_spy_price, K=strike, T=time_to_expiry_years,
            r=DEFAULT_RISK_FREE_RATE, sigma=iv,
            option_type=option_type,
        )

        expiry_date = expiry_for_dte(
            datetime.strptime(date, "%Y-%m-%d").date(), self._expiry_dte
        )

        # Precompute stop/target levels for use in should_exit
        if self._stop_mode == "underlying":
            if direction == "LONG":
                target_spy = entry_spy_price * (1.0 + self._target_underlying_pct / 100.0)
                stop_spy = entry_spy_price * (1.0 - self._stop_underlying_pct / 100.0)
            else:
                target_spy = entry_spy_price * (1.0 - self._target_underlying_pct / 100.0)
                stop_spy = entry_spy_price * (1.0 + self._stop_underlying_pct / 100.0)
            take_profit_price = float("inf")
            stop_loss_price = 0.0
        else:
            target_spy = None
            stop_spy = None
            take_profit_price = entry_price * (1.0 + self._take_profit_pct)
            stop_loss_price = entry_price * (1.0 - self._stop_loss_pct)

        time_stop_mins = self._time_to_minutes(self._time_stop)

        def _get_current_price(current_bars, ts):
            return self._reprice_option(
                current_bars=current_bars, ts=ts, date=date,
                strike=strike, option_type=option_type,
                iv_base=iv, entry_spy_price=entry_spy_price,
            )

        return {
            "type": "option",
            "symbol": build_occ_symbol("SPY", expiry_date, option_type, strike),
            "option_type": option_type,
            "strike": strike,
            "expiry": expiry_date,
            "entry_price": entry_price,
            "iv": iv,
            "greeks": greeks,
            # State for should_exit
            "_direction": direction,
            "_stop_mode": self._stop_mode,
            "_target_spy": target_spy,
            "_stop_spy": stop_spy,
            "_take_profit_price": take_profit_price,
            "_stop_loss_price": stop_loss_price,
            "_time_stop_mins": time_stop_mins,
            "_close_mins": close_mins,
            "_entry_spy": entry_spy_price,
            "_get_current_price": _get_current_price,
            "current_price": entry_price,
        }

    def should_exit(self, position: dict, bars: Dict[str, pd.DataFrame]) -> Optional[str]:
        """Bar-by-bar exit check for the 0DTE reversal."""
        instrument = position.get("instrument", {})
        spy_bars = bars.get("SPY_1Min", pd.DataFrame())

        if spy_bars.empty:
            return None

        current_spy = float(spy_bars.iloc[-1]["close"])
        current_ts = spy_bars.index[-1]
        current_time_mins = current_ts.hour * 60 + current_ts.minute

        direction = instrument.get("_direction", "LONG")
        stop_mode = instrument.get("_stop_mode", "underlying")
        time_stop_mins = instrument.get("_time_stop_mins", self._time_to_minutes(self._time_stop))
        close_mins = instrument.get("_close_mins", self._time_to_minutes("16:00"))

        # Time stop
        effective_time_stop = min(time_stop_mins, close_mins)
        if current_time_mins >= effective_time_stop:
            return "time_stop"

        if stop_mode == "underlying":
            target_spy = instrument.get("_target_spy")
            stop_spy = instrument.get("_stop_spy")

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
        else:
            # Premium-based: look up live option price via the _get_current_price closure.
            # The closure (set in select_instrument) calls _reprice_option which uses
            # real options data (when available) then falls back to Black-Scholes.
            take_profit = instrument.get("_take_profit_price", float("inf"))
            stop_loss = instrument.get("_stop_loss_price", 0.0)

            get_price_fn = instrument.get("_get_current_price")
            if callable(get_price_fn):
                current_option_price = get_price_fn(bars, current_ts)
            else:
                current_option_price = instrument.get("current_price", position.get("entry_price", 0))

            if current_option_price >= take_profit:
                return "target"
            if current_option_price <= stop_loss:
                return "stop"

        return None

    # -- Internal helpers --------------------------------------------------

    def _get_real_options_price(
        self, date: str, entry_spy_price: float, option_type: str, strike: float
    ) -> Optional[float]:
        """Look up actual option price from stored options data."""
        if date not in self._options_cache:
            self._options_cache[date] = self._storage.load_options_day(date)
        options_df = self._options_cache.get(date)
        if options_df is None:
            return None

        expiry_date = expiry_for_dte(datetime.strptime(date, "%Y-%m-%d").date(), self._expiry_dte)
        occ = build_occ_symbol("SPY", expiry_date, option_type, strike)
        if occ not in options_df.columns:
            return None

        # Get price at entry time
        try:
            # Find the bar closest to entry time
            entry_hour, entry_min = map(int, self._entry_time.split(":"))
            if options_df.index.tz is not None:
                cutoff = pd.Timestamp(
                    f"{date} {entry_hour:02d}:{entry_min:02d}:00"
                ).tz_localize(str(options_df.index.tz))
            else:
                cutoff = pd.Timestamp(f"{date} {entry_hour:02d}:{entry_min:02d}:00")
            nearest_ts = options_df.index.asof(cutoff)
            if pd.isnull(nearest_ts):
                return None
            price = float(options_df.loc[nearest_ts, occ])
            return price if price > 0 else None
        except Exception:
            return None

    def _reprice_option(
        self, current_bars: dict, ts, date: str, strike: float,
        option_type: str, iv_base: float, entry_spy_price: float,
    ) -> float:
        """
        Reprice the option at a given bar.

        If use_real_options=True, try to look up the real market price first;
        fall back to Black-Scholes only when real data is unavailable for this bar.
        This keeps entry and exit pricing consistent.
        """
        # Try real options data first (keeps pricing consistent with entry)
        if self._use_real_options:
            real_price = self._get_real_options_price_at(date, strike, option_type, ts)
            if real_price and real_price > 0:
                return real_price

        # Fall back to Black-Scholes
        spy_bars = current_bars.get("SPY_1Min", pd.DataFrame())
        vixy_bars = current_bars.get("VIXY_1Min", pd.DataFrame())

        if spy_bars.empty:
            return entry_spy_price

        current_spy = float(spy_bars.iloc[-1]["close"])

        try:
            close_time = self._calendar.market_close_time(date)
        except ValueError:
            return 0.0

        close_mins = self._time_to_minutes(close_time)
        current_mins = ts.hour * 60 + ts.minute
        extra_dte_mins = self._expiry_dte * TRADING_MINUTES_PER_DAY
        remaining_mins = max(0, (close_mins - current_mins) + extra_dte_mins)
        time_to_expiry = remaining_mins / MINUTES_PER_YEAR

        vixy_price = iv_base / 0.8 / 1.3 * 100  # rough inverse from iv_base
        if not vixy_bars.empty:
            vixy_price = float(vixy_bars.iloc[-1]["close"])

        current_iv = self._vol_engine.get_iv_for_strike(
            vixy_price=vixy_price, strike=strike, spot=current_spy,
            option_type=option_type, time_to_expiry_years=time_to_expiry,
        )

        return self._bs.price(
            S=current_spy, K=strike, T=max(time_to_expiry, 1e-6),
            r=DEFAULT_RISK_FREE_RATE, sigma=current_iv,
            option_type=option_type,
        )

    def _get_real_options_price_at(
        self, date: str, strike: float, option_type: str, ts
    ) -> Optional[float]:
        """Look up real option price at a specific timestamp (for bar-by-bar repricing)."""
        if date not in self._options_cache:
            self._options_cache[date] = self._storage.load_options_day(date)
        options_df = self._options_cache.get(date)
        if options_df is None:
            return None

        expiry_date = expiry_for_dte(datetime.strptime(date, "%Y-%m-%d").date(), self._expiry_dte)
        occ = build_occ_symbol("SPY", expiry_date, option_type, strike)
        if occ not in options_df.columns:
            return None

        try:
            nearest_ts = options_df.index.asof(ts)
            if pd.isnull(nearest_ts):
                return None
            price = float(options_df.loc[nearest_ts, occ])
            return price if price > 0 else None
        except Exception:
            return None

    @staticmethod
    def _time_to_minutes(t: str) -> int:
        h, m = map(int, t.split(":"))
        return h * 60 + m

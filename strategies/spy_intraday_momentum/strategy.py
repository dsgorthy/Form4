"""
SPY Intraday Momentum Strategy — 0DTE Options

Academic basis: Gao, Han, Li & Zhou (2018) "Market Intraday Momentum"
  The first half-hour return (prev close → 10:00 AM) predicts the last
  half-hour return (3:30 → 4:00 PM) with the same sign. Driven by informed
  trading early and portfolio rebalancing/hedging flows at close.

Published Sharpe: 0.87–1.73 annualized at asset class level.

Entry:  3:30 PM — buy ATM 0DTE call (morning up) or put (morning down)
Exit:   3:55 PM — sell before settlement risk
Signal: morning_return = (SPY @ 10:00 - prev_close) / prev_close
"""

from __future__ import annotations

import logging
import math
from typing import Dict, Optional

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements
from framework.pricing.black_scholes import BlackScholes
from framework.pricing.vol_engine import VolEngine

logger = logging.getLogger(__name__)

_RISK_FREE_RATE = 0.05
_DEFAULT_IV = 0.20


class SPYIntradayMomentumStrategy(BaseStrategy):

    def __init__(self, config: dict) -> None:
        super().__init__(config)

        entry_cfg = config.get("entry", {})
        exit_cfg = config.get("exit", {})
        filters_cfg = config.get("filters", {})
        inst_cfg = config.get("instrument", {})

        self._morning_cutoff = entry_cfg.get("morning_cutoff", "10:00")
        self._entry_time = entry_cfg.get("entry_time", "15:30")
        self._min_morning_move = float(entry_cfg.get("min_morning_move_pct", 0.0))

        self._exit_time = exit_cfg.get("time_stop", "15:55")
        self._stop_pct = float(exit_cfg.get("stop_pct", 0.50))
        self._target_pct = float(exit_cfg.get("target_pct", 1.00))

        self._instrument_type = inst_cfg.get("type", "option").lower()

        self._min_vixy = float(filters_cfg.get("min_vixy", 0.0))
        self._max_vixy = float(filters_cfg.get("max_vixy", 100.0))

        self._vol_engine = VolEngine()

    def strategy_name(self) -> str:
        return "spy_intraday_momentum"

    def data_requirements(self) -> DataRequirements:
        return DataRequirements(
            symbols=["SPY", "VIXY"],
            timeframes=["1Min"],
            lookback_days=1,
            requires_options=(self._instrument_type == "option"),
        )

    def generate_signal(
        self, bars: Dict[str, pd.DataFrame], date: str
    ) -> Signal:
        spy = bars.get("SPY_1Min", pd.DataFrame())
        if spy.empty:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "no_data"})

        meta = bars.get("_meta", {})
        prev_close = float(meta.get("prev_close", 0.0))
        if prev_close <= 0:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "no_prev_close"})

        # Morning session: 9:30 → morning_cutoff (default 10:00)
        try:
            morning = spy.between_time("09:30", self._morning_cutoff)
        except Exception:
            morning = spy

        if len(morning) < 5:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "insufficient_morning_bars"})

        # Entry bar: the bar at entry_time (15:30)
        try:
            entry_bars = spy.between_time(self._entry_time, self._entry_time)
        except Exception:
            entry_bars = pd.DataFrame()

        if entry_bars.empty:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "no_entry_bar"})

        # Morning return: prev_close → 10:00 AM close
        morning_price = float(morning.iloc[-1]["close"])
        morning_return = (morning_price - prev_close) / prev_close

        # Filter: minimum morning move
        if abs(morning_return) < self._min_morning_move / 100.0:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "morning_move_too_small",
                                    "morning_return": round(morning_return * 100, 3)})

        # VIXY filter
        vixy = bars.get("VIXY_1Min", pd.DataFrame())
        vixy_level = np.nan
        if not vixy.empty:
            try:
                vx = vixy.between_time("09:30", self._entry_time)
                if not vx.empty:
                    vixy_level = float(vx.iloc[-1]["close"])
            except Exception:
                pass
        if not np.isnan(vixy_level):
            if vixy_level < self._min_vixy or vixy_level > self._max_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip": "vixy_filter", "vixy": round(vixy_level, 2)})

        # Direction: momentum → same direction as morning
        direction = "long" if morning_return > 0 else "short"

        # Confidence scales with morning move magnitude
        confidence = min(0.95, 0.5 + abs(morning_return) * 50)

        entry_price = float(entry_bars.iloc[-1]["close"])

        return Signal(
            direction=direction,
            confidence=round(confidence, 3),
            instrument={
                "prev_close": prev_close,
                "morning_return": round(morning_return * 100, 3),
                "entry_price": entry_price,
                "vixy": round(vixy_level, 2) if not np.isnan(vixy_level) else None,
            },
            metadata={
                "morning_return_pct": round(morning_return * 100, 3),
                "morning_price": morning_price,
                "entry_price": entry_price,
            },
        )

    def select_instrument(
        self,
        signal: Signal,
        bars: Dict[str, pd.DataFrame],
        date: str,
    ) -> dict:
        spy = bars.get("SPY_1Min", pd.DataFrame())
        entry_price = signal.instrument["entry_price"]

        if self._instrument_type == "equity":
            return {
                "symbol": "SPY",
                "type": "equity",
                "entry_price": entry_price,
            }

        # 0DTE option
        option_type = "call" if signal.direction == "long" else "put"
        strike = round(entry_price)

        # Time to expiry: entry at 15:30, expiry at 16:00 = 30 min
        minutes_to_expiry = 25  # 15:30 → 15:55 (our exit time)
        T = minutes_to_expiry / (365 * 24 * 60)

        # IV from VIXY
        vixy = bars.get("VIXY_1Min", pd.DataFrame())
        if not vixy.empty:
            try:
                vx = vixy.between_time(self._entry_time, self._entry_time)
                vixy_price = float(vx.iloc[-1]["close"]) if not vx.empty else None
            except Exception:
                vixy_price = None
        else:
            vixy_price = None

        iv = self._vol_engine.estimate_iv(vixy_price) if vixy_price else _DEFAULT_IV

        greeks = BlackScholes.all_greeks(entry_price, strike, T, _RISK_FREE_RATE, iv, option_type)
        option_price = greeks.get("price", 1.0)

        _MINS_PER_YEAR = 365 * 24 * 60
        _CLOSE_HOUR = 16
        symbol_key = "SPY_1Min"
        entry_premium = option_price

        def _price_option_at(spot: float, ts) -> float:
            current_mins = ts.hour * 60 + ts.minute
            mins_left = max(1, _CLOSE_HOUR * 60 - current_mins)
            T_rem = mins_left / _MINS_PER_YEAR
            prem = BlackScholes.price(spot, strike, T_rem, _RISK_FREE_RATE, iv, option_type)
            return max(0.01, prem)

        def _get_current_price(bars_snap: dict, ts) -> float:
            override = instrument.get("_exit_price_override")
            if override is not None:
                return override
            df = bars_snap.get(symbol_key, pd.DataFrame())
            if df.empty:
                return entry_premium
            subset = df.loc[df.index <= ts]
            if subset.empty:
                return entry_premium
            current_spot = float(subset.iloc[-1]["close"])
            return _price_option_at(current_spot, ts)

        instrument = {
            "symbol": f"SPY_{date}_{strike}{option_type[0].upper()}",
            "type": "option",
            "option_type": option_type,
            "strike": strike,
            "expiry": date,
            "entry_price": option_price,
            "iv": iv,
            "greeks": greeks,
            "_get_current_price": _get_current_price,
            "_exit_price_override": None,
        }
        return instrument

    def should_exit(
        self, position: dict, bars: Dict[str, pd.DataFrame]
    ) -> Optional[str]:
        spy = bars.get("SPY_1Min", pd.DataFrame())
        if spy.empty:
            return None

        current_bar = spy.iloc[-1]
        current_time = current_bar.name
        if hasattr(current_time, "strftime"):
            t = current_time.strftime("%H:%M")
        else:
            t = str(current_time)[-5:]

        # Time stop: exit at 15:55
        if t >= self._exit_time:
            return "time_exit"

        # Premium-based stops (for options)
        entry_price = position.get("entry_price", 0)
        if entry_price <= 0:
            return None

        current_price = position.get("current_price", entry_price)
        pnl_pct = (current_price - entry_price) / entry_price

        if pnl_pct <= -self._stop_pct:
            return "stop_loss"
        if pnl_pct >= self._target_pct:
            return "target_hit"

        return None

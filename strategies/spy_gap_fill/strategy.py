"""
SPY Gap Fill Strategy — v1.1.0

Hypothesis: SPY gaps (open vs. prior close) mean-revert to fill the gap.
The strongest predictors of same-day fill are:
  1. Gap size: small gaps (0.05–0.30%) fill 66–82% of the time
  2. F30 direction: if first 30 min fades the gap, fill rate jumps ~10pp
  3. VIXY level: higher vol = higher fill probability (choppier market)

Entry:  10:00 AM, after first-30-min (F30) confirmation
Target: Previous day's close (the gap fill level)
Stop:   Adverse move of stop_pct from entry
Hold:   Up to max_hold_days if not filled same day

Instrument support (config: instrument.type):
  "equity"    — SPY shares (default), no commission at Alpaca
  "leveraged" — Synthetic 3x leverage on SPY underlying; no commission at Alpaca
  "option"    — 0DTE ATM put (gap up) / call (gap down), synthetic B-S pricing;
                $0.65/contract at Alpaca, both legs = $1.30 round-trip

Empirical baseline (2020–2026, N=420):
  Small gap (0.05–0.30%) + F30 fades = 76.7% fill rate
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

_MINS_PER_YEAR = 365 * 24 * 60
_RISK_FREE_RATE = 0.05       # annualized, approximate
_CLOSE_HOUR = 16             # 4:00 PM market close
_DEFAULT_IV = 0.20           # fallback if VIXY unavailable


class SPYGapFillStrategy(BaseStrategy):
    """
    SPY gap fill: fade morning gaps after 30-min direction confirmation.

    bars["_meta"]["prev_close"] is injected by the engine each day.
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)

        entry_cfg = config.get("entry", {})
        exit_cfg = config.get("exit", {})
        filters_cfg = config.get("filters", {})
        inst_cfg = config.get("instrument", {})
        data_cfg = config.get("data", {})

        self._entry_time = entry_cfg.get("time", "10:00")
        self._min_gap_pct = float(entry_cfg.get("min_gap_pct", 0.05))
        self._max_gap_pct = float(entry_cfg.get("max_gap_pct", 0.30))
        self._require_f30_fade = bool(entry_cfg.get("require_f30_fade", True))

        self._time_stop = exit_cfg.get("time_stop", "15:30")
        self._stop_pct = float(exit_cfg.get("stop_pct", 0.25))
        self._max_hold_days = int(exit_cfg.get("max_hold_days", 1))

        self._min_vixy = float(filters_cfg.get("min_vixy", 0.0))
        self._max_vixy = float(filters_cfg.get("max_vixy", 80.0))

        # Instrument type: "equity" | "leveraged" | "option"
        self._instrument_type = inst_cfg.get("type", "equity").lower()
        self._leverage = float(inst_cfg.get("leverage", 3.0))

        # Configurable primary symbol — drives all bars lookups
        self._symbol = data_cfg.get("primary_symbol", "SPY").upper()
        self._symbol_key = f"{self._symbol}_1Min"
        self._data_symbols = data_cfg.get("symbols", [self._symbol, "VIXY"])

        self._vol_engine = VolEngine()

    def strategy_name(self) -> str:
        return "spy_gap_fill"

    def data_requirements(self) -> DataRequirements:
        return DataRequirements(
            symbols=self._data_symbols,
            timeframes=["1Min"],
            lookback_days=1,
            requires_options=(self._instrument_type == "option"),
        )

    # ── Signal Generation ─────────────────────────────────────────────────────

    def generate_signal(
        self, bars: Dict[str, pd.DataFrame], date: str
    ) -> Signal:
        """
        Evaluate whether today's gap is worth fading at 10:00 AM.

        Uses bars["_meta"]["prev_close"] (injected by engine) to compute the gap.
        bars contains SPY_1Min data up to the entry time (10:00 AM).
        """
        spy = bars.get(self._symbol_key, pd.DataFrame())
        if spy.empty:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "no_data"})

        meta = bars.get("_meta", {})
        prev_close = float(meta.get("prev_close", 0.0))
        if prev_close <= 0:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "no_prev_close"})

        # Session bars: 9:30–entry_time
        try:
            session = spy.between_time("09:30", self._entry_time)
        except Exception:
            session = spy

        if len(session) < 5:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "insufficient_bars"})

        # Gap calculation from first bar open
        open_price = float(session.iloc[0]["open"])
        if open_price <= 0:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "invalid_open"})

        gap_pct = (open_price - prev_close) / prev_close * 100.0
        abs_gap = abs(gap_pct)

        # Gap size filter
        if abs_gap < self._min_gap_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "gap_too_small", "gap_pct": round(gap_pct, 3)})
        if abs_gap > self._max_gap_pct:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "gap_too_large", "gap_pct": round(gap_pct, 3)})

        # F30 return: first-30-min price action (9:30 → entry bar)
        f30_return = (float(session.iloc[-1]["close"]) - open_price) / open_price * 100.0

        # F30 fading check: F30 must move opposite to gap
        f30_fades_gap = (gap_pct > 0 and f30_return < 0) or (gap_pct < 0 and f30_return > 0)

        if self._require_f30_fade and not f30_fades_gap:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={
                              "skip": "f30_continues_gap",
                              "gap_pct": round(gap_pct, 3),
                              "f30_ret": round(f30_return, 3),
                          })

        # Critical: if price has already crossed prev_close during F30, the gap
        # is filled before we enter. Entering now would make our "target" the
        # wrong side of entry, producing guaranteed losses.
        current_price = float(session.iloc[-1]["close"])
        if gap_pct > 0 and current_price <= prev_close:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "gap_filled_during_f30", "gap_pct": round(gap_pct, 3)})
        if gap_pct < 0 and current_price >= prev_close:
            return Signal(direction=None, confidence=0.0, instrument=None,
                          metadata={"skip": "gap_filled_during_f30", "gap_pct": round(gap_pct, 3)})

        # VIXY filter
        vixy = bars.get("VIXY_1Min", pd.DataFrame())
        vixy_level = np.nan
        if not vixy.empty:
            try:
                vx_session = vixy.between_time("09:30", self._entry_time)
                if not vx_session.empty:
                    vixy_level = float(vx_session.iloc[-1]["close"])
            except Exception:
                pass

        if not np.isnan(vixy_level):
            if vixy_level < self._min_vixy or vixy_level > self._max_vixy:
                return Signal(direction=None, confidence=0.0, instrument=None,
                              metadata={"skip": "vixy_filter", "vixy": round(vixy_level, 2)})

        # Signal direction: fade the gap (gap up → short, gap down → long)
        direction = "short" if gap_pct > 0 else "long"

        # Confidence: inversely proportional to gap size
        confidence = max(0.5, 0.95 - (abs_gap / self._max_gap_pct) * 0.45)

        return Signal(
            direction=direction,
            confidence=round(confidence, 3),
            instrument={
                "prev_close": prev_close,
                "gap_pct": round(gap_pct, 3),
                "f30_ret": round(f30_return, 3),
                "f30_fades": f30_fades_gap,
                "vixy": round(vixy_level, 2) if not np.isnan(vixy_level) else None,
            },
            metadata={
                "gap_pct": round(gap_pct, 3),
                "gap_dir": "up" if gap_pct > 0 else "down",
                "f30_ret": round(f30_return, 3),
                "f30_fades": f30_fades_gap,
                "vixy": round(vixy_level, 2) if not np.isnan(vixy_level) else None,
                "open_price": round(open_price, 4),
                "prev_close": round(prev_close, 4),
            },
        )

    # ── Instrument Selection ──────────────────────────────────────────────────

    def select_instrument(
        self,
        signal: Signal,
        bars: Dict[str, pd.DataFrame],
        date: str,
    ) -> dict:
        if self._instrument_type == "option":
            return self._select_option(signal, bars, date)
        elif self._instrument_type == "leveraged":
            return self._select_leveraged(signal, bars, date)
        else:
            return self._select_equity(signal, bars, date)

    def _spy_entry_price(self, bars: Dict[str, pd.DataFrame]) -> float:
        """Current underlying price at entry time."""
        spy = bars.get(self._symbol_key, pd.DataFrame())
        if spy.empty:
            return 0.0
        try:
            session = spy.between_time("09:30", self._entry_time)
        except Exception:
            session = spy
        if session.empty:
            return 0.0
        return float(session.iloc[-1]["close"])

    def _select_equity(self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str) -> dict:
        """SPY shares. $0 commission at Alpaca, minimal slippage."""
        entry_price = self._spy_entry_price(bars)
        if entry_price <= 0:
            return {}

        prev_close = float(signal.instrument.get("prev_close", 0.0))
        if prev_close <= 0:
            return {}

        direction = signal.direction
        if direction == "long":
            stop_price = entry_price * (1.0 - self._stop_pct / 100.0)
        else:
            stop_price = entry_price * (1.0 + self._stop_pct / 100.0)

        symbol_key = self._symbol_key
        instrument: dict = {
            "symbol": self._symbol,
            "type": "equity",
            "entry_price": entry_price,
            "_spy_entry_price": entry_price,
            "_leverage": 1.0,
            "_max_hold_days": self._max_hold_days,
            "target_price": prev_close,
            "stop_price": stop_price,
            "direction": direction,
            "prev_close": prev_close,
            "gap_pct": signal.instrument.get("gap_pct", 0.0),
        }

        def _get_current_price(bars_snap: dict, ts) -> float:
            override = instrument.get("_exit_price_override")
            if override is not None:
                return override
            df = bars_snap.get(symbol_key, pd.DataFrame())
            if df.empty:
                return entry_price
            subset = df.loc[df.index <= ts]
            return float(subset.iloc[-1]["close"]) if not subset.empty else entry_price

        instrument["_get_current_price"] = _get_current_price
        return instrument

    def _select_leveraged(self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str) -> dict:
        """
        Synthetic 3x leveraged SPY exposure.

        Uses virtual_price = spy_price / leverage for position sizing, so
        the 3% allocation buys 3x more 'units'. P&L is computed by the engine as:
            pnl = (spy_exit - spy_entry) * direction_mult * leverage * num_units

        Equivalent to holding SPXL/SPXS but using SPY as the underlying data source.
        $0 commission (equity at Alpaca).
        """
        spy_price = self._spy_entry_price(bars)
        if spy_price <= 0:
            return {}

        prev_close = float(signal.instrument.get("prev_close", 0.0))
        if prev_close <= 0:
            return {}

        direction = signal.direction
        if direction == "long":
            stop_price = spy_price * (1.0 - self._stop_pct / 100.0)
        else:
            stop_price = spy_price * (1.0 + self._stop_pct / 100.0)

        # Virtual price for position sizing: lower price → more units → 3x exposure
        virtual_price = spy_price / self._leverage
        symbol_key = self._symbol_key

        instrument: dict = {
            "symbol": self._symbol,
            "type": "equity",
            "entry_price": virtual_price,       # used for num_units calc
            "_spy_entry_price": spy_price,       # used for P&L
            "_leverage": self._leverage,
            "_max_hold_days": self._max_hold_days,
            "target_price": prev_close,
            "stop_price": stop_price,
            "direction": direction,
            "prev_close": prev_close,
            "gap_pct": signal.instrument.get("gap_pct", 0.0),
        }

        def _get_current_price(bars_snap: dict, ts) -> float:
            # Override set by should_exit for precise target/stop fills
            override = instrument.get("_exit_price_override")
            if override is not None:
                return override
            df = bars_snap.get(symbol_key, pd.DataFrame())
            if df.empty:
                return spy_price
            subset = df.loc[df.index <= ts]
            return float(subset.iloc[-1]["close"]) if not subset.empty else spy_price

        instrument["_get_current_price"] = _get_current_price
        return instrument

    def _select_option(self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str) -> dict:
        """
        Synthetic 0DTE ATM option.

        Gap up (short) → buy PUT. Gap down (long) → buy CALL.
        Priced with Black-Scholes using VIXY-derived IV.
        Alpaca commission: $0.65/contract per leg = $1.30 round-trip.

        Exit pricing:
          - Target hit (underlying reaches prev_close): B-S price at target spot
          - Stop hit: B-S price at stop spot (may be near intrinsic value)
          - Time stop / expiry: B-S price at current spot + current time
        """
        spy_price = self._spy_entry_price(bars)
        if spy_price <= 0:
            return {}

        prev_close = float(signal.instrument.get("prev_close", 0.0))
        if prev_close <= 0:
            return {}

        direction = signal.direction
        option_type = "put" if direction == "short" else "call"

        # Strike: ATM (round to nearest $1)
        strike = round(spy_price)

        # IV from VIXY
        vixy_bars = bars.get("VIXY_1Min", pd.DataFrame())
        vixy_level = np.nan
        if not vixy_bars.empty:
            try:
                vx = vixy_bars.between_time("09:30", self._entry_time)
                if not vx.empty:
                    vixy_level = float(vx.iloc[-1]["close"])
            except Exception:
                pass
        iv = self._vol_engine.estimate_iv(vixy_level) if not np.isnan(vixy_level) else _DEFAULT_IV
        iv = max(0.10, min(iv, 1.50))  # clamp to realistic range

        # Time to expiry (minutes from entry to 16:00)
        entry_h, entry_m = map(int, self._entry_time.split(":"))
        entry_mins = entry_h * 60 + entry_m
        mins_to_close = _CLOSE_HOUR * 60 - entry_mins  # e.g., 360 for 10:00 entry
        T_entry = mins_to_close / _MINS_PER_YEAR

        entry_premium = BlackScholes.price(spy_price, strike, T_entry, _RISK_FREE_RATE, iv, option_type)

        if entry_premium < 0.10:
            return {}  # premium too low — skip

        # Stop: when underlying moves this many $ against us
        if direction == "long":
            stop_price = spy_price * (1.0 - self._stop_pct / 100.0)
        else:
            stop_price = spy_price * (1.0 + self._stop_pct / 100.0)

        symbol_key = self._symbol_key
        instrument: dict = {
            "symbol": self._symbol,
            "type": "option",
            "option_type": option_type,
            "strike": float(strike),
            "expiry": date,
            "entry_price": entry_premium,
            "_max_hold_days": 1,  # 0DTE always closes same day
            "target_price": prev_close,
            "stop_price": stop_price,
            "direction": direction,
            # Option params for B-S exit pricing
            "_opt_strike": float(strike),
            "_opt_iv": iv,
            "_opt_type": option_type,
            "_opt_entry_mins": entry_mins,
            "prev_close": prev_close,
            "gap_pct": signal.instrument.get("gap_pct", 0.0),
        }

        def _price_option_at(spot: float, ts) -> float:
            """Price the option at a given spot price and timestamp."""
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

        instrument["_get_current_price"] = _get_current_price
        instrument["_price_option_at"] = _price_option_at
        return instrument

    # ── Exit Logic ────────────────────────────────────────────────────────────

    def should_exit(
        self,
        position: dict,
        bars: Dict[str, pd.DataFrame],
    ) -> Optional[str]:
        """
        Bar-by-bar exit. Works for all instrument types.

        Exit logic is keyed on the SPY UNDERLYING price for target/stop.
        Exit PRICE depends on instrument type:
          - equity/leveraged: set _exit_price_override to target_price / stop_price
          - option: compute B-S option price at the trigger underlying level
        """
        instrument = position.get("instrument", {})
        # Use the symbol stored in the instrument dict (set at select_instrument time)
        sym_key = f"{instrument.get('symbol', self._symbol)}_1Min"
        spy_bars = bars.get(sym_key, pd.DataFrame())

        if spy_bars.empty:
            return None

        current_bar = spy_bars.iloc[-1]
        current_high = float(current_bar["high"])
        current_low = float(current_bar["low"])
        current_ts = spy_bars.index[-1]
        current_mins = current_ts.hour * 60 + current_ts.minute

        direction = instrument.get("direction", position.get("direction", "long"))
        target_price = float(instrument.get("target_price", 0.0))
        stop_price = float(instrument.get("stop_price", 0.0))
        inst_type = instrument.get("type", "equity")
        price_fn = instrument.get("_price_option_at")  # only set for options

        # ── 1. Target hit ───────────────────────────────────────────────────
        if target_price > 0:
            if direction == "long" and current_high >= target_price:
                self._set_exit_price(instrument, target_price, inst_type, price_fn, current_ts)
                return "target"
            if direction == "short" and current_low <= target_price:
                self._set_exit_price(instrument, target_price, inst_type, price_fn, current_ts)
                return "target"

        # ── 2. Stop hit ────────────────────────────────────────────────────
        if stop_price > 0:
            if direction == "long" and current_low <= stop_price:
                self._set_exit_price(instrument, stop_price, inst_type, price_fn, current_ts)
                return "stop"
            if direction == "short" and current_high >= stop_price:
                self._set_exit_price(instrument, stop_price, inst_type, price_fn, current_ts)
                return "stop"

        # ── 3. Daily time stop (only on final hold day) ────────────────────
        hold_days = position.get("hold_days", 1)
        max_hold = instrument.get("_max_hold_days", self._max_hold_days)
        time_stop_h, time_stop_m = map(int, self._time_stop.split(":"))
        time_stop_mins = time_stop_h * 60 + time_stop_m
        if hold_days >= max_hold and current_mins >= time_stop_mins:
            return "time_stop"

        return None

    @staticmethod
    def _set_exit_price(
        instrument: dict,
        trigger_spot: float,
        inst_type: str,
        price_fn,
        ts,
    ) -> None:
        """
        Set _exit_price_override based on instrument type.

        Equity/leveraged: fill at exact trigger_spot (target or stop price).
        Options: B-S value at trigger_spot with time remaining.
        """
        if inst_type == "option" and callable(price_fn):
            instrument["_exit_price_override"] = price_fn(trigger_spot, ts)
        else:
            instrument["_exit_price_override"] = trigger_spot

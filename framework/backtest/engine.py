"""
Generic event-driven backtest engine.

Runs any BaseStrategy over historical data. The strategy handles signal
generation and instrument selection; the engine handles:
  - Day loop and bar loading
  - Position sizing
  - Bar-by-bar exit simulation
  - P&L tracking and equity curve
  - Multi-day position holding (opt-in via max_hold_days in strategy config)
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from framework.strategy import BaseStrategy, Signal
from framework.data.loader import DataLoader
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar
from framework.backtest.result import BacktestResult, TradeRecord

logger = logging.getLogger(__name__)

OPTION_MULTIPLIER = 100    # shares per contract
EQUITY_MULTIPLIER = 1      # shares


class BacktestEngine:
    """
    Generic event-driven backtester.

    Parameters
    ----------
    strategy : BaseStrategy
        The strategy to backtest.
    config : dict, optional
        Engine-level overrides:
            starting_capital (float)   default 30000
            position_size_pct (float)  default 3.0
            min_position_pct (float)   default 2.0
            max_position_pct (float)   default 5.0
    storage : DataStorage, optional
        Data source. Defaults to framework root data/.
    """

    DEFAULTS: Dict[str, Any] = {
        "starting_capital": 30_000.0,
        "position_size_pct": 3.0,
        "min_position_pct": 2.0,
        "max_position_pct": 5.0,
        "min_capital_floor": 500.0,
        # Fee modeling
        "commission_per_contract": 0.65,   # $ per option contract (entry + exit)
        "slippage_pct": 0.01,              # fraction of premium (bid-ask on 0DTE)
    }

    def __init__(
        self,
        strategy: BaseStrategy,
        config: Optional[Dict[str, Any]] = None,
        storage: Optional[DataStorage] = None,
    ) -> None:
        self.strategy = strategy
        self.cfg = {**self.DEFAULTS}
        if config:
            self.cfg.update(config)

        self.starting_capital = float(self.cfg["starting_capital"])
        self.position_size_pct = float(self.cfg["position_size_pct"])

        _storage = storage or DataStorage()
        self.loader = DataLoader(storage=_storage)
        self.calendar = MarketCalendar()
        self.data_req = strategy.data_requirements()

    def run(self, start_date: str, end_date: str) -> BacktestResult:
        """
        Execute the backtest over a date range.

        Parameters
        ----------
        start_date, end_date : str
            Inclusive date range "YYYY-MM-DD".

        Returns
        -------
        BacktestResult
        """
        trading_days = self.calendar.get_trading_days(start_date, end_date)
        logger.info(
            "Backtest [%s] %s to %s — %d trading days",
            self.strategy.strategy_name(), start_date, end_date, len(trading_days),
        )

        capital = self.starting_capital
        trades: List[TradeRecord] = []
        equity_curve: List[float] = [capital]
        daily_returns: List[float] = []
        consecutive_losses = 0

        # Multi-day position tracking: persists across day loop iterations
        open_position: Optional[dict] = None

        for day_idx, date_str in enumerate(trading_days):
            prev_date_str = trading_days[day_idx - 1] if day_idx > 0 else None

            if open_position is not None:
                # Continue monitoring an existing multi-day position
                closed_trade = self._continue_position_day(
                    open_position, date_str, prev_date_str, capital
                )
                if closed_trade is not None:
                    # Position closed today
                    open_position = None
                    capital_before = capital
                    capital += closed_trade.total_pnl
                    trades.append(closed_trade)

                    if capital_before > 0:
                        daily_returns.append(closed_trade.total_pnl / capital_before)
                    else:
                        daily_returns.append(0.0)
                    equity_curve.append(capital)

                    if closed_trade.total_pnl < 0:
                        consecutive_losses += 1
                    else:
                        consecutive_losses = 0

                    logger.info(
                        "Day %d/%d [%s]: CONT %s — P&L $%.2f — Capital $%.2f (held %d days)",
                        day_idx + 1, len(trading_days), date_str,
                        closed_trade.exit_reason.upper(), closed_trade.total_pnl,
                        capital, closed_trade.features.get("hold_days", 1),
                    )
                else:
                    # Still holding
                    daily_returns.append(0.0)
                    equity_curve.append(capital)

                if capital < self.cfg["min_capital_floor"]:
                    logger.warning(
                        "Capital dropped to $%.2f on %s — halting backtest.",
                        capital, date_str,
                    )
                    break
                continue

            # No open position — look for new signal
            result = self._process_day(date_str, capital, consecutive_losses, prev_date_str)

            if result is None:
                daily_returns.append(0.0)
                equity_curve.append(capital)
                continue

            if isinstance(result, dict):
                # Multi-day position started — carry forward
                open_position = result
                daily_returns.append(0.0)
                equity_curve.append(capital)
                logger.info(
                    "Day %d/%d [%s]: ENTRY (multi-day) %s @ $%.2f",
                    day_idx + 1, len(trading_days), date_str,
                    result.get("direction", "").upper(),
                    result.get("entry_price", 0.0),
                )
                continue

            # Same-day trade completed
            trade = result
            capital_before = capital
            capital += trade.total_pnl
            trades.append(trade)

            if capital_before > 0:
                daily_returns.append(trade.total_pnl / capital_before)
            else:
                daily_returns.append(0.0)

            equity_curve.append(capital)

            if trade.total_pnl < 0:
                consecutive_losses += 1
            else:
                consecutive_losses = 0

            logger.info(
                "Day %d/%d [%s]: %s — P&L $%.2f — Capital $%.2f",
                day_idx + 1, len(trading_days), date_str,
                trade.exit_reason.upper(), trade.total_pnl, capital,
            )

            if capital < self.cfg["min_capital_floor"]:
                logger.warning(
                    "Capital dropped to $%.2f on %s — halting backtest.",
                    capital, date_str,
                )
                break

        return BacktestResult(
            trades=trades,
            equity_curve=equity_curve,
            daily_returns=daily_returns,
            starting_capital=self.starting_capital,
            strategy_name=self.strategy.strategy_name(),
            config=self.cfg,
        )

    # -- Per-day processing ------------------------------------------------

    def _process_day(
        self,
        date_str: str,
        capital: float,
        consecutive_losses: int = 0,
        prev_date_str: Optional[str] = None,
    ) -> Optional[Union[TradeRecord, dict]]:
        """
        Run the strategy for a single day.

        Returns:
          - TradeRecord  : trade entered and closed same day
          - dict         : position dict (trade entered, carries to next day)
          - None         : no trade taken
        """
        # Load prev close for _meta context
        prev_close = self._get_prev_close(prev_date_str) if prev_date_str else 0.0

        # Load bars up to the strategy's entry time
        entry_time = self.strategy.config.get("entry", {}).get("time") or "15:29"
        bars = self.loader.load_bars_for_date(
            date_str, self.data_req, up_to_time=entry_time
        )

        # Inject market context — strategies may use prev_close for gap computation
        bars["_meta"] = {
            "date": date_str,
            "prev_date": prev_date_str,
            "prev_close": prev_close,
            "hold_day": 0,   # 0 = entry day
        }

        # Check we have data for the primary symbol
        primary = self.data_req.primary_symbol()
        primary_key = f"{primary}_1Min"
        if primary_key not in bars or bars[primary_key].empty:
            logger.debug("Skipping %s: no data for %s", date_str, primary_key)
            return None

        # Generate signal
        signal = self.strategy.generate_signal(bars, date_str)
        if not signal.is_valid():
            logger.debug("Skipping %s: no signal", date_str)
            return None

        # Select instrument
        instrument = self.strategy.select_instrument(signal, bars, date_str)
        if not instrument or instrument.get("entry_price", 0) <= 0:
            logger.debug("Skipping %s: instrument selection failed", date_str)
            return None

        entry_price = float(instrument["entry_price"])
        instrument_type = instrument.get("type", "option")
        multiplier = OPTION_MULTIPLIER if instrument_type == "option" else EQUITY_MULTIPLIER

        # Position sizing
        max_allocation = capital * self.position_size_pct / 100.0
        cost_per_unit = entry_price * multiplier

        if cost_per_unit <= 0:
            logger.debug("Skipping %s: cost_per_unit <= 0", date_str)
            return None

        num_units = max(1, math.floor(max_allocation / cost_per_unit))
        # Cap so total cost doesn't exceed allocation
        while num_units * cost_per_unit > max_allocation and num_units > 1:
            num_units -= 1

        if cost_per_unit > capital:
            logger.debug("Skipping %s: cannot afford 1 unit ($%.2f > $%.2f)", date_str, cost_per_unit, capital)
            return None

        cost_basis = entry_price * multiplier * num_units

        # Determine multi-day config
        max_hold_days = int(
            instrument.get("_max_hold_days",
            self.strategy.config.get("exit", {}).get("max_hold_days", 1))
        )

        # Build position dict
        position = {
            "entry_price": entry_price,
            "entry_time": entry_time,
            "direction": signal.direction,
            "instrument": instrument,
            "num_units": num_units,
            "multiplier": multiplier,
            "date": date_str,
            # Cached for _build_trade_record
            "cost_basis": cost_basis,
            "instrument_type": instrument_type,
            "signal_confidence": signal.confidence,
            "signal_metadata": signal.metadata,
            # Multi-day tracking
            "hold_days": 1,
            "_max_hold_days": max_hold_days,
        }

        # Load ALL bars for the day (post-entry) for exit simulation
        all_bars = self.loader.load_bars_for_date(date_str, self.data_req, up_to_time=None)
        all_bars["_meta"] = bars["_meta"]  # carry context forward

        # Use actual instrument entry bar time if available
        exit_monitor_from = instrument.get("_entry_bar_time") or entry_time

        # Simulate bar-by-bar exit
        exit_price, exit_time_str, exit_reason = self._simulate_exit(
            position, all_bars, date_str, exit_monitor_from
        )

        if exit_reason == "expiry" and max_hold_days > 1:
            # No same-day exit — carry position forward
            return position

        # Build and return same-day trade record
        return self._build_trade_record(
            position, exit_price, exit_time_str, exit_reason, all_bars, date_str
        )

    def _continue_position_day(
        self,
        position: dict,
        date_str: str,
        prev_date_str: Optional[str],
        capital: float,
    ) -> Optional[TradeRecord]:
        """
        Monitor an open multi-day position on a new trading day.

        Returns TradeRecord if the position closed today, None if still holding.
        """
        prev_close = self._get_prev_close(prev_date_str) if prev_date_str else 0.0

        all_bars = self.loader.load_bars_for_date(date_str, self.data_req, up_to_time=None)
        all_bars["_meta"] = {
            "date": date_str,
            "prev_date": prev_date_str,
            "prev_close": prev_close,
            "hold_day": position.get("hold_days", 1),
            "entry_date": position["date"],
        }

        primary = self.data_req.primary_symbol()
        primary_key = f"{primary}_1Min"
        if primary_key not in all_bars or all_bars[primary_key].empty:
            # No data — increment hold and continue
            position["hold_days"] = position.get("hold_days", 1) + 1
            return None

        # Monitor from market open on continuation days
        exit_price, exit_time_str, exit_reason = self._simulate_exit(
            position, all_bars, date_str, "09:29"
        )

        hold_days = position.get("hold_days", 1)
        max_hold_days = position.get("_max_hold_days", 1)
        reached_max = hold_days >= max_hold_days

        if exit_reason != "expiry" or reached_max:
            if exit_reason == "expiry" and reached_max:
                exit_reason = "max_hold"
            return self._build_trade_record(
                position, exit_price, exit_time_str, exit_reason, all_bars, date_str
            )

        # Still holding
        position["hold_days"] = hold_days + 1
        return None

    def _build_trade_record(
        self,
        position: dict,
        exit_price: float,
        exit_time_str: str,
        exit_reason: str,
        all_bars: dict,
        exit_date_str: str,
    ) -> TradeRecord:
        """
        Construct a TradeRecord from a completed position.

        Works for both same-day and multi-day trades.
        """
        instrument = position["instrument"]
        instrument_type = position.get("instrument_type", instrument.get("type", "option"))
        entry_price = position["entry_price"]
        num_units = position["num_units"]
        multiplier = position["multiplier"]
        cost_basis = position.get("cost_basis", entry_price * multiplier * num_units)
        direction = position["direction"]

        # P&L direction
        if instrument_type == "option":
            direction_mult = 1.0
        else:
            direction_mult = 1.0 if direction == "long" else -1.0

        leverage_factor = float(instrument.get("_leverage", 1.0))
        spy_entry = float(instrument.get("_spy_entry_price", entry_price))
        if instrument_type == "equity" and leverage_factor != 1.0:
            pnl_per_unit = (exit_price - spy_entry) * multiplier * direction_mult * leverage_factor
        else:
            pnl_per_unit = (exit_price - entry_price) * multiplier * direction_mult
        total_pnl = pnl_per_unit * num_units

        # Fees
        commission = self.cfg["commission_per_contract"]
        slippage_pct = self.cfg["slippage_pct"]
        _EQUITY_SLIPPAGE_PCT = 0.0001
        if instrument_type == "option":
            total_commission = commission * num_units * 2
            total_slippage = entry_price * multiplier * num_units * slippage_pct * 2
        else:
            total_commission = 0.0
            total_slippage = entry_price * num_units * _EQUITY_SLIPPAGE_PCT * 2
        total_fees = total_commission + total_slippage
        total_pnl -= total_fees

        return_pct = total_pnl / cost_basis if cost_basis > 0 else 0.0

        # Underlying price reference
        primary = self.data_req.primary_symbol()
        entry_date_str = position["date"]
        primary_1min = all_bars.get(f"{primary}_1Min", pd.DataFrame())
        entry_underlying = self._price_at_time(primary_1min, exit_date_str, position["entry_time"])
        exit_underlying = self._price_at_time(primary_1min, exit_date_str, exit_time_str)

        # Metadata
        features = dict(position.get("signal_metadata", {}))
        hold_days = position.get("hold_days", 1)
        if hold_days > 1 or entry_date_str != exit_date_str:
            features["hold_days"] = hold_days
            features["exit_date"] = exit_date_str

        return TradeRecord(
            date=entry_date_str,
            direction=direction,
            entry_time=position["entry_time"],
            exit_time=exit_time_str,
            exit_reason=exit_reason,
            instrument_type=instrument_type,
            symbol=instrument.get("symbol", primary),
            option_type=instrument.get("option_type"),
            strike=instrument.get("strike"),
            expiry=str(instrument.get("expiry", "")),
            entry_price=entry_price,
            exit_price=exit_price,
            entry_underlying_price=entry_underlying,
            exit_underlying_price=exit_underlying,
            num_units=num_units,
            multiplier=multiplier,
            cost_basis=round(cost_basis, 2),
            pnl_per_unit=round(pnl_per_unit, 2),
            total_pnl=round(total_pnl, 2),
            total_fees=round(total_fees, 2),
            return_pct=round(return_pct, 4),
            signal_confidence=position.get("signal_confidence", 0.0),
            features=features,
            greeks=instrument.get("greeks", {}),
        )

    def _simulate_exit(
        self,
        position: dict,
        all_bars: Dict[str, pd.DataFrame],
        date_str: str,
        entry_time: str,
    ):
        """
        Bar-by-bar exit simulation.

        Calls strategy.should_exit() on each bar after entry.
        Returns (exit_price, exit_time_str, exit_reason).
        """
        primary = self.data_req.primary_symbol()
        primary_bars = all_bars.get(f"{primary}_1Min", pd.DataFrame())

        if primary_bars.empty:
            return position["entry_price"], entry_time, "no_data"

        # Filter to bars strictly after entry
        entry_hour, entry_min = map(int, entry_time.split(":"))
        if primary_bars.index.tz is not None:
            entry_cutoff = pd.Timestamp(
                f"{date_str} {entry_hour:02d}:{entry_min:02d}:00"
            ).tz_localize(str(primary_bars.index.tz))
        else:
            entry_cutoff = pd.Timestamp(f"{date_str} {entry_hour:02d}:{entry_min:02d}:00")

        post_entry_bars = primary_bars.loc[primary_bars.index > entry_cutoff]

        if post_entry_bars.empty:
            return position["entry_price"], entry_time, "no_post_entry_bars"

        for ts, row in post_entry_bars.iterrows():
            # Build current_bars snapshot up to this bar
            current_bars = {}
            for key, df in all_bars.items():
                if key == "_meta":
                    current_bars[key] = df  # pass through dict unchanged
                elif not df.empty:
                    current_bars[key] = df.loc[df.index <= ts]

            exit_reason = self.strategy.should_exit(position, current_bars)

            if exit_reason is not None:
                current_price = self._get_current_instrument_price(
                    position, current_bars, date_str, ts
                )
                exit_time_str = ts.strftime("%H:%M")
                return current_price, exit_time_str, exit_reason

        # No exit triggered — return last bar
        last_ts = post_entry_bars.index[-1]
        last_price = self._get_current_instrument_price(
            position, all_bars, date_str, last_ts
        )
        return last_price, last_ts.strftime("%H:%M"), "expiry"

    def _get_current_instrument_price(
        self, position: dict, bars: dict, date_str: str, ts
    ) -> float:
        """
        Get current instrument price from position's instrument dict.
        """
        instrument = position.get("instrument", {})

        get_price_fn = instrument.get("_get_current_price")
        if callable(get_price_fn):
            return get_price_fn(bars, ts)

        return instrument.get("current_price", position.get("entry_price", 0.0))

    def _get_prev_close(self, prev_date_str: str) -> float:
        """Load the previous trading day's closing price for the primary symbol."""
        try:
            prev_bars = self.loader.load_bars_for_date(
                prev_date_str, self.data_req, up_to_time=None
            )
            primary_key = f"{self.data_req.primary_symbol()}_1Min"
            prev_1min = prev_bars.get(primary_key, pd.DataFrame())
            if not prev_1min.empty:
                return float(prev_1min.iloc[-1]["close"])
        except Exception:
            pass
        return 0.0

    @staticmethod
    def _price_at_time(df: pd.DataFrame, date_str: str, time_str: str) -> float:
        """Get close price from a DataFrame at a specific time."""
        if df.empty:
            return 0.0
        hour, minute = map(int, time_str.split(":"))
        if df.index.tz is not None:
            target = pd.Timestamp(f"{date_str} {hour:02d}:{minute:02d}:00").tz_localize(str(df.index.tz))
        else:
            target = pd.Timestamp(f"{date_str} {hour:02d}:{minute:02d}:00")
        subset = df.loc[df.index <= target]
        if subset.empty:
            return 0.0
        return float(subset.iloc[-1]["close"])

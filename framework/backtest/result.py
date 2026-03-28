"""
BacktestResult — generic result container and metrics computation.
No strategy-specific assumptions.
"""

from __future__ import annotations

import math
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """
    Single trade record produced by the generic backtest engine.

    Fields cover all instrument types (options and equity).
    Options-specific fields default to None for equity trades.
    """

    date: str                       # "YYYY-MM-DD"
    direction: str                  # "long" or "short"
    entry_time: str                 # "HH:MM"
    exit_time: str                  # "HH:MM"
    exit_reason: str                # "target", "stop", "time_stop", "expiry", etc.

    # Instrument
    instrument_type: str            # "option" or "equity"
    symbol: str                     # Primary symbol traded
    option_type: Optional[str] = None   # "call" or "put" (options only)
    strike: Optional[float] = None      # Strike price (options only)
    expiry: Optional[str] = None        # Expiry date string (options only)

    # Prices
    entry_price: float = 0.0        # Per-share / per-unit price
    exit_price: float = 0.0

    # Underlying price (for reference)
    entry_underlying_price: float = 0.0
    exit_underlying_price: float = 0.0

    # Position
    num_units: int = 1              # Contracts for options, shares for equity
    multiplier: int = 1             # 100 for options, 1 for equity

    # P&L
    cost_basis: float = 0.0        # entry_price * multiplier * num_units
    pnl_per_unit: float = 0.0      # (exit - entry) * multiplier (gross, before fees)
    total_pnl: float = 0.0         # net after fees
    total_fees: float = 0.0        # commission + slippage deducted
    return_pct: float = 0.0        # total_pnl / cost_basis

    # Metadata
    signal_confidence: float = 0.0
    features: dict = field(default_factory=dict)
    greeks: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        sign = "+" if self.total_pnl >= 0 else ""
        inst = f"{self.option_type.upper() if self.option_type else ''} ${self.strike:.0f}" if self.option_type else self.symbol
        return (
            f"TradeRecord({self.date} {inst} | "
            f"{self.entry_time}->{self.exit_time} [{self.exit_reason}] | "
            f"{self.num_units}x @ ${self.entry_price:.2f}->${self.exit_price:.2f} | "
            f"P&L {sign}${self.total_pnl:.2f} ({self.return_pct:+.1%}))"
        )


class BacktestResult:
    """
    Aggregated results and performance metrics from a generic backtest run.
    """

    def __init__(
        self,
        trades: List[TradeRecord],
        equity_curve: List[float],
        daily_returns: List[float],
        starting_capital: float,
        strategy_name: str = "",
        config: dict = None,
    ) -> None:
        self.trades = trades
        self.equity_curve = equity_curve
        self.daily_returns = daily_returns
        self.starting_capital = starting_capital
        self.strategy_name = strategy_name
        self.config = config or {}

    # -- Core counts --------------------------------------------------------

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def winning_trades(self) -> int:
        return sum(1 for t in self.trades if t.total_pnl > 0)

    @property
    def losing_trades(self) -> int:
        return sum(1 for t in self.trades if t.total_pnl < 0)

    @property
    def scratch_trades(self) -> int:
        return sum(1 for t in self.trades if t.total_pnl == 0)

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

    # -- P&L metrics --------------------------------------------------------

    @property
    def total_pnl(self) -> float:
        return sum(t.total_pnl for t in self.trades)

    @property
    def gross_wins(self) -> float:
        return sum(t.total_pnl for t in self.trades if t.total_pnl > 0)

    @property
    def gross_losses(self) -> float:
        return abs(sum(t.total_pnl for t in self.trades if t.total_pnl < 0))

    @property
    def avg_win(self) -> float:
        wins = [t.total_pnl for t in self.trades if t.total_pnl > 0]
        return float(np.mean(wins)) if wins else 0.0

    @property
    def avg_loss(self) -> float:
        losses = [t.total_pnl for t in self.trades if t.total_pnl < 0]
        return float(np.mean(losses)) if losses else 0.0

    @property
    def profit_factor(self) -> float:
        if self.gross_losses == 0:
            return float("inf") if self.gross_wins > 0 else 0.0
        return self.gross_wins / self.gross_losses

    @property
    def expectancy(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.total_pnl / self.total_trades

    # -- Drawdown -----------------------------------------------------------

    @property
    def max_drawdown(self) -> float:
        if not self.equity_curve:
            return 0.0
        peak = self.equity_curve[0]
        max_dd = 0.0
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
        return max_dd

    @property
    def max_drawdown_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        peak = self.equity_curve[0]
        max_dd_pct = 0.0
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            if peak > 0:
                dd_pct = (peak - equity) / peak
                if dd_pct > max_dd_pct:
                    max_dd_pct = dd_pct
        return max_dd_pct

    # -- Sharpe ratio -------------------------------------------------------

    @property
    def sharpe_ratio(self) -> float:
        if len(self.daily_returns) < 2:
            return 0.0
        returns = np.array(self.daily_returns)
        mean_return = np.mean(returns)
        std_return = np.std(returns, ddof=1)
        if std_return == 0:
            return 0.0
        return float(mean_return / std_return * math.sqrt(252))

    # -- Streaks ------------------------------------------------------------

    @property
    def max_consecutive_losses(self) -> int:
        if not self.trades:
            return 0
        max_streak, current = 0, 0
        for t in self.trades:
            if t.total_pnl < 0:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak

    @property
    def max_consecutive_wins(self) -> int:
        if not self.trades:
            return 0
        max_streak, current = 0, 0
        for t in self.trades:
            if t.total_pnl > 0:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak

    # -- Summary ------------------------------------------------------------

    @property
    def total_fees(self) -> float:
        return sum(t.total_fees for t in self.trades)

    def summary(self) -> Dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "starting_capital": self.starting_capital,
            "ending_capital": self.equity_curve[-1] if self.equity_curve else self.starting_capital,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "scratch_trades": self.scratch_trades,
            "win_rate": round(self.win_rate, 4),
            "total_pnl": round(self.total_pnl, 2),
            "total_fees": round(self.total_fees, 2),
            "total_return_pct": round(self.total_pnl / self.starting_capital * 100, 2) if self.starting_capital > 0 else 0.0,
            "avg_win": round(self.avg_win, 2),
            "avg_loss": round(self.avg_loss, 2),
            "profit_factor": round(self.profit_factor, 4) if self.profit_factor != float("inf") else "inf",
            "expectancy": round(self.expectancy, 2),
            "max_drawdown": round(self.max_drawdown, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 4),
            "max_consecutive_losses": self.max_consecutive_losses,
            "max_consecutive_wins": self.max_consecutive_wins,
            "gross_wins": round(self.gross_wins, 2),
            "gross_losses": round(self.gross_losses, 2),
        }

    def print_summary(self) -> None:
        s = self.summary()
        width = 60
        strat = s.get("strategy_name") or "STRATEGY"
        print("=" * width)
        print(f"  {strat.upper()} — BACKTEST RESULTS")
        print("=" * width)
        print()
        print("  CAPITAL")
        print(f"    Starting:                ${s['starting_capital']:>12,.2f}")
        print(f"    Ending:                  ${s['ending_capital']:>12,.2f}")
        print(f"    Net P&L:                 ${s['total_pnl']:>+12,.2f}")
        print(f"    Total Fees:              ${s['total_fees']:>12,.2f}")
        print(f"    Total Return:            {s['total_return_pct']:>+12.2f}%")
        print()
        print("  TRADES")
        print(f"    Total:                   {s['total_trades']:>12d}")
        print(f"    Winners:                 {s['winning_trades']:>12d}")
        print(f"    Losers:                  {s['losing_trades']:>12d}")
        print(f"    Scratch:                 {s['scratch_trades']:>12d}")
        print(f"    Win Rate:                {s['win_rate']:>12.1%}")
        print()
        print("  P&L BREAKDOWN")
        print(f"    Avg Win:                 ${s['avg_win']:>+12,.2f}")
        print(f"    Avg Loss:                ${s['avg_loss']:>+12,.2f}")
        pf_str = s['profit_factor'] if isinstance(s['profit_factor'], str) else f"{s['profit_factor']:.2f}"
        print(f"    Profit Factor:           {pf_str:>12s}")
        print(f"    Expectancy (per trade):  ${s['expectancy']:>+12,.2f}")
        print()
        print("  RISK")
        print(f"    Max Drawdown:            ${s['max_drawdown']:>12,.2f}")
        print(f"    Max Drawdown %:          {s['max_drawdown_pct']:>12.2%}")
        print(f"    Sharpe Ratio:            {s['sharpe_ratio']:>12.2f}")
        print(f"    Max Consec. Losses:      {s['max_consecutive_losses']:>12d}")
        print(f"    Max Consec. Wins:        {s['max_consecutive_wins']:>12d}")
        print()
        print("=" * width)

    def to_dataframe(self) -> pd.DataFrame:
        if not self.trades:
            return pd.DataFrame()
        records = []
        for t in self.trades:
            row = {
                "date": t.date,
                "direction": t.direction,
                "instrument_type": t.instrument_type,
                "symbol": t.symbol,
                "option_type": t.option_type,
                "strike": t.strike,
                "entry_time": t.entry_time,
                "entry_price": t.entry_price,
                "exit_time": t.exit_time,
                "exit_price": t.exit_price,
                "exit_reason": t.exit_reason,
                "num_units": t.num_units,
                "total_pnl": t.total_pnl,
                "return_pct": t.return_pct,
                "signal_confidence": t.signal_confidence,
            }
            for k, v in t.greeks.items():
                row[f"greek_{k}"] = v
            records.append(row)
        return pd.DataFrame(records)

    def save_json(self, path) -> None:
        """Save summary + trades to a JSON file for the board runner."""
        import json
        from pathlib import Path
        data = {
            "summary": self.summary(),
            "trades": [
                {k: v for k, v in t.__dict__.items() if not isinstance(v, dict) or k == "features"}
                for t in self.trades
            ],
        }
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

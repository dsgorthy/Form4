"""
Telegram alert system for the trading framework.
Constructor-param version: no settings import.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import telegram
import telegram.error

logger = logging.getLogger(__name__)


class TelegramAlerts:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot = telegram.Bot(token=bot_token)
        self.chat_id = chat_id

    async def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """
        Send a plain text message.

        Returns True on success, False on failure (logs the error).
        """
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
            )
            return True
        except telegram.error.TelegramError as exc:
            logger.error("Telegram send_message failed: %s", exc)
            return False

    async def send_morning_briefing(
        self,
        trade_date: date,
        spy_prev_close: float,
        spy_open: float,
        vix_level: float,
        events: list,
        notes: str = "",
    ) -> bool:
        """
        Send pre-market morning briefing.

        Parameters
        ----------
        trade_date : date
        spy_prev_close : float
        spy_open : float
        vix_level : float
        events : list of str
            Economic events today (e.g. ["cpi", "fomc"]).
        notes : str
            Any additional context.
        """
        gap_pct = ((spy_open - spy_prev_close) / spy_prev_close) * 100.0 if spy_prev_close else 0.0
        events_str = ", ".join(events).upper() if events else "None"
        gap_arrow = "+" if gap_pct >= 0 else ""

        lines = [
            f"*Morning Briefing — {trade_date.strftime('%Y-%m-%d')}*",
            "",
            f"SPY Prev Close: ${spy_prev_close:.2f}",
            f"SPY Open: ${spy_open:.2f} ({gap_arrow}{gap_pct:.2f}%)",
            f"VIX: {vix_level:.1f}",
            f"Events: {events_str}",
        ]
        if notes:
            lines += ["", notes]

        return await self.send_message("\n".join(lines))

    async def send_pre_trade_alert(
        self,
        trade_date: date,
        direction: str,
        strike: float,
        expiry: str,
        option_type: str,
        entry_price: float,
        confidence: float,
        num_contracts: int,
        total_cost: float,
        iv: float,
        greeks: dict,
        signal_metadata: Optional[dict] = None,
    ) -> bool:
        """
        Send pre-trade alert just before order submission.

        Parameters
        ----------
        direction : str  "long" or "short"
        strike : float
        expiry : str  "YYYY-MM-DD"
        option_type : str  "call" or "put"
        entry_price : float  per share
        confidence : float  [0,1]
        num_contracts : int
        total_cost : float  total debit/credit
        iv : float  implied vol used (annualized decimal)
        greeks : dict  {"delta", "gamma", "theta", "vega"}
        signal_metadata : dict, optional  arbitrary signal context
        """
        dir_symbol = "LONG" if direction == "long" else "SHORT"
        ot = option_type.upper()
        delta_str = f"{greeks.get('delta', 0.0):.3f}"
        theta_str = f"{greeks.get('theta', 0.0):.4f}"
        iv_pct = iv * 100.0

        lines = [
            f"*Pre-Trade Alert — {trade_date.strftime('%Y-%m-%d')}*",
            "",
            f"Direction: {dir_symbol} {ot}",
            f"Strike: ${strike:.1f}  |  Expiry: {expiry}",
            f"Entry: ${entry_price:.2f}/sh  x{num_contracts} contracts = ${total_cost:.0f}",
            f"IV: {iv_pct:.1f}%  |  Confidence: {int(confidence * 100)}%",
            f"Delta: {delta_str}  |  Theta: {theta_str}/day",
        ]
        if signal_metadata:
            move_pct = signal_metadata.get("intraday_move_pct", 0.0)
            lines.append(f"Intraday move: {move_pct:+.2f}%")

        return await self.send_message("\n".join(lines))

    async def send_post_trade_summary(
        self,
        trade_date: date,
        direction: str,
        option_type: str,
        strike: float,
        entry_price: float,
        exit_price: float,
        num_contracts: int,
        pnl: float,
        pnl_pct: float,
        exit_reason: str,
        entry_time: str,
        exit_time: str,
    ) -> bool:
        """
        Send post-trade summary after position is closed.

        Parameters
        ----------
        pnl : float  dollar P&L (positive = profit)
        pnl_pct : float  P&L as percent of entry cost
        exit_reason : str  e.g. "target", "stop", "time_stop"
        """
        win_loss = "WIN" if pnl >= 0 else "LOSS"
        pnl_sign = "+" if pnl >= 0 else ""
        ot = option_type.upper()

        lines = [
            f"*Trade Closed — {trade_date.strftime('%Y-%m-%d')}*  [{win_loss}]",
            "",
            f"{direction.upper()} {ot} ${strike:.1f}",
            f"Entry: ${entry_price:.2f}  ({entry_time})",
            f"Exit:  ${exit_price:.2f}  ({exit_time})",
            f"Contracts: {num_contracts}",
            f"P&L: {pnl_sign}${pnl:.2f}  ({pnl_sign}{pnl_pct:.1f}%)",
            f"Exit reason: {exit_reason}",
        ]

        return await self.send_message("\n".join(lines))

    async def send_error_alert(self, context: str, error: Exception) -> bool:
        """
        Send an error/exception alert.

        Parameters
        ----------
        context : str
            Human-readable description of what was happening.
        error : Exception
        """
        lines = [
            "*ERROR ALERT*",
            "",
            f"Context: {context}",
            f"Error: {type(error).__name__}: {error}",
        ]
        try:
            return await self.send_message("\n".join(lines))
        except Exception as inner:
            logger.error("Failed to send error alert: %s", inner)
            return False

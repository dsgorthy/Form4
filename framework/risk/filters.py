"""
Trade filters for the trading framework.

Applies pre-trade risk checks before allowing order submission.
All filters are configurable via a config dict passed at construction.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional

from framework.data.calendar import EconomicCalendar

logger = logging.getLogger(__name__)

DEFAULT_CONFIG: dict = {
    # Intraday move filter
    "min_intraday_move_pct": 0.10,   # minimum % move from open to be interesting
    "max_intraday_move_pct": 3.0,    # skip extreme trend days

    # Volatility filter (VIXY-based)
    "min_vix": 15.0,                 # skip thin-premium environments
    "max_vix": 70.0,                 # skip likely trend/panic days

    # Time filters
    "latest_entry_time": "15:35",    # no new entries after this time (Eastern)

    # Capital / drawdown protection
    "daily_loss_cap_pct": 5.0,       # stop trading if daily P&L < -5% of capital

    # Economic event filters
    "reject_on_fomc": False,         # if True, skip FOMC days entirely
    "reject_on_cpi": False,          # if True, skip CPI days entirely
    "reject_on_nfp": False,          # if True, skip NFP days entirely
    "reject_on_ppi": False,          # if True, skip PPI days entirely
}


class TradeFilter:
    """
    Pre-trade filter battery.

    Call check_all() to run all filters. Each _check_* method returns
    (passed: bool, reason: str). check_all() returns (True, "") on pass
    or (False, "rejection reason") on failure.
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._economic_calendar = EconomicCalendar()

    def check_all(
        self,
        date: str,
        intraday_move_pct: float,
        vix_level: float,
        entry_time: str,
        daily_pnl_pct: float = 0.0,
    ) -> tuple:
        """
        Run all filters and return (passed, reason).

        Parameters
        ----------
        date : str
            Trading date "YYYY-MM-DD".
        intraday_move_pct : float
            Percent move from open to current (signed, e.g., -0.5 = down 0.5%).
        vix_level : float
            Current VIX / vol-proxy level.
        entry_time : str
            Proposed entry time "HH:MM" Eastern.
        daily_pnl_pct : float
            Today's realized P&L as a percent of starting capital (negative = loss).

        Returns
        -------
        (bool, str)
            (True, "") if all filters pass, (False, "reason") on first failure.
        """
        checks = [
            self._check_intraday_move(intraday_move_pct),
            self._check_vix(vix_level),
            self._check_entry_time(entry_time),
            self._check_daily_loss_cap(daily_pnl_pct),
            self._check_economic_events(date),
        ]

        for passed, reason in checks:
            if not passed:
                logger.info("TradeFilter REJECT [%s]: %s", date, reason)
                return False, reason

        logger.debug("TradeFilter PASS [%s]", date)
        return True, ""

    def _check_intraday_move(self, move_pct: float) -> tuple:
        """Require a minimum intraday move; skip extreme trend days."""
        abs_move = abs(move_pct)
        min_move = self.config["min_intraday_move_pct"]
        max_move = self.config["max_intraday_move_pct"]

        if abs_move < min_move:
            return False, f"intraday move {abs_move:.2f}% < min {min_move:.2f}% (flat day)"
        if abs_move > max_move:
            return False, f"intraday move {abs_move:.2f}% > max {max_move:.2f}% (extreme trend)"
        return True, ""

    def _check_vix(self, vix_level: float) -> tuple:
        """Skip when vol is too low (thin premiums) or too high (trend day)."""
        min_vix = self.config["min_vix"]
        max_vix = self.config["max_vix"]

        if vix_level < min_vix:
            return False, f"VIX {vix_level:.1f} < min {min_vix:.1f} (thin premiums)"
        if vix_level > max_vix:
            return False, f"VIX {vix_level:.1f} > max {max_vix:.1f} (extreme volatility)"
        return True, ""

    def _check_entry_time(self, entry_time: str) -> tuple:
        """Reject if entry time is past the latest allowed entry."""
        latest_str = self.config["latest_entry_time"]

        def _to_minutes(t: str) -> int:
            h, m = map(int, t.split(":"))
            return h * 60 + m

        entry_mins = _to_minutes(entry_time)
        latest_mins = _to_minutes(latest_str)

        if entry_mins > latest_mins:
            return False, f"entry time {entry_time} is after latest allowed {latest_str}"
        return True, ""

    def _check_daily_loss_cap(self, daily_pnl_pct: float) -> tuple:
        """Stop trading if daily losses exceed the cap."""
        cap = self.config["daily_loss_cap_pct"]
        if daily_pnl_pct < -cap:
            return False, f"daily P&L {daily_pnl_pct:.2f}% breached loss cap -{cap:.1f}%"
        return True, ""

    def _check_economic_events(self, date: str) -> tuple:
        """Optionally reject on major economic event days."""
        ec = self._economic_calendar

        if self.config.get("reject_on_fomc") and ec.is_fomc_day(date):
            return False, "FOMC day — filter active"
        if self.config.get("reject_on_cpi") and ec.is_cpi_day(date):
            return False, "CPI release day — filter active"
        if self.config.get("reject_on_nfp") and ec.is_nfp_day(date):
            return False, "NFP release day — filter active"
        if self.config.get("reject_on_ppi") and ec.is_ppi_day(date):
            return False, "PPI release day — filter active"

        return True, ""

    def get_events_summary(self, date: str) -> Dict:
        """Return a summary of economic events on the given date (informational)."""
        ec = self._economic_calendar
        events = ec.get_events(date)
        return {
            "date": date,
            "events": events,
            "is_high_impact": ec.is_high_impact_day(date),
            "days_since_fomc": ec.days_since_fomc(date),
            "days_until_fomc": ec.days_until_fomc(date),
        }

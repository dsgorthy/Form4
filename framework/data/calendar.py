"""
Market calendar and economic events for the trading framework.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, Union

logger = logging.getLogger(__name__)

# Timezone and market hours constants (no external config dependency)
TIMEZONE = "US/Eastern"
MARKET_OPEN = "09:30"
MARKET_CLOSE = "16:00"

DateLike = Union[str, datetime, date]


def _to_date(d: DateLike) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(d, "%Y-%m-%d").date()


def _to_str(d: DateLike) -> str:
    if isinstance(d, str):
        return d
    if isinstance(d, datetime):
        return d.date().isoformat()
    return d.isoformat()


# ── NYSE Holidays ─────────────────────────────────────────────────────────────
# Full 2024-2026 holiday schedule

_NYSE_HOLIDAYS: set = {
    # 2024
    date(2024, 1, 1),   # New Year's Day
    date(2024, 1, 15),  # MLK Day
    date(2024, 2, 19),  # Presidents' Day
    date(2024, 3, 29),  # Good Friday
    date(2024, 5, 27),  # Memorial Day
    date(2024, 6, 19),  # Juneteenth
    date(2024, 7, 4),   # Independence Day
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2024, 12, 25), # Christmas

    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 9),   # National Day of Mourning (Carter)
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas

    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed, July 4 is Saturday)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
}

# ── Early Close Days (1:00 PM ET) ─────────────────────────────────────────────

_EARLY_CLOSE_DAYS: set = {
    # 2024
    date(2024, 7, 3),   # Day before Independence Day
    date(2024, 11, 29), # Day after Thanksgiving
    date(2024, 12, 24), # Christmas Eve

    # 2025
    date(2025, 7, 3),   # Day before Independence Day (observed)
    date(2025, 11, 28), # Day after Thanksgiving
    date(2025, 12, 24), # Christmas Eve

    # 2026
    date(2026, 11, 27), # Day after Thanksgiving
    date(2026, 12, 24), # Christmas Eve
}

# ── FOMC Meeting Dates ────────────────────────────────────────────────────────

_FOMC_DATES: set = {
    # 2024 FOMC decision dates
    date(2024, 1, 31),
    date(2024, 3, 20),
    date(2024, 5, 1),
    date(2024, 6, 12),
    date(2024, 7, 31),
    date(2024, 9, 18),
    date(2024, 11, 7),
    date(2024, 12, 18),

    # 2025 FOMC decision dates
    date(2025, 1, 29),
    date(2025, 3, 19),
    date(2025, 5, 7),
    date(2025, 6, 18),
    date(2025, 7, 30),
    date(2025, 9, 17),
    date(2025, 11, 5),
    date(2025, 12, 17),

    # 2026 FOMC decision dates
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 4, 29),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 11, 4),
    date(2026, 12, 16),
}

# ── CPI Release Dates ─────────────────────────────────────────────────────────

_CPI_DATES: set = {
    # 2024
    date(2024, 1, 11),
    date(2024, 2, 13),
    date(2024, 3, 12),
    date(2024, 4, 10),
    date(2024, 5, 15),
    date(2024, 6, 12),
    date(2024, 7, 11),
    date(2024, 8, 14),
    date(2024, 9, 11),
    date(2024, 10, 10),
    date(2024, 11, 13),
    date(2024, 12, 11),

    # 2025
    date(2025, 1, 15),
    date(2025, 2, 12),
    date(2025, 3, 12),
    date(2025, 4, 10),
    date(2025, 5, 13),
    date(2025, 6, 11),
    date(2025, 7, 15),
    date(2025, 8, 12),
    date(2025, 9, 10),
    date(2025, 10, 15),
    date(2025, 11, 13),
    date(2025, 12, 10),

    # 2026
    date(2026, 1, 14),
    date(2026, 2, 11),
    date(2026, 3, 11),
    date(2026, 4, 9),
    date(2026, 5, 13),
    date(2026, 6, 10),
    date(2026, 7, 15),
    date(2026, 8, 12),
    date(2026, 9, 9),
    date(2026, 10, 14),
    date(2026, 11, 12),
    date(2026, 12, 9),
}

# ── PPI Release Dates ─────────────────────────────────────────────────────────

_PPI_DATES: set = {
    # 2024
    date(2024, 1, 12),
    date(2024, 2, 16),
    date(2024, 3, 14),
    date(2024, 4, 11),
    date(2024, 5, 14),
    date(2024, 6, 13),
    date(2024, 7, 12),
    date(2024, 8, 13),
    date(2024, 9, 12),
    date(2024, 10, 11),
    date(2024, 11, 14),
    date(2024, 12, 12),

    # 2025
    date(2025, 1, 14),
    date(2025, 2, 13),
    date(2025, 3, 13),
    date(2025, 4, 11),
    date(2025, 5, 15),
    date(2025, 6, 12),
    date(2025, 7, 11),
    date(2025, 8, 14),
    date(2025, 9, 11),
    date(2025, 10, 16),
    date(2025, 11, 14),
    date(2025, 12, 11),

    # 2026
    date(2026, 1, 15),
    date(2026, 2, 12),
    date(2026, 3, 12),
    date(2026, 4, 14),
    date(2026, 5, 14),
    date(2026, 6, 11),
    date(2026, 7, 14),
    date(2026, 8, 13),
    date(2026, 9, 10),
    date(2026, 10, 15),
    date(2026, 11, 13),
    date(2026, 12, 10),
}

# ── NFP (Non-Farm Payrolls) Release Dates ─────────────────────────────────────

_NFP_DATES: set = {
    # 2024
    date(2024, 1, 5),
    date(2024, 2, 2),
    date(2024, 3, 8),
    date(2024, 4, 5),
    date(2024, 5, 3),
    date(2024, 6, 7),
    date(2024, 7, 5),
    date(2024, 8, 2),
    date(2024, 9, 6),
    date(2024, 10, 4),
    date(2024, 11, 1),
    date(2024, 12, 6),

    # 2025
    date(2025, 1, 10),
    date(2025, 2, 7),
    date(2025, 3, 7),
    date(2025, 4, 4),
    date(2025, 5, 2),
    date(2025, 6, 6),
    date(2025, 7, 3),  # Independence Day eve — check actual date
    date(2025, 8, 1),
    date(2025, 9, 5),
    date(2025, 10, 3),
    date(2025, 11, 7),
    date(2025, 12, 5),

    # 2026
    date(2026, 1, 9),
    date(2026, 2, 6),
    date(2026, 3, 6),
    date(2026, 4, 3),
    date(2026, 5, 8),
    date(2026, 6, 5),
    date(2026, 7, 2),
    date(2026, 8, 7),
    date(2026, 9, 4),
    date(2026, 10, 2),
    date(2026, 11, 6),
    date(2026, 12, 4),
}

# ── SPY Ex-Dividend Dates ─────────────────────────────────────────────────────
# SPY pays quarterly dividends, ex-div dates typically in mid-March, June, Sep, Dec

_SPY_EXDIV_DATES: set = {
    # 2024
    date(2024, 3, 15),
    date(2024, 6, 21),
    date(2024, 9, 20),
    date(2024, 12, 20),

    # 2025
    date(2025, 3, 21),
    date(2025, 6, 20),
    date(2025, 9, 19),
    date(2025, 12, 19),

    # 2026
    date(2026, 3, 20),
    date(2026, 6, 19),
    date(2026, 9, 18),
    date(2026, 12, 18),
}


class MarketCalendar:
    """
    NYSE market calendar: trading days, holidays, early closes.

    Usage
    -----
    cal = MarketCalendar()
    cal.is_trading_day("2025-01-20")   # False (MLK Day)
    cal.get_trading_days("2025-01-01", "2025-01-31")  # list of date strings
    cal.is_early_close("2025-07-03")   # True
    """

    def is_trading_day(self, dt: DateLike) -> bool:
        """Return True if the given date is an NYSE trading day."""
        d = _to_date(dt)
        if d.weekday() >= 5:
            return False
        if d in _NYSE_HOLIDAYS:
            return False
        return True

    def is_holiday(self, dt: DateLike) -> bool:
        """Return True if the given date is an NYSE holiday."""
        return _to_date(dt) in _NYSE_HOLIDAYS

    def is_early_close(self, dt: DateLike) -> bool:
        """Return True if the given date is an early-close (1 PM ET) day."""
        return _to_date(dt) in _EARLY_CLOSE_DAYS

    def next_trading_day(self, dt: DateLike) -> date:
        """Return the next NYSE trading day after the given date."""
        d = _to_date(dt) + timedelta(days=1)
        while not self.is_trading_day(d):
            d += timedelta(days=1)
        return d

    def prev_trading_day(self, dt: DateLike) -> date:
        """Return the most recent NYSE trading day before the given date."""
        d = _to_date(dt) - timedelta(days=1)
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    def get_trading_days(self, start: DateLike, end: DateLike) -> list:
        """
        Return a list of trading day strings ("YYYY-MM-DD") inclusive.

        Parameters
        ----------
        start, end : DateLike
            Inclusive range boundaries.

        Returns
        -------
        list of str
        """
        start_d = _to_date(start)
        end_d = _to_date(end)
        if start_d > end_d:
            return []
        days = []
        current = start_d
        while current <= end_d:
            if self.is_trading_day(current):
                days.append(current.isoformat())
            current += timedelta(days=1)
        return days

    def trading_days_count(self, start: DateLike, end: DateLike) -> int:
        """Return the count of trading days in the inclusive range."""
        return len(self.get_trading_days(start, end))

    def market_open_time(self, dt: DateLike) -> Optional[str]:
        """Return market open time for the given date, or None if closed."""
        if not self.is_trading_day(dt):
            return None
        return MARKET_OPEN

    def market_close_time(self, dt: DateLike) -> Optional[str]:
        """Return market close time for the given date, or None if closed."""
        if not self.is_trading_day(dt):
            return None
        if self.is_early_close(dt):
            return "13:00"
        return MARKET_CLOSE


class EconomicCalendar:
    """
    Economic event calendar: FOMC, CPI, PPI, NFP, SPY ex-dividend.

    Usage
    -----
    ec = EconomicCalendar()
    ec.is_fomc_day("2025-03-19")     # True
    ec.is_high_impact_day("2025-03-19")  # True
    ec.get_events("2025-03-19")      # ["fomc"]
    """

    def is_fomc_day(self, dt: DateLike) -> bool:
        return _to_date(dt) in _FOMC_DATES

    def is_cpi_day(self, dt: DateLike) -> bool:
        return _to_date(dt) in _CPI_DATES

    def is_ppi_day(self, dt: DateLike) -> bool:
        return _to_date(dt) in _PPI_DATES

    def is_nfp_day(self, dt: DateLike) -> bool:
        return _to_date(dt) in _NFP_DATES

    def is_spy_exdiv_day(self, dt: DateLike) -> bool:
        return _to_date(dt) in _SPY_EXDIV_DATES

    def is_high_impact_day(self, dt: DateLike) -> bool:
        """True if FOMC, CPI, PPI, or NFP release on this date."""
        d = _to_date(dt)
        return d in _FOMC_DATES or d in _CPI_DATES or d in _PPI_DATES or d in _NFP_DATES

    def get_events(self, dt: DateLike) -> list:
        """
        Return a list of event names on the given date.

        Returns
        -------
        list of str — may include "fomc", "cpi", "ppi", "nfp", "spy_exdiv"
        """
        d = _to_date(dt)
        events = []
        if d in _FOMC_DATES:
            events.append("fomc")
        if d in _CPI_DATES:
            events.append("cpi")
        if d in _PPI_DATES:
            events.append("ppi")
        if d in _NFP_DATES:
            events.append("nfp")
        if d in _SPY_EXDIV_DATES:
            events.append("spy_exdiv")
        return events

    def days_since_fomc(self, dt: DateLike) -> int:
        """Return calendar days since the most recent FOMC date, or -1 if none found."""
        d = _to_date(dt)
        past = sorted((fd for fd in _FOMC_DATES if fd <= d), reverse=True)
        if not past:
            return -1
        return (d - past[0]).days

    def days_until_fomc(self, dt: DateLike) -> int:
        """Return calendar days until the next FOMC date, or -1 if none found."""
        d = _to_date(dt)
        future = sorted(fd for fd in _FOMC_DATES if fd > d)
        if not future:
            return -1
        return (future[0] - d).days

"""
Tests for MarketCalendar and EconomicCalendar.

Covers: weekends, holidays, trading day ranges, early close days, FOMC dates.
"""

from __future__ import annotations

from datetime import date

import pytest

from framework.data.calendar import MarketCalendar, EconomicCalendar


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cal() -> MarketCalendar:
    return MarketCalendar()


@pytest.fixture
def econ_cal() -> EconomicCalendar:
    return EconomicCalendar()


# ---------------------------------------------------------------------------
# Tests: weekends excluded
# ---------------------------------------------------------------------------

class TestWeekendsExcluded:
    """Saturdays and Sundays should never be trading days."""

    def test_saturday_not_trading_day(self, cal):
        # 2025-06-14 is a Saturday
        assert cal.is_trading_day("2025-06-14") is False

    def test_sunday_not_trading_day(self, cal):
        # 2025-06-15 is a Sunday
        assert cal.is_trading_day("2025-06-15") is False

    def test_monday_is_trading_day(self, cal):
        # 2025-06-16 is a Monday (not a holiday)
        assert cal.is_trading_day("2025-06-16") is True

    def test_friday_is_trading_day(self, cal):
        # 2025-06-13 is a Friday (not a holiday)
        assert cal.is_trading_day("2025-06-13") is True

    def test_weekends_not_in_range(self, cal):
        """A full week range with no holidays should return exactly 5 trading days."""
        # 2025-06-02 Mon through 2025-06-08 Sun — no holidays this week
        days = cal.get_trading_days("2025-06-02", "2025-06-08")
        assert len(days) == 5
        # Verify all are weekdays
        for d_str in days:
            d = date.fromisoformat(d_str)
            assert d.weekday() < 5, f"{d_str} is a weekend day"


# ---------------------------------------------------------------------------
# Tests: known holidays excluded
# ---------------------------------------------------------------------------

class TestHolidaysExcluded:
    """NYSE holidays should not be trading days."""

    def test_mlk_day_2025(self, cal):
        """2025-01-20 is MLK Day — not a trading day."""
        assert cal.is_trading_day("2025-01-20") is False
        assert cal.is_holiday("2025-01-20") is True

    def test_christmas_2025(self, cal):
        """2025-12-25 is Christmas — not a trading day."""
        assert cal.is_trading_day("2025-12-25") is False
        assert cal.is_holiday("2025-12-25") is True

    def test_new_years_2025(self, cal):
        """2025-01-01 is New Year's Day — not a trading day."""
        assert cal.is_trading_day("2025-01-01") is False

    def test_good_friday_2025(self, cal):
        """2025-04-18 is Good Friday — not a trading day."""
        assert cal.is_trading_day("2025-04-18") is False

    def test_independence_day_2025(self, cal):
        """2025-07-04 is Independence Day — not a trading day."""
        assert cal.is_trading_day("2025-07-04") is False

    def test_thanksgiving_2025(self, cal):
        """2025-11-27 is Thanksgiving — not a trading day."""
        assert cal.is_trading_day("2025-11-27") is False

    def test_carter_mourning_day_2025(self, cal):
        """2025-01-09 is National Day of Mourning (Carter) — not a trading day."""
        assert cal.is_trading_day("2025-01-09") is False

    def test_non_holiday_weekday_is_trading_day(self, cal):
        """A regular weekday that is not a holiday should be a trading day."""
        # 2025-06-16 is a Monday, no holiday
        assert cal.is_trading_day("2025-06-16") is True
        assert cal.is_holiday("2025-06-16") is False


# ---------------------------------------------------------------------------
# Tests: trading day range counts
# ---------------------------------------------------------------------------

class TestTradingDayRange:
    """get_trading_days and trading_days_count correctness."""

    def test_january_2025_count(self, cal):
        """
        January 2025 has:
        - 23 weekdays
        - Minus holidays: Jan 1 (New Year), Jan 9 (Carter mourning), Jan 20 (MLK)
        - = 20 trading days
        """
        days = cal.get_trading_days("2025-01-01", "2025-01-31")
        assert len(days) == 20

    def test_trading_days_count_matches_list(self, cal):
        count = cal.trading_days_count("2025-01-01", "2025-01-31")
        days = cal.get_trading_days("2025-01-01", "2025-01-31")
        assert count == len(days)

    def test_single_day_range(self, cal):
        """Range of one trading day."""
        days = cal.get_trading_days("2025-06-16", "2025-06-16")
        assert days == ["2025-06-16"]

    def test_single_day_holiday(self, cal):
        """Range of one day that's a holiday -> empty."""
        days = cal.get_trading_days("2025-12-25", "2025-12-25")
        assert days == []

    def test_empty_range_when_start_after_end(self, cal):
        days = cal.get_trading_days("2025-06-20", "2025-06-16")
        assert days == []

    def test_range_returns_strings(self, cal):
        """get_trading_days should return list of YYYY-MM-DD strings."""
        days = cal.get_trading_days("2025-06-16", "2025-06-18")
        for d in days:
            assert isinstance(d, str)
            assert len(d) == 10  # "YYYY-MM-DD"

    def test_date_objects_accepted(self, cal):
        """get_trading_days should accept date objects."""
        days = cal.get_trading_days(date(2025, 6, 16), date(2025, 6, 18))
        assert len(days) == 3


# ---------------------------------------------------------------------------
# Tests: early close days
# ---------------------------------------------------------------------------

class TestEarlyCloseDays:
    """Early close days should return the correct close time."""

    def test_day_after_thanksgiving_2025(self, cal):
        """2025-11-28 (day after Thanksgiving) is an early close day."""
        assert cal.is_early_close("2025-11-28") is True

    def test_christmas_eve_2025(self, cal):
        """2025-12-24 (Christmas Eve) is an early close day."""
        assert cal.is_early_close("2025-12-24") is True

    def test_july_3_2025(self, cal):
        """2025-07-03 (day before Independence Day observed) is early close."""
        assert cal.is_early_close("2025-07-03") is True

    def test_normal_day_not_early_close(self, cal):
        """A normal trading day is NOT an early close."""
        assert cal.is_early_close("2025-06-16") is False

    def test_early_close_market_time(self, cal):
        """Early close day should have market close at 13:00."""
        assert cal.market_close_time("2025-11-28") == "13:00"

    def test_normal_close_time(self, cal):
        """Normal trading day should close at 16:00."""
        assert cal.market_close_time("2025-06-16") == "16:00"

    def test_holiday_close_time_none(self, cal):
        """Holiday should have no close time (None)."""
        assert cal.market_close_time("2025-12-25") is None

    def test_market_open_time_normal(self, cal):
        assert cal.market_open_time("2025-06-16") == "09:30"

    def test_market_open_time_holiday(self, cal):
        assert cal.market_open_time("2025-12-25") is None


# ---------------------------------------------------------------------------
# Tests: FOMC dates
# ---------------------------------------------------------------------------

class TestFOMCDates:
    """EconomicCalendar.is_fomc_day and related methods."""

    def test_known_fomc_date_2025_march(self, econ_cal):
        """2025-03-19 is a known FOMC decision date."""
        assert econ_cal.is_fomc_day("2025-03-19") is True

    def test_known_fomc_date_2025_january(self, econ_cal):
        assert econ_cal.is_fomc_day("2025-01-29") is True

    def test_non_fomc_date(self, econ_cal):
        assert econ_cal.is_fomc_day("2025-06-16") is False

    def test_fomc_in_events(self, econ_cal):
        events = econ_cal.get_events("2025-03-19")
        assert "fomc" in events

    def test_no_events_on_normal_day(self, econ_cal):
        events = econ_cal.get_events("2025-06-16")
        assert events == []

    def test_is_high_impact_fomc(self, econ_cal):
        assert econ_cal.is_high_impact_day("2025-03-19") is True

    def test_normal_day_not_high_impact(self, econ_cal):
        assert econ_cal.is_high_impact_day("2025-06-16") is False

    def test_days_until_fomc(self, econ_cal):
        """2025-03-17 -> next FOMC is 2025-03-19, so 2 days until."""
        assert econ_cal.days_until_fomc("2025-03-17") == 2

    def test_days_since_fomc(self, econ_cal):
        """2025-03-20 -> most recent FOMC was 2025-03-19, so 1 day since."""
        assert econ_cal.days_since_fomc("2025-03-20") == 1

    def test_days_since_fomc_on_fomc_day(self, econ_cal):
        """On FOMC day itself, days_since should be 0."""
        assert econ_cal.days_since_fomc("2025-03-19") == 0


# ---------------------------------------------------------------------------
# Tests: next/prev trading day
# ---------------------------------------------------------------------------

class TestNextPrevTradingDay:
    """MarketCalendar.next_trading_day and prev_trading_day."""

    def test_next_after_friday(self, cal):
        """Next trading day after Friday 2025-06-13 is Monday 2025-06-16."""
        assert cal.next_trading_day("2025-06-13") == date(2025, 6, 16)

    def test_next_after_saturday(self, cal):
        """Next trading day after Saturday 2025-06-14 is Monday 2025-06-16."""
        assert cal.next_trading_day("2025-06-14") == date(2025, 6, 16)

    def test_next_skips_holiday(self, cal):
        """Next after Wednesday before Thanksgiving should skip Thursday."""
        # 2025-11-26 (Wed) -> next is 2025-11-28 (Fri, early close but still trading)
        assert cal.next_trading_day("2025-11-26") == date(2025, 11, 28)

    def test_prev_before_monday(self, cal):
        """Previous trading day before Monday 2025-06-16 is Friday 2025-06-13."""
        assert cal.prev_trading_day("2025-06-16") == date(2025, 6, 13)

    def test_prev_skips_holiday(self, cal):
        """Previous trading day before 2025-01-21 (Tue) should skip MLK Monday."""
        assert cal.prev_trading_day("2025-01-21") == date(2025, 1, 17)


# ---------------------------------------------------------------------------
# Tests: CPI, PPI, NFP events
# ---------------------------------------------------------------------------

class TestOtherEconomicEvents:
    """Verify other economic event lookups."""

    def test_cpi_date(self, econ_cal):
        assert econ_cal.is_cpi_day("2025-01-15") is True

    def test_ppi_date(self, econ_cal):
        assert econ_cal.is_ppi_day("2025-01-14") is True

    def test_nfp_date(self, econ_cal):
        assert econ_cal.is_nfp_day("2025-01-10") is True

    def test_spy_exdiv_date(self, econ_cal):
        assert econ_cal.is_spy_exdiv_day("2025-03-21") is True

    def test_multiple_events_same_day(self, econ_cal):
        """Some dates may have overlapping events — verify list contains all."""
        # Manually check if any date has multiple events in the test data
        # 2025-01-15 is CPI day — check if anything else is on that date
        events = econ_cal.get_events("2025-01-15")
        assert "cpi" in events

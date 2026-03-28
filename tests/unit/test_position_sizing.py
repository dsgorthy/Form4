"""
Tests for position sizing logic.

The position sizing logic lives in BacktestEngine._process_day (lines ~274-291
of engine.py). We extract and test the core math in isolation rather than
running the full engine, using the same formulas:

    max_allocation = capital * position_size_pct / 100.0
    cost_per_unit  = entry_price * multiplier
    num_units      = max(1, floor(max_allocation / cost_per_unit))
    # cap so total cost doesn't exceed allocation
    while num_units * cost_per_unit > max_allocation and num_units > 1:
        num_units -= 1
    # skip if cost_per_unit > capital
"""

from __future__ import annotations

import math

import pytest


# ---------------------------------------------------------------------------
# Extracted sizing function (mirrors engine.py lines 274-291)
# ---------------------------------------------------------------------------

def compute_position_size(
    capital: float,
    position_size_pct: float,
    entry_price: float,
    multiplier: int,
) -> int | None:
    """
    Compute number of units to trade.

    Returns the number of units (contracts for options, shares for equity),
    or None if the trade cannot be taken (price too high for capital).

    This mirrors the sizing logic in BacktestEngine._process_day.
    """
    max_allocation = capital * position_size_pct / 100.0
    cost_per_unit = entry_price * multiplier

    if cost_per_unit <= 0:
        return None

    num_units = max(1, math.floor(max_allocation / cost_per_unit))

    # Cap so total cost doesn't exceed allocation
    while num_units * cost_per_unit > max_allocation and num_units > 1:
        num_units -= 1

    # Skip if can't afford even 1 unit
    if cost_per_unit > capital:
        return None

    return num_units


# ---------------------------------------------------------------------------
# Tests: basic equity sizing
# ---------------------------------------------------------------------------

class TestBasicEquitySizing:
    """Equity trades: multiplier=1, shares as units."""

    def test_standard_sizing(self):
        """
        $30K capital, 5% allocation, $500 stock.
        max_allocation = 30000 * 5/100 = $1500
        cost_per_unit = 500 * 1 = $500
        num_units = floor(1500 / 500) = 3
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=5.0,
            entry_price=500.0,
            multiplier=1,
        )
        assert result == 3

    def test_small_allocation(self):
        """
        $30K capital, 2% allocation, $500 stock.
        max_allocation = 30000 * 2/100 = $600
        cost_per_unit = 500
        num_units = max(1, floor(600/500)) = max(1, 1) = 1
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=2.0,
            entry_price=500.0,
            multiplier=1,
        )
        assert result == 1

    def test_large_allocation(self):
        """
        $100K capital, 10% allocation, $50 stock.
        max_allocation = 100000 * 10/100 = $10000
        cost_per_unit = 50
        num_units = floor(10000/50) = 200
        """
        result = compute_position_size(
            capital=100_000.0,
            position_size_pct=10.0,
            entry_price=50.0,
            multiplier=1,
        )
        assert result == 200

    def test_fractional_units_round_down(self):
        """
        $30K capital, 3% allocation, $400 stock.
        max_allocation = 30000 * 3/100 = $900
        cost_per_unit = 400
        num_units = max(1, floor(900/400)) = max(1, 2) = 2
        2 * 400 = 800 <= 900, so 2 shares.
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=3.0,
            entry_price=400.0,
            multiplier=1,
        )
        assert result == 2


# ---------------------------------------------------------------------------
# Tests: options sizing
# ---------------------------------------------------------------------------

class TestOptionsSizing:
    """Options trades: multiplier=100."""

    def test_standard_options_sizing(self):
        """
        $30K capital, 3% allocation, $5.00 option premium.
        max_allocation = 30000 * 3/100 = $900
        cost_per_unit = 5.00 * 100 = $500
        num_units = max(1, floor(900/500)) = 1
        1 * 500 = 500 <= 900 -> 1 contract.
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=3.0,
            entry_price=5.0,
            multiplier=100,
        )
        assert result == 1

    def test_cheap_options_multiple_contracts(self):
        """
        $30K capital, 5% allocation, $1.50 option premium.
        max_allocation = 30000 * 5/100 = $1500
        cost_per_unit = 1.50 * 100 = $150
        num_units = floor(1500/150) = 10
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=5.0,
            entry_price=1.50,
            multiplier=100,
        )
        assert result == 10

    def test_expensive_option_one_contract(self):
        """
        $30K capital, 3% allocation, $8.00 option.
        max_allocation = $900
        cost_per_unit = 800
        num_units = max(1, floor(900/800)) = 1
        1 * 800 = 800 <= 900 -> 1 contract
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=3.0,
            entry_price=8.0,
            multiplier=100,
        )
        assert result == 1


# ---------------------------------------------------------------------------
# Tests: price too high (skip trade)
# ---------------------------------------------------------------------------

class TestPriceTooHigh:
    """When cost_per_unit > capital, the trade should be skipped."""

    def test_option_too_expensive_for_capital(self):
        """
        $500 capital, $6.00 option.
        cost_per_unit = 6.00 * 100 = $600 > $500 capital -> skip.
        """
        result = compute_position_size(
            capital=500.0,
            position_size_pct=5.0,
            entry_price=6.0,
            multiplier=100,
        )
        assert result is None

    def test_equity_too_expensive_for_capital(self):
        """
        $400 capital, $500 stock.
        cost_per_unit = 500 * 1 = $500 > $400 -> skip.
        """
        result = compute_position_size(
            capital=400.0,
            position_size_pct=5.0,
            entry_price=500.0,
            multiplier=1,
        )
        assert result is None

    def test_zero_price_returns_none(self):
        """Zero entry price -> cost_per_unit=0 -> skip."""
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=5.0,
            entry_price=0.0,
            multiplier=100,
        )
        assert result is None


# ---------------------------------------------------------------------------
# Tests: exactly at allocation limit
# ---------------------------------------------------------------------------

class TestExactAllocationLimit:
    """Edge case: cost exactly matches the allocation."""

    def test_exact_fit_one_unit(self):
        """
        $30K capital, 5% allocation = $1500 max.
        entry_price=15.0, multiplier=100 -> cost_per_unit=1500.
        num_units = max(1, floor(1500/1500)) = 1
        1 * 1500 = 1500 <= 1500 -> 1 contract.
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=5.0,
            entry_price=15.0,
            multiplier=100,
        )
        assert result == 1

    def test_exact_fit_multiple_units(self):
        """
        $30K capital, 5% allocation = $1500 max.
        entry_price=5.0, multiplier=100 -> cost_per_unit=500.
        num_units = floor(1500/500) = 3
        3 * 500 = 1500 <= 1500 -> 3 contracts exactly.
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=5.0,
            entry_price=5.0,
            multiplier=100,
        )
        assert result == 3

    def test_slightly_over_allocation_caps_down(self):
        """
        $30K capital, 5% allocation = $1500 max.
        entry_price=5.10, multiplier=100 -> cost_per_unit=510.
        num_units = max(1, floor(1500/510)) = max(1, 2) = 2
        2 * 510 = 1020 <= 1500 -> 2 contracts (not 3, which would be 1530 > 1500).

        Note: floor(1500/510) = floor(2.94) = 2. The while loop doesn't
        further reduce because 2*510=1020 <= 1500.
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=5.0,
            entry_price=5.10,
            multiplier=100,
        )
        assert result == 2
        # Verify cost is within allocation
        assert result * 5.10 * 100 <= 30_000.0 * 5.0 / 100.0

    def test_minimum_one_unit_even_if_over_allocation(self):
        """
        When allocation is less than cost_per_unit but capital is sufficient,
        the engine allows 1 unit (floor gives 0, max(1,0)=1, while loop
        can't reduce below 1).

        $30K capital, 1% allocation = $300.
        entry_price=5.0, multiplier=100 -> cost_per_unit=500.
        num_units = max(1, floor(300/500)) = max(1, 0) = 1
        1 * 500 = 500 > 300 but while stops at num_units=1.
        500 <= 30000 capital -> trade proceeds with 1 unit.
        """
        result = compute_position_size(
            capital=30_000.0,
            position_size_pct=1.0,
            entry_price=5.0,
            multiplier=100,
        )
        assert result == 1

"""
Tests for P&L calculation logic.

The P&L logic lives in BacktestEngine._build_trade_record (lines ~411-436
of engine.py). We extract and test the core formulas in isolation:

For options (direction_mult always 1.0):
    pnl_per_unit = (exit_price - entry_price) * multiplier * 1.0
    total_pnl    = pnl_per_unit * num_units
    fees:
        total_commission = commission_per_contract * num_units * 2
        total_slippage   = entry_price * multiplier * num_units * slippage_pct * 2
        total_fees       = total_commission + total_slippage
    total_pnl -= total_fees

For equity (direction_mult = 1.0 if long, -1.0 if short):
    If leveraged (leverage != 1.0):
        pnl_per_unit = (exit_price - spy_entry_price) * multiplier * direction_mult * leverage
    Else:
        pnl_per_unit = (exit_price - entry_price) * multiplier * direction_mult
    total_pnl = pnl_per_unit * num_units
    fees:
        total_commission = 0.0
        total_slippage   = entry_price * num_units * 0.0001 * 2
        total_fees       = total_slippage
    total_pnl -= total_fees
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Extracted P&L function (mirrors engine.py lines 411-436)
# ---------------------------------------------------------------------------

_EQUITY_SLIPPAGE_PCT = 0.0001  # from engine.py


def compute_pnl(
    entry_price: float,
    exit_price: float,
    num_units: int,
    multiplier: int,
    direction: str,
    instrument_type: str,
    commission_per_contract: float = 0.65,
    slippage_pct: float = 0.01,
    leverage_factor: float = 1.0,
    spy_entry_price: float | None = None,
) -> dict:
    """
    Compute P&L for a trade, mirroring BacktestEngine._build_trade_record.

    Returns a dict with:
        pnl_per_unit, total_pnl (before fees), total_fees,
        net_pnl (total_pnl after fee deduction), cost_basis, return_pct.
    """
    cost_basis = entry_price * multiplier * num_units

    # Direction multiplier
    if instrument_type == "option":
        direction_mult = 1.0
    else:
        direction_mult = 1.0 if direction == "long" else -1.0

    # P&L per unit
    if instrument_type == "equity" and leverage_factor != 1.0:
        _spy_entry = spy_entry_price if spy_entry_price is not None else entry_price
        pnl_per_unit = (exit_price - _spy_entry) * multiplier * direction_mult * leverage_factor
    else:
        pnl_per_unit = (exit_price - entry_price) * multiplier * direction_mult

    total_pnl_gross = pnl_per_unit * num_units

    # Fees
    if instrument_type == "option":
        total_commission = commission_per_contract * num_units * 2
        total_slippage = entry_price * multiplier * num_units * slippage_pct * 2
    else:
        total_commission = 0.0
        total_slippage = entry_price * num_units * _EQUITY_SLIPPAGE_PCT * 2

    total_fees = total_commission + total_slippage
    net_pnl = total_pnl_gross - total_fees
    return_pct = net_pnl / cost_basis if cost_basis > 0 else 0.0

    return {
        "pnl_per_unit": pnl_per_unit,
        "total_pnl_gross": total_pnl_gross,
        "total_fees": total_fees,
        "total_commission": total_commission,
        "total_slippage": total_slippage,
        "net_pnl": net_pnl,
        "cost_basis": cost_basis,
        "return_pct": return_pct,
    }


# ---------------------------------------------------------------------------
# Tests: long equity P&L
# ---------------------------------------------------------------------------

class TestLongEquityPnL:
    """Long equity: buy at entry, sell at exit."""

    def test_basic_long_profit(self):
        """
        Buy $100, sell $105, 10 shares.
        pnl_per_unit = (105 - 100) * 1 * 1.0 = $5
        total_pnl_gross = 5 * 10 = $50
        fees = 100 * 10 * 0.0001 * 2 = $0.20
        net_pnl = 50 - 0.20 = $49.80
        """
        result = compute_pnl(
            entry_price=100.0,
            exit_price=105.0,
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
        )
        assert result["pnl_per_unit"] == pytest.approx(5.0)
        assert result["total_pnl_gross"] == pytest.approx(50.0)
        assert result["total_fees"] == pytest.approx(0.20)
        assert result["net_pnl"] == pytest.approx(49.80)

    def test_long_loss(self):
        """
        Buy $100, sell $95, 10 shares.
        pnl_per_unit = (95 - 100) * 1 * 1.0 = -$5
        total_pnl_gross = -5 * 10 = -$50
        fees = 100 * 10 * 0.0001 * 2 = $0.20
        net_pnl = -50 - 0.20 = -$50.20
        """
        result = compute_pnl(
            entry_price=100.0,
            exit_price=95.0,
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
        )
        assert result["pnl_per_unit"] == pytest.approx(-5.0)
        assert result["total_pnl_gross"] == pytest.approx(-50.0)
        assert result["net_pnl"] == pytest.approx(-50.20)

    def test_long_breakeven(self):
        """
        Buy $100, sell $100, 10 shares.
        pnl_per_unit = 0
        total_pnl_gross = 0
        fees = $0.20
        net_pnl = -$0.20 (slight loss from fees)
        """
        result = compute_pnl(
            entry_price=100.0,
            exit_price=100.0,
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
        )
        assert result["pnl_per_unit"] == pytest.approx(0.0)
        assert result["total_pnl_gross"] == pytest.approx(0.0)
        assert result["net_pnl"] == pytest.approx(-0.20)


# ---------------------------------------------------------------------------
# Tests: short equity P&L
# ---------------------------------------------------------------------------

class TestShortEquityPnL:
    """Short equity: sell at entry, buy at exit."""

    def test_short_profit(self):
        """
        Short $100, cover $95, 10 shares.
        direction_mult = -1.0
        pnl_per_unit = (95 - 100) * 1 * (-1.0) = 5.0
        total_pnl_gross = 5 * 10 = $50
        fees = 100 * 10 * 0.0001 * 2 = $0.20
        net_pnl = $49.80
        """
        result = compute_pnl(
            entry_price=100.0,
            exit_price=95.0,
            num_units=10,
            multiplier=1,
            direction="short",
            instrument_type="equity",
        )
        assert result["pnl_per_unit"] == pytest.approx(5.0)
        assert result["total_pnl_gross"] == pytest.approx(50.0)
        assert result["net_pnl"] == pytest.approx(49.80)

    def test_short_loss(self):
        """
        Short $100, cover $105, 10 shares.
        direction_mult = -1.0
        pnl_per_unit = (105 - 100) * 1 * (-1.0) = -5.0
        total_pnl_gross = -5 * 10 = -$50
        fees = $0.20
        net_pnl = -$50.20
        """
        result = compute_pnl(
            entry_price=100.0,
            exit_price=105.0,
            num_units=10,
            multiplier=1,
            direction="short",
            instrument_type="equity",
        )
        assert result["pnl_per_unit"] == pytest.approx(-5.0)
        assert result["total_pnl_gross"] == pytest.approx(-50.0)
        assert result["net_pnl"] == pytest.approx(-50.20)


# ---------------------------------------------------------------------------
# Tests: leveraged equity P&L
# ---------------------------------------------------------------------------

class TestLeveragedEquityPnL:
    """Leveraged equity using _spy_entry_price and _leverage."""

    def test_3x_leverage_long(self):
        """
        3x leveraged long: spy_entry_price=$500, exit=$505, 10 shares.
        pnl_per_unit = (505 - 500) * 1 * 1.0 * 3.0 = 15.0
        total_pnl_gross = 15 * 10 = $150
        fees = entry_price * 10 * 0.0001 * 2

        Note: entry_price is the ETF entry (may differ from spy_entry_price),
        but for this test we use entry_price=$500 for fee calc.
        """
        result = compute_pnl(
            entry_price=500.0,
            exit_price=505.0,
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
            leverage_factor=3.0,
            spy_entry_price=500.0,
        )
        assert result["pnl_per_unit"] == pytest.approx(15.0)
        assert result["total_pnl_gross"] == pytest.approx(150.0)
        # fees = 500 * 10 * 0.0001 * 2 = $1.00
        assert result["total_fees"] == pytest.approx(1.0)
        assert result["net_pnl"] == pytest.approx(149.0)

    def test_3x_leverage_short(self):
        """
        3x leveraged short: spy_entry_price=$500, exit=$505, 10 shares.
        direction_mult = -1.0
        pnl_per_unit = (505 - 500) * 1 * (-1.0) * 3.0 = -15.0
        total_pnl_gross = -15 * 10 = -$150
        """
        result = compute_pnl(
            entry_price=500.0,
            exit_price=505.0,
            num_units=10,
            multiplier=1,
            direction="short",
            instrument_type="equity",
            leverage_factor=3.0,
            spy_entry_price=500.0,
        )
        assert result["pnl_per_unit"] == pytest.approx(-15.0)
        assert result["total_pnl_gross"] == pytest.approx(-150.0)

    def test_leverage_uses_spy_entry_not_entry_price(self):
        """
        When leveraged, P&L is computed from spy_entry_price, not entry_price.
        entry_price=$100 (leveraged ETF price), spy_entry=$500, exit_spy=$505.

        pnl_per_unit = (505 - 500) * 1 * 1.0 * 3.0 = 15.0

        But exit_price here represents the SPY exit, so the formula uses
        exit_price directly. The key point is spy_entry_price is used as
        the base, not entry_price.
        """
        result = compute_pnl(
            entry_price=100.0,   # leveraged ETF price (only for cost_basis & fees)
            exit_price=505.0,    # exit at SPY equivalent
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
            leverage_factor=3.0,
            spy_entry_price=500.0,
        )
        # pnl_per_unit = (505 - 500) * 1 * 1.0 * 3.0 = 15.0
        assert result["pnl_per_unit"] == pytest.approx(15.0)
        assert result["total_pnl_gross"] == pytest.approx(150.0)
        # cost_basis uses entry_price for the ETF: 100 * 1 * 10 = 1000
        assert result["cost_basis"] == pytest.approx(1000.0)

    def test_1x_leverage_same_as_unleveraged(self):
        """With leverage_factor=1.0, the code takes the non-leveraged path."""
        result_lev = compute_pnl(
            entry_price=100.0,
            exit_price=105.0,
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
            leverage_factor=1.0,
        )
        result_plain = compute_pnl(
            entry_price=100.0,
            exit_price=105.0,
            num_units=10,
            multiplier=1,
            direction="long",
            instrument_type="equity",
        )
        assert result_lev["net_pnl"] == pytest.approx(result_plain["net_pnl"])


# ---------------------------------------------------------------------------
# Tests: fee deduction — options vs equity
# ---------------------------------------------------------------------------

class TestFeeDeduction:
    """Verify fee calculation differs correctly for options vs equity."""

    def test_option_fees(self):
        """
        Options: 2 contracts, $5 premium, multiplier=100.
        commission = 0.65 * 2 * 2 = $2.60
        slippage = 5.0 * 100 * 2 * 0.01 * 2 = $20.00
        total_fees = $22.60
        """
        result = compute_pnl(
            entry_price=5.0,
            exit_price=6.0,
            num_units=2,
            multiplier=100,
            direction="long",
            instrument_type="option",
            commission_per_contract=0.65,
            slippage_pct=0.01,
        )
        assert result["total_commission"] == pytest.approx(2.60)
        assert result["total_slippage"] == pytest.approx(20.0)
        assert result["total_fees"] == pytest.approx(22.60)

    def test_equity_fees_no_commission(self):
        """
        Equity: 100 shares at $500. No commission, only slippage.
        commission = 0.0
        slippage = 500 * 100 * 0.0001 * 2 = $10.00
        total_fees = $10.00
        """
        result = compute_pnl(
            entry_price=500.0,
            exit_price=505.0,
            num_units=100,
            multiplier=1,
            direction="long",
            instrument_type="equity",
        )
        assert result["total_commission"] == pytest.approx(0.0)
        assert result["total_slippage"] == pytest.approx(10.0)
        assert result["total_fees"] == pytest.approx(10.0)

    def test_option_fees_single_contract(self):
        """
        Single option contract, $3 premium.
        commission = 0.65 * 1 * 2 = $1.30
        slippage = 3.0 * 100 * 1 * 0.01 * 2 = $6.00
        total_fees = $7.30
        """
        result = compute_pnl(
            entry_price=3.0,
            exit_price=3.50,
            num_units=1,
            multiplier=100,
            direction="long",
            instrument_type="option",
        )
        assert result["total_fees"] == pytest.approx(7.30)

    def test_equity_single_share_minimal_fees(self):
        """
        1 share of $100 stock.
        slippage = 100 * 1 * 0.0001 * 2 = $0.02
        """
        result = compute_pnl(
            entry_price=100.0,
            exit_price=105.0,
            num_units=1,
            multiplier=1,
            direction="long",
            instrument_type="equity",
        )
        assert result["total_fees"] == pytest.approx(0.02)

    def test_option_direction_mult_always_one(self):
        """
        For options, direction_mult is always 1.0 regardless of signal direction.
        A 'short' direction option trade still uses positive pnl_per_unit calc
        (i.e., the engine buys puts for a bearish signal, profit = exit - entry).
        """
        result_long = compute_pnl(
            entry_price=5.0, exit_price=6.0, num_units=1,
            multiplier=100, direction="long", instrument_type="option",
        )
        result_short = compute_pnl(
            entry_price=5.0, exit_price=6.0, num_units=1,
            multiplier=100, direction="short", instrument_type="option",
        )
        # Both should have the same P&L since direction_mult=1.0 for options
        assert result_long["pnl_per_unit"] == result_short["pnl_per_unit"]
        assert result_long["net_pnl"] == result_short["net_pnl"]


# ---------------------------------------------------------------------------
# Tests: return percentage
# ---------------------------------------------------------------------------

class TestReturnPercentage:
    """return_pct = net_pnl / cost_basis."""

    def test_return_pct_equity(self):
        """
        Buy 10 shares @ $100 = cost_basis $1000.
        Sell @ $105 -> gross P&L $50, fees $0.20, net $49.80.
        return_pct = 49.80 / 1000 = 0.0498
        """
        result = compute_pnl(
            entry_price=100.0, exit_price=105.0, num_units=10,
            multiplier=1, direction="long", instrument_type="equity",
        )
        expected = 49.80 / 1000.0
        assert result["return_pct"] == pytest.approx(expected, rel=1e-4)

    def test_return_pct_option(self):
        """
        Buy 1 contract @ $5 = cost_basis $500.
        Sell @ $6 -> gross $100, fees = 1.30 + 10.0 = $11.30, net $88.70.
        return_pct = 88.70 / 500 = 0.1774
        """
        result = compute_pnl(
            entry_price=5.0, exit_price=6.0, num_units=1,
            multiplier=100, direction="long", instrument_type="option",
        )
        # Commission: 0.65 * 1 * 2 = 1.30
        # Slippage: 5.0 * 100 * 1 * 0.01 * 2 = 10.0
        # Fees: 11.30
        # Gross: (6 - 5) * 100 * 1 = 100
        # Net: 100 - 11.30 = 88.70
        expected_return = 88.70 / 500.0
        assert result["return_pct"] == pytest.approx(expected_return, rel=1e-4)

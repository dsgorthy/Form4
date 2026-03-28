"""
Tests for BacktestResult metrics computation.

Covers: empty trades, single win/loss, mixed trades, win_rate,
profit_factor, sharpe_ratio, max_consecutive_losses, max_drawdown.
"""

from __future__ import annotations

import math

import pytest

from framework.backtest.result import BacktestResult, TradeRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trade(
    total_pnl: float,
    entry_price: float = 5.0,
    exit_price: float = 5.50,
    num_units: int = 1,
    multiplier: int = 100,
    total_fees: float = 0.0,
    direction: str = "long",
    instrument_type: str = "option",
    date: str = "2025-06-16",
) -> TradeRecord:
    """Create a minimal TradeRecord with the given P&L."""
    cost_basis = entry_price * multiplier * num_units
    return_pct = total_pnl / cost_basis if cost_basis > 0 else 0.0
    return TradeRecord(
        date=date,
        direction=direction,
        entry_time="10:00",
        exit_time="15:30",
        exit_reason="target" if total_pnl >= 0 else "stop",
        instrument_type=instrument_type,
        symbol="SPY",
        entry_price=entry_price,
        exit_price=exit_price,
        num_units=num_units,
        multiplier=multiplier,
        cost_basis=round(cost_basis, 2),
        pnl_per_unit=round(total_pnl / num_units, 2) if num_units else 0.0,
        total_pnl=round(total_pnl, 2),
        total_fees=round(total_fees, 2),
        return_pct=round(return_pct, 4),
    )


def _make_result(
    trades: list[TradeRecord],
    starting_capital: float = 30_000.0,
    equity_curve: list[float] | None = None,
    daily_returns: list[float] | None = None,
) -> BacktestResult:
    """Build a BacktestResult from a list of trades with auto-generated equity curve."""
    if equity_curve is None:
        curve = [starting_capital]
        running = starting_capital
        for t in trades:
            running += t.total_pnl
            curve.append(running)
        equity_curve = curve

    if daily_returns is None:
        daily_returns = []
        cap = starting_capital
        for t in trades:
            daily_returns.append(t.total_pnl / cap if cap > 0 else 0.0)
            cap += t.total_pnl

    return BacktestResult(
        trades=trades,
        equity_curve=equity_curve,
        daily_returns=daily_returns,
        starting_capital=starting_capital,
        strategy_name="TestStrategy",
    )


# ---------------------------------------------------------------------------
# Tests: empty trades
# ---------------------------------------------------------------------------

class TestEmptyTrades:
    """BacktestResult with no trades should return sensible zero defaults."""

    def test_total_trades_is_zero(self):
        result = _make_result([])
        assert result.total_trades == 0

    def test_win_rate_is_zero(self):
        result = _make_result([])
        assert result.win_rate == 0.0

    def test_sharpe_is_zero(self):
        result = _make_result([])
        assert result.sharpe_ratio == 0.0

    def test_total_pnl_is_zero(self):
        result = _make_result([])
        assert result.total_pnl == 0.0

    def test_profit_factor_is_zero(self):
        result = _make_result([])
        assert result.profit_factor == 0.0

    def test_max_drawdown_is_zero(self):
        result = _make_result([])
        assert result.max_drawdown == 0.0

    def test_max_consecutive_losses_is_zero(self):
        result = _make_result([])
        assert result.max_consecutive_losses == 0

    def test_expectancy_is_zero(self):
        result = _make_result([])
        assert result.expectancy == 0.0


# ---------------------------------------------------------------------------
# Tests: single winning trade
# ---------------------------------------------------------------------------

class TestSingleWinningTrade:
    """One trade with positive P&L."""

    @pytest.fixture
    def result(self):
        trade = _make_trade(total_pnl=150.0, entry_price=5.0, exit_price=6.50)
        return _make_result([trade])

    def test_total_trades(self, result):
        assert result.total_trades == 1

    def test_winning_trades(self, result):
        assert result.winning_trades == 1

    def test_losing_trades(self, result):
        assert result.losing_trades == 0

    def test_win_rate_is_one(self, result):
        assert result.win_rate == 1.0

    def test_total_pnl_positive(self, result):
        assert result.total_pnl == 150.0

    def test_gross_wins(self, result):
        assert result.gross_wins == 150.0

    def test_gross_losses_zero(self, result):
        assert result.gross_losses == 0.0

    def test_avg_win(self, result):
        assert result.avg_win == 150.0

    def test_avg_loss_zero(self, result):
        assert result.avg_loss == 0.0

    def test_max_consecutive_losses_zero(self, result):
        assert result.max_consecutive_losses == 0

    def test_max_consecutive_wins(self, result):
        assert result.max_consecutive_wins == 1


# ---------------------------------------------------------------------------
# Tests: single losing trade
# ---------------------------------------------------------------------------

class TestSingleLosingTrade:
    """One trade with negative P&L."""

    @pytest.fixture
    def result(self):
        trade = _make_trade(total_pnl=-200.0, entry_price=5.0, exit_price=3.0)
        return _make_result([trade])

    def test_total_trades(self, result):
        assert result.total_trades == 1

    def test_winning_trades(self, result):
        assert result.winning_trades == 0

    def test_losing_trades(self, result):
        assert result.losing_trades == 1

    def test_win_rate_is_zero(self, result):
        assert result.win_rate == 0.0

    def test_total_pnl_negative(self, result):
        assert result.total_pnl == -200.0

    def test_gross_wins_zero(self, result):
        assert result.gross_wins == 0.0

    def test_gross_losses(self, result):
        assert result.gross_losses == 200.0

    def test_avg_loss(self, result):
        assert result.avg_loss == -200.0

    def test_max_consecutive_losses(self, result):
        assert result.max_consecutive_losses == 1

    def test_max_consecutive_wins_zero(self, result):
        assert result.max_consecutive_wins == 0


# ---------------------------------------------------------------------------
# Tests: mixed trades
# ---------------------------------------------------------------------------

class TestMixedTrades:
    """Multiple trades with a mix of wins and losses."""

    @pytest.fixture
    def trades(self):
        return [
            _make_trade(total_pnl=200.0, date="2025-06-16"),   # win
            _make_trade(total_pnl=-100.0, date="2025-06-17"),  # loss
            _make_trade(total_pnl=300.0, date="2025-06-18"),   # win
            _make_trade(total_pnl=-50.0, date="2025-06-19"),   # loss
            _make_trade(total_pnl=150.0, date="2025-06-20"),   # win
        ]

    @pytest.fixture
    def result(self, trades):
        return _make_result(trades)

    def test_total_trades(self, result):
        assert result.total_trades == 5

    def test_winning_trades(self, result):
        assert result.winning_trades == 3

    def test_losing_trades(self, result):
        assert result.losing_trades == 2

    def test_win_rate(self, result):
        assert result.win_rate == pytest.approx(3 / 5)

    def test_total_pnl(self, result):
        # 200 - 100 + 300 - 50 + 150 = 500
        assert result.total_pnl == pytest.approx(500.0)

    def test_gross_wins(self, result):
        # 200 + 300 + 150 = 650
        assert result.gross_wins == pytest.approx(650.0)

    def test_gross_losses(self, result):
        # abs(-100 + -50) = 150
        assert result.gross_losses == pytest.approx(150.0)

    def test_profit_factor(self, result):
        # 650 / 150 = 4.333...
        assert result.profit_factor == pytest.approx(650.0 / 150.0)

    def test_sharpe_sign_positive(self, result):
        # Net positive daily returns -> positive Sharpe
        assert result.sharpe_ratio > 0

    def test_expectancy(self, result):
        # 500 / 5 = 100
        assert result.expectancy == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Tests: max consecutive losses
# ---------------------------------------------------------------------------

class TestMaxConsecutiveLosses:
    """Verify correct tracking of the longest losing streak."""

    def test_no_losses(self):
        trades = [_make_trade(total_pnl=100.0) for _ in range(3)]
        result = _make_result(trades)
        assert result.max_consecutive_losses == 0

    def test_all_losses(self):
        trades = [_make_trade(total_pnl=-50.0) for _ in range(4)]
        result = _make_result(trades)
        assert result.max_consecutive_losses == 4

    def test_streak_in_middle(self):
        # W, L, L, L, W, L
        trades = [
            _make_trade(total_pnl=100.0),
            _make_trade(total_pnl=-50.0),
            _make_trade(total_pnl=-30.0),
            _make_trade(total_pnl=-20.0),
            _make_trade(total_pnl=80.0),
            _make_trade(total_pnl=-10.0),
        ]
        result = _make_result(trades)
        assert result.max_consecutive_losses == 3

    def test_streak_at_end(self):
        # W, W, L, L, L, L
        trades = [
            _make_trade(total_pnl=100.0),
            _make_trade(total_pnl=50.0),
            _make_trade(total_pnl=-20.0),
            _make_trade(total_pnl=-30.0),
            _make_trade(total_pnl=-40.0),
            _make_trade(total_pnl=-10.0),
        ]
        result = _make_result(trades)
        assert result.max_consecutive_losses == 4

    def test_scratch_trade_breaks_streak(self):
        """A scratch trade (pnl=0) is neither a win nor a loss, so it resets the losing streak."""
        trades = [
            _make_trade(total_pnl=-50.0),
            _make_trade(total_pnl=-30.0),
            _make_trade(total_pnl=0.0),   # scratch
            _make_trade(total_pnl=-20.0),
        ]
        result = _make_result(trades)
        assert result.max_consecutive_losses == 2


# ---------------------------------------------------------------------------
# Tests: max drawdown
# ---------------------------------------------------------------------------

class TestMaxDrawdown:
    """Verify max drawdown calculation with a known equity curve."""

    def test_monotonically_increasing(self):
        """No drawdown when equity only goes up."""
        equity_curve = [10000.0, 10100.0, 10300.0, 10500.0]
        result = BacktestResult(
            trades=[], equity_curve=equity_curve,
            daily_returns=[], starting_capital=10000.0,
        )
        assert result.max_drawdown == 0.0
        assert result.max_drawdown_pct == 0.0

    def test_known_drawdown(self):
        """
        Construct an equity curve with a known drawdown:
        10000 -> 11000 -> 10000 -> 9000 -> 10500
        Peak = 11000, trough = 9000, max DD = $2000
        """
        equity_curve = [10000.0, 11000.0, 10000.0, 9000.0, 10500.0]
        result = BacktestResult(
            trades=[], equity_curve=equity_curve,
            daily_returns=[], starting_capital=10000.0,
        )
        assert result.max_drawdown == pytest.approx(2000.0)
        assert result.max_drawdown_pct == pytest.approx(2000.0 / 11000.0)

    def test_drawdown_at_end(self):
        """
        Drawdown that continues to the end of the curve.
        10000 -> 12000 -> 11000 -> 10000
        Peak = 12000, trough = 10000, max DD = $2000
        """
        equity_curve = [10000.0, 12000.0, 11000.0, 10000.0]
        result = BacktestResult(
            trades=[], equity_curve=equity_curve,
            daily_returns=[], starting_capital=10000.0,
        )
        assert result.max_drawdown == pytest.approx(2000.0)
        assert result.max_drawdown_pct == pytest.approx(2000.0 / 12000.0)

    def test_multiple_drawdowns_returns_largest(self):
        """
        Two drawdowns: first smaller, second larger.
        10000 -> 10500 -> 10200 -> 11000 -> 9500
        DD1: 10500 -> 10200 = 300
        DD2: 11000 -> 9500 = 1500
        Max DD = 1500
        """
        equity_curve = [10000.0, 10500.0, 10200.0, 11000.0, 9500.0]
        result = BacktestResult(
            trades=[], equity_curve=equity_curve,
            daily_returns=[], starting_capital=10000.0,
        )
        assert result.max_drawdown == pytest.approx(1500.0)

    def test_empty_equity_curve(self):
        result = BacktestResult(
            trades=[], equity_curve=[], daily_returns=[], starting_capital=10000.0,
        )
        assert result.max_drawdown == 0.0
        assert result.max_drawdown_pct == 0.0


# ---------------------------------------------------------------------------
# Tests: profit factor
# ---------------------------------------------------------------------------

class TestProfitFactor:
    """Profit factor = gross_wins / gross_losses."""

    def test_no_losses_returns_inf(self):
        """When all trades are winners, profit factor should be inf."""
        trades = [
            _make_trade(total_pnl=100.0),
            _make_trade(total_pnl=200.0),
        ]
        result = _make_result(trades)
        assert result.profit_factor == float("inf")

    def test_no_wins_returns_zero(self):
        """When all trades are losers, gross_wins=0, so profit_factor=0/losses => 0.0 (via inf branch)."""
        trades = [
            _make_trade(total_pnl=-100.0),
            _make_trade(total_pnl=-50.0),
        ]
        result = _make_result(trades)
        # gross_wins = 0, gross_losses = 150 => 0 / 150 = 0.0
        assert result.profit_factor == pytest.approx(0.0)

    def test_balanced_trades(self):
        """Equal wins and losses -> profit factor = 1.0."""
        trades = [
            _make_trade(total_pnl=100.0),
            _make_trade(total_pnl=-100.0),
        ]
        result = _make_result(trades)
        assert result.profit_factor == pytest.approx(1.0)

    def test_no_trades(self):
        """No trades -> profit factor 0.0."""
        result = _make_result([])
        assert result.profit_factor == 0.0

    def test_asymmetric_profit_factor(self):
        """Wins of $300, losses of $100 -> PF = 3.0."""
        trades = [
            _make_trade(total_pnl=300.0),
            _make_trade(total_pnl=-100.0),
        ]
        result = _make_result(trades)
        assert result.profit_factor == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Tests: sharpe ratio edge cases
# ---------------------------------------------------------------------------

class TestSharpeRatio:
    """Sharpe ratio edge cases."""

    def test_single_return_is_zero(self):
        """With only 1 daily return, std is undefined -> Sharpe = 0."""
        result = BacktestResult(
            trades=[], equity_curve=[10000.0, 10100.0],
            daily_returns=[0.01], starting_capital=10000.0,
        )
        assert result.sharpe_ratio == 0.0

    def test_zero_variance_returns_zero(self):
        """All identical daily returns -> std=0 -> Sharpe=0."""
        result = BacktestResult(
            trades=[], equity_curve=[10000.0, 10100.0, 10200.0, 10300.0],
            daily_returns=[0.01, 0.01, 0.01], starting_capital=10000.0,
        )
        assert result.sharpe_ratio == 0.0

    def test_positive_returns_positive_sharpe(self):
        """Mostly positive returns should yield a positive Sharpe."""
        daily = [0.01, 0.02, -0.005, 0.015, 0.01, 0.02, -0.003, 0.01]
        result = BacktestResult(
            trades=[], equity_curve=[10000.0] * (len(daily) + 1),
            daily_returns=daily, starting_capital=10000.0,
        )
        assert result.sharpe_ratio > 0

    def test_negative_returns_negative_sharpe(self):
        """Mostly negative returns should yield a negative Sharpe."""
        daily = [-0.01, -0.02, 0.005, -0.015, -0.01, -0.02, 0.003, -0.01]
        result = BacktestResult(
            trades=[], equity_curve=[10000.0] * (len(daily) + 1),
            daily_returns=daily, starting_capital=10000.0,
        )
        assert result.sharpe_ratio < 0


# ---------------------------------------------------------------------------
# Tests: summary dict
# ---------------------------------------------------------------------------

class TestSummary:
    """The summary() method should return a well-formed dict."""

    def test_summary_keys(self):
        result = _make_result([])
        s = result.summary()
        expected_keys = {
            "strategy_name", "starting_capital", "ending_capital",
            "total_trades", "winning_trades", "losing_trades", "scratch_trades",
            "win_rate", "total_pnl", "total_fees", "total_return_pct",
            "avg_win", "avg_loss", "profit_factor", "expectancy",
            "max_drawdown", "max_drawdown_pct", "sharpe_ratio",
            "max_consecutive_losses", "max_consecutive_wins",
            "gross_wins", "gross_losses",
        }
        assert set(s.keys()) == expected_keys

    def test_summary_strategy_name(self):
        result = _make_result([])
        assert result.summary()["strategy_name"] == "TestStrategy"

    def test_summary_starting_capital(self):
        result = _make_result([], starting_capital=50_000.0)
        assert result.summary()["starting_capital"] == 50_000.0

"""
Unit tests for the options overlay module (options_leg.py).

Tests strike/expiry selection, sizing, entry/exit logic, and P&L computation
using a mock PaperBackend.
"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add strategy directory to path
STRATEGY_DIR = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_cluster_buy"
sys.path.insert(0, str(STRATEGY_DIR))

from options_leg import (
    _get_strike_interval,
    _nearest_strike,
    _third_friday,
    _build_occ_symbol,
    _lookup_contract_alpaca,
    select_strike_and_expiry,
    compute_options_size,
    submit_options_entry,
    check_options_exit,
    close_options_leg,
)
from framework.execution.base import OrderResult


# ── Strike / Expiry Helpers ──────────────────────────────────────────────

class TestStrikeInterval:
    def test_under_5(self):
        assert _get_strike_interval(3.50) == 0.5

    def test_under_25(self):
        assert _get_strike_interval(12.00) == 1.0

    def test_under_200(self):
        assert _get_strike_interval(75.00) == 2.5

    def test_over_200(self):
        assert _get_strike_interval(350.00) == 5.0

    def test_boundary_5(self):
        assert _get_strike_interval(5.0) == 1.0

    def test_boundary_25(self):
        assert _get_strike_interval(25.0) == 2.5

    def test_boundary_200(self):
        assert _get_strike_interval(200.0) == 5.0


class TestNearestStrike:
    def test_5pct_otm_on_100(self):
        # 100 * 1.05 = 105, interval = 2.5 → nearest = 105.0
        assert _nearest_strike(100.0, 1.05) == 105.0

    def test_5pct_otm_on_50(self):
        # 50 * 1.05 = 52.5, interval = 2.5 → nearest = 52.5
        assert _nearest_strike(50.0, 1.05) == 52.5

    def test_5pct_otm_on_15(self):
        # 15 * 1.05 = 15.75, interval = 1.0 → nearest = 16.0
        assert _nearest_strike(15.0, 1.05) == 16.0

    def test_rounds_to_nearest(self):
        # 73 * 1.05 = 76.65, interval = 2.5 → nearest = 77.5
        assert _nearest_strike(73.0, 1.05) == 77.5


class TestThirdFriday:
    def test_march_2026(self):
        d = _third_friday(2026, 3)
        assert d == date(2026, 3, 20)
        assert d.weekday() == 4

    def test_june_2026(self):
        d = _third_friday(2026, 6)
        assert d == date(2026, 6, 19)
        assert d.weekday() == 4

    def test_january_2026(self):
        d = _third_friday(2026, 1)
        assert d == date(2026, 1, 16)
        assert d.weekday() == 4


class TestLookupContractAlpaca:
    @patch("options_leg.requests.Session")
    def test_picks_best_match(self, MockSession):
        """When Alpaca returns multiple contracts, picks closest to target."""
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"option_contracts": [
            {"symbol": "TEST260515C00019000", "strike_price": "19", "expiration_date": "2026-05-15", "status": "active"},
            {"symbol": "TEST260618C00020000", "strike_price": "20", "expiration_date": "2026-06-18", "status": "active"},
            {"symbol": "TEST260717C00019000", "strike_price": "19", "expiration_date": "2026-07-17", "status": "active"},
        ]}
        mock_session.get.return_value = mock_resp

        result = _lookup_contract_alpaca("TEST", 19.0, date(2026, 3, 2), 90, session=mock_session)
        assert result is not None
        assert result["strike"] == 19.0
        # May 15 = 74d, Jun 18 strike=20 (off by $1), Jul 17 = 137d
        # Scoring: May=19 strike_diff=0 + dte_diff=|74-90|/3=5.3 → 5.3
        #          Jun=20 strike_diff=1 + dte_diff=|108-90|/3=6.0 → 7.0
        #          Jul=19 strike_diff=0 + dte_diff=|137-90|/3=15.7 → 15.7
        assert result["occ_symbol"] == "TEST260515C00019000"

    @patch("options_leg.requests.Session")
    def test_returns_none_on_empty(self, MockSession):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"option_contracts": []}
        mock_session.get.return_value = mock_resp

        result = _lookup_contract_alpaca("TEST", 19.0, date(2026, 3, 2), 90, session=mock_session)
        assert result is None

    def test_returns_none_on_network_error(self):
        mock_session = MagicMock()
        mock_session.get.side_effect = ConnectionError("timeout")

        result = _lookup_contract_alpaca("TEST", 19.0, date(2026, 3, 2), 90, session=mock_session)
        assert result is None


class TestSelectStrikeAndExpiry:
    @patch("options_leg._lookup_contract_alpaca")
    def test_uses_alpaca_when_available(self, mock_lookup):
        mock_lookup.return_value = {
            "occ_symbol": "AAPL260619C00190000",
            "strike": 190.0,
            "expiry_date": "2026-06-19",
            "dte": 109,
        }
        result = select_strike_and_expiry("AAPL", 180.0, date(2026, 3, 2))
        assert result["occ_symbol"] == "AAPL260619C00190000"
        mock_lookup.assert_called_once()

    @patch("options_leg._lookup_contract_alpaca")
    def test_falls_back_to_third_friday(self, mock_lookup):
        mock_lookup.return_value = None  # Alpaca lookup failed
        result = select_strike_and_expiry("AAPL", 180.0, date(2026, 3, 2))
        assert "occ_symbol" in result
        assert result["strike"] >= 180.0
        # Fallback should use 3rd Friday of target month
        expiry = date.fromisoformat(result["expiry_date"])
        assert expiry.weekday() == 4  # Friday

    @patch("options_leg._lookup_contract_alpaca")
    def test_strike_is_otm(self, mock_lookup):
        mock_lookup.return_value = None
        result = select_strike_and_expiry("AAPL", 100.0, date(2026, 3, 2), strike_mult=1.05)
        assert result["strike"] >= 100.0


class TestBuildOccSymbol:
    def test_standard_symbol(self):
        result = _build_occ_symbol("AAPL", date(2026, 6, 19), 200.0)
        assert result == "AAPL260619C00200000"

    def test_fractional_strike(self):
        result = _build_occ_symbol("XYZ", date(2026, 9, 18), 52.5)
        assert result == "XYZ260918C00052500"

    def test_low_strike(self):
        result = _build_occ_symbol("SMAL", date(2026, 6, 19), 2.5)
        assert result == "SMAL260619C00002500"


class TestSelectStrikeAndExpiry:
    def test_returns_all_fields(self):
        result = select_strike_and_expiry("AAPL", 180.0, date(2026, 3, 2))
        assert "occ_symbol" in result
        assert "strike" in result
        assert "expiry_date" in result
        assert "dte" in result
        assert result["dte"] >= 87

    def test_strike_is_otm(self):
        result = select_strike_and_expiry("AAPL", 100.0, date(2026, 3, 2), strike_mult=1.05)
        assert result["strike"] >= 100.0


# ── Sizing ───────────────────────────────────────────────────────────────

class TestComputeOptionsSize:
    def test_normal_sizing(self):
        # $30K portfolio, 1% = $300 budget, option = $1.50/share = $150/contract → 2 contracts
        assert compute_options_size(30000, 1.50, 0.01, 2) == 2

    def test_capped_at_max(self):
        # $30K, 1% = $300, option = $0.50 = $50/contract → 6 contracts, capped at 2
        assert compute_options_size(30000, 0.50, 0.01, 2) == 2

    def test_too_expensive(self):
        # $30K, 1% = $300, option = $5.00 = $500/contract → 0
        assert compute_options_size(30000, 5.00, 0.01, 2) == 0

    def test_zero_price(self):
        assert compute_options_size(30000, 0.0, 0.01, 2) == 0

    def test_negative_price(self):
        assert compute_options_size(30000, -1.0, 0.01, 2) == 0


# ── Entry / Exit / Close ─────────────────────────────────────────────────

def _make_mock_backend(fill_price=1.50, position_price=2.00):
    """Create a mock backend with configurable fill and position prices."""
    backend = MagicMock()

    filled_result = OrderResult(
        order_id="OPT001", status="filled",
        symbol="TEST260529C00052500", qty=1, side="buy",
        filled_price=fill_price, filled_qty=1,
    )
    backend.submit_order.return_value = filled_result
    backend.wait_for_fill.return_value = filled_result

    backend.get_position.return_value = {
        "symbol": "TEST260529C00052500",
        "qty": 1.0,
        "avg_entry_price": fill_price,
        "current_price": position_price,
        "unrealized_pl": (position_price - fill_price) * 100,
    }

    close_result = OrderResult(
        order_id="OPT_CLOSE", status="filled",
        symbol="TEST260529C00052500", qty=1, side="sell",
        filled_price=position_price, filled_qty=1,
    )
    backend.close_position.return_value = close_result

    return backend


class TestSubmitOptionsEntry:
    @patch("options_leg.date")
    def test_successful_entry(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        backend = _make_mock_backend(fill_price=1.50)
        signal = {"ticker": "TEST", "company": "Test Corp"}

        result = submit_options_entry(
            signal=signal,
            backend=backend,
            current_price=50.0,
            portfolio_value=30000,
            strike_mult=1.05,
            target_dte=90,
            hold_days=14,
            size_pct=0.01,
            max_contracts=2,
        )

        assert result is not None
        assert result["status"] == "open"
        assert result["qty"] >= 1
        assert result["entry_price"] == 1.50
        assert "occ_symbol" in result
        assert result["exit_date_target"] is not None

    @patch("options_leg.date")
    def test_order_rejection(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        backend = MagicMock()
        rejected = OrderResult(
            order_id="err", status="rejected",
            symbol="X", qty=1, side="buy",
            error="Insufficient buying power",
        )
        backend.submit_order.return_value = rejected

        result = submit_options_entry(
            signal={"ticker": "TEST"},
            backend=backend,
            current_price=50.0,
            portfolio_value=30000,
        )
        assert result is None

    @patch("options_leg.date")
    def test_fill_timeout(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        backend = MagicMock()
        pending = OrderResult(
            order_id="ORD1", status="pending",
            symbol="X", qty=1, side="buy",
        )
        backend.submit_order.return_value = pending
        # wait_for_fill returns unfilled
        backend.wait_for_fill.return_value = OrderResult(
            order_id="ORD1", status="pending",
            symbol="X", qty=1, side="buy",
        )

        result = submit_options_entry(
            signal={"ticker": "TEST"},
            backend=backend,
            current_price=50.0,
            portfolio_value=30000,
        )
        assert result is None


class TestCheckOptionsExit:
    def test_profit_target_hit(self):
        backend = _make_mock_backend(fill_price=1.50, position_price=2.50)
        leg = {
            "occ_symbol": "TEST260529C00052500",
            "entry_price": 1.50,
            "exit_date_target": "2026-12-31",
            "status": "open",
        }
        should_exit, reason = check_options_exit(leg, backend, profit_target=0.50)
        assert should_exit is True
        assert reason == "profit_target"

    def test_below_target(self):
        backend = _make_mock_backend(fill_price=1.50, position_price=1.80)
        leg = {
            "occ_symbol": "TEST260529C00052500",
            "entry_price": 1.50,
            "exit_date_target": "2026-12-31",
            "status": "open",
        }
        should_exit, reason = check_options_exit(leg, backend, profit_target=0.50)
        assert should_exit is False

    @patch("options_leg.date")
    def test_time_exit(self, mock_date):
        mock_date.today.return_value = date(2026, 3, 20)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        backend = _make_mock_backend(fill_price=1.50, position_price=1.50)
        leg = {
            "occ_symbol": "TEST260529C00052500",
            "entry_price": 1.50,
            "exit_date_target": "2026-03-20",
            "status": "open",
        }
        should_exit, reason = check_options_exit(leg, backend, profit_target=0.50)
        assert should_exit is True
        assert reason == "time_exit"

    def test_position_gone(self):
        backend = MagicMock()
        backend.get_position.return_value = None
        leg = {
            "occ_symbol": "TEST260529C00052500",
            "entry_price": 1.50,
            "exit_date_target": "2026-12-31",
            "status": "open",
        }
        should_exit, reason = check_options_exit(leg, backend, profit_target=0.50)
        assert should_exit is True
        assert reason == "position_gone"

    def test_already_closed(self):
        backend = _make_mock_backend()
        leg = {"status": "closed", "occ_symbol": "X", "entry_price": 1.0, "exit_date_target": "2026-12-31"}
        should_exit, _ = check_options_exit(leg, backend)
        assert should_exit is False


class TestCloseOptionsLeg:
    def test_successful_close(self):
        backend = _make_mock_backend(fill_price=1.50, position_price=2.25)
        leg = {
            "occ_symbol": "TEST260529C00052500",
            "entry_price": 1.50,
            "qty": 2,
            "status": "open",
            "exit_reason": "profit_target",
        }
        result = close_options_leg(leg, backend, portfolio_value=30000)
        assert result["status"] == "closed"
        assert result["exit_price"] == 2.25
        assert result["pnl"] == (2.25 - 1.50) * 100 * 2  # $150
        assert result["pnl_pct"] == round((2.25 - 1.50) / 1.50, 4)  # 50%

    def test_expired_worthless(self):
        backend = MagicMock()
        backend.get_position.return_value = None  # position gone
        leg = {
            "occ_symbol": "TEST260529C00052500",
            "entry_price": 1.50,
            "qty": 1,
            "status": "open",
        }
        result = close_options_leg(leg, backend)
        assert result["status"] == "closed"
        assert result["exit_price"] == 0.0
        assert result["pnl"] == -150.0  # Lost full premium
        assert result["exit_reason"] == "expired_worthless"

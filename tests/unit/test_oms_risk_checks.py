"""Unit tests for framework.oms.risk_checks."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.oms.order_manager import OrderIntent
from framework.oms.risk_checks import (
    DailyLossLimitCheck,
    MarginCheck,
    MaxConcurrentCheck,
    MaxPositionSizeCheck,
    OpenPosition,
    RiskCheck,
    RiskCheckPipeline,
    RiskContext,
    RiskResult,
    SectorConcentrationCheck,
    SymbolBlocklistCheck,
)


def make_intent(
    *,
    ticker: str = "AAPL",
    side: str = "buy",
    qty: float = 100.0,
    estimated_value_usd=None,
    limit_price=None,
    order_type: str = "market",
) -> OrderIntent:
    return OrderIntent(
        intent_id="i1",
        decision_id="d1",
        strategy="qm",
        strategy_version="v",
        ticker=ticker,
        side=side,
        qty=qty,
        order_type=order_type,
        limit_price=limit_price,
        estimated_value_usd=estimated_value_usd,
    )


def make_ctx(
    *,
    portfolio_value: float = 100_000.0,
    cash_balance: float = 50_000.0,
    open_positions: tuple[OpenPosition, ...] = (),
    daily_pnl: float = 0.0,
    is_live: bool = False,
    blocklist: frozenset = frozenset(),
) -> RiskContext:
    return RiskContext(
        portfolio_value=portfolio_value,
        cash_balance=cash_balance,
        open_positions=open_positions,
        daily_pnl=daily_pnl,
        is_live=is_live,
        blocklist=blocklist,
    )


# ── MaxPositionSizeCheck ────────────────────────────────────────────────────


class TestMaxPositionSize:

    def test_under_limit_passes(self):
        check = MaxPositionSizeCheck(pct=10)
        intent = make_intent(estimated_value_usd=5_000)
        ctx = make_ctx(portfolio_value=100_000)
        result = check.evaluate(intent, ctx)
        assert result.passed
        assert result.check_name == check.name

    def test_over_limit_rejects(self):
        check = MaxPositionSizeCheck(pct=10)
        intent = make_intent(estimated_value_usd=15_000)
        ctx = make_ctx(portfolio_value=100_000)
        result = check.evaluate(intent, ctx)
        assert not result.passed
        assert "15.00%" in result.reason
        assert "10%" in result.reason

    def test_zero_portfolio_value_rejects(self):
        check = MaxPositionSizeCheck(pct=10)
        intent = make_intent(estimated_value_usd=5_000)
        ctx = make_ctx(portfolio_value=0)
        result = check.evaluate(intent, ctx)
        assert not result.passed

    def test_no_estimated_value_passes_through(self):
        # If we can't size it, defer (don't reject blindly)
        check = MaxPositionSizeCheck(pct=10)
        intent = make_intent(estimated_value_usd=None, limit_price=None)
        ctx = make_ctx()
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_pct_validation(self):
        with pytest.raises(ValueError):
            MaxPositionSizeCheck(pct=0)
        with pytest.raises(ValueError):
            MaxPositionSizeCheck(pct=-5)
        with pytest.raises(ValueError):
            MaxPositionSizeCheck(pct=101)


# ── MaxConcurrentCheck ──────────────────────────────────────────────────────


class TestMaxConcurrent:

    def test_under_limit_passes(self):
        check = MaxConcurrentCheck(n=10)
        intent = make_intent(ticker="AAPL")
        ctx = make_ctx(open_positions=(
            OpenPosition("MSFT", 50, 100),
            OpenPosition("GOOG", 10, 200),
        ))
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_at_limit_rejects(self):
        check = MaxConcurrentCheck(n=2)
        intent = make_intent(ticker="AAPL")
        ctx = make_ctx(open_positions=(
            OpenPosition("MSFT", 50, 100),
            OpenPosition("GOOG", 10, 200),
        ))
        result = check.evaluate(intent, ctx)
        assert not result.passed
        assert "open_positions=2" in result.reason

    def test_adding_to_existing_position_passes(self):
        # Same ticker as an existing position — not a new "slot"
        check = MaxConcurrentCheck(n=2)
        intent = make_intent(ticker="MSFT")
        ctx = make_ctx(open_positions=(
            OpenPosition("MSFT", 50, 100),
            OpenPosition("GOOG", 10, 200),
        ))
        result = check.evaluate(intent, ctx)
        assert result.passed


# ── DailyLossLimitCheck ─────────────────────────────────────────────────────


class TestDailyLossLimit:

    def test_no_loss_passes(self):
        check = DailyLossLimitCheck(pct=5)
        intent = make_intent(side="buy")
        ctx = make_ctx(portfolio_value=100_000, daily_pnl=500)
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_under_limit_passes(self):
        check = DailyLossLimitCheck(pct=5)
        intent = make_intent(side="buy")
        ctx = make_ctx(portfolio_value=100_000, daily_pnl=-2_000)  # -2%
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_at_limit_rejects(self):
        check = DailyLossLimitCheck(pct=5)
        intent = make_intent(side="buy")
        ctx = make_ctx(portfolio_value=100_000, daily_pnl=-5_500)  # -5.5%
        result = check.evaluate(intent, ctx)
        assert not result.passed
        assert "5.5" in result.reason

    def test_sells_pass_through(self):
        # Sells (exits) shouldn't be blocked by daily loss limit
        check = DailyLossLimitCheck(pct=5)
        intent = make_intent(side="sell")
        ctx = make_ctx(portfolio_value=100_000, daily_pnl=-50_000)  # -50%
        result = check.evaluate(intent, ctx)
        assert result.passed


# ── SectorConcentrationCheck ────────────────────────────────────────────────


class TestSectorConcentration:

    def test_under_limit_passes(self):
        sector_lookup = {"AAPL": "Tech", "MSFT": "Tech", "JPM": "Financials"}.get
        check = SectorConcentrationCheck(max_pct=30, sector_lookup=sector_lookup)
        intent = make_intent(ticker="AAPL", estimated_value_usd=10_000)
        ctx = make_ctx(
            portfolio_value=100_000,
            open_positions=(
                OpenPosition("MSFT", 100, 50, sector="Tech", market_value=5_000),
                OpenPosition("JPM", 100, 100, sector="Financials", market_value=10_000),
            ),
        )
        result = check.evaluate(intent, ctx)
        # Existing Tech: 5K. New: 10K. Total: 15K = 15% of 100K. Under 30%.
        assert result.passed

    def test_over_limit_rejects(self):
        sector_lookup = {"AAPL": "Tech", "MSFT": "Tech"}.get
        check = SectorConcentrationCheck(max_pct=30, sector_lookup=sector_lookup)
        intent = make_intent(ticker="AAPL", estimated_value_usd=20_000)
        ctx = make_ctx(
            portfolio_value=100_000,
            open_positions=(
                OpenPosition("MSFT", 100, 200, sector="Tech", market_value=20_000),
            ),
        )
        result = check.evaluate(intent, ctx)
        # Existing Tech: 20K. New: 20K. Total: 40K = 40% of 100K. Over 30%.
        assert not result.passed
        assert "Tech" in result.reason


# ── MarginCheck ─────────────────────────────────────────────────────────────


class TestMargin:

    def test_under_buying_power_passes(self):
        check = MarginCheck(margin_multiplier=1.0)
        intent = make_intent(side="buy", estimated_value_usd=10_000)
        ctx = make_ctx(cash_balance=50_000)
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_over_buying_power_rejects(self):
        check = MarginCheck(margin_multiplier=1.0)
        intent = make_intent(side="buy", estimated_value_usd=60_000)
        ctx = make_ctx(cash_balance=50_000)
        result = check.evaluate(intent, ctx)
        assert not result.passed
        assert "buying_power" in result.reason

    def test_margin_multiplier_2x(self):
        check = MarginCheck(margin_multiplier=2.0)
        intent = make_intent(side="buy", estimated_value_usd=80_000)
        ctx = make_ctx(cash_balance=50_000)
        # Buying power = 50K * 2 = 100K. 80K under that.
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_sells_pass_through(self):
        check = MarginCheck()
        intent = make_intent(side="sell", estimated_value_usd=999_999)
        ctx = make_ctx(cash_balance=0)
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_margin_validation(self):
        with pytest.raises(ValueError):
            MarginCheck(margin_multiplier=0.5)


# ── SymbolBlocklistCheck ────────────────────────────────────────────────────


class TestSymbolBlocklist:

    def test_not_in_blocklist_passes(self):
        check = SymbolBlocklistCheck()
        intent = make_intent(ticker="AAPL")
        ctx = make_ctx(blocklist=frozenset({"DEAD", "HALT"}))
        result = check.evaluate(intent, ctx)
        assert result.passed

    def test_in_blocklist_rejects(self):
        check = SymbolBlocklistCheck()
        intent = make_intent(ticker="DEAD")
        ctx = make_ctx(blocklist=frozenset({"DEAD"}))
        result = check.evaluate(intent, ctx)
        assert not result.passed
        assert "DEAD" in result.reason

    def test_empty_blocklist_passes(self):
        check = SymbolBlocklistCheck()
        intent = make_intent(ticker="AAPL")
        ctx = make_ctx()  # default empty blocklist
        result = check.evaluate(intent, ctx)
        assert result.passed


# ── RiskCheckPipeline ───────────────────────────────────────────────────────


class TestRiskCheckPipeline:

    def test_all_pass(self):
        pipeline = RiskCheckPipeline([
            MaxPositionSizeCheck(pct=10),
            MaxConcurrentCheck(n=10),
            MarginCheck(),
        ])
        intent = make_intent(estimated_value_usd=5_000)
        ctx = make_ctx(portfolio_value=100_000, cash_balance=50_000)
        result = pipeline.run(intent, ctx)
        assert result.passed
        assert result.metadata["checks_run"] == [
            "max_position_size_10pct", "max_concurrent_10", "margin_1.0x"
        ]

    def test_short_circuits_on_first_failure(self):
        # Use a "tagged" check that records whether it ran
        ran: list[str] = []

        class TaggedCheck(RiskCheck):
            def __init__(self, n, fail=False):
                self.n = n
                self.fail = fail
                self.name = f"tagged_{n}"

            def evaluate(self, intent, ctx):
                ran.append(self.name)
                if self.fail:
                    return RiskResult.reject(self.name, "intentional fail")
                return RiskResult.pass_(self.name)

        pipeline = RiskCheckPipeline([
            TaggedCheck(1),
            TaggedCheck(2, fail=True),
            TaggedCheck(3),  # should NOT run
        ])
        result = pipeline.run(make_intent(), make_ctx())
        assert not result.passed
        assert result.check_name == "tagged_2"
        assert ran == ["tagged_1", "tagged_2"]
        # tagged_3 didn't run

    def test_run_all_does_not_short_circuit(self):
        pipeline = RiskCheckPipeline([
            MaxPositionSizeCheck(pct=10),
            MaxConcurrentCheck(n=0),  # always rejects
            MarginCheck(),
        ])
        intent = make_intent(estimated_value_usd=5_000)
        ctx = make_ctx(open_positions=(OpenPosition("X", 1, 1),))
        results = pipeline.run_all(intent, ctx)
        assert len(results) == 3

    def test_empty_pipeline_invalid(self):
        with pytest.raises(ValueError):
            RiskCheckPipeline([])

    def test_duplicate_check_names_invalid(self):
        with pytest.raises(ValueError, match="Duplicate"):
            RiskCheckPipeline([
                MarginCheck(),
                MarginCheck(),
            ])


# ── RiskResult helpers ──────────────────────────────────────────────────────


class TestRiskResult:

    def test_pass_helper(self):
        r = RiskResult.pass_("my_check")
        assert r.passed
        assert r.check_name == "my_check"
        assert r.reason is None

    def test_reject_helper_with_metadata(self):
        r = RiskResult.reject("my_check", "too big", value=100)
        assert not r.passed
        assert r.check_name == "my_check"
        assert r.reason == "too big"
        assert r.metadata == {"value": 100}

"""The guardrails must admit the position the yaml itself asks for, and the
relative check must catch a position that is half the book.

2026-09-21: every yaml carried max_dollar_amount 25_000 and max_qty 10_000,
written for a $10k live account that had not existed since Alpaca went
data-only. On the $100k alert-only stake A-List's designed 33% position is
$33k, so the cap rejected every candidate that passed conviction; the qty
cap did the same to any stock under ~$3.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from framework.risk.guardrails import validate_entry_order, DEFAULT_GUARDRAILS  # noqa: E402

CONFIGS = REPO / "strategies" / "cw_strategies" / "configs"
LIVE = ("quality_notrend", "quality_momentum", "reversal_dip")


class _NoOrdersConn:
    def execute(self, sql, params=()):
        class _C:
            def fetchone(self):
                return {"n": 0}
        return _C()


def _cfg(name):
    return yaml.safe_load((CONFIGS / f"{name}.yaml").read_text())


def test_each_live_yaml_admits_its_own_designed_position():
    for name in LIVE:
        c = _cfg(name)
        g = c["guardrails"]
        designed = c["position_size_pct"] * c["starting_capital"]
        assert g["max_dollar_amount"] >= 3 * designed, (
            f"{name}: max_dollar_amount {g['max_dollar_amount']} leaves no room "
            f"for the designed ${designed:,.0f} position to compound"
        )
        # A $0.50 stock (the min_price floor) at the designed size.
        assert g["max_qty"] >= designed / g["min_price"], (
            f"{name}: max_qty {g['max_qty']} rejects the designed position in a "
            f"stock at the min_price floor"
        )


def test_designed_positions_pass_at_stake_and_after_compounding():
    for name in LIVE:
        c = _cfg(name)
        for equity in (100_000, 250_000):
            dollars = c["position_size_pct"] * equity
            ok, reason = validate_entry_order(
                _NoOrdersConn(), strategy=name, side="buy",
                qty=int(dollars / 12.5), dollar_amount=dollars,
                current_price=12.5, equity=equity,
                guardrails_cfg=c["guardrails"],
            )
            assert ok, f"{name} at equity {equity}: {reason}"


def test_half_the_book_in_one_name_is_rejected():
    ok, reason = validate_entry_order(
        _NoOrdersConn(), strategy="s", side="buy",
        qty=1000, dollar_amount=60_000, current_price=60.0, equity=100_000,
        guardrails_cfg={"max_dollar_amount": 1_000_000},
    )
    assert not ok and "of equity" in reason


def test_relative_cap_is_a_default():
    assert 0 < DEFAULT_GUARDRAILS["max_position_pct_of_equity"] <= 0.5

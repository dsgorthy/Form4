"""An alert-only strategy must not need a TRADING account to read prices.

WHAT WENT WRONG

cw_runner.get_alpaca() demanded `alpaca_env_prefix` from every strategy. All
three published books are `execution_mode: alert_only` and place no orders —
they need Alpaca only to read prices and bars. Two of them survived the check
by accident, carrying prefixes left over from when they had paper accounts.

quality_notrend (A-List Buys) was created after Alpaca went data-only, so it
correctly has no account and its yaml says `alpaca_env_prefix: null`. From
2026-08-18 every one of its daily cycles died in get_alpaca — 367 times — and
it has never written a single row to trade_decision_audit, while the other
strategies have 124k-218k.

Nothing caught it. The daemon stayed up and its heartbeat stayed fresh, because
the failure is INSIDE the cycle, not the process.

THE RULE

The no-commingling constraint is about ORDERS. A dedicated trading prefix is
still mandatory for paper and live. Read-only data credentials cannot place a
trade, so alert-only strategies share them.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "strategies/cw_strategies/cw_runner.py"
CONFIGS = REPO / "strategies/cw_strategies/configs"
ACTIVE = ["quality_notrend", "quality_momentum", "reversal_dip"]


@pytest.fixture
def get_alpaca(monkeypatch):
    import importlib
    mod = importlib.import_module("strategies.cw_strategies.cw_runner")
    monkeypatch.setattr(mod, "PaperBackend",
                        lambda k, s: {"key": k, "secret": s}, raising=True)
    return mod.get_alpaca


def test_alert_only_with_no_prefix_uses_the_shared_data_credentials(
        get_alpaca, monkeypatch):
    monkeypatch.setenv("ALPACA_DATA_API_KEY", "DATAKEY")
    monkeypatch.setenv("ALPACA_DATA_API_SECRET", "DATASECRET")
    backend = get_alpaca({
        "strategy_name": "quality_notrend",
        "alpaca_env_prefix": None,
        "execution_mode": "alert_only",
    })
    assert backend == {"key": "DATAKEY", "secret": "DATASECRET"}, (
        "an alert-only strategy still demands a dedicated trading account; "
        "this is the failure that killed A-List Buys for five trading days"
    )


def test_paper_mode_still_requires_its_own_account(get_alpaca):
    """The commingling rule must survive the fix."""
    with pytest.raises(RuntimeError, match="alpaca_env_prefix"):
        get_alpaca({"strategy_name": "x", "alpaca_env_prefix": None,
                    "execution_mode": "paper"})


def test_live_mode_still_requires_its_own_account(get_alpaca):
    with pytest.raises(RuntimeError, match="alpaca_env_prefix"):
        get_alpaca({"strategy_name": "x", "alpaca_env_prefix": None,
                    "live_money": True})


def test_alert_only_with_a_prefix_still_uses_that_account(get_alpaca, monkeypatch):
    """Two books still carry a prefix. Honour it rather than silently
    switching them onto shared credentials."""
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM", "TRADEKEY")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM", "TRADESECRET")
    backend = get_alpaca({
        "strategy_name": "quality_momentum",
        "alpaca_env_prefix": "QUALITY_MOMENTUM",
        "execution_mode": "alert_only",
    })
    assert backend == {"key": "TRADEKEY", "secret": "TRADESECRET"}


def test_missing_data_credentials_fail_loudly(get_alpaca, monkeypatch):
    monkeypatch.delenv("ALPACA_DATA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_DATA_API_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="ALPACA_DATA_API_KEY"):
        get_alpaca({"strategy_name": "quality_notrend",
                    "alpaca_env_prefix": None,
                    "execution_mode": "alert_only"})


@pytest.mark.parametrize("strategy", ACTIVE)
def test_every_published_strategy_can_build_a_backend(strategy, get_alpaca,
                                                      monkeypatch):
    """The real configs, as shipped. This is the check that would have caught
    it: load each yaml and ask whether the runner can even start its cycle."""
    cfg = yaml.safe_load((CONFIGS / f"{strategy}.yaml").read_text())
    cfg.setdefault("strategy_name", strategy)
    monkeypatch.setenv("ALPACA_DATA_API_KEY", "D")
    monkeypatch.setenv("ALPACA_DATA_API_SECRET", "D")
    if cfg.get("alpaca_env_prefix"):
        p = cfg["alpaca_env_prefix"]
        monkeypatch.setenv(f"ALPACA_API_KEY_{p}", "T")
        monkeypatch.setenv(f"ALPACA_API_SECRET_{p}", "T")
    backend = get_alpaca(cfg)   # must not raise
    assert backend


# ── no order may escape an alert-only book ─────────────────────────────────


def _submit_order_calls(tree):
    """(lineno, [ancestor if-tests as source]) for each alpaca.submit_order."""
    import ast
    out = []

    class V(ast.NodeVisitor):
        def __init__(self):
            self.ifs = []      # enclosing If tests, innermost last
            self.scope = []    # enclosing function/loop bodies

        def visit_If(self, node):
            self.ifs.append(node.test)
            for child in node.body:
                self.visit(child)
            self.ifs.pop()
            for child in node.orelse:
                self.visit(child)

        def _enter_scope(self, node):
            self.scope.append(node)
            self.generic_visit(node)
            self.scope.pop()

        visit_For = visit_While = visit_FunctionDef = _enter_scope

        def visit_Call(self, node):
            f = node.func
            if (isinstance(f, ast.Attribute) and f.attr == "submit_order"
                    and isinstance(f.value, ast.Name) and f.value.id == "alpaca"):
                out.append((node.lineno,
                            [ast.unparse(t) for t in self.ifs],
                            list(self.scope)))
            self.generic_visit(node)

    V().visit(tree)
    return out


def _guard_names(tree):
    """Variables assigned from an alert_only expression.

    The guard is not always the literal — `skip_broker = execution_mode ==
    "alert_only" or ...` and the branch then reads `not skip_broker`. A test
    that only greps for the literal calls that path unguarded, which is a false
    positive on correct code and trains you to ignore the test.
    """
    import ast
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and "alert_only" in ast.unparse(node.value):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    names.add(t.id)
    return names


def _guarded(text, names):
    return "alert_only" in text or any(n in text for n in names)


def _has_early_exit_guard(scopes, call_line, names):
    """An `if ... alert_only ...: continue/return` earlier in an enclosing
    loop or function protects everything after it."""
    import ast
    for scope in scopes:
        for node in ast.walk(scope):
            if not isinstance(node, ast.If) or node.lineno >= call_line:
                continue
            if not _guarded(ast.unparse(node.test), names):
                continue
            if any(isinstance(st, (ast.Continue, ast.Return))
                   for st in ast.walk(node)):
                return True
    return False


def test_no_submit_order_call_is_ungated():
    """Two eviction paths called submit_order with no execution_mode check.
    They had never fired in production, which is the only reason an alert-only
    book never sold into a real account from them.

    Checked on the AST, not on text proximity: an earlier version of this test
    looked for "alert_only" within 60 lines and passed while one of the two
    paths was ungated, because the OTHER path's guard was in the window.
    """
    import ast
    tree = ast.parse(RUNNER.read_text())
    calls = _submit_order_calls(tree)
    assert calls, "no alpaca.submit_order calls found — has the API changed?"
    names = _guard_names(tree)

    unguarded = [
        line for line, if_tests, scopes in calls
        if not any(_guarded(t, names) for t in if_tests)
        and not _has_early_exit_guard(scopes, line, names)
    ]
    assert not unguarded, (
        f"submit_order at line(s) {unguarded} is not gated on execution_mode. "
        "Every published strategy is alert-only; an ungated order path sells "
        "into a real paper account."
    )

"""The live runner evaluates a stop on the session CLOSE, like the book.

simulate_strategy_portfolio checks `close_today <= stop_price` once a
session. cw_runner checked the latest trade on every ten-minute scan — a
tighter rule than the one published. PRTS on 2026-09-10 closed −17.6% (no
stop in the book) and was stopped live at −24.4% intraday; CLAUDE.md records
PDYN (+286.9%) surviving −20% intraday only because the rule is close-based.
"""
from __future__ import annotations

import ast
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
RUNNER = REPO / "strategies" / "cw_strategies" / "cw_runner.py"

from strategies.cw_strategies.cw_runner import _stop_evaluation_open  # noqa: E402


def test_stop_is_not_consulted_during_the_session():
    for h, m in ((9, 31), (12, 0), (15, 59), (16, 4)):
        assert not _stop_evaluation_open(datetime(2026, 9, 21, h, m))


def test_stop_is_consulted_once_the_close_is_official():
    for h, m in ((16, 5), (16, 10), (16, 50)):
        assert _stop_evaluation_open(datetime(2026, 9, 21, h, m))


def _check_exits():
    tree = ast.parse(RUNNER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "check_exits":
            return node
    raise AssertionError("check_exits not found")


def test_every_stop_loss_exit_sits_behind_the_close_gate():
    """Each `exit_reason = "stop_loss"` in check_exits must be inside an `if`
    whose test calls _stop_evaluation_open, and that block must fetch the
    session close."""
    fn = _check_exits()
    found = 0
    for node in ast.walk(fn):
        if not isinstance(node, ast.If):
            continue
        body_src = "".join(ast.dump(s) for s in node.body)
        if "'stop_loss'" not in body_src:
            continue
        # Skip the inner `if pnl_pct <= stop_loss` — we want the enclosing gate.
        if "_stop_evaluation_open" not in ast.dump(node.test):
            continue
        found += 1
        assert "_get_session_close" in body_src, (
            "the close-gated stop block does not fetch the session close; it "
            "would still measure the stop on an intraday print"
        )
    assert found >= 1, (
        "no stop_loss exit in check_exits is gated on _stop_evaluation_open — "
        "the live stop is intraday again"
    )
    # And no stop_loss assignment exists OUTSIDE such a gate.
    gated_ids = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.If) and "_stop_evaluation_open" in ast.dump(node.test):
            for sub in ast.walk(node):
                gated_ids.add(id(sub))
    for node in ast.walk(fn):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
                and node.value.value == "stop_loss"):
            assert id(node) in gated_ids, "a stop_loss exit is reachable outside the close gate"

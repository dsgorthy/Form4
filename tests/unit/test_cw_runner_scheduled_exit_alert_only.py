"""Guard: cw_runner.check_scheduled_exits must honour execution_mode before
submitting an Alpaca sell.

Root cause of the 2026-08-12 near-miss: there are TWO exit paths, and only one
of them respected the decision sink.

  check_exits()            → had `if execution_source == 'alert' or
                             execution_mode == 'alert_only': continue`
                             (does NOT touch Alpaca)  ✅
  check_scheduled_exits()  → the daily 15:45 ET planned_exit_date pass.
                             Selected rows with
                             `execution_source IN ('paper','live')` and called
                             submit_order() unconditionally.              ❌

So an alert_only strategy — one explicitly configured never to trade — would
still liquidate real broker positions through the scheduled path. Alpaca
execution was deprecated on 2026-08-12 (Alpaca is a data provider here, not an
execution venue), which makes "no strategy submits an order, ever" an
invariant rather than a preference.

Pure source/AST test: no DB, no Alpaca (matches test_cw_runner_entry_dedup.py
and test_at_capacity_rotate.py).
"""
from __future__ import annotations

import ast
from pathlib import Path

RUNNER = (
    Path(__file__).resolve().parents[2]
    / "strategies" / "cw_strategies" / "cw_runner.py"
)

GATE_NAMES = {"skip_broker", "execution_mode"}


def _fn(name: str) -> ast.FunctionDef:
    tree = ast.parse(RUNNER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in cw_runner.py")


def _submit_order_calls(fn: ast.AST) -> list[ast.Call]:
    return [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "submit_order"
    ]


def _names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def test_scheduled_exits_derives_execution_mode():
    """The function must know the decision sink at all."""
    fn = _fn("check_scheduled_exits")
    assigned = {
        t.id
        for n in ast.walk(fn) if isinstance(n, ast.Assign)
        for t in n.targets if isinstance(t, ast.Name)
    }
    assert "execution_mode" in assigned, (
        "check_scheduled_exits does not derive execution_mode — it cannot "
        "distinguish alert_only from paper/live and will submit Alpaca orders "
        "for strategies configured never to trade."
    )


def test_every_scheduled_exit_submit_order_is_gated():
    """Each submit_order() must sit under an `if` that consults the gate."""
    fn = _fn("check_scheduled_exits")
    calls = _submit_order_calls(fn)
    assert calls, "expected at least one submit_order() in check_scheduled_exits"

    # Map each submit_order call to the enclosing If tests that guard it.
    for call in calls:
        guarded = False
        for node in ast.walk(fn):
            if not isinstance(node, ast.If):
                continue
            if call not in list(ast.walk(node)):
                continue
            if _names(node.test) & GATE_NAMES:
                guarded = True
                break
        assert guarded, (
            "a submit_order() call in check_scheduled_exits is not guarded by "
            f"any of {sorted(GATE_NAMES)}. An alert_only strategy would "
            "liquidate real broker positions on the scheduled-exit pass."
        )


def test_check_exits_still_gated():
    """The originally-correct path must not regress."""
    fn = _fn("check_exits")
    src = ast.dump(fn)
    assert "alert_only" in RUNNER.read_text(), "alert_only handling vanished"
    assert _submit_order_calls(fn), "expected submit_order() in check_exits"
    assert "execution_mode" in src or "alert_only" in src, (
        "check_exits no longer references execution_mode/alert_only"
    )

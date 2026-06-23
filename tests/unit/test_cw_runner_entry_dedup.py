"""Guard: every entry path in cw_runner.execute_entries marks the ticker
held within the batch, so exactly one position — and in alert_only mode, one
ntfy push — is created per ticker per run.

Root cause of the 2026-06-22 duplicate-alert bug: the alert_only branch did
``entered.append(...)`` but never ``held_tickers.add(ticker)`` / ``slots -= 1``
(the dry_run and paper/live paths both did). So two same-day Form 4 buys on
one ticker by one insider (DIFFERENT trade_ids) both passed the
``if ticker in held_tickers`` guard and double-entered — two phone alerts for
AMR (trade_ids 1775205 + 1775206).

Pure source/AST test: no DB, no Alpaca (matches the existing cw_runner test
style in test_at_capacity_rotate.py).
"""
from __future__ import annotations

import ast
from pathlib import Path

RUNNER = (
    Path(__file__).resolve().parents[2]
    / "strategies" / "cw_strategies" / "cw_runner.py"
)


def _execute_entries() -> ast.FunctionDef:
    tree = ast.parse(RUNNER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "execute_entries":
            return node
    raise AssertionError("execute_entries not found in cw_runner.py")


def _marks_held(block: ast.AST) -> bool:
    """True if `block` contains a `held_tickers.add(...)` call."""
    for n in ast.walk(block):
        if (
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "add"
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "held_tickers"
        ):
            return True
    return False


def test_all_entry_paths_mark_ticker_held() -> None:
    fn = _execute_entries()
    n_adds = sum(
        1
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "add"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "held_tickers"
    )
    assert n_adds >= 3, (
        f"execute_entries has only {n_adds} held_tickers.add() call(s); expected "
        ">=3 (dry_run + alert_only + paper/live). An entry path that records a "
        "position without marking the ticker held re-opens the duplicate-alert "
        "bug (two same-ticker candidates in one batch both enter)."
    )


def test_alert_only_push_block_marks_ticker_held() -> None:
    """The alert_only block that actually fires the ntfy push must mark the
    ticker held — this is the exact path that double-alerted AMR."""
    fn = _execute_entries()
    found = False
    for node in ast.walk(fn):
        if not isinstance(node, ast.If):
            continue
        if "alert_only" not in ast.dump(node.test):
            continue
        body = "".join(ast.dump(s) for s in node.body)
        if "send_entry_alert" not in body:  # skip the execution_source branch
            continue
        found = True
        assert _marks_held(node), (
            "the alert_only push block sends an alert but never "
            "held_tickers.add(ticker) — duplicate-alert regression "
            "(2026-06-22: AMR alerted twice)."
        )
    assert found, "could not locate the alert_only push block in execute_entries"

"""Guard test: the public /portfolio API must read only the canonical
``execution_source = 'simulated'`` rows from ``strategy_portfolio``.

Root cause of the 2026-06-22 duplicate-open-positions bug: ``get_portfolio``'s
queries filtered on ``(strategy, is_live)`` but NOT ``execution_source``, so
cw_runner's operational ``alert`` / ``paper`` / ``live`` rows — written for
dedup + capacity tracking, not for display — leaked into the view and doubled
up the simulated positions (e.g. AMR showed 3x: two ``alert`` rows + one
``simulated``). The ``/overlay`` endpoint already filtered to ``simulated``;
the trade-log and summary queries never did.

This test parses the router source (no DB, no app import) and asserts that
EVERY SQL string in ``get_portfolio`` that reads ``strategy_portfolio`` also
pins ``execution_source = 'simulated'``. A new query that forgets the filter —
the exact way this bug crept in — fails here.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROUTER = Path(__file__).resolve().parents[2] / "api" / "routers" / "portfolio.py"


def _func(name: str) -> ast.FunctionDef:
    tree = ast.parse(ROUTER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {ROUTER}")


def _strategy_portfolio_reads(func: ast.AST) -> list[str]:
    """Every string literal in ``func`` that SELECTs from strategy_portfolio."""
    out: list[str] = []
    for node in ast.walk(func):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            s = node.value
            if "strategy_portfolio" in s and "select" in s.lower():
                out.append(s)
    return out


def test_get_portfolio_reads_are_simulated_only() -> None:
    reads = _strategy_portfolio_reads(_func("get_portfolio"))
    # Sanity: confirm we actually located the reads (the function has ~9).
    assert len(reads) >= 8, (
        f"expected >=8 strategy_portfolio reads in get_portfolio, found "
        f"{len(reads)} — did the function change shape?"
    )
    offenders = [s for s in reads if "execution_source = 'simulated'" not in s]
    assert not offenders, (
        "get_portfolio has strategy_portfolio read(s) that do not pin "
        "execution_source = 'simulated' — this re-opens the duplicate "
        "open-positions bug (cw_runner alert/paper/live rows leak in). "
        "Offending queries:\n\n"
        + "\n---\n".join(s.strip()[:220] for s in offenders)
    )

"""Guard test: every ``strategy_portfolio`` read in the public /portfolio API
must discriminate on ``execution_source`` — never a bare read across all
sources.

Root cause of the 2026-06-22 duplicate-positions bug: ``get_portfolio``'s
queries filtered on ``(strategy, is_live)`` but NOT ``execution_source``, so
cw_runner's operational ``alert`` / ``paper`` / ``live`` rows (its own
dedup/capacity bookkeeping) leaked in and doubled up the simulated positions
(AMR showed 3x: two ``alert`` rows + one ``simulated``).

The fix splits the view by source on purpose:
  * closed trades + stats + equity curve  → ``execution_source = 'simulated'``
    (the canonical nightly track record).
  * open positions in the trade log        → ONE row per ticker, preferring
    the operational entry (alert/paper/live) over simulated — the live entry
    the user actually got pinged on, marked to market for unrealized P&L.

Either way, no read may omit the ``execution_source`` discriminator — that is
the exact regression that caused the bug. This test parses the router source
(no DB, no app import) and fails if a read forgets it.
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


def test_get_portfolio_reads_discriminate_execution_source() -> None:
    reads = _strategy_portfolio_reads(_func("get_portfolio"))
    # Sanity: confirm we actually located the reads (the function has ~9).
    assert len(reads) >= 8, (
        f"expected >=8 strategy_portfolio reads in get_portfolio, found "
        f"{len(reads)} — did the function change shape?"
    )
    offenders = [s for s in reads if "execution_source" not in s]
    assert not offenders, (
        "get_portfolio has strategy_portfolio read(s) with no execution_source "
        "discriminator. A bare read unions simulated + cw_runner's operational "
        "alert/paper/live rows and double-counts positions (the 2026-06-22 "
        "duplicate-positions bug). Closed/stats/curve must pin "
        "execution_source = 'simulated'; the open trade-log picks one row per "
        "ticker across the operational+simulated sources. Offending queries:\n\n"
        + "\n---\n".join(s.strip()[:220] for s in offenders)
    )

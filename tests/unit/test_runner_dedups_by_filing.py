"""A filing is a decision; the runner must not alert it twice.

Dedup keyed on trade_id alone let PRTS re-enter on 2026-09-11 — a second
execution lot of the 2026-09-09 filing, one day after the first lot had been
stopped out — so the subscriber got the same filing twice. The simulator
cannot do this (every lot of a filing lands on one day and `entered_today`
blocks the rest); the runner re-scans a two-day window and has to remember.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

RUNNER = Path(__file__).resolve().parents[2] / "strategies" / "cw_strategies" / "cw_runner.py"


def _fn(name):
    tree = ast.parse(RUNNER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found")


def test_scan_signals_collects_filings_already_acted_on():
    src = ast.get_source_segment(RUNNER.read_text(), _fn("scan_signals"))
    assert "used_filings" in src
    assert re.search(r"SELECT ticker, filing_date FROM strategy_portfolio", src), (
        "used_filings is not built from the strategy's ledger"
    )


def test_engine_path_receives_and_applies_the_filing_dedup():
    fn = _fn("_scan_signals_engine")
    assert any(a.arg == "used_filings" for a in fn.args.args + fn.args.kwonlyargs), (
        "_scan_signals_engine no longer takes used_filings"
    )
    src = ast.get_source_segment(RUNNER.read_text(), fn)
    assert "filing already acted on this strategy" in src


def test_legacy_sql_path_applies_the_filing_dedup_too():
    src = ast.get_source_segment(RUNNER.read_text(), _fn("scan_signals"))
    assert src.count("filing already acted on this strategy") >= 1

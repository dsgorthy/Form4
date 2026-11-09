"""Logging a closed position must never crash the runner.

2026-11-09 06:01: Insider Breakout's runner sent the TISI and FGBI exit alerts,
then crashed formatting `cl["pnl_pct"] * 100` for its log line -- the
alert-only exit path hands back the raw OPEN position row, whose pnl_pct,
pnl_dollar and hold_days are still NULL. run_daily died before processing
entries and Dagster paged "Dataplane run failed". Every alert-only exit day,
on every published book, did the same.
"""
from __future__ import annotations

import importlib.util
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
RUNNER = ROOT / "strategies" / "cw_strategies" / "cw_runner.py"


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("cw_runner_logs_under_test", RUNNER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_a_raw_open_position_row_logs_instead_of_crashing(runner):
    """What the alert-only path actually appends: the open row, NULLs and all."""
    row = {"id": 120401, "ticker": "TISI", "status": "open", "exit_reason": None,
           "pnl_pct": None, "pnl_dollar": None, "hold_days": None}
    line = runner._describe_closed(row)
    assert "TISI" in line and "pnl n/a" in line


def test_a_complete_record_logs_as_before(runner):
    line = runner._describe_closed({"ticker": "FGBI", "exit_reason": "time_exit",
                                    "pnl_pct": 0.477, "pnl_dollar": 9512.4, "hold_days": 60})
    assert line == "  FGBI: time_exit (47.7%, $9512, 60d)"


def test_postgres_numerics_format_as_numbers(runner):
    line = runner._describe_closed({"ticker": "X", "exit_reason": "stop",
                                    "pnl_pct": Decimal("-0.175"), "pnl_dollar": Decimal("-3500"),
                                    "hold_days": 12})
    assert line == "  X: stop (-17.5%, $-3500, 12d)"


def test_run_daily_logs_closed_positions_through_the_safe_helper():
    src = RUNNER.read_text()
    assert 'cl["pnl_pct"] * 100' not in src, (
        "run_daily formats pnl_pct directly again; an alert-only exit row has it NULL")
    assert "logger.info(_describe_closed(cl))" in src

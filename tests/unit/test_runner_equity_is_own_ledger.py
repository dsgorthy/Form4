"""cw_runner sizes positions off ITS OWN ledger, never the simulated book.

get_theoretical_equity summed pnl_dollar over every closed row for the
strategy. The simulator writes ten years of `simulated` rows into the same
table, so A-List read $616k of equity on a $100k stake, asked for a $203,290
position, and the guardrail rejected every candidate it ever saw — zero
alerts, ever. Insider Breakout's six alerts fired only inside the daily
window where the simulator had wiped its rows and equity read $100,000.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from strategies.cw_strategies.cw_runner import get_theoretical_equity  # noqa: E402


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE strategy_portfolio (
        strategy TEXT, status TEXT, pnl_dollar REAL, execution_source TEXT)""")
    rows = [
        ("s", "closed", 400_000.0, "simulated"),
        ("s", "closed", 116_000.0, "simulated"),
        ("s", "closed", -5_000.0, "alert"),
        ("s", "closed", -4_321.0, "alert"),
        ("s", "open", 999_999.0, "alert"),        # open rows never count
        ("s", "closed", 2_000.0, "paper"),
        ("s", "closed", 1_000.0, "live"),
        ("other", "closed", 50_000.0, "alert"),   # another strategy
    ]
    conn.executemany("INSERT INTO strategy_portfolio VALUES (?, ?, ?, ?)", rows)
    return conn


def test_simulated_rows_do_not_count():
    eq = get_theoretical_equity(_conn(), {"strategy_name": "s", "starting_capital": 100_000})
    assert eq == 100_000 - 5_000 - 4_321 + 2_000 + 1_000


def test_empty_ledger_is_the_stake():
    conn = _conn()
    conn.execute("DELETE FROM strategy_portfolio WHERE execution_source <> 'simulated'")
    eq = get_theoretical_equity(conn, {"strategy_name": "s", "starting_capital": 100_000})
    assert eq == 100_000

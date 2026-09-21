"""The simulator replaces a book in ONE transaction.

run() used to delete the simulated rows and commit, simulate for ~5 minutes,
then insert. Every reader in that window saw an empty book: the public
/portfolio page flashed empty at 07:00 PT daily, and cw_runner — whose
equity summed every closed row — read $100,000 for a strategy that
otherwise showed $542k, which is the only reason its alerts ever cleared the
$25k guardrail. The delete now happens inside persist_positions, uncommitted,
immediately before the inserts.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

SIM = Path(__file__).resolve().parents[2] / "pipelines" / "insider_study" / "simulate_strategy_portfolio.py"


def _fn_src(name):
    src = SIM.read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node)
    raise AssertionError(f"{name} not found")


def test_run_does_not_delete_before_simulating():
    run = _fn_src("run")
    assert "DELETE" not in run and "wipe_strategy(" not in run, (
        "run() deletes the book before simulating again; readers see it empty "
        "for the whole simulation"
    )


def test_persist_deletes_uncommitted_then_inserts_then_commits_once():
    persist = _fn_src("persist_positions")
    m = re.search(r"wipe_strategy\(conn, strategy_name, commit=False\)", persist)
    assert m, "persist_positions must wipe with commit=False so the delete rides the insert transaction"
    first_insert = persist.index("INSERT INTO")
    assert m.start() < first_insert, "the wipe must precede the inserts"
    between = persist[m.end():]
    assert between.count("conn.commit()") == 1, (
        "persist_positions must commit exactly once, after the inserts"
    )
    assert between.rindex("conn.commit()") > between.rindex("INSERT INTO")


def test_wipe_strategy_can_defer_its_commit():
    wipe = _fn_src("wipe_strategy")
    assert "commit: bool = True" in wipe and "if commit:" in wipe

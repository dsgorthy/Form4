"""A parsed filing that stored nothing is a failure, not a success.

`process_filing` marked every filing processed using the PARSED trade count,
not the stored one. So when the insert path broke (an unbound cursor, 2026-09-08)
each filing was parsed, every row raised NameError into a broad handler, and
the filing was still retired as "ok" with a positive trade_count. It would
never be looked at again.

Measured the day it was found: the 15:15 run reported "37 new filings -> 0
trades" and exited 0, having logged a NameError traceback for every single row.

A CORRECTION WORTH KEEPING. "status='ok' with zero rows in `trades`" is NOT by
itself evidence of loss, and reading it that way sent this investigation down a
false path. The unique key on `trades` is
(insider_id, ticker, trade_date, trade_type, value) — accession is NOT in it —
so a second filing of an already-reported transaction is correctly suppressed
by INSERT OR IGNORE and stores nothing. Of 127 such filings, five were sampled,
re-parsed, and every one was already present under a different accession:

    0001094629-26-000002  TILE 2026-09-02 sell $297,040
        -> stored under 0001094629-26-000001

34 of the 127 were 4/A amendments, where that is the whole point. So the
invariant this file protects is narrow and specific: a filing whose inserts
RAISED must not be retired. A filing that stored nothing because the rows were
already there is finished, and re-opening it just churns it through retries
until it is abandoned.

The xml-unavailable branch immediately above already had this exact guard,
with a comment naming the bug — the insert branch just never got it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FETCH = (REPO / "strategies" / "insider_catalog" / "fetch_latest.py").read_text(encoding="utf-8")
BACKFILL = (REPO / "strategies" / "insider_catalog" / "backfill_live.py").read_text(encoding="utf-8")


def _process_filing_src() -> str:
    tree = ast.parse(FETCH)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and "insert_trades" in ast.unparse(node):
            return ast.unparse(node)
    raise AssertionError("no function in fetch_latest.py calls insert_trades")


def test_insert_failures_are_reported_to_the_caller():
    """Logging a failure nobody can see is how this stayed invisible."""
    assert re.search(r"def insert_trades\([^)]*errors", BACKFILL, re.S), (
        "insert_trades must accept an errors channel; otherwise a row that "
        "raised is indistinguishable from a row that stored"
    )
    handler = BACKFILL[BACKFILL.index("except Exception as exc:"):]
    handler = handler[:handler.index("\n    if rejected_future")]
    assert "errors.append" in handler, (
        "the broad row handler logs and swallows: the caller cannot tell that "
        "nothing was stored"
    )


def test_a_filing_whose_insert_failed_is_not_marked_processed():
    src = _process_filing_src()
    assert "mark_attempt_failed" in src, (
        "process_filing has no failure path for a broken insert, so a filing "
        "that stored nothing is retired forever"
    )
    # The failure branch must return BEFORE mark_processed.
    i_err = src.index("insert_errors")
    i_failed = src.index("mark_attempt_failed", i_err)
    i_proc = src.index("mark_processed", i_err)
    assert i_failed < i_proc, (
        "mark_processed is reached even when the insert errored; the failure "
        "branch must return first"
    )
    branch = src[i_failed:i_proc]
    assert "return" in branch, (
        "the failure branch falls through into mark_processed, which retires "
        "the filing anyway"
    )


def test_the_parsed_count_is_not_what_marks_success():
    """mark_processed(..., len(trades)) is the literal defect."""
    src = _process_filing_src()
    assert re.search(r"if insert_errors", src), (
        "nothing checks whether the insert actually stored anything"
    )

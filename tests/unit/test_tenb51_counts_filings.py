"""The 10b5-1 prior-sell gate counts filings, not execution lots.

WHAT WAS WRONG

`min_prior_10b5_1_sells` asks "has this insider sold on a schedule at least N
times before?" Both surfaces answered it by counting trade ROWS:

    cw_runner.py         SELECT COUNT(*) FROM trades WHERE ... is_10b5_1 = 1
    tenb51_surprise.py   sum(1 for p in priors if p.is_10b5_1 and ...)

A scheduled sale filled in five tranches is one scheduled sale. Counting rows
made it five, so an insider with two prior 10b5-1 sales could clear a gate of
ten. This is the same defect that moved 21% of career grades in August 2026 —
found in six places then, and these are the seventh and eighth.

WHY IT WAS NOT CAUGHT

tenb51_surprise was retired 2026-08-18, so the path is dormant. CLAUDE.md keeps
it deliberately reversible: "re-add it to ACTIVE_STRATEGIES and STRATEGY_CONFIG
to resume". Re-enabling it would have reintroduced the bug silently. It also
could not have been caught: no fixture in the suite had a filing_key column, so
no test could express "one filing, five rows".

Both surfaces must stay identical — a gate that differs between the simulated
book and the live alerts is the -30% stop defect again.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "strategies/cw_strategies/cw_runner.py"
PIT_STRATEGY = REPO / "framework/pit/strategies/tenb51_surprise.py"

FILING_KEY = "COALESCE(t.filing_key, t.accession, CAST(t.trade_date AS TEXT))"


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute("""CREATE TABLE trades (
        trade_id INTEGER PRIMARY KEY, insider_id INTEGER, ticker TEXT,
        trans_code TEXT, trade_date TEXT, filing_date TEXT,
        filing_key TEXT, accession TEXT, is_10b5_1 INTEGER)""")
    return db


def _sale(db, trade_id, filing_key, filing_date="2024-01-05"):
    db.execute(
        "INSERT INTO trades(trade_id, insider_id, ticker, trans_code, trade_date,"
        " filing_date, filing_key, is_10b5_1) VALUES (?,1,'AAA','S',?,?,?,1)",
        (trade_id, filing_date, filing_date, filing_key))


def _count(db, before="2026-01-01"):
    """The query cw_runner runs, extracted so the test exercises the real SQL."""
    src = RUNNER.read_text()
    m = re.search(r"SELECT COUNT\(DISTINCT COALESCE\(\s*"
                  r"filing_key, accession, CAST\(trade_date AS TEXT\)\)\)\s*"
                  r"FROM trades\s*WHERE insider_id = \? AND ticker = \?\s*"
                  r"AND trans_code = 'S' AND is_10b5_1 = 1\s*AND filing_date < \?",
                  src)
    assert m, "cw_runner's 10b5-1 count query changed shape — update this test"
    return db.execute(m.group(0).replace("?", "{}").format(
        1, "'AAA'", f"'{before}'")).fetchone()[0]


def test_five_tranches_of_one_sale_count_once(conn):
    for i in range(5):
        _sale(conn, 10 + i, "ONE-FILING")
    conn.commit()
    assert _count(conn) == 1, (
        "a 10b5-1 sale filled in five tranches counted as five prior sales; "
        "an insider with one scheduled sale could clear a gate of five"
    )


def test_distinct_filings_still_count_separately(conn):
    _sale(conn, 1, "A", "2024-01-05")
    _sale(conn, 2, "B", "2024-02-05")
    _sale(conn, 3, "C", "2024-03-05")
    conn.commit()
    assert _count(conn) == 3


def test_a_row_with_no_filing_identity_still_counts(conn):
    """COALESCE falls through to trade_date. Two such rows on different days
    are two filings; the fallback must not collapse unrelated sales."""
    conn.execute("INSERT INTO trades(trade_id, insider_id, ticker, trans_code,"
                 " trade_date, filing_date, is_10b5_1) VALUES (1,1,'AAA','S',"
                 "'2024-01-05','2024-01-05',1)")
    conn.execute("INSERT INTO trades(trade_id, insider_id, ticker, trans_code,"
                 " trade_date, filing_date, is_10b5_1) VALUES (2,1,'AAA','S',"
                 "'2024-02-05','2024-02-05',1)")
    conn.commit()
    assert _count(conn) == 2


def test_the_pit_path_deduplicates_too():
    """Source-level: the PIT strategy must collapse on filing_key, not sum rows.
    The two surfaces gate live alerts and the published book respectively."""
    src = PIT_STRATEGY.read_text()
    assert "sum(\n                1 for p in priors" not in src, (
        "tenb51_surprise counts prior 10b5-1 rows again instead of filings"
    )
    assert "p.filing_key" in src, (
        "tenb51_surprise no longer groups prior sales by filing_key"
    )


def test_the_live_runner_does_not_count_rows():
    src = RUNNER.read_text()
    assert "SELECT COUNT(*) FROM trades" not in src.replace(" ", " "), (
        "cw_runner counts trade rows somewhere again — check whether the unit "
        "should be a filing"
    )

"""`is_largest_ever` must compare purchases, not tranches.

There was no test for this function at all, which is how it shipped wrong
twice:

  * August 2026 — a `--since` window was mistaken for an insider's whole
    career, so 23.7% of flags were wrong (fixed by loading full history for
    comparison and filtering only the writes).
  * August 22, 2026 — it compared individual execution rows, so "the largest
    purchase they have ever made" meant "a bigger slice than any previous
    slice". 26.6% of flags flip once filings are compared.

The second one published a false claim. Benjamin Wood's August CDNL filing was
$1,014,594 across two lots; the larger lot, $534,451, was flagged largest-ever
and the trade page said so. His May filing was $1,025,900 across five lots —
bigger. August was not his largest and we published that it was.

Both failure modes are covered below, because the fix for one does not imply
the other.
"""
from __future__ import annotations

import sqlite3

import pytest

import pipelines.insider_study.compute_cw_indicators as ci


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute(
        """CREATE TABLE trades (
               trade_id INTEGER PRIMARY KEY,
               insider_id INTEGER,
               ticker TEXT,
               trans_code TEXT,
               trade_date TEXT,
               filing_key TEXT,
               accession TEXT,
               value REAL,
               purchase_size_ratio REAL,
               is_largest_ever INTEGER
           )"""
    )
    return db


def _buy(db, trade_id, insider_id, ticker, date, value, filing_key=None):
    """filing_key defaults to the trade_id — one row, one filing. Pass the same
    key for several rows to model one purchase filled in tranches."""
    db.execute(
        "INSERT INTO trades(trade_id, insider_id, ticker, trans_code, trade_date,"
        " filing_key, value) VALUES (?,?,?,'P',?,?,?)",
        (trade_id, insider_id, ticker, date, filing_key or f"F{trade_id}", value),
    )


def _flag(db, trade_id):
    return db.execute(
        "SELECT is_largest_ever FROM trades WHERE trade_id=?", (trade_id,)
    ).fetchone()[0]


def _run(db, since="2016-01-01"):
    ci.MIN_DATE = since
    ci.compute_purchase_size_metrics(db)


def test_the_wood_case(conn):
    """The exact filing that published a false claim.

    May: $1,025,900 across five tranches. August: $1,014,594 across two.
    August is NOT the largest, however its individual lots compare.
    """
    for i, v in enumerate([336209, 271927, 218291, 194040, 5433]):
        _buy(conn, 10 + i, 1, "CDNL", "2026-05-27", v, filing_key="MAY")
    for i, v in enumerate([534451, 480143]):
        _buy(conn, 20 + i, 1, "CDNL", "2026-08-14", v, filing_key="AUG")
    conn.commit()
    _run(conn)

    assert _flag(conn, 10) == 1, "the first purchase is trivially the largest"
    assert _flag(conn, 20) == 0, (
        "August was flagged largest-ever. Its biggest lot ($534,451) beats "
        "every May lot, but the PURCHASE ($1,014,594) is smaller than May's "
        "($1,025,900)."
    )


def test_every_lot_of_a_filing_carries_the_filing_verdict(conn):
    """Downstream readers look at single rows, so all lots must agree."""
    _buy(conn, 1, 2, "AAA", "2026-01-05", 100_000, filing_key="A")
    for i, v in enumerate([90_000, 80_000]):
        _buy(conn, 10 + i, 2, "AAA", "2026-02-05", v, filing_key="B")
    conn.commit()
    _run(conn)
    # Filing B totals 170,000 > 100,000, so BOTH its lots are largest-ever.
    assert _flag(conn, 10) == 1 and _flag(conn, 11) == 1, (
        "lots of one filing disagree about whether that filing was the largest"
    )


def test_a_genuinely_larger_later_purchase_is_flagged(conn):
    _buy(conn, 1, 3, "BBB", "2026-01-05", 50_000)
    _buy(conn, 2, 3, "BBB", "2026-02-05", 150_000)
    conn.commit()
    _run(conn)
    assert _flag(conn, 1) == 1
    assert _flag(conn, 2) == 1


def test_a_smaller_later_purchase_is_not(conn):
    _buy(conn, 1, 4, "CCC", "2026-01-05", 150_000)
    _buy(conn, 2, 4, "CCC", "2026-02-05", 50_000)
    conn.commit()
    _run(conn)
    assert _flag(conn, 2) == 0


def test_a_short_since_window_does_not_invent_a_career_start(conn):
    """The August 2026 failure mode: --since must bound the WRITES, never the
    comparison. A window boundary is not the start of someone's career."""
    _buy(conn, 1, 5, "DDD", "2020-01-05", 900_000)     # big, outside the window
    _buy(conn, 2, 5, "DDD", "2026-06-05", 10_000)      # small, inside it
    conn.commit()
    _run(conn, since="2026-01-01")
    assert _flag(conn, 2) == 0, (
        "a $10k purchase was called the largest ever because the --since "
        "window hid a $900k one — 23.7% of flags were wrong this way"
    )


def test_two_filings_on_one_day_stay_separate(conn):
    """Same trade_date, different accessions: two decisions."""
    _buy(conn, 1, 6, "EEE", "2026-01-05", 100_000, filing_key="X")
    _buy(conn, 2, 6, "EEE", "2026-01-05", 40_000, filing_key="Y")
    conn.commit()
    _run(conn)
    assert _flag(conn, 1) == 1
    assert _flag(conn, 2) == 0, "the smaller of two same-day filings is not the largest"

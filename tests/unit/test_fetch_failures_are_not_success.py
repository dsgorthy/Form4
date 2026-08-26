"""A filing we could not download is not a filing with no trades.

WHAT WENT WRONG

`fetch_latest._run_fetch_inner` called mark_processed(..., trade_count=0) when
`fetch_form4_xml` returned None, with the comment "Still mark as processed to
avoid retrying bad XMLs every run". `fetch_form4_xml` returns None on ANY
non-200, timeout or RequestException, and `get_known_accessions` read every row
in the table — so one transient EDGAR error retired a filing permanently.

EDGAR rate-limits hard. The zero-trade rate in `processed_filings` was 0.0%
every month through 2026-02 and then 21.5% in April and 25.5% in July. Of 24
zero-trade rows sampled against EDGAR on 2026-08-26, fourteen held real
non-derivative transactions and were simply absent from `trades` — Crawford &
Company filed six in one month and we held none of them.

Measured against EDGAR's own index for 2026-08-14: 506 filings published, 425
held.

THE PROPERTY THAT MUST SURVIVE

Not retrying is still the right behaviour for a filing we have actually READ
and which genuinely reports no non-derivative transactions. The distinction
between "read it, nothing there" and "never got it" is the entire fix, so
these tests pin both sides of it.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "strategies" / "insider_catalog"))

import fetch_latest as F  # noqa: E402

SCHEMA = """
CREATE TABLE processed_filings (
    accession TEXT PRIMARY KEY,
    filing_date TEXT,
    trade_count INTEGER DEFAULT 0,
    processed_at TEXT DEFAULT (datetime('now')),
    status TEXT,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    cik TEXT,
    company TEXT
)
"""

ACC = "0001007549-26-000008"      # a real filing we lost: MAIN, 3 transactions
DAY = "2026-08-14"


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute(SCHEMA)
    return db


def _row(conn, acc=ACC):
    cur = conn.execute(
        "SELECT status, attempts, trade_count FROM processed_filings WHERE accession = ?",
        (acc,))
    return cur.fetchone()


# ── the bug ────────────────────────────────────────────────────────────────

def test_a_failed_fetch_is_not_treated_as_known(conn):
    F.mark_attempt_failed(conn, ACC, DAY, "HTTP 429", cik="1007549", company="MAIN")
    assert ACC not in F.get_known_accessions(conn), (
        "a filing that failed to download is being skipped as though we had "
        "read it — this is the defect that lost ~12% of every day's filings"
    )


def test_a_failed_fetch_is_offered_for_retry(conn):
    F.mark_attempt_failed(conn, ACC, DAY, "HTTP 429", cik="1007549", company="MAIN")
    queued = [r[0] for r in F.get_retryable(conn, 10)]
    assert queued == [ACC]


def test_the_retry_queue_carries_what_a_refetch_needs(conn):
    """processed_filings held only the accession, so nothing COULD be retried."""
    F.mark_attempt_failed(conn, ACC, DAY, "timeout", cik="1007549", company="MAIN")
    acc, fdate, attempts, cik, company = F.get_retryable(conn, 10)[0]
    assert (acc, fdate, cik, company) == (ACC, DAY, "1007549", "MAIN")


# ── the property that must survive ─────────────────────────────────────────

def test_a_filing_we_read_with_no_transactions_is_never_retried(conn):
    """Holdings-only and derivative-only Form 4s legitimately yield nothing."""
    F.mark_processed(conn, ACC, DAY, 0)
    assert _row(conn)[0] == "empty"
    assert ACC in F.get_known_accessions(conn)
    assert F.get_retryable(conn, 10) == []


def test_a_filing_with_trades_is_recorded_ok(conn):
    F.mark_processed(conn, ACC, DAY, 3)
    status, attempts, count = _row(conn)
    assert (status, count) == ("ok", 3)
    assert ACC in F.get_known_accessions(conn)


# ── retry bookkeeping ──────────────────────────────────────────────────────

def test_a_successful_retry_overwrites_the_failure(conn):
    """INSERT OR IGNORE kept the failure row and dropped the recovery."""
    F.mark_attempt_failed(conn, ACC, DAY, "HTTP 503", cik="1007549", company="MAIN")
    F.mark_processed(conn, ACC, DAY, 3)
    status, attempts, count = _row(conn)
    assert (status, count) == ("ok", 3), "the recovered filing was not written"
    assert attempts == 2
    assert F.get_retryable(conn, 10) == []


def test_retrying_forever_is_not_the_answer_either(conn):
    """A genuinely dead accession must stop consuming the sweep."""
    for _ in range(F.MAX_FETCH_ATTEMPTS):
        F.mark_attempt_failed(conn, ACC, DAY, "HTTP 404", cik="1007549", company="MAIN")
    status, attempts, _ = _row(conn)
    assert status == "abandoned" and attempts == F.MAX_FETCH_ATTEMPTS
    assert F.get_retryable(conn, 10) == [], "abandoned rows must leave the queue"
    assert ACC in F.get_known_accessions(conn)


def test_rows_written_before_the_fix_are_left_alone(conn):
    """948k pre-existing rows have NULL status. Re-driving them through EDGAR
    one at a time is the wrong tool; the bulk SEC datasets cover that ground."""
    conn.execute("INSERT INTO processed_filings (accession, filing_date, trade_count) "
                 "VALUES ('0000000000-00-000000', '2021-03-16', 0)")
    assert "0000000000-00-000000" in F.get_known_accessions(conn)
    assert F.get_retryable(conn, 10) == []


# ── the worker both paths share ────────────────────────────────────────────

FILING = {"accession": ACC, "filing_date": DAY, "cik": "1007549", "company": "MAIN"}


def test_process_one_queues_the_filing_when_the_download_fails(conn, monkeypatch):
    monkeypatch.setattr(F, "fetch_form4_xml", lambda cik, acc: (None, None))
    inserted, outcome, parsed, _, _ = F._process_one(conn, FILING, dry_run=False)
    assert (inserted, outcome, parsed) == (0, "failed", 0)
    assert ACC not in F.get_known_accessions(conn)


def test_process_one_records_a_real_parse(conn, monkeypatch):
    monkeypatch.setattr(F, "fetch_form4_xml", lambda cik, acc: ("<xml/>", "2026-08-14 16:00:00"))
    monkeypatch.setattr(F, "parse_form4_xml", lambda *a, **k: [
        {"trade_type": "buy"}, {"trade_type": "sell"}, {"trade_type": "sell"}])
    monkeypatch.setattr(F, "insert_trades", lambda *a, **k: 3)
    inserted, outcome, parsed, buys, sells = F._process_one(conn, FILING, dry_run=False)
    assert (inserted, outcome, parsed, buys, sells) == (3, "ok", 3, 1, 2)
    assert ACC in F.get_known_accessions(conn)


def test_process_one_does_not_write_anything_on_a_dry_run(conn, monkeypatch):
    monkeypatch.setattr(F, "fetch_form4_xml", lambda cik, acc: (None, None))
    F._process_one(conn, FILING, dry_run=True)
    assert _row(conn) is None

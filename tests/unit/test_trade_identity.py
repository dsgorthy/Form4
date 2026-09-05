"""A Form 4 transaction's identity is its position in its own filing.

WHAT WENT WRONG

Uniqueness on `trades` was

    UNIQUE (insider_id, ticker, trade_date, trade_type, value)

where `value` is DOUBLE PRECISION. Ingest uses INSERT OR IGNORE, which the
compat layer turns into ON CONFLICT DO NOTHING, so that constraint WAS the
idempotency guarantee — and floats differing in the last cents are different
keys:

    HLT  2017-03-15  $3,952,018,935.87  vs  $3,952,018,936.00
    CRBG 2024-12-09  $3,837,963,376.00  vs  $3,837,963,376.32

The 2026-08-26 reload therefore re-inserted filings that already existed:
458,314 accessions ingested more than once, 402,118 excess rows, ~$803B
double-counted in published figures.

THE FIX

(accession, line_no), where line_no is the transaction's index in the Form 4
document. ElementTree preserves document order, so the parser already had it
and threw it away.

Verified against production 2026-09-04: 5,263,326 rows numbered, 0 violations,
and an insert differing only by a cent is refused where the old key admitted
it.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PARSER = (REPO / "strategies" / "insider_catalog" / "backfill_live.py").read_text(encoding="utf-8")

# Comments in this module QUOTE the old broken code by design, so a raw
# substring scan flags the documentation of a rule as a violation of it.
# Third time that has bitten today; strip them once, here.
PARSER_CODE = "\n".join(
    line.split("#", 1)[0] if not line.lstrip().startswith("#") else ""
    for line in PARSER.split("\n")
)
MIGRATION = (REPO / "migrations" / "2026-09-04_trades_line_no.sql").read_text(encoding="utf-8")
BACKFILL = (REPO / "scripts" / "backfill_line_no.py").read_text(encoding="utf-8")


def test_the_parser_stamps_a_line_number():
    assert re.search(r"for i, t in enumerate\(trades\):\s*\n\s*t\[\"line_no\"\] = i", PARSER), (
        "parse_form4_xml no longer assigns line_no. Without it (accession, "
        "line_no) cannot be the key and re-ingest duplicates again."
    )


def test_line_numbers_are_assigned_after_both_parse_loops():
    """Numbering inside each loop restarts at 0 for the derivative/swap
    section, so two rows in one filing would share a line_no."""
    assign = PARSER.index('t["line_no"] = i')
    ret = PARSER.index('return {"trades": trades, "derivative_trades": derivative_trades}')
    deriv = PARSER.index("dtxn_tag = ")
    assert deriv < assign < ret, (
        "line_no is assigned before the derivative loop has finished "
        "appending, so the numbering is not unique across the filing"
    )


def test_the_insert_binds_line_no():
    stmt = PARSER[PARSER.index("INSERT OR IGNORE INTO trades"):]
    stmt = stmt[:stmt.index("))")]
    assert "line_no" in stmt, "insert_trades no longer stores line_no"
    assert 't.get("line_no")' in stmt, "line_no is declared but never bound"


def test_the_unique_index_is_partial_and_concurrent():
    assert "CREATE UNIQUE INDEX CONCURRENTLY" in MIGRATION, (
        "the index is no longer CONCURRENT — a plain CREATE INDEX takes "
        "ACCESS EXCLUSIVE on a 28 GB table, which is what took form4.app down "
        "on 2026-08-27"
    )
    assert "WHERE line_no IS NOT NULL" in MIGRATION, (
        "the index is no longer partial. 1,858,181 rows are deliberately "
        "unnumbered pending Phase 2 dedup, plus 47,388 with no accession."
    )


def test_the_backfill_skips_duplicated_accessions():
    """Numbering across an accession ingested twice turns a five-line filing
    into line_no 0..9 and calls it ten distinct lines — unique, and false."""
    assert "count(DISTINCT ingested_at::date) = 1" in BACKFILL, (
        "the backfill no longer restricts itself to accessions ingested once"
    )


def test_the_backfill_is_batched_and_resumable():
    """One windowed UPDATE over 6.7M rows ran 30 minutes, hit
    statement_timeout and rolled back every row. A retry starts from zero."""
    assert "conn.commit()" in BACKFILL and "for i in range(0, len(todo)" in BACKFILL, (
        "the backfill is back to a single transaction; it will time out and "
        "roll back the whole thing"
    )
    assert "count(*) FILTER (WHERE line_no IS NULL) > 0" in BACKFILL, (
        "the backfill no longer skips already-numbered accessions, so it is "
        "not resumable"
    )


def test_insert_conflicts_are_caught_by_type_not_by_string():
    """Two wrong versions preceded this one.

    v1  `except Exception: pass  # duplicate` — counted schema errors, type
        errors and real constraint violations alike as filings we already had.
    v2  matched "duplicate key"/"unique" in the message. This repo already
        names that as the bug pattern (backfill.migrate_schema: "catching by
        string is the bug pattern, not the fix", after the April 2026 outage),
        and it would swallow "there is no unique or exclusion constraint
        matching the ON CONFLICT specification" — a broken deployment.
    """
    i = PARSER_CODE.index("INSERT OR IGNORE INTO trades")
    tail = PARSER_CODE[i:i + 4000]
    assert "pass" not in tail.split("except Exception")[0][-200:], (
        "the blind `except: pass` handler is back"
    )
    assert "except _INSERT_CONFLICT" in tail, (
        "insert conflicts are no longer caught by exception type"
    )
    assert "duplicate key" not in tail, (
        "string matching on the error message is back"
    )
    assert "UniqueViolation" in PARSER, "_INSERT_CONFLICT is not typed"


def test_inserted_counts_rows_that_landed_not_attempts():
    """ON CONFLICT DO NOTHING makes a suppressed row look identical to a
    stored one. fetch_latest uses this number to decide whether to run the
    indicator jobs and reports it as "inserted"."""
    i = PARSER_CODE.index("INSERT OR IGNORE INTO trades")
    tail = PARSER_CODE[i:i + 4000]
    assert "inserted += cur.rowcount" in tail, (
        "the insert counter is back to a blind += 1, so it counts attempts"
    )

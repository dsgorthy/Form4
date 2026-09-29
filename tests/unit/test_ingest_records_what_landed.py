"""`processed_filings.status` must describe what STORED, not what parsed.

WHY, AND WHAT IT COST

The unique index on `trades` is
`(insider_id, ticker, trade_date, trade_type, value)` — NOT accession. So
`INSERT OR IGNORE` suppresses a row WITHOUT RAISING whenever the same economic
trade is already present under a different filing. `insert_trades` returned only
the landed count, the insert-error list stayed empty, and the caller then ran:

    mark_processed(conn, acc, fdate, len(trades))   # the PARSED count

`mark_processed` set `status = "ok" if trade_count > 0`, so a filing that stored
NOTHING was recorded as `ok` with a positive `trade_count`. Ten such accessions
sampled on 2026-09-29 all read `status=ok, trade_count=1..3` with zero rows in
`trades`, and `processed_at` NULL because the INSERT never named that column.

That row is identical whether the rows were already held or genuinely lost, and
the ambiguity produced THREE wrong conclusions in one afternoon while chasing an
apparent 7% ingestion gap. 464 accessions in Silver and absent from `trades` by
accession resolved to:

    325   derivative-only          product is right to skip
    127   grants / exercises       non-derivative, no P or S
     12   real purchases or sales  of which 13 of 14 rows were ALREADY STORED
                                   under a different accession

Genuine loss: one trade, on a malformed ticker. The data was fine. The bookkeeping
was not, and the bookkeeping is what anyone has to trust at 3am.

THE INVARIANT: `ok` means rows landed. Nothing landed is `duplicate` when every
parsed row was already present, and `failed` when it was not — because an
unexplained zero-store is a failure and must be retried.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FETCH = ROOT / "strategies" / "insider_catalog" / "fetch_latest.py"
INSERT = ROOT / "strategies" / "insider_catalog" / "backfill_live.py"


def _code(src: str) -> str:
    """Docstrings and comments out, SQL KEPT.

    Stripping every triple-quoted block with a regex also deletes every SQL
    literal in these modules, because those are triple-quoted too — so
    `processed_at`, `parsed_count` and the whole `status IN (...)` list
    disappeared and three assertions failed against correct code. Same trap hit
    twice in one day (see test_sitemap_submits_what_can_rank).

    `ast` knows which strings are docstrings and which are not. Their lines are
    blanked rather than removed so reported line numbers stay usable.
    """
    tree = ast.parse(src)
    lines = src.splitlines()
    doc_lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef,
                                 ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = getattr(node, "body", None) or []
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            first = body[0]
            doc_lines.update(range(first.lineno, (first.end_lineno or first.lineno) + 1))
    out = []
    for i, ln in enumerate(lines, start=1):
        if i in doc_lines or ln.lstrip().startswith("#"):
            out.append("")
        else:
            out.append(ln)
    return "\n".join(out)


def test_the_insert_reports_duplicates_separately_from_stores():
    code = _code(INSERT.read_text())
    body = code[code.index("def insert_trades("): code.index("def insert_derivative_trades(")]
    assert "duplicates += 1" in body, (
        "insert_trades no longer counts rows suppressed by the unique index, so "
        "stored == 0 is ambiguous again"
    )
    assert re.search(r'"duplicate":\s*duplicates', body), (
        "the outcome dict does not carry the duplicate count"
    )
    assert re.search(r'"stored":\s*inserted', body), (
        "the outcome dict does not carry the stored count"
    )
    # rowcount, never a blind increment.
    assert "cur.rowcount" in body and "inserted += 1" not in body, (
        "the landed count is not taken from rowcount"
    )


def test_status_is_decided_by_what_landed():
    code = _code(FETCH.read_text())
    body = code[code.index("def mark_processed("): code.index("def mark_attempt_failed(")]
    assert 'status = "ok" if trade_count > 0 else "empty"' not in body, (
        "status is back to keying on the PARSED count, which records a filing "
        "that stored nothing as ok"
    )
    assert re.search(r"stored > 0[\s\S]{0,80}\"ok\"", body), (
        "`ok` is no longer conditioned on stored > 0"
    )
    assert '"duplicate"' in body, "no duplicate status"
    assert re.search(r'else:\s*\n\s*status = "failed"', body), (
        "an unexplained zero-store no longer becomes `failed`, so it is retired "
        "instead of retried"
    )


def test_the_recorded_trade_count_is_the_stored_count():
    """`trade_count` is read as 'how many trades this filing gave us'. It has to
    be what landed; the parsed figure gets its own column."""
    code = _code(FETCH.read_text())
    body = code[code.index("def mark_processed("): code.index("def mark_attempt_failed(")]
    m = re.search(r"\(accession, filing_date, ([a-z_]+), ([a-z_]+), ([a-z_]+), status\)", body)
    assert m, "the mark_processed parameter tuple changed shape"
    assert m.group(1) == "stored", (
        f"trade_count is bound to {m.group(1)!r}; it must be the stored count"
    )
    assert "parsed_count" in body and "duplicate_count" in body, (
        "the parsed and duplicate counts are not persisted, so the difference "
        "that distinguishes 'already had it' from 'lost it' is unrecoverable"
    )


def test_processed_at_is_set_on_the_upsert_path():
    """It was NULL on every row this path wrote: a column DEFAULT applies to an
    omitted column on INSERT, not on UPDATE, and the INSERT never named it."""
    code = _code(FETCH.read_text())
    body = code[code.index("def mark_processed("): code.index("def mark_attempt_failed(")]
    assert "processed_at" in body, "mark_processed still never sets processed_at"
    assert "COALESCE(processed_filings.processed_at" in body, (
        "the upsert overwrites processed_at on every retry, losing when the "
        "filing was first read"
    )


def test_duplicate_is_terminal_and_not_re_driven():
    """The trade is already stored. Re-fetching it from EDGAR on every run would
    spend the rate limit to learn nothing."""
    code = _code(FETCH.read_text())
    body = code[code.index("def get_known_accessions("):]
    body = body[: body.index("def ", 10)]
    m = re.search(r"status IN \(([^)]*)\)", body)
    assert m, "the done-status list is gone"
    done = {x.strip().strip("'\"") for x in m.group(1).split(",")}
    assert "duplicate" in done, (
        "`duplicate` is not treated as done, so every already-stored filing is "
        "re-fetched forever"
    )
    assert "failed" not in done, (
        "`failed` became terminal; that is the 2026-08-26 bug, where one bad "
        "download retired a filing permanently"
    )


def test_the_returned_outcome_matches_what_was_recorded():
    """The run summary is built from these outcomes. If they disagree with the
    status written, the log claims filings the table does not."""
    code = _code(FETCH.read_text())
    body = code[code.index("def _process_one("):]
    body = body[: body.index("def _run_fetch_inner(")]
    assert 'return inserted, "duplicate"' in body, (
        "_process_one never returns the duplicate outcome, so a run reports a "
        "store it did not make"
    )
    assert re.search(r'if inserted > 0:\s*\n\s*return inserted, "ok"', body), (
        "the ok outcome is not conditioned on rows having landed"
    )
    assert "stored=inserted" in body, (
        "_process_one does not pass the stored count to mark_processed"
    )

"""Bronze is the layer that makes a refetch unnecessary. Guard it.

An audit on 2026-09-05 noted `grep -rn "bronze" tests/` returned nothing, and
then found five defects in it — three of which are a single assertion away
from being caught here. This file is that assertion set.

THE CORPUS IS SEC'S, NOT OURS. The first fetcher took its work list from
`trades`, which holds ~82% of Form 4 filings (2021Q1: 54,611 of 66,015). The
SEC quarterly index enumerates 4,060,695 accessions against `trades`'
3,198,926 — an 861,769-filing hole that would have made "we never refetch"
false the day the backfill finished.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FETCH = (REPO / "scripts" / "fetch_bronze.py").read_text(encoding="utf-8")
INDEX = (REPO / "scripts" / "fetch_sec_index.py").read_text(encoding="utf-8")
MIG_A = (REPO / "migrations" / "2026-09-05_bronze_edgar_submission.sql").read_text(encoding="utf-8")
MIG_B = (REPO / "migrations" / "2026-09-06_bronze_edgar_index.sql").read_text(encoding="utf-8")


def _code(src: str) -> str:
    return "\n".join(l.split("#", 1)[0] if not l.lstrip().startswith("#") else ""
                     for l in src.split("\n"))


def test_the_work_list_comes_from_the_sec_index():
    q = _code(FETCH)
    assert "bronze.edgar_index" in q, (
        "the fetcher's work list no longer derives from SEC's own index. "
        "Driving it off `trades` inherits an 18% blind spot."
    )
    i = q.index("TODO_SQL")
    stmt = q[i:q.index('"""', q.index('"""', i) + 3)]
    # The DRIVING table must be the index. `trades` may still appear in a
    # correlated subquery — it supplies a fallback CIK, which is a different
    # job from defining the corpus.
    assert re.search(r"FROM\s+bronze\.edgar_index", stmt), (
        "the work list is not driven by the SEC index"
    )
    assert not re.search(r"FROM\s+trades\s+t\s*\n\s*(LEFT\s+)?JOIN", stmt), (
        "the work list is back on `trades`, which is not the corpus"
    )


def test_failures_record_their_real_status():
    """Every transient failure was written as 404 — 'SEC says this does not
    exist' — because a requests ConnectionError stringifies containing 'HTTP',
    so the `0 if "HTTP" not in err else 404` branch was dead code."""
    q = _code(FETCH)
    assert '"HTTP" not in last_err' not in q, (
        "transient failures are being classified by searching the error "
        "message for 'HTTP' again; every timeout becomes a permanent 404"
    )
    assert "last_status" in q, "the real HTTP status is not captured"


def test_failures_are_retryable_from_the_scheduled_job():
    """The work list is 'has no bronze row', so a recorded failure is excluded
    forever unless something clears it."""
    ops = (REPO / "dataplane" / "dagster_project" / "assets" / "form4_ops.py"
           ).read_text(encoding="utf-8")
    assert "--retry-failed" in ops, (
        "the hourly top-up no longer retries failures, so the one gap class "
        "it exists to heal is the one it cannot see"
    )


def test_the_checksum_covers_the_bytes_that_are_stored():
    """sha256 was computed over r.content while r.text was stored. SEC sends
    text/plain with no charset, so requests decodes latin-1 and psycopg2
    re-encodes UTF-8 — any byte >= 0x80 and the checksum stops describing the
    row."""
    q = _code(FETCH)
    assert "hashlib.sha256(r.content)" not in q, (
        "the checksum is back on the response bytes rather than the stored text"
    )
    assert 'body.encode("utf-8")' in q, "the stored bytes are not what is hashed"


def test_only_one_fetcher_runs_at_a_time():
    """SEC's 10 req/s is a budget per client; the token bucket is a module
    global, so two processes are two budgets — 16/s sustained, 23/s peak."""
    q = _code(FETCH)
    assert "pg_try_advisory_lock" in q, (
        "the mutual-exclusion lock is gone; the backfill and the hourly "
        "top-up will double SEC's rate and re-download each other's work"
    )


def test_no_transaction_is_held_across_network_io():
    """Holding the read transaction open across ~65s of fetching left the
    connection 'idle in transaction', which blocked an ALTER on this table."""
    q = _code(FETCH)
    i = q.index("cur.execute(TODO_SQL")
    window = q[i:i + 400]
    assert "conn.commit()" in window, (
        "the work-list read is not committed before fetching begins"
    )


def test_one_bad_row_cannot_end_the_run():
    """The per-row insert must sit inside a try/except.

    Asserted STRUCTURALLY, not by scanning a fixed window after the INSERT:
    the window version broke the moment the upsert grew a DO UPDATE clause,
    which is a brittleness that reads as a regression in the code under test.
    """
    import ast

    tree = ast.parse(FETCH)
    guarded = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        body = ast.unparse(node.body)
        if "INSERT INTO bronze.edgar_submission" not in body:
            continue
        if any(h.type is None or "Exception" in ast.unparse(h.type)
               for h in node.handlers):
            guarded = True
    assert guarded, (
        "the per-row insert has no handler again; a single NUL byte or "
        "constraint violation ends a multi-day unattended job"
    )

def test_the_check_constraint_actually_constrains():
    """The first version admitted http_status=200 with an empty string, a NULL
    byte_len, or a byte_len disagreeing with the content — which IS the
    truncated body it was documented to catch."""
    assert "byte_len = octet_length(content)" in MIG_B, (
        "the constraint no longer cross-checks the stored length"
    )
    assert "last_error IS NOT NULL" in MIG_B, (
        "a failure row can again be stored with no explanation"
    )


def test_the_index_parser_accepts_both_date_formats():
    """The quarterly form.idx hyphenates the date (2021-01-05); the daily
    form.YYYYMMDD.idx does not (20210105). Reusing the daily guard rejected
    every row in all 83 quarters and reported success."""
    assert re.search(r"len\(filed\) == 8", INDEX), (
        "the index parser no longer handles the daily 8-char date form"
    )
    assert re.search(r"\\d\{4\}-\\d\{2\}-\\d\{2\}", INDEX), (
        "the index parser no longer handles the quarterly hyphenated date"
    )


def test_the_index_parser_matches_the_form_column_not_a_substring():
    """`type=4` style matching pulls in 424B2 and 40-F. Learned twice already:
    once in the daily-index parser, once in the live-queue reader.

    Asserts the PROPERTY -- the form token is extracted and compared for
    equality against an allowed set -- not one spelling of it. The first
    version pinned `startswith("4 ")`, which went red the moment Form 5 was
    added to the corpus even though the anti-substring property was preserved.
    """
    assert "INDEX_FORMS" in INDEX, "the allowed form set is gone"
    assert re.search(r"form\s*=\s*line\.split", INDEX), (
        "the form type is no longer extracted as its own token"
    )
    assert re.search(r"form\s+not\s+in\s+INDEX_FORMS", INDEX), (
        "the form token is no longer compared for EQUALITY against the "
        "allowed set -- a substring or prefix test admits 424B2 and 40-F"
    )
    m = re.search(r"INDEX_FORMS\s*=\s*\(([^)]*)\)", INDEX)
    assert m and "3" not in m.group(1).replace("'", "").split(","), (
        "Form 3 is in the corpus; it reports no transactions"
    )


# ── a failure is an attempt, not a verdict (2026-09-08) ────────────────────
#
# The work list was "no bronze row", so the FIRST non-200 written for an
# accession removed it from the corpus permanently. At 29.7% through the
# backfill that had already poisoned 575 filings, 522 of them HTTP 404 — and
# six sampled 404s ALL RETURN 200 on a re-fetch to the same URL with the CIK
# the index already holds. They are SEC hiccups under sustained load, not
# missing documents, running at a steady ~0.1% of each hour: about 2,000
# filings over the full 4.1M corpus, in the one layer whose whole purpose is
# that we never have to refetch.

def test_the_work_list_re_offers_failures():
    code = _code(FETCH)
    todo = code[code.index("TODO_SQL"):]
    todo = todo[:todo.index('"""', todo.index('"""') + 3)]
    assert "http_status <> 200" in todo, (
        "the work list only asks for accessions with NO bronze row, so a "
        "transient 404 is a permanent hole. It must also re-offer failures."
    )
    assert "attempts <" in todo, (
        "retries must be bounded, or a genuinely absent document is fetched "
        "forever on every pass"
    )
    assert "INTERVAL" in todo and "attempts *" in todo, (
        "retries need a widening backoff — hammering a throttled window with "
        "the process that provoked it just re-manufactures the failure"
    )


def test_fresh_work_is_not_starved_by_retries():
    code = _code(FETCH)
    todo = code[code.index("TODO_SQL"):]
    todo = todo[:todo.index('"""', todo.index('"""') + 3)]
    order = todo[todo.index("ORDER BY"):]
    assert "b.accession IS NOT NULL" in order, (
        "retries must sort AFTER unfetched accessions. Otherwise a batch can "
        "fill with the same failing rows and the forward scan stalls."
    )


def test_a_retry_overwrites_the_stale_failure():
    code = _code(FETCH)
    assert "ON CONFLICT (accession) DO NOTHING" not in code, (
        "DO NOTHING discards the retry: the row stays a failure forever even "
        "when the re-fetch succeeded"
    )
    assert "DO UPDATE" in code
    assert re.search(r"attempts\s*=\s*bronze\.edgar_submission\.attempts \+ 1", code), (
        "attempts must increment, or the bound in the work list never bites"
    )


def test_a_retry_can_only_move_a_row_forward():
    """A failed pass must never overwrite content already stored."""
    code = _code(FETCH)
    upsert = code[code.index("ON CONFLICT"):]
    upsert = upsert[:upsert.index('"""')]
    assert "http_status <> 200" in upsert, (
        "the DO UPDATE needs a WHERE guard, or a concurrent failing pass can "
        "turn a good row back into a hole"
    )


def test_retry_bounds_are_sane():
    ns = {}
    for line in _code(FETCH).split("\n"):
        if re.match(r"^(MAX_ATTEMPTS|BACKOFF_MIN)\s*=", line):
            exec(line, ns)
    assert 2 <= ns.get("MAX_ATTEMPTS", 0) <= 10, "MAX_ATTEMPTS missing or absurd"
    assert ns.get("BACKOFF_MIN", 0) >= 1, "BACKOFF_MIN missing or zero"


def test_a_failed_row_records_what_it_tried():
    """Every diagnosis of the 404s started by guessing the URL, because the
    failure path stored None for both cik_used and source_url."""
    code = _code(FETCH)
    fail = code[code.rindex("return {\"accession\": accession"):]
    fail = fail[:fail.index("}") + 1]
    assert '"cik_used": None' not in fail and '"source_url": None' not in fail, (
        "the failure record must name the URL it tried"
    )
    assert "submission_url" in fail

"""verify_bronze's coverage check must fail on a hole, and must name it.

The module docstring of scripts/verify_bronze.py lists five properties. The
second reads: "Every accession in the index has a submission row. Names what
is missing rather than reporting a percentage." Until 2026-09-11 the code
selected corpus, stored, failed and orphaned, logged a percentage, and
reported ok whenever failed and orphaned were both zero -- it never subtracted
stored from corpus. Run against the backfill at 55.3% it printed the
percentage and passed. Run against a "complete" archive with holes it would
have passed identically, and those holes would have stayed invisible for as
long as the script was trusted. The archive exists so that a filing can never
go missing; its verifier could not see one go missing.

These pin the decision function, which is pure so it needs no database, and
the shape of the query behind it.
"""
import importlib.util
import re
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "verify_bronze.py"


@pytest.fixture(scope="module")
def vb():
    spec = importlib.util.spec_from_file_location("verify_bronze", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _row(**over):
    r = {"corpus": 100, "stored": 100, "failed": 0, "orphaned": 0, "missing": 0,
         "missing_sample": []}
    r.update(over)
    return r


def _levels(verdicts):
    return {check: level for level, check, _ in verdicts}


def test_a_hole_fails_and_is_named(vb):
    """The regression. 10 index accessions with no row: fail, and say which."""
    v = vb.coverage_verdicts(_row(stored=90, missing=10,
                                  missing_sample=["0001234567-26-000001"]), False)
    levels = _levels(v)
    assert levels.get("coverage/missing") == "fail"
    assert "coverage" not in levels, "an archive with holes must never be ok"
    detail = next(d for lvl, c, d in v if c == "coverage/missing")
    assert "10" in detail and "0001234567-26-000001" in detail


def test_expect_partial_reports_without_failing_and_without_ok(vb):
    """During the backfill the hole is expected. Report it; do not fail; and
    do not pretend it is fine either -- no ok while anything is missing."""
    v = vb.coverage_verdicts(_row(stored=55, missing=45), True)
    levels = _levels(v)
    assert "fail" not in levels.values()
    assert "coverage" not in levels, "--expect-partial must not produce an ok"
    assert levels.get("coverage/partial") == "note"


def test_full_coverage_is_ok(vb):
    v = vb.coverage_verdicts(_row(), False)
    levels = _levels(v)
    assert levels.get("coverage") == "ok"
    assert "fail" not in levels.values()


@pytest.mark.parametrize("expect_partial", [False, True])
def test_recorded_failures_still_fail_even_while_partial(vb, expect_partial):
    """--expect-partial tolerates NOT-YET-FETCHED. It does not tolerate a row
    that records a non-200: that fetch happened and went wrong."""
    v = vb.coverage_verdicts(_row(stored=97, failed=3), expect_partial)
    assert _levels(v).get("coverage/failed") == "fail"


def test_orphans_still_fail(vb):
    v = vb.coverage_verdicts(_row(orphaned=2), False)
    assert _levels(v).get("coverage/orphaned") == "fail"


def test_the_query_counts_missing_in_the_index_to_submission_direction(vb):
    """`orphaned` already existed and points the other way (submission rows
    absent from the index). `missing` must be the index rows absent from
    submissions -- the direction that catches a filing that was never fetched."""
    src = SCRIPT.read_text()
    body = src[src.index("def check_coverage"):src.index("def check_integrity")]
    assert re.search(
        r"FROM bronze\.edgar_index i\s+WHERE NOT EXISTS \(SELECT 1 FROM bronze\.edgar_submission s",
        body,
    ), "the missing count is not computed index -> submission"
    assert "AS missing" in body
    assert "coverage_verdicts(r, expect_partial)" in body, \
        "check_coverage no longer routes through the pure decision function"


def test_main_exposes_expect_partial(vb):
    src = SCRIPT.read_text()
    assert '"--expect-partial"' in src
    assert "check_coverage, conn, args.expect_partial" in src

"""The ways data went wrong quietly, each now gated on and each pinned here.

Three separate silences were found on 2026-09-29, and none of them was a broken
pipeline. Every one was a check that could not fail, or failed where nobody read
it:

1. **Nothing gated on the product missing filings.** `record_parity.py` gates on
   `recall = matched / distinct_b` — "does the PLANE have what the PRODUCT has" —
   which is ~100% every day by construction. The reverse figure was written to
   `coverage_a` and gated on by nothing, so a real gap could have sat in a column
   indefinitely. `check_ingest_completeness.py` now gates the other direction and
   EXITS NON-ZERO, which the Dagster failure sensor turns into a page.

2. **`refresh_ticker_metadata` could not finish, and said so where nobody
   looked.** Weekly schedule against a 7-DAY staleness window meant every ticker
   fell stale exactly as the job ran: 9,619 of 18,269 on 2026-09-29, at ~1s per
   yfinance call, against a 7,200s subprocess timeout. Killed mid-run on 09-20
   and 09-27, its last two attempts. At a 90-day window exactly THREE were stale,
   with 18,266 of 18,269 already covered — the weekly churn bought nothing and
   cost the job its ability to complete.

3. **The Monday monitor failed every Monday on a false premise.**
   `qm_scan_today` asserted `trade_decision_audit` must be non-empty, reasoning
   that a scan always leaves audit rows. It does not: the thesis filters in SQL,
   so with no qualifying candidate there is nothing to audit. It failed 09-14,
   09-21 and 09-28 while all three runners completed 77 cycles a day with status
   `ok`, and while 0 of 13 discretionary buys that day carried an A+/A/B grade.
   A monitor that cries on quiet days teaches people to ignore it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPLETENESS = ROOT / "scripts" / "check_ingest_completeness.py"
TICKERS = ROOT / "scripts" / "refresh_ticker_metadata.py"
MONDAY = ROOT / "scripts" / "monday_paper_monitor.py"
OPS = ROOT / "dataplane" / "dagster_project" / "assets" / "form4_ops.py"
PROBE = ROOT / "scripts" / "heartbeat_probe.py"


def _code(src: str) -> str:
    """Docstrings and comments out, SQL kept. A regex over triple quotes would
    delete the SQL too, which is where most of these assertions live."""
    tree = ast.parse(src)
    doc_lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef,
                                 ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = getattr(node, "body", None) or []
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            f = body[0]
            doc_lines.update(range(f.lineno, (f.end_lineno or f.lineno) + 1))
    return "\n".join(
        "" if (i in doc_lines or ln.lstrip().startswith("#")) else ln
        for i, ln in enumerate(src.splitlines(), start=1)
    )


# ── 1. The reverse-direction gate ────────────────────────────────────────────

def test_the_completeness_check_exits_non_zero_so_it_can_page():
    code = _code(COMPLETENESS.read_text())
    assert "return 1" in code, (
        "the completeness check no longer exits non-zero, so a real gap is a log "
        "line instead of a page"
    )
    assert "raise SystemExit(main())" in code, "the exit code is not propagated"


def test_it_only_counts_decisions_not_everything_silver_parsed():
    """Derivative rows and comp grants are skipped BY DESIGN. Counting them makes
    the alarm permanently red, which is how an alarm gets muted."""
    code = _code(COMPLETENESS.read_text())
    assert "NOT s.is_derivative" in code, (
        "derivative rows are back in the population; the product deliberately "
        "excludes them and they were 325 of the 464 apparent gaps"
    )
    assert "s.trans_code IN ('P', 'S')" in code, (
        "the population is no longer restricted to purchases and sales"
    )


def test_it_matches_on_trade_identity_not_accession():
    """The unique index on `trades` is
    (insider_id, ticker, trade_date, trade_type, value) — NOT accession — so the
    same trade legitimately arrives under a different accession. An
    accession-keyed test calls that missing, which is what produced three wrong
    conclusions in one afternoon."""
    code = _code(COMPLETENESS.read_text())
    where = code[code.index("WHERE NOT EXISTS"):]
    assert "t.accession" not in where, (
        "the existence test keys on accession again"
    )
    assert "t.ticker = d.ticker" in where and "t.trade_date" in where
    assert "t.trade_type = CASE d.trans_code" in where, (
        "matching is back on the exact trans_code; 10 of 1,534 Silver `P` rows "
        "are held as `A` with the same ticker, date and quantity, so that "
        "produces false losses on trades we do have"
    )


def test_the_threshold_sits_above_the_measured_baseline():
    """A threshold equal to the normal reading flaps, and a flapping alarm is a
    muted alarm. Baseline was 5 over 7 days on 2026-09-29."""
    src = TICKERS  # noqa: F841  (kept explicit below for readability)
    m = re.search(r"DEFAULT_MAX_MISSING = (\d+)", COMPLETENESS.read_text())
    assert m, "DEFAULT_MAX_MISSING is gone"
    assert int(m.group(1)) > 5, (
        f"threshold {m.group(1)} is at or below the measured steady state of 5; "
        "it will flap"
    )
    assert int(m.group(1)) <= 20, (
        f"threshold {m.group(1)} is loose enough to hide a fourfold regression"
    )


def test_the_gate_is_scheduled_and_the_old_one_is_documented():
    ops = _code(OPS.read_text())
    assert "check_ingest_completeness.py" in ops, (
        "the completeness gate is not wired into Dagster, so nothing runs it"
    )
    assert re.search(r'_sched\("ops_ingest_completeness_daily",\s*\[ops_ingest_completeness\]',
                     ops), "the completeness gate has no schedule"
    parity = (ROOT / "scripts" / "record_parity.py").read_text()
    assert "check_ingest_completeness" in parity, (
        "record_parity does not point at the check that covers the direction it "
        "deliberately ignores, so the next person re-derives the confusion"
    )


# ── 2. The job that could not finish ────────────────────────────────────────

def test_ticker_metadata_staleness_is_wider_than_its_schedule():
    """A staleness window equal to the run interval means everything is always
    stale, and the full set does not fit in the timeout."""
    src = TICKERS.read_text()
    m = re.search(r"STALENESS_DAYS = (\d+)", src)
    assert m, "STALENESS_DAYS is gone"
    days = int(m.group(1))
    assert days >= 30, (
        f"STALENESS_DAYS={days} against a WEEKLY schedule: every ticker falls "
        "stale exactly as the job runs, which is why it was killed at the 7,200s "
        "timeout on both 09-20 and 09-27"
    )


def test_ticker_metadata_stops_on_its_own_terms():
    code = _code(TICKERS.read_text())
    # The COMPARISON, not the word. Replacing the condition with `if False:`
    # left both "max_seconds" and "stopped_early" in the file and this assertion
    # passed against a disabled budget. Mutation testing caught it.
    assert re.search(r"if args\.max_seconds and \(time\.monotonic\(\) - t0\) > args\.max_seconds",
                     code), (
        "the wall-clock budget is not compared against elapsed time inside the "
        "loop, so the job is still killed by the subprocess timeout"
    )
    assert re.search(r"stopped_early = len\(tickers\) - i \+ 1", code), (
        "a budget hit does not record how many tickers it deferred"
    )
    assert "stopped_early" in code and "deferred_to_next_run" in code, (
        "a partial run does not report how much it deferred, so a budget hit "
        "looks identical to a complete run"
    )
    m = re.search(r"DEFAULT_MAX_SECONDS = (\d+)", TICKERS.read_text())
    assert m and int(m.group(1)) < 7200, (
        "the budget is not below the 7,200s subprocess timeout, so the timeout "
        "still wins and the process is still killed"
    )


# ── 3. The monitor that cried on quiet days ─────────────────────────────────

def test_qm_scan_checks_that_the_loop_turned_not_that_it_decided():
    code = _code(MONDAY.read_text())
    body = code[code.index("def check_qm_scan_today("):]
    body = body[: body.index("CHECKS = [")]
    assert "pipeline_runs" in body, (
        "the check no longer looks at completed runner cycles, which is the only "
        "real liveness evidence; it is back to inferring from the audit table"
    )
    # The SERVICE NAME, not just the table. Pointing the query at a service that
    # does not exist leaves "pipeline_runs" in the file and always reports zero
    # runs -- a permanently critical check. Mutation testing caught it.
    assert '"cw_runner_quality_momentum"' in body, (
        "the run query does not name the quality_momentum runner service, so it "
        "counts cycles of nothing"
    )
    assert "ok_runs == 0" in body, (
        "zero completed cycles is no longer the failing condition"
    )


def test_a_quiet_day_passes_and_an_unexplained_empty_table_does_not():
    code = _code(MONDAY.read_text())
    body = code[code.index("def check_qm_scan_today("):]
    body = body[: body.index("CHECKS = [")]
    # Quiet: cycles ran, nothing qualified -> PASS. Sliced to the branch, not a
    # 400-char window: the window reached past the mutated branch into the final
    # `ok=True` return, so flipping this branch to ok=False still passed.
    at = re.search(r"^    if n_audit == 0:$", body, re.M)
    assert at, "the quiet-day branch is gone"
    branch = body[at.end(): at.end() + 320]
    assert "ok=True" in branch and "ok=False" not in branch, (
        "an empty audit table with completed cycles and no graded candidates no "
        "longer passes; the monitor will fail on every quiet day again"
    )
    # Suspicious: cycles ran, something qualified, still nothing audited -> FAIL.
    assert "n_graded > 0" in body, (
        "the check no longer asks whether there was anything to decide, so it "
        "cannot tell a quiet day from a silently broken scan"
    )
    assert re.search(r"n_audit == 0 and n_graded > 0[\s\S]{0,300}ok=False", body), (
        "a graded candidate with no audit row does not fail"
    )


# ── 4. The alarm that fired on correct behaviour ────────────────────────────

def test_heartbeat_staleness_is_derived_from_the_runner_schedule():
    """SIX CRITICALS A DAY, EVERY WEEKDAY, for a system behaving as scheduled.

    The runners are Dagster one-shots on `*/10 6-13 * * 1-5` PACIFIC, so the
    last fire is 13:50 PT and nothing is due until 06:00 PT next weekday. A flat
    90-minute off-hours threshold therefore paged every weekday at 22:30 ET with
    "age=100m threshold=90m", and again next morning with "recovered" — which was
    ALSO logged at critical severity. Measured 2026-09-29: 7 criticals a day
    since 09-21, which tripped the Monday monitor's `unexpected_criticals` check,
    which is why that monitor had failed three Mondays running. The real signals
    were buried under announcements of things being fine.
    """
    code = _code(PROBE.read_text())
    assert "def _runner_is_due_now" in code, (
        "the probe no longer knows when a heartbeat is DUE, so it is back to a "
        "flat duration"
    )
    assert "RUNNER_SCHEDULE_END_ET" in code, "the schedule window is gone"
    # The window has to be consulted by the freshness verdict, not merely defined.
    assert re.search(r"fresh = \(age is not None and age <= threshold\) or not due",
                     code), (
        "the age verdict ignores the schedule window, so an expected overnight "
        "gap is reported as staleness again"
    )


def test_a_recovery_is_not_logged_as_critical():
    code = _code(PROBE.read_text())
    at = code.index("heartbeat recovered")
    before = code[max(0, at - 260): at]
    assert "alert.info(" in before, (
        "a heartbeat recovery is logged at critical severity again; half the "
        "six-a-day were announcements that things were FINE"
    )
    assert "alert.critical(" not in before.split("alert.info(")[-1]


def test_the_monday_monitor_shares_the_one_window():
    """A second copy of the schedule window drifts from the probe's."""
    code = _code(MONDAY.read_text())
    assert "from heartbeat_probe import _runner_is_due_now" in code, (
        "the Monday monitor defines its own staleness window instead of "
        "importing the one the probe uses"
    )
    assert re.search(r"age_min > HEARTBEAT_MAX_AGE_MIN and _runner_is_due_now\(\)",
                     code), (
        "the Monday monitor's heartbeat check is back to a flat threshold, so it "
        "reports every runner stale each afternoon and all weekend"
    )

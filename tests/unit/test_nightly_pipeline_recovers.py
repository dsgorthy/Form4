"""A transient failure in the nightly chain must not cost the next trading day.

form4_pipeline runs at 17:30 PT on weekdays only. It failed on Friday 2026-11-06
(backfill_returns.py ran past its 3600 s budget) and on Friday 2026-11-13
(compute_career_grades.py died on DeadlockDetected against the ingestion job's
own compute_cw_indicators). Nothing retried either run, so the features stayed
stale until Monday evening and the runners' preflight halted all three books
for every scan on Monday 11-09 and Monday 11-16.

Two properties are pinned here:

1. The four nightly assets retry a failed step. The digest does not, because
   retrying a partial send emails the same people twice.
2. A timeout stops the whole process tree. subprocess.run(timeout=) kills only
   the direct child, so the python step under /bin/bash kept writing to trades
   beside the retry. That is a second writer, which is how the deadlock happens.

Dagster is not installed in the test venv, so the asset module is read as
source. proc_tree.py is stdlib only and is executed for real.
"""
from __future__ import annotations

import ast
import importlib.util
import os
import re
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PIPELINE = REPO / "dataplane/dagster_project/assets/form4_pipeline.py"
PROC_TREE = REPO / "dataplane/dagster_project/assets/proc_tree.py"
NIGHTLY = ("form4_daily_prices", "form4_trade_returns", "form4_features", "form4_signals")


def _load_proc_tree():
    spec = importlib.util.spec_from_file_location("proc_tree_under_test", PROC_TREE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _asset_decorators() -> dict[str, ast.Call]:
    tree = ast.parse(PIPELINE.read_text())
    out = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "asset":
                    out[node.name] = dec
    return out


def _kw(call: ast.Call, name: str):
    return next((k.value for k in call.keywords if k.arg == name), None)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # A zombie still answers kill(0); it is not running, it is waiting to be reaped.
    try:
        import subprocess
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                               capture_output=True, text=True).stdout.strip()
        return bool(state) and not state.startswith("Z")
    except Exception:
        return True


# --- 1. retries -------------------------------------------------------------

def test_every_nightly_asset_retries_a_failed_step():
    decs = _asset_decorators()
    for name in NIGHTLY:
        assert name in decs, f"{name} is no longer an @asset in form4_pipeline.py"
        policy = _kw(decs[name], "retry_policy")
        assert isinstance(policy, ast.Name) and policy.id == "NIGHTLY_RETRY", (
            f"{name} has no retry_policy=NIGHTLY_RETRY: one transient failure on a "
            "Friday halts every book for the whole of Monday"
        )


def test_the_retry_budget_is_real():
    src = PIPELINE.read_text()
    m = re.search(r"NIGHTLY_RETRY\s*=\s*RetryPolicy\(\s*max_retries\s*=\s*(\d+)\s*,\s*delay\s*=\s*(\d+)", src)
    assert m, "NIGHTLY_RETRY must be a RetryPolicy(max_retries=..., delay=...)"
    retries, delay = int(m.group(1)), int(m.group(2))
    assert retries >= 1
    # A deadlock retry needs the ingestion job's indicator run to be finished
    # first; an immediate retry would usually meet it again.
    assert delay >= 60


def test_the_digest_does_not_retry():
    policy = _kw(_asset_decorators()["form4_notification_digest"], "retry_policy")
    assert policy is None, "a retried digest emails the people it already reached a second time"


def test_the_weekly_crawl_does_not_retry():
    # A two-hour yfinance crawl against a rate-limited endpoint; a retry doubles it.
    assert _kw(_asset_decorators()["form4_ticker_metadata"], "retry_policy") is None


# --- 2. a timeout stops the tree ---------------------------------------------

def test_the_runner_goes_through_run_tree():
    src = PIPELINE.read_text()
    fn = src.split("def _run(", 1)[1].split("\ndef ", 1)[0]
    code = "\n".join(ln for ln in fn.splitlines() if not ln.lstrip().startswith("#"))
    assert "run_tree(" in code
    assert "subprocess.run(" not in code, "subprocess.run(timeout=) kills bash and orphans its python step"


def test_a_timeout_kills_the_grandchild_too(tmp_path):
    pt = _load_proc_tree()
    pidfile = tmp_path / "grandchild.pid"
    # bash starts a long-running grandchild and waits on it, the shape of
    # refresh_features_daily.sh running one python step after another.
    script = f"sleep 120 & echo $! > {pidfile}; wait"
    t0 = time.monotonic()
    with pytest.raises(pt.TreeTimeout):
        pt.run_tree(["/bin/bash", "-c", script], timeout=1, grace=2)
    assert time.monotonic() - t0 < 15
    grandchild = int(pidfile.read_text().strip())
    deadline = time.monotonic() + 5
    while _alive(grandchild) and time.monotonic() < deadline:
        time.sleep(0.1)
    alive = _alive(grandchild)
    if alive:
        os.kill(grandchild, 9)  # do not leave it behind if the assertion fails
    assert not alive, "the grandchild survived the timeout and would race the retry"


def test_a_grandchild_that_ignores_sigterm_is_still_stopped(tmp_path):
    pt = _load_proc_tree()
    pidfile = tmp_path / "stubborn.pid"
    script = (f"{sys.executable} -c \"import os, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
              f"open('{pidfile}', 'w').write(str(os.getpid())); time.sleep(120)\" & wait")
    with pytest.raises(pt.TreeTimeout):
        pt.run_tree(["/bin/bash", "-c", script], timeout=2, grace=1)
    deadline = time.monotonic() + 5
    while not pidfile.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    pid = int(pidfile.read_text().strip())
    deadline = time.monotonic() + 5
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    alive = _alive(pid)
    if alive:
        os.kill(pid, 9)
    assert not alive, "SIGTERM was ignored and nothing followed it with SIGKILL"


def test_a_normal_command_behaves_like_subprocess_run():
    pt = _load_proc_tree()
    ok = pt.run_tree(["/bin/bash", "-c", "echo out; echo err >&2; exit 0"], timeout=10)
    assert (ok.returncode, ok.stdout, ok.stderr) == (0, "out\n", "err\n")
    bad = pt.run_tree(["/bin/bash", "-c", "exit 3"], timeout=10)
    assert bad.returncode == 3


def test_the_timeout_keeps_the_output_for_the_error_message():
    pt = _load_proc_tree()
    with pytest.raises(pt.TreeTimeout) as exc:
        pt.run_tree(["/bin/bash", "-c", "echo started; sleep 30"], timeout=1, grace=1)
    assert "started" in exc.value.stdout
    assert "timed out after 1s" in str(exc.value)

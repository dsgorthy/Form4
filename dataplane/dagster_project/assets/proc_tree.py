"""Run a command in its own process group, and stop the whole group when it overruns.

subprocess.run(timeout=) kills only the direct child. When that child is
/bin/bash running refresh_features_daily.sh, bash dies and the python step it
was running carries on, still writing to trades, while Dagster records the
step as failed. Once the step retries, that orphan and the fresh chain update
the same rows side by side, which is the deadlock that killed the nightly
features step on 2026-11-13. A timeout or an interrupt must take the tree down
with it.

Standard library only, so the tests can load this file without Dagster.
"""
import os
import signal
import subprocess


class TreeTimeout(Exception):
    """The command overran its budget and its whole process group was stopped."""

    def __init__(self, args, timeout, stdout, stderr):
        super().__init__(f"{args[0]} timed out after {timeout}s; stopped its whole process group")
        self.cmd = list(args)
        self.timeout = timeout
        self.stdout = stdout or ""
        self.stderr = stderr or ""


def stop_group(proc: subprocess.Popen, grace: float) -> None:
    """SIGTERM the group, wait up to `grace` seconds for its leader, then SIGKILL what is left.

    The group id is the leader's pid because the command was started with
    start_new_session=True. Grandchildren keep the group alive after the
    leader exits, so the SIGKILL goes to the group either way.
    """
    pgid = proc.pid
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run_tree(args, *, cwd=None, env=None, timeout, grace: float = 30.0) -> subprocess.CompletedProcess:
    """Like subprocess.run(capture_output=True, text=True), but a timeout stops every descendant."""
    proc = subprocess.Popen(args, cwd=cwd, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        stop_group(proc, grace)
        try:
            out, err = proc.communicate(timeout=grace)
        except subprocess.TimeoutExpired:
            out, err = "", ""
        raise TreeTimeout(args, timeout, out, err) from None
    except BaseException:
        # Run terminated from the UI, or the worker interrupted: the tree goes too.
        stop_group(proc, 5.0)
        raise
    return subprocess.CompletedProcess(args, proc.returncode, out, err)

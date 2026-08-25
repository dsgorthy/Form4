"""A job that cannot identify its users must not report success.

WHAT HAPPENED

`api/config.py` loads `api/.env` first and the repo-root `.env` second with
override=False, so `api/.env` wins. On Studio that file is a leftover from
March holding a complete set of `sk_test_` Clerk and Stripe keys -- all seven
of which also exist in the root `.env` with live values.

notification_scanner imports `api.email` -> `api.config`, so it resolved
production user IDs against Clerk's TEST instance. Every lookup returned 404.
`_get_user_email` returns None on 404, the send is skipped, the notification
stays unemailed, and nothing is logged above INFO.

Result: 6,887 notifications for 4 email-enabled users, ZERO ever delivered.
It read as normal because a 404 is exactly what a deleted account looks like
-- the scanner even caches such users as _TIER_GONE, so live subscribers were
being written off as departed.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines/notification_scanner.py"


def test_a_test_key_stops_the_run():
    import pipelines.notification_scanner as NS
    original = NS.CLERK_SECRET_KEY
    try:
        NS.CLERK_SECRET_KEY = "sk_test_" + "x" * 40
        with pytest.raises(SystemExit) as e:
            NS.assert_production_credentials()
        assert "TEST key" in str(e.value)
    finally:
        NS.CLERK_SECRET_KEY = original


def test_an_absent_key_stops_the_run():
    import pipelines.notification_scanner as NS
    original = NS.CLERK_SECRET_KEY
    try:
        NS.CLERK_SECRET_KEY = ""
        with pytest.raises(SystemExit):
            NS.assert_production_credentials()
    finally:
        NS.CLERK_SECRET_KEY = original


def test_a_live_key_runs():
    import pipelines.notification_scanner as NS
    original = NS.CLERK_SECRET_KEY
    try:
        NS.CLERK_SECRET_KEY = "sk_live_" + "x" * 40
        NS.assert_production_credentials()      # must not raise
    finally:
        NS.CLERK_SECRET_KEY = original


def test_the_escape_hatch_is_explicit(monkeypatch):
    """Running against test keys has to be a deliberate act, not a default."""
    import pipelines.notification_scanner as NS
    original = NS.CLERK_SECRET_KEY
    try:
        NS.CLERK_SECRET_KEY = "sk_test_" + "x" * 40
        monkeypatch.setenv("FORM4_ALLOW_TEST_CREDENTIALS", "1")
        NS.assert_production_credentials()      # must not raise
    finally:
        NS.CLERK_SECRET_KEY = original


def test_the_guard_runs_before_any_work():
    """After init_db() or after the scan, it would still have shipped the
    silent failure for that run."""
    tree = ast.parse(SCANNER.read_text())
    main = next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == "main")
    calls = [ast.unparse(st.value) for st in main.body
             if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call)]
    # Argparse may precede it -- `--help` should not need credentials. What
    # must not precede it is any real work.
    work = [c for c in calls if not c.startswith(("parser.", "ap."))]
    assert work, "main() has no top-level work calls"
    assert work[0] == "assert_production_credentials()", (
        f"credential check is not the first real thing main() does: {work[:3]}")

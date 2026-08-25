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


@pytest.fixture
def NS():
    import pipelines.notification_scanner as mod
    original = mod.CLERK_SECRET_KEY
    yield mod
    mod.CLERK_SECRET_KEY = original
    mod._MAIL_BLOCKED = None


def test_a_test_key_blocks_mail(NS):
    NS.CLERK_SECRET_KEY = "sk_test_" + "x" * 40
    reason = NS.check_production_credentials()
    assert reason and "TEST key" in reason


def test_an_absent_key_blocks_mail(NS):
    NS.CLERK_SECRET_KEY = ""
    assert NS.check_production_credentials()


def test_a_live_key_allows_mail(NS):
    NS.CLERK_SECRET_KEY = "sk_live_" + "x" * 40
    assert NS.check_production_credentials() is None


def test_the_escape_hatch_is_explicit(NS, monkeypatch):
    """Running against test keys has to be a deliberate act, not a default."""
    NS.CLERK_SECRET_KEY = "sk_test_" + "x" * 40
    monkeypatch.setenv("FORM4_ALLOW_TEST_CREDENTIALS", "1")
    assert NS.check_production_credentials() is None


def test_bad_credentials_do_not_stop_the_scan(NS):
    """In-app notifications never touch Clerk and currently work.

    The first version of this guard raised SystemExit before init_db(), which
    would have stopped notifications being CREATED at all -- breaking a
    working feature to report a broken one.
    """
    NS._MAIL_BLOCKED = "test key"
    sent = []
    NS._maybe_send_realtime_email = lambda *a, **k: sent.append(a)
    NS._try_send_realtime(None, {"user_id": "u"}, "t", "b")
    assert sent == [], "email was attempted while mail is blocked"


def test_both_send_paths_are_gated():
    src = SCANNER.read_text()
    for fn in ("_try_send_realtime", "send_daily_digests"):
        i = src.index(f"def {fn}(")
        body = src[i:src.index("\ndef ", i + 1)]
        assert "_MAIL_BLOCKED" in body, f"{fn} does not check _MAIL_BLOCKED"


def test_the_run_exits_non_zero_when_mail_is_blocked():
    """An ERROR line alone is what this failure already survived for months."""
    tree = ast.parse(SCANNER.read_text())
    main = next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == "main")
    exits = [n for n in ast.walk(main)
             if isinstance(n, ast.If) and "_MAIL_BLOCKED" in ast.unparse(n.test)
             and "sys.exit(1)" in ast.unparse(n)]
    assert len(exits) >= 2, (
        f"expected both the digest and the scan path to exit non-zero, "
        f"found {len(exits)}")


def test_the_guard_runs_before_any_work():
    """After init_db() or after the scan, it would still have shipped the
    silent failure for that run."""
    tree = ast.parse(SCANNER.read_text())
    main = next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == "main")
    calls = [ast.unparse(st.value) if isinstance(st, ast.Expr)
             else ast.unparse(st.value)
             for st in main.body
             if (isinstance(st, ast.Expr) and isinstance(st.value, ast.Call))
             or (isinstance(st, ast.Assign) and isinstance(st.value, ast.Call))]
    # Argparse may precede it -- `--help` should not need credentials. What
    # must not precede it is any real work.
    work = [c for c in calls
            if not c.startswith(("parser.", "ap.", "argparse."))]
    assert work, "main() has no top-level work calls"
    assert work[0].startswith("check_production_credentials"), (
        f"credential check is not the first real thing main() does: {work[:3]}")

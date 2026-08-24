"""Realtime email must work, and a failure must not abort the scan.

TWO BUGS, ONE CALL SITE.

_maybe_send_realtime_email referenced `_email_cache` three times. The name was
never defined anywhere in the module, so EVERY realtime delivery raised
NameError. Exactly one account on Studio has email_frequency='realtime' with
email enabled.

And the call sites were unguarded, so that NameError propagated out of
scan_portfolio_alerts and killed the whole cycle — one subscriber's delivery
problem silencing everyone else's alerts. The notification row is already
committed at that point; the email is a best-effort second channel and must be
treated as one.

I reported this path as working earlier in the day. It imports and it is
called; it just cannot succeed.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

import pipelines.notification_scanner as ns

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines/notification_scanner.py"


def test_the_email_cache_exists():
    assert hasattr(ns, "_email_cache"), (
        "_email_cache is undefined again — every realtime delivery raises "
        "NameError"
    )
    assert isinstance(ns._email_cache, dict)


def test_every_name_the_delivery_path_reads_is_defined():
    """Generalises the bug: any free variable in the realtime path must
    resolve at module level."""
    import inspect
    tree = ast.parse(inspect.getsource(ns._maybe_send_realtime_email))
    fn = tree.body[0]
    assigned = {n.id for n in ast.walk(fn)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
    args = {a.arg for a in fn.args.args}
    read = {n.id for n in ast.walk(fn)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    unresolved = {
        n for n in read - assigned - args
        if not hasattr(ns, n) and n not in dir(__builtins__) + dir(__import__("builtins"))
    }
    assert not unresolved, f"undefined names in the realtime path: {sorted(unresolved)}"


def test_a_delivery_failure_does_not_propagate(monkeypatch, caplog):
    """One bad address must not silence every other subscriber."""
    def boom(*a, **k):
        raise RuntimeError("resend is down")
    monkeypatch.setattr(ns, "_maybe_send_realtime_email", boom)
    # Must not raise.
    ns._try_send_realtime(None, {"user_id": "u1", "email_enabled": 1,
                                 "email_frequency": "realtime"}, "t", "b")


def test_the_realtime_path_is_reachable_without_error(monkeypatch):
    """The actual regression: a realtime user with an address gets a send."""
    sent = {}
    monkeypatch.setattr(ns, "_get_user_email", lambda uid: "x@example.com")
    monkeypatch.setattr(ns, "send_email",
                        lambda to, subj, html: sent.update(to=to, subj=subj) or True)
    monkeypatch.setattr(ns, "build_notification_email", lambda t, b: "<p></p>")
    ns._email_cache.pop("u_rt", None)
    ns._maybe_send_realtime_email(
        None, {"user_id": "u_rt", "email_enabled": 1, "email_frequency": "realtime"},
        "Title", "Body")
    assert sent.get("to") == "x@example.com", (
        "a realtime subscriber with a valid address received nothing"
    )


@pytest.mark.parametrize("user", [
    {"user_id": "u", "email_enabled": 0, "email_frequency": "realtime"},
    {"user_id": "u", "email_enabled": 1, "email_frequency": "daily"},
])
def test_non_realtime_users_are_not_emailed_immediately(user, monkeypatch):
    sent = []
    monkeypatch.setattr(ns, "send_email", lambda *a, **k: sent.append(a) or True)
    ns._maybe_send_realtime_email(None, user, "t", "b")
    assert not sent


def test_no_call_site_bypasses_the_guard():
    """Every caller must go through _try_send_realtime. Checked on the AST."""
    tree = ast.parse(SCANNER.read_text())
    bad = []
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef) or fn.name in (
                "_maybe_send_realtime_email", "_try_send_realtime"):
            continue
        for node in ast.walk(fn):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "_maybe_send_realtime_email"):
                bad.append(f"{fn.name}:{node.lineno}")
    assert not bad, (
        f"unguarded realtime send at {bad} — an exception there aborts the "
        "scan for every remaining user"
    )

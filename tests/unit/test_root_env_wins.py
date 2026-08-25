"""The repo-root .env outranks api/.env. Not the other way round.

api/config.py loaded api/.env FIRST and the root .env second with
override=False, so a stale local file silently beat deployed credentials on
every host-run script. On Studio that file held sk_test_ Clerk keys, and the
notification scanner spent months resolving production users against Clerk's
test instance: 0 subscribers on all six event types, 6,887 notifications
created, none emailed.

Production containers were never affected -- docker-compose passes the root
.env and the image does not copy api/.env -- which is precisely why nobody
saw it.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "api/config.py"


def _load_calls() -> list[tuple[str, bool]]:
    """(which file, is override suppressed) for each load_dotenv, in order."""
    tree = ast.parse(CONFIG.read_text())
    out = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and getattr(node.func, "id", None) == "load_dotenv"):
            continue
        target = ast.unparse(node.args[0]) if node.args else "?"
        no_override = any(k.arg == "override"
                          and k.value.value is False for k in node.keywords)
        out.append((target, no_override))
    return out


def test_the_root_env_is_loaded_first():
    calls = _load_calls()
    assert len(calls) == 2, f"expected two load_dotenv calls, got {calls}"
    first, second = calls
    assert first[0] == "_root_env", (
        f"the repo-root .env must load first so it wins; got {first[0]}")
    assert second[0] == "_api_env", f"second load should be api/.env: {second[0]}"


def test_api_env_can_only_fill_gaps():
    _, second = _load_calls()
    assert second[1], (
        "api/.env must be loaded with override=False, so it can supply a key "
        "the root .env omits but never replace one it defines")


def test_the_root_load_is_not_itself_suppressed():
    first, _ = _load_calls()
    assert not first[1], (
        "loading the root .env with override=False would let anything already "
        "in the environment beat it, recreating the bug through a different "
        "door")

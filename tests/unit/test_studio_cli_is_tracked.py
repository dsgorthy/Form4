"""The deploy CLI must have a tracked copy, and it must not drift.

WHAT WAS WRONG

`studio` — the only way anything reaches production — existed at exactly one
path on one machine, `~/.local/bin/studio`, with no copy and no history. It
carries the deploy sequence, the smoke test invocation, the Studio-only plist
guard and the strategy reload. Losing the Mini meant losing the ability to
deploy, and every change to it was invisible to review.

WHAT THIS PINS

A tracked copy exists at scripts/bin/studio and matches the live one. The
check is skipped where the live file is absent (CI, the Studio itself), since
the point is to catch drift on the machine that deploys, not to demand the
CLI be installed everywhere.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TRACKED = REPO / "scripts" / "bin" / "studio"
LIVE = Path(os.path.expanduser("~/.local/bin/studio"))


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_a_tracked_copy_exists():
    assert TRACKED.exists(), (
        "scripts/bin/studio is gone. The deploy CLI is back to living on one "
        "machine with no history."
    )
    assert TRACKED.stat().st_size > 1000, "the tracked copy is a stub"


def test_the_tracked_copy_carries_no_secrets():
    src = TRACKED.read_text(encoding="utf-8").lower()
    for marker in ("sk_live", "sk_test", "pk_live", "begin rsa", "begin openssh",
                   "password=", "api_key="):
        assert marker not in src, (
            f"the tracked deploy CLI now contains {marker!r}. Credentials "
            "belong in .env on the target host or the macOS Keychain."
        )


@pytest.mark.skipif(not LIVE.exists(), reason="no installed studio CLI here")
def test_the_tracked_copy_matches_the_installed_one():
    assert _sha(TRACKED) == _sha(LIVE), (
        "scripts/bin/studio and ~/.local/bin/studio have drifted.\n"
        "  repo  -> live:  cp scripts/bin/studio ~/.local/bin/studio\n"
        "  live  -> repo:  cp ~/.local/bin/studio scripts/bin/studio\n"
        "Whichever is newer, copy it over and commit — a tracked copy that "
        "does not match the thing that actually deploys is worse than none, "
        "because it invites review of code that never runs."
    )

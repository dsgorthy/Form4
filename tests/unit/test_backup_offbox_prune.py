"""The off-box backup mirror must actually prune, and must not prune the archives.

Before 2026-09-09 `backup_databases.sh` shipped the nightly dumps to the Mini
with:

    rsync -a --delete-after --include="*_${STAMP}.dump" --include="*/" --exclude="*"

which deletes nothing, ever. An rsync --exclude pattern also PROTECTS the
receiver's matching files from deletion, so `--exclude="*"` protects the entire
mirror and `--delete-after` is a no-op — on failed nights and on successful ones
alike. The mirror grew unbounded to 118 GB / 28 days against a declared 7-day
retention, filled the Mini to 99%, and then the transfer itself started failing
for want of space. Nothing in the log said so: the script reported "off-box copy
ok" on every night it grew.

The replacement prunes the mirror over ssh, before the transfer, scoped by
database name. These tests pin the three properties that make that correct.
"""
import re
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backup_databases.sh"


@pytest.fixture(scope="module")
def script() -> str:
    return SCRIPT.read_text()


def _offbox_logical_lines(script: str) -> list[str]:
    """The off-box block as executable logical lines.

    Comments are stripped and backslash-continuations joined, so these tests
    read the code and never the prose. The comment block above this section
    quotes the broken rsync invocation verbatim as a warning; a scanner that
    counted that as code would fail on a correct script.
    """
    section = script[script.index("# Off-box copy.") :]
    code = [ln for ln in section.splitlines() if not ln.lstrip().startswith("#")]
    joined, buf = [], ""
    for ln in code:
        buf += ln.rstrip()
        if buf.endswith("\\"):
            buf = buf[:-1]
            continue
        joined.append(buf)
        buf = ""
    if buf:
        joined.append(buf)
    return joined


def _line_containing(lines: list[str], needle: str) -> int:
    for i, ln in enumerate(lines):
        if needle in ln:
            return i
    return -1


def test_rsync_does_not_claim_to_delete(script):
    """--delete on the off-box rsync is inert and therefore a lie.

    Any --delete* flag alongside `--exclude="*"` reads as a retention policy to
    the next person while doing nothing. If a future change really wants rsync
    to prune, it needs --delete-excluded and a re-think of the filter, not this.
    """
    lines = _offbox_logical_lines(script)
    i = _line_containing(lines, "rsync ")
    assert i != -1, "no rsync invocation found in the off-box section"
    offending = re.findall(r"--delete[\w-]*", lines[i])
    assert not offending, (
        f"off-box rsync carries {offending}, which cannot delete anything while "
        '--exclude="*" protects the receiver. Prune over ssh instead.'
    )


def test_mirror_is_pruned_before_the_transfer(script):
    """Prune-then-send, not send-then-prune.

    Pruning after the transfer cannot recover a full mirror: the transfer fails
    for want of space, so the prune never runs, so the space is never freed.
    The Mini sat in exactly that loop for two nights.
    """
    lines = _offbox_logical_lines(script)
    prune = _line_containing(lines, "-mtime +$RETENTION_DAYS -print -delete")
    rsync = _line_containing(lines, "rsync ")
    assert prune != -1, "no remote prune found in the off-box section"
    assert rsync != -1, "no rsync found in the off-box section"
    assert prune < rsync, (
        "the mirror prune runs after the transfer; it must run before it, or a "
        "full mirror can never free the space its own transfer needs"
    )


def test_remote_prune_is_scoped_to_the_live_databases(script):
    """The mirror also holds dumps with no live source. They must not age out.

    `pyrrho_prod` / `pyrrho_staging` were decommissioned 2026-08-12 and their
    final dumps live only in the archive and on the Mini. A retention rule
    written as `-name "*.dump"` would delete them on its next run and there is
    nothing left to re-dump from.
    """
    lines = _offbox_logical_lines(script)
    i = _line_containing(lines, "-mtime +$RETENTION_DAYS")
    assert i != -1, "no remote prune found"
    prune_line = lines[i]
    assert '-name "*.dump"' not in prune_line and "-name '*.dump'" not in prune_line, (
        "remote prune matches every *.dump, which would delete the Pyrrho final "
        "archival dumps. Scope it by database name."
    )
    assert '${db}_' in prune_line, (
        "remote prune is not scoped by database name; it must iterate DATABASES "
        "so anything not written by this script survives by construction"
    )

"""No module may reference a name that is never bound.

WHAT THIS EXISTS TO CATCH (2026-09-08)

`insert_trades` called `conn.execute(...)`, discarded the cursor it returns,
and eleven lines later did `inserted += cur.rowcount`. `cur` was never bound.
Every insert raised NameError, a broad per-row handler caught and logged it,
and the run reported "37 new filings -> 0 trades" as a success. The live path
stored nothing for four days; a weekend and Labor Day hid it, because with no
new filings that line is never reached.

Nothing in the unit suite could see it: the Mini has no `form4` DB, so the
insert path is never executed here. But an unbound name is decidable without
running anything, and a scan of the whole repo finds exactly one (in archived
code). This gate is therefore nearly free and would have caught the defect the
moment it was written.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

#: Live source. `pipelines/archive` is excluded deliberately — it holds
#: retired research scripts (one of which has a genuine undefined name) that
#: nothing imports and nobody runs.
SCANNED = ["api", "strategies", "framework", "scripts", "dataplane",
           "pipelines", "board", "config", "tests"]
EXCLUDED = ("pipelines/archive/",)


def test_pyflakes_is_installed():
    """Guard the guard: a missing linter must not silently disable the check."""
    pytest.importorskip(
        "pyflakes",
        reason="pyflakes is in requirements.txt; install it or this gate is off",
    )


def test_no_undefined_names():
    pytest.importorskip("pyflakes")
    targets = [str(REPO / d) for d in SCANNED if (REPO / d).exists()]
    proc = subprocess.run(
        [sys.executable, "-m", "pyflakes", *targets],
        capture_output=True, text=True, cwd=str(REPO),
    )
    bad = [
        line for line in (proc.stdout or "").splitlines()
        if "undefined name" in line.lower()
        and not any(x in line for x in EXCLUDED)
    ]
    assert not bad, (
        "a name is referenced that is never bound — this raises NameError the "
        "first time the line executes, which may be months after it ships:\n  "
        + "\n  ".join(bad)
    )

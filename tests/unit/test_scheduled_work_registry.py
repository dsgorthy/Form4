"""Every unit of scheduled work is declared, and every exemption is argued.

Dagster owns scheduling. The registry at dataplane/deploy/scheduled_work.yaml
is the single place that records which work has moved, which still owes the
move, and which is deliberately staying — with a reason.

It exists because on 2026-08-26 nobody could answer "what is still on cron?"
without SSHing into Studio and reading plists out of four separate directories,
and because the migration had stalled for nine days with nothing anywhere
showing that.

These tests lint the file. The reality check — is a `dagster` unit still loaded
in launchd, i.e. is the work running TWICE — needs launchctl and lives in
scripts/check_scheduled_work.py, which runs on Studio.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
REGISTRY = REPO / "dataplane" / "deploy" / "scheduled_work.yaml"
CHECKER = REPO / "scripts" / "check_scheduled_work.py"

DATA = yaml.safe_load(REGISTRY.read_text())
SECTIONS = ("dagster", "pending", "exempt")


def _names(section: str) -> list[str]:
    return [e["name"] for e in DATA.get(section) or []]


def test_the_registry_has_the_three_sections():
    for s in SECTIONS:
        assert s in DATA, f"scheduled_work.yaml lost its `{s}` section"


@pytest.mark.parametrize("section", SECTIONS)
def test_every_entry_is_named(section: str):
    for entry in DATA.get(section) or []:
        assert (entry.get("name") or "").strip(), f"unnamed entry in `{section}`"


def test_nothing_appears_in_two_sections():
    seen: dict[str, str] = {}
    for section in SECTIONS:
        for name in _names(section):
            assert name not in seen, (
                f"{name} is in both `{seen[name]}` and `{section}` — the whole "
                "point of this file is one unambiguous answer per unit"
            )
            seen[name] = section


def test_every_exemption_carries_a_reason():
    """'Not in Dagster' is a position to be argued, not asserted."""
    for entry in DATA["exempt"]:
        reason = (entry.get("reason") or "").strip()
        assert len(reason) > 30, (
            f"{entry['name']} is exempt from Dagster with reason {reason!r}. "
            "Write the argument down — a future reader has to be able to tell "
            "a real constraint from someone not getting round to it."
        )


def test_the_scheduler_itself_stays_exempt():
    """A guard against an over-enthusiastic reading of 'everything in Dagster'."""
    exempt = set(_names("exempt"))
    for required in ("dagster-daemon", "dagster-webserver"):
        assert required in exempt, (
            f"{required} must stay exempt. You cannot schedule the scheduler "
            "with itself; something outside Dagster has to keep Dagster alive."
        )


def test_the_backup_and_the_watchdogs_stay_exempt():
    """Anything whose job is to notice the plane is broken must outlive it."""
    exempt = set(_names("exempt"))
    for required in ("pg-backup", "heartbeat-probe", "form4-uptime"):
        assert required in exempt, (
            f"{required} must stay exempt: a watchdog or backup that runs "
            "inside the system it protects goes quiet exactly when needed."
        )


def test_the_checker_exists_and_is_executable():
    assert CHECKER.exists(), "scripts/check_scheduled_work.py is gone"
    src = CHECKER.read_text()
    assert "DOUBLE-RUN" in src, (
        "the checker must still flag a `dagster` unit with a loaded plist — "
        "that is the failure that silently runs the work twice"
    )
    assert "UNREGISTERED" in src


def test_pending_is_debt_and_is_visible():
    """Not an upper bound on quality — a tripwire against silent growth."""
    pending = _names("pending")
    assert len(pending) <= 30, (
        f"{len(pending)} units pending migration. If this is growing, work is "
        "being ADDED to launchd, which is the opposite of the direction."
    )
    assert "insider-fetch" in pending or "insider-fetch" in _names("dagster"), (
        "insider-fetch must be tracked: it is the core Form 4 ingestion and "
        "its silent failures lost ~12% of filings for five months"
    )

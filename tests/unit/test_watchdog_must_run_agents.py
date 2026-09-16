"""The off-box watchdog must name a long-running Studio agent that launchd has
loaded but is not running.

On 2026-09-15 23:25:37 a Colima restart stopped six such agents at once and
launchd left every one of them at "- 0": no pid, exit 0, KeepAlive or not.
Both cloudflared tunnels other than form4's, the GitHub deploy runner,
dagster-daemon, dagster-webserver and Ollama stayed dead 12 to 19 hours. The
heartbeat check saw Dagster's jobs go stale; nothing said why, and nothing
said the runner was gone until a push sat queued the next morning.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "offbox_watchdog",
    Path(__file__).resolve().parents[2] / "scripts" / "offbox_watchdog.py",
)
watchdog = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(watchdog)

LABELS = [label for label, _ in watchdog.MUST_RUN_AGENTS]


def _listing(pids: dict[str, str]) -> str:
    """launchctl list output: PID <tab> status <tab> label, with the header
    line and some Apple noise, the way the real command prints it."""
    lines = ["PID\tStatus\tLabel", "512\t0\tcom.apple.Finder", "-\t0\tcom.apple.siriactionsd"]
    for label in LABELS:
        lines.append(f"{pids.get(label, '4242')}\t0\t{label}")
    return "\n".join(lines) + "\n"


def test_all_running_is_clean():
    problems, report = watchdog.evaluate_must_run_agents(_listing({}))
    assert problems == []
    assert len(report) == len(LABELS)
    assert all(line.startswith("  OK  ") for line in report)


def test_the_incident_listing_names_all_six_with_the_kickstart_command():
    dead = {
        "com.openclaw.tailorly-tunnel": "-",
        "com.cloudflare.cloudflared.designquiz": "-",
        "actions.runner.dsgorthy-Form4.dereks-mac-studio": "-",
        "com.openclaw.dagster-daemon": "-",
        "com.openclaw.dagster-webserver": "-",
        "com.ollama.server": "-",
    }
    problems, report = watchdog.evaluate_must_run_agents(_listing(dead))
    assert len(problems) == 6
    for label in dead:
        (p,) = [p for p in problems if p.startswith(label)]
        assert "NOT RUNNING" in p
        assert f"launchctl kickstart -k gui/$(id -u)/{label}" in p
    # form4's tunnel survived that night and must read OK.
    assert any(line.startswith("  OK   com.cloudflare.cloudflared:") for line in report)


def test_an_unloaded_agent_is_a_different_problem_from_a_stopped_one():
    listing = "\n".join(
        line for line in _listing({}).splitlines()
        if "lima-master-keepalive" not in line
    )
    problems, _ = watchdog.evaluate_must_run_agents(listing)
    assert problems == [
        "com.derekg.lima-master-keepalive (the VM's port forwards) is not loaded in launchd on Studio"
    ]


def test_every_must_run_label_carries_a_role_for_the_page():
    for label, role in watchdog.MUST_RUN_AGENTS:
        assert label and role, (label, role)


def test_the_deploy_runner_and_the_scheduler_are_watched():
    # The two whose silent death cost the most on 09-15/16.
    assert "actions.runner.dsgorthy-Form4.dereks-mac-studio" in LABELS
    assert "com.openclaw.dagster-daemon" in LABELS
    assert "com.derekg.lima-master-keepalive" in LABELS

"""Unit tests for the Monday paper-trading monitor.

Pure-Python checks (no DB / network). The DB-touching checks
(refresh_features_chain, writer_registry_runtime, qm_scan_today) are
exercised end-to-end on Studio during the live plist run; here we only
guard the heartbeat parsing and unexpected-criticals filter logic.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# ── Heartbeat freshness ──────────────────────────────────────────────────


@pytest.fixture
def hb_dir(tmp_path, monkeypatch):
    """Override the heartbeat directory to a tmp_path the test controls."""
    sub = tmp_path / "strategies" / "cw_strategies" / "data"
    sub.mkdir(parents=True)
    monkeypatch.setattr(
        "scripts.monday_paper_monitor.REPO",
        tmp_path,
    )
    return sub


def _write_heartbeat(dir_, strategy, age_min, status="active"):
    ts = (datetime.now(timezone.utc) - timedelta(minutes=age_min)).isoformat()
    (dir_ / f"{strategy}_heartbeat.json").write_text(json.dumps({
        "strategy": strategy, "mode": "paper", "status": status,
        "timestamp": ts, "pid": 12345, "detail": "test",
    }))


def test_heartbeats_pass_when_all_fresh(hb_dir):
    from scripts.monday_paper_monitor import check_heartbeats
    from scripts.monday_paper_monitor import STRATEGIES
    for s in STRATEGIES:
        _write_heartbeat(hb_dir, s, age_min=2)
    r = check_heartbeats()
    assert r.ok is True
    assert r.severity == "info"
    assert "all 3 fresh" in r.detail


def test_heartbeats_fail_when_one_stale(hb_dir, monkeypatch):
    """Stale WHILE A BEAT IS DUE. The window is patched, not inherited from the
    clock: since 2026-09-29 the check only treats age as a fault inside the
    runners' schedule (`*/10 6-13 * * 1-5` PT), so an unpatched version of this
    test passed or failed depending on what time the suite happened to run."""
    import scripts.monday_paper_monitor as mpm
    monkeypatch.setattr(mpm, "_runner_is_due_now", lambda: True)
    _write_heartbeat(hb_dir, "quality_momentum", age_min=2)
    _write_heartbeat(hb_dir, "reversal_dip", age_min=2)
    _write_heartbeat(hb_dir, "quality_notrend", age_min=90)
    r = mpm.check_heartbeats()
    assert r.ok is False
    assert r.severity == "warn"
    assert "quality_notrend" in r.detail


def test_heartbeats_tolerate_age_outside_the_runner_schedule(hb_dir, monkeypatch):
    """The last fire of the day is 13:50 PT and nothing is due until 06:00 PT, so
    an overnight gap is the CORRECT state. Reporting it as a fault produced six
    criticals a day and buried the real ones — the reason the Monday monitor had
    failed three Mondays running."""
    import scripts.monday_paper_monitor as mpm
    monkeypatch.setattr(mpm, "_runner_is_due_now", lambda: False)
    for strategy in ("quality_momentum", "reversal_dip", "quality_notrend"):
        _write_heartbeat(hb_dir, strategy, age_min=900)
    r = mpm.check_heartbeats()
    assert r.ok is True, (
        "a 15-hour-old heartbeat outside the schedule window is reported as a "
        "fault; that is the alarm-on-correct-behaviour bug"
    )


def test_heartbeats_fail_when_file_missing(hb_dir):
    from scripts.monday_paper_monitor import check_heartbeats
    _write_heartbeat(hb_dir, "quality_momentum", age_min=2)
    # reversal_dip + quality_notrend missing
    r = check_heartbeats()
    assert r.ok is False
    assert "reversal_dip" in r.detail
    assert "quality_notrend" in r.detail


# ── Unexpected criticals filter ──────────────────────────────────────────


@pytest.fixture
def alert_log(tmp_path, monkeypatch):
    """Point the monitor at a tmp alerts.ndjson."""
    p = tmp_path / "logs" / "alerts.ndjson"
    p.parent.mkdir(parents=True)
    monkeypatch.setattr("scripts.monday_paper_monitor.ALERT_LOG", p)
    return p


def _append_alert(path, severity, component, message, ts):
    entry = {"ts": ts, "severity": severity, "component": component, "message": message}
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def test_unexpected_criticals_returns_pass_when_none(alert_log):
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    # Empty log
    alert_log.touch()
    r = check_unexpected_critical_alerts()
    assert r.ok is True


def test_unexpected_criticals_filters_deploy_noise(alert_log):
    """uptime_monitor criticals during deploy-window are expected noise."""
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    from datetime import datetime, timedelta, timezone
    post_deploy_ts = (datetime.now(timezone.utc)
                      - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")   # after deploy reference
    _append_alert(alert_log, "critical", "uptime_monitor",
                  "form4.app DOWN — 3 consecutive failures", post_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is True, "uptime_monitor must be filtered as expected noise"


def test_unexpected_criticals_flags_unrelated_failures(alert_log):
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    from datetime import datetime, timedelta, timezone
    post_deploy_ts = (datetime.now(timezone.utc)
                      - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _append_alert(alert_log, "critical", "cw_runner.quality_momentum",
                  "HALT — freshness contract breached", post_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is False
    assert r.severity == "warn"
    assert "cw_runner.quality_momentum" in r.detail


def test_unexpected_criticals_ignores_alerts_older_than_the_window(alert_log):
    """Was "before DEPLOY_COMMIT_UTC". The cutoff is now a rolling window,
    because counting since a fixed date meant the check could never go green —
    it sat at 257 and was ignored for months, including the week it was the
    only thing reporting that A-List Buys' runner was dead."""
    from datetime import datetime, timedelta, timezone
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    pre_deploy_ts = (datetime.now(timezone.utc)
                     - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _append_alert(alert_log, "critical", "cw_runner.reversal_dip",
                  "HALT — input freshness", pre_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is True


def test_unexpected_criticals_skips_own_alerts(alert_log):
    """The monitor's own critical alerts must NOT count as unexpected
    (it would otherwise self-page on every failed check)."""
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    from datetime import datetime, timedelta, timezone
    post_deploy_ts = (datetime.now(timezone.utc)
                      - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _append_alert(alert_log, "critical", "monday_paper_monitor",
                  "FAIL refresh_features_chain: ...", post_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is True

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
    for s in ("quality_momentum", "reversal_dip", "tenb51_surprise"):
        _write_heartbeat(hb_dir, s, age_min=2)
    r = check_heartbeats()
    assert r.ok is True
    assert r.severity == "info"
    assert "all 3 fresh" in r.detail


def test_heartbeats_fail_when_one_stale(hb_dir):
    from scripts.monday_paper_monitor import check_heartbeats
    _write_heartbeat(hb_dir, "quality_momentum", age_min=2)
    _write_heartbeat(hb_dir, "reversal_dip", age_min=2)
    _write_heartbeat(hb_dir, "tenb51_surprise", age_min=90)
    r = check_heartbeats()
    assert r.ok is False
    assert r.severity == "warn"
    assert "tenb51_surprise" in r.detail


def test_heartbeats_fail_when_file_missing(hb_dir):
    from scripts.monday_paper_monitor import check_heartbeats
    _write_heartbeat(hb_dir, "quality_momentum", age_min=2)
    # reversal_dip + tenb51 missing
    r = check_heartbeats()
    assert r.ok is False
    assert "reversal_dip" in r.detail
    assert "tenb51_surprise" in r.detail


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
    post_deploy_ts = "2026-05-18T15:00:00Z"   # after deploy reference
    _append_alert(alert_log, "critical", "uptime_monitor",
                  "form4.app DOWN — 3 consecutive failures", post_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is True, "uptime_monitor must be filtered as expected noise"


def test_unexpected_criticals_flags_unrelated_failures(alert_log):
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    post_deploy_ts = "2026-05-18T15:00:00Z"
    _append_alert(alert_log, "critical", "cw_runner.quality_momentum",
                  "HALT — freshness contract breached", post_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is False
    assert r.severity == "warn"
    assert "cw_runner.quality_momentum" in r.detail


def test_unexpected_criticals_ignores_predeploy_alerts(alert_log):
    """Anything before DEPLOY_COMMIT_UTC is ignored — it's prior-state noise."""
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    pre_deploy_ts = "2026-05-15T15:00:00Z"   # 2 days before deploy
    _append_alert(alert_log, "critical", "cw_runner.reversal_dip",
                  "HALT — input freshness", pre_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is True


def test_unexpected_criticals_skips_own_alerts(alert_log):
    """The monitor's own critical alerts must NOT count as unexpected
    (it would otherwise self-page on every failed check)."""
    from scripts.monday_paper_monitor import check_unexpected_critical_alerts
    post_deploy_ts = "2026-05-18T15:00:00Z"
    _append_alert(alert_log, "critical", "monday_paper_monitor",
                  "FAIL refresh_features_chain: ...", post_deploy_ts)
    r = check_unexpected_critical_alerts()
    assert r.ok is True

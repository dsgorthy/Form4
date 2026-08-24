"""A fresh heartbeat from a runner whose cycle is failing must not read green.

WHAT WENT WRONG

cw_runner writes a heartbeat every loop tick with status="active". The daily
cycle is a try/except INSIDE that loop, so when it throws the loop keeps
turning and the heartbeat keeps saying "active".

A-List Buys' daily cycle raised on every run from 2026-08-18 to 08-24 — 367
times — because get_alpaca() demanded a trading account the alert-only book
correctly does not have. Throughout, heartbeat_probe reported all three
strategies fresh, monday_paper_monitor's heartbeats_fresh check passed, and the
strategy never scanned or wrote a single decision row.

Liveness is not health. The process being up says nothing about whether the
work inside it succeeded.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "strategies/cw_strategies/cw_runner.py"

_spec = importlib.util.spec_from_file_location(
    "heartbeat_probe", REPO / "scripts/heartbeat_probe.py")
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)


def _beat(tmp_path, monkeypatch, **fields):
    """Write a heartbeat for every strategy and evaluate it."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    monkeypatch.setattr(probe, "DATA_DIR", tmp_path, raising=False)
    base = {
        "strategy": "quality_notrend", "mode": "paper", "status": "active",
        "timestamp": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        "pid": 1, "detail": "Market hours",
    }
    base.update(fields)
    monkeypatch.setattr(probe, "_read_heartbeat",
                        lambda s, suffix: dict(base, strategy=s)
                        if suffix == "" else None)
    return probe.check_strategies()


def test_a_fresh_beat_with_a_failed_cycle_is_not_ok(tmp_path, monkeypatch):
    res = _beat(tmp_path, monkeypatch,
                last_cycle_ok=False, last_cycle_date="2026-08-24",
                last_cycle_error="RuntimeError: missing alpaca_env_prefix")
    r = res["quality_notrend_paper"]
    assert r["status"] == "cycle_failed", (
        "a runner that is alive but whose daily cycle threw is reported as "
        f"{r['status']!r}; this is the exact state A-List Buys sat in for five "
        "trading days while every check said green"
    )
    assert r["ok"] is False
    assert r["age_min"] is not None and r["age_min"] < 5, (
        "the beat really is fresh — that is the point"
    )


def test_a_fresh_beat_with_a_good_cycle_is_ok(tmp_path, monkeypatch):
    res = _beat(tmp_path, monkeypatch,
                last_cycle_ok=True, last_cycle_date="2026-08-24",
                last_cycle_error=None)
    assert res["quality_notrend_paper"]["status"] == "fresh"
    assert res["quality_notrend_paper"]["ok"] is True


def test_a_runner_that_has_not_cycled_yet_is_not_failed(tmp_path, monkeypatch):
    """Before the first 9:31 cycle of the day the outcome is unknown, which is
    not the same as failed. Alerting on None would page every morning."""
    res = _beat(tmp_path, monkeypatch, last_cycle_ok=None,
                last_cycle_date=None, last_cycle_error=None)
    assert res["quality_notrend_paper"]["ok"] is True
    assert res["quality_notrend_paper"]["status"] == "fresh"


def test_an_old_heartbeat_without_the_field_still_works(tmp_path, monkeypatch):
    """A runner on older code writes no last_cycle_ok. Must not be treated as
    a failure, or every deploy pages until each daemon restarts."""
    res = _beat(tmp_path, monkeypatch)   # no last_cycle_* keys at all
    assert res["quality_notrend_paper"]["ok"] is True


# ── the runner must actually record the outcome ────────────────────────────


def test_the_runner_records_both_outcomes():
    src = RUNNER.read_text()
    assert '_last_cycle["ok"] = True' in src, (
        "the runner no longer records a SUCCESSFUL cycle, so a failure can "
        "never be cleared and the alert would latch forever"
    )
    assert '_last_cycle["ok"] = False' in src, (
        "the runner no longer records a FAILED cycle — the heartbeat goes "
        "back to being pure liveness"
    )


def test_the_heartbeat_carries_the_outcome():
    src = RUNNER.read_text()
    block = src[src.index("def _write_heartbeat("):]
    block = block[:block.index("\ndef ")]
    for field in ("last_cycle_ok", "last_cycle_date", "last_cycle_error"):
        assert field in block, f"_write_heartbeat no longer emits {field}"

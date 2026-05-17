"""Regression guard: `cw_runner.get_alpaca` must return a `LiveBackend`
(NOT `PaperBackend` with a swapped base_url) when the strategy config
sets `live_money: true`.

Pre-2026-05-17 behavior: `PaperBackend(api_key, api_secret,
base_url=LIVE_API_BASE)`. That bypassed `LiveBackend.__init__`'s
`enable_live=True` safety guard AND skipped the per-instantiation
WARNING log that surfaces live-trading in every cw_runner restart.

These tests lock in the new behavior so a future refactor that
re-introduces the URL-swap pattern fails CI.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from strategies.cw_strategies.cw_runner import get_alpaca
from framework.execution.paper import PaperBackend
from framework.execution.live import LiveBackend


PAPER_CONFIG = {
    "strategy_name": "quality_momentum",
    "alpaca_env_prefix": "QUALITY_MOMENTUM",
    "live_money": False,
}

LIVE_CONFIG = {
    "strategy_name": "quality_momentum_live",
    "alpaca_env_prefix": "QUALITY_MOMENTUM",
    "live_money": True,
}


def test_paper_mode_returns_paperbackend(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM", "paper-key-test")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM", "paper-secret-test")
    backend = get_alpaca(PAPER_CONFIG)
    assert isinstance(backend, PaperBackend)
    assert not isinstance(backend, LiveBackend), \
        "paper mode must NOT return a LiveBackend"
    assert "paper-api.alpaca.markets" in backend._base_url


def test_live_mode_returns_livebackend(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM_LIVE", "live-key-test")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM_LIVE", "live-secret-test")
    backend = get_alpaca(LIVE_CONFIG)
    assert isinstance(backend, LiveBackend), \
        "live_money: true must return a LiveBackend (with enable_live=True)"
    # LiveBackend.__init__ requires enable_live=True or it raises RuntimeError.
    # If we got an instance back, that guard was satisfied.
    assert "api.alpaca.markets" in backend._base_url
    assert "paper" not in backend._base_url, \
        "live backend must route to non-paper URL"


def test_live_mode_reads_LIVE_suffixed_env_vars(monkeypatch):
    """Live and paper must use DIFFERENT env vars — never share creds."""
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM", "paper-secret")
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM_LIVE", "live-key")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM_LIVE", "live-secret")

    paper_backend = get_alpaca(PAPER_CONFIG)
    live_backend = get_alpaca(LIVE_CONFIG)

    # Cred storage is inside the session headers dict
    paper_key = paper_backend._session.headers.get("APCA-API-KEY-ID")
    live_key = live_backend._session.headers.get("APCA-API-KEY-ID")
    assert paper_key == "paper-key"
    assert live_key == "live-key"
    assert paper_key != live_key


def test_missing_live_creds_raises_not_silently_paper(monkeypatch):
    """If LIVE creds are missing, refuse to fall back to paper. The
    yaml's `live_money: true` is a contract; silently downgrading to
    paper would route real-money intent to a paper account."""
    monkeypatch.delenv("ALPACA_API_KEY_QUALITY_MOMENTUM_LIVE", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_QUALITY_MOMENTUM_LIVE", raising=False)
    # Even if paper creds are set, live mode must raise.
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM", "paper-secret")
    with pytest.raises(RuntimeError, match="QUALITY_MOMENTUM_LIVE"):
        get_alpaca(LIVE_CONFIG)


def test_livebackend_constructed_with_enable_live_true(monkeypatch):
    """The enable_live=True safety opt-in must be passed; otherwise
    LiveBackend.__init__ raises a different RuntimeError."""
    monkeypatch.setenv("ALPACA_API_KEY_QUALITY_MOMENTUM_LIVE", "live-key")
    monkeypatch.setenv("ALPACA_API_SECRET_QUALITY_MOMENTUM_LIVE", "live-secret")
    # If get_alpaca passed enable_live=False (or omitted it), this would
    # raise "LiveBackend requires enable_live=True." We expect a clean
    # construction.
    backend = get_alpaca(LIVE_CONFIG)
    assert backend.mode() == "LIVE"

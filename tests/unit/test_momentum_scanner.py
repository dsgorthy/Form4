"""Unit tests for the low-float momentum scanner's pure signal logic.

No network / no creds — builds synthetic 1-min bars and asserts the level
detection, RVOL, scoring, and filtering behave correctly. This is what lets us
verify the scanner core on the Mini without live market data.
"""
import numpy as np
import pandas as pd
import pytest

from framework.data.alpaca_screener import parse_gainers, parse_most_actives
from framework.data.fundamentals import parse_shares_outstanding_m
from pipelines.momentum_scanner.config import ScannerConfig
from pipelines.momentum_scanner.signals import evaluate_candidate


def _make_intraday(
    day="2026-06-16",
    n_pm=60,
    n_rth=120,
    p0=0.80,
    p_open=1.00,
    p_last=1.20,
    vol=20_000,
):
    """Build a clean 'runner': premarket rising p0->p_open, RTH rising p_open->p_last."""
    rows = []
    t = pd.Timestamp(f"{day} 08:30", tz="US/Eastern")
    pm = np.linspace(p0, p_open, n_pm)
    for i in range(n_pm):
        c = pm[i]
        o = pm[i - 1] if i > 0 else p0
        rows.append((t, o, max(o, c) * 1.005, min(o, c) * 0.995, c, vol))
        t = t + pd.Timedelta(minutes=1)
    t = pd.Timestamp(f"{day} 09:30", tz="US/Eastern")
    rth = np.linspace(p_open, p_last, n_rth)
    for i in range(n_rth):
        c = rth[i]
        o = rth[i - 1] if i > 0 else p_open
        rows.append((t, o, max(o, c) * 1.003, min(o, c) * 0.997, c, vol))
        t = t + pd.Timedelta(minutes=1)
    idx = pd.DatetimeIndex([r[0] for r in rows])
    return pd.DataFrame(
        [{"open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} for r in rows],
        index=idx,
    )


def test_clean_runner_passes_and_scores_high():
    df = _make_intraday()
    ev = evaluate_candidate(
        "TEST",
        intraday=df,
        last_price=1.20,
        prev_close=0.85,
        day_open=1.00,
        prior_day_high=0.95,
        avg_daily_volume=1_000_000,   # cum intraday vol = 180*20k = 3.6M -> rvol 3.6
        shares_outstanding_m=12.0,
    )
    assert ev.passes_filters, ev.reasons
    assert ev.gap_pct == pytest.approx(17.647, abs=0.01)
    assert ev.rvol == pytest.approx(3.6, abs=0.01)
    assert ev.broke_pm_high is True      # 1.20 > premarket high (~1.005)
    assert ev.broke_pd_high is True      # 1.20 > 0.95
    assert ev.above_vwap is True
    assert ev.above_ema_fast is True
    assert ev.above_ema_slow is True     # 120 RTH bars >= 90
    assert ev.score > 60.0


def test_fresh_round_number_break_detected():
    # last few bars cross up through $1.00 from $0.95 -> $1.02
    df = _make_intraday(n_pm=0, n_rth=20, p_open=0.95, p_last=1.02)
    ev = evaluate_candidate(
        "RND", intraday=df, last_price=1.02, prev_close=0.80, day_open=0.95,
    )
    assert ev.nearest_round == 1.0
    assert ev.broke_round is True


def test_no_premarket_bars_handles_gracefully():
    df = _make_intraday(n_pm=0, n_rth=120)
    ev = evaluate_candidate("NOPM", intraday=df, last_price=1.20, prev_close=0.85, day_open=1.00)
    assert ev.premarket_high is None
    assert ev.broke_pm_high is False
    assert ev.above_vwap is True          # RTH-only still computes


def test_too_few_bars_for_slow_ema_does_not_crash():
    df = _make_intraday(n_pm=0, n_rth=10)   # < ema_slow(90)
    ev = evaluate_candidate("SHORT", intraday=df, last_price=1.20, prev_close=0.85, day_open=1.00)
    assert ev.ema_slow is None
    assert ev.above_ema_slow is False
    assert ev.ema_fast is not None          # 10 bars >= 9
    assert ev.above_ema_fast is True


def test_price_over_max_is_filtered_out():
    df = _make_intraday()
    ev = evaluate_candidate("BIG", intraday=df, last_price=15.0, prev_close=12.0, day_open=14.0,
                            avg_daily_volume=1_000_000)
    assert ev.passes_filters is False
    assert any("price>" in r for r in ev.reasons)


def test_missing_prev_close_fails_gap_filter():
    df = _make_intraday()
    ev = evaluate_candidate("NOREF", intraday=df, last_price=1.20)  # no prev_close
    assert ev.gap_pct is None and ev.change_pct is None
    assert ev.passes_filters is False
    assert any("gap<" in r for r in ev.reasons)


def test_low_rvol_filtered_when_known():
    df = _make_intraday()
    ev = evaluate_candidate("SLOW", intraday=df, last_price=1.20, prev_close=0.85, day_open=1.00,
                            avg_daily_volume=100_000_000)   # rvol ~0.036
    assert ev.passes_filters is False
    assert any("rvol<" in r for r in ev.reasons)


def test_empty_intraday_is_safe():
    ev = evaluate_candidate("EMPTY", intraday=pd.DataFrame(), last_price=1.20,
                            prev_close=0.85, day_open=1.00)
    assert ev.session_vwap is None
    assert ev.score >= 0.0   # gap component only; no crash


# --- discovery / fundamentals payload parsing (pure, no network) ---

def test_parse_gainers_filters_and_sorts():
    payload = {"gainers": [
        {"symbol": "AAA", "percent_change": 12.0, "price": 1.2},
        {"symbol": "BBB", "percent_change": 80.0, "price": 0.9},
        {"symbol": "CCC", "percent_change": 5.0, "price": 3.0},   # below min_pct
        {"percent_change": 50.0},                                  # malformed: no symbol
    ]}
    g = parse_gainers(payload, min_pct=10.0)
    assert [d["symbol"] for d in g] == ["BBB", "AAA"]   # sorted desc, CCC + malformed dropped


def test_parse_most_actives_skips_malformed():
    payload = {"most_actives": [
        {"symbol": "AAA", "volume": 1000, "trade_count": 5},
        {"foo": "bar"},
    ]}
    assert [d["symbol"] for d in parse_most_actives(payload)] == ["AAA"]


def test_parse_shares_outstanding():
    assert parse_shares_outstanding_m({"shareOutstanding": 12.5}) == 12.5
    assert parse_shares_outstanding_m({}) is None
    assert parse_shares_outstanding_m({"shareOutstanding": 0}) is None
    assert parse_shares_outstanding_m({"shareOutstanding": "bad"}) is None

"""Pure signal evaluation for the low-float momentum scanner.

No network, no DB: takes one candidate's market data (intraday 1-min bars +
a few scalars) and returns a scored `CandidateEval`. This is the part with the
real bug-risk (level detection, RVOL, scoring) and is fully unit-tested in
tests/unit/test_momentum_scanner.py — keep it deterministic and side-effect free.

Reuses the framework indicator primitives (ema, vwap) rather than re-implementing.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import time
from typing import Any, Dict, List, Optional

import pandas as pd

from framework.signals.indicators import ema, vwap

from .config import ScannerConfig

_RTH_OPEN = time(9, 30)
_RTH_CLOSE = time(16, 0)


@dataclass
class CandidateEval:
    """Scored evaluation of one candidate at one point in time."""

    symbol: str
    last_price: float
    prev_close: Optional[float] = None
    day_open: Optional[float] = None
    gap_pct: Optional[float] = None          # (open - prev_close) / prev_close
    change_pct: Optional[float] = None        # (last - prev_close) / prev_close
    rvol: Optional[float] = None              # cum today volume / avg daily volume
    shares_outstanding_m: Optional[float] = None  # float PROXY (true float is paid data)

    session_vwap: Optional[float] = None
    above_vwap: bool = False
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    above_ema_fast: bool = False
    above_ema_slow: bool = False

    premarket_high: Optional[float] = None
    broke_pm_high: bool = False
    prior_day_high: Optional[float] = None
    broke_pd_high: bool = False

    nearest_round: Optional[float] = None
    broke_round: bool = False

    score: float = 0.0
    passes_filters: bool = False
    reasons: List[str] = field(default_factory=list)  # filter-exclusion reasons (empty == passes)

    def to_row(self) -> Dict[str, Any]:
        """Flatten to a JSON/DB-friendly dict (reasons joined to a string)."""
        d = dict(self.__dict__)
        d["reasons"] = ",".join(self.reasons)
        return d


def _premarket_high(intraday: pd.DataFrame) -> Optional[float]:
    if intraday.empty:
        return None
    pm = intraday[[t < _RTH_OPEN for t in intraday.index.time]]
    return float(pm["high"].max()) if not pm.empty else None


def _rth(intraday: pd.DataFrame) -> pd.DataFrame:
    """Regular-trading-hours slice (09:30–16:00 ET)."""
    if intraday.empty:
        return intraday
    mask = [(_RTH_OPEN <= t <= _RTH_CLOSE) for t in intraday.index.time]
    return intraday[mask]


def _round_step(price: float) -> float:
    """Psychological-level granularity by price tier."""
    if price >= 5.0:
        return 1.0    # whole dollars
    if price >= 1.0:
        return 0.5    # halves (captures the $1.00 / $1.50 / $2.00 ... levels)
    if price >= 0.5:
        return 0.10
    return 0.05


def _round_break(base: pd.DataFrame, last_price: float, lookback: int) -> tuple[Optional[float], bool]:
    """Nearest round level at/below price, and whether price *freshly* broke up through it."""
    if base.empty or last_price <= 0:
        return (None, False)
    step = _round_step(last_price)
    level = round(math.floor(last_price / step + 1e-9) * step, 2)
    recent = base.tail(max(lookback, 1))
    recent_low = float(recent["low"].min())
    broke = bool(level > 0 and recent_low < level <= last_price)
    return (level, broke)


def evaluate_candidate(
    symbol: str,
    *,
    intraday: Optional[pd.DataFrame],
    last_price: float,
    prev_close: Optional[float] = None,
    day_open: Optional[float] = None,
    prior_day_high: Optional[float] = None,
    avg_daily_volume: Optional[float] = None,
    shares_outstanding_m: Optional[float] = None,
    cfg: Optional[ScannerConfig] = None,
) -> CandidateEval:
    """Score one candidate. `intraday` is a 1-min OHLCV DataFrame indexed by ET
    timestamps (cols: open/high/low/close/volume), spanning premarket through now."""
    cfg = cfg or ScannerConfig()
    ev = CandidateEval(
        symbol=symbol,
        last_price=float(last_price),
        prev_close=prev_close,
        day_open=day_open,
        prior_day_high=prior_day_high,
        shares_outstanding_m=shares_outstanding_m,
    )

    if prev_close:
        ev.change_pct = (ev.last_price - prev_close) / prev_close * 100.0
        if day_open:
            ev.gap_pct = (day_open - prev_close) / prev_close * 100.0

    intraday = intraday if intraday is not None else pd.DataFrame()
    if not intraday.empty:
        ev.premarket_high = _premarket_high(intraday)
        ev.broke_pm_high = bool(ev.premarket_high is not None and ev.last_price > ev.premarket_high)

        rth = _rth(intraday)
        base = rth if not rth.empty else intraday  # before 09:30 there's no RTH yet

        vw = vwap(base).dropna()
        if len(vw):
            ev.session_vwap = float(vw.iloc[-1])
            ev.above_vwap = ev.last_price >= ev.session_vwap

        close = base["close"]
        ef = ema(close, cfg.ema_fast).dropna()
        if len(ef):
            ev.ema_fast = float(ef.iloc[-1])
            ev.above_ema_fast = ev.last_price >= ev.ema_fast
        es = ema(close, cfg.ema_slow).dropna()
        if len(es):
            ev.ema_slow = float(es.iloc[-1])
            ev.above_ema_slow = ev.last_price >= ev.ema_slow

        if avg_daily_volume and avg_daily_volume > 0:
            ev.rvol = float(intraday["volume"].sum()) / avg_daily_volume

        ev.nearest_round, ev.broke_round = _round_break(base, ev.last_price, cfg.round_break_lookback)

    if prior_day_high:
        ev.broke_pd_high = ev.last_price > prior_day_high

    ev.score = _score(ev, cfg)
    ev.passes_filters, ev.reasons = _filters(ev, cfg)
    return ev


def _score(ev: CandidateEval, cfg: ScannerConfig) -> float:
    """Technical-confluence score in [0, 100]. Transparent + tunable (see ScannerConfig)."""
    s = 0.0
    if ev.above_vwap:
        s += cfg.w_vwap
    if ev.above_ema_fast:
        s += cfg.w_ema_fast
    if ev.above_ema_slow:
        s += cfg.w_ema_slow
    if ev.broke_pm_high:
        s += cfg.w_pm_high
    if ev.broke_pd_high:
        s += cfg.w_pd_high
    if ev.broke_round:
        s += cfg.w_round
    g = ev.gap_pct if ev.gap_pct is not None else ev.change_pct
    if g is not None:
        s += min(max(g, 0.0), 30.0) / 30.0 * cfg.w_gap
    if ev.rvol is not None:
        s += min(max(ev.rvol, 0.0), 5.0) / 5.0 * cfg.w_rvol
    return round(s, 1)


def _filters(ev: CandidateEval, cfg: ScannerConfig) -> tuple[bool, List[str]]:
    """Universe membership. Unknown values (None) are NOT excluded — we can't filter
    on data we don't have, so they pass and get flagged downstream instead."""
    reasons: List[str] = []
    if ev.last_price > cfg.max_price:
        reasons.append(f"price>{cfg.max_price}")
    if ev.last_price < cfg.min_price:
        reasons.append(f"price<{cfg.min_price}")
    g = ev.gap_pct if ev.gap_pct is not None else ev.change_pct
    if g is None or g < cfg.min_gap_pct:
        reasons.append(f"gap<{cfg.min_gap_pct}")
    if cfg.min_rvol and ev.rvol is not None and ev.rvol < cfg.min_rvol:
        reasons.append(f"rvol<{cfg.min_rvol}")
    if cfg.max_float_m and ev.shares_outstanding_m is not None and ev.shares_outstanding_m > cfg.max_float_m:
        reasons.append(f"float>{cfg.max_float_m}M")
    return (len(reasons) == 0, reasons)

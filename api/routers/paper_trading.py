"""Paper trading dashboard — admin-only view of the 3 live Alpaca paper accounts.

Reads each strategy's Alpaca creds from env vars, fetches account + positions
state on demand, computes backtest expectation delta, returns aggregated JSON.

Cached server-side for 60s to avoid hammering Alpaca on every page load.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from api.auth import UserContext, get_current_user
from framework.execution.alpaca_account import AlpacaAccount

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/paper-trading", tags=["paper-trading"])


# Strategy registry. Backtest stats are baked in here (the YAML configs have
# them only as comments). When we add a 4th strategy, just append a row.
STRATEGIES = [
    {
        "name": "quality_momentum",
        "label": "Quality + Momentum",
        "starting_capital": 100_000,
        "started_at": "2026-04-05",
        "key_env": "ALPACA_API_KEY_QUALITY_MOMENTUM",
        "secret_env": "ALPACA_API_SECRET_QUALITY_MOMENTUM",
        "backtest": {
            "cagr": 18.5,
            "sharpe": 1.18,
            "win_rate": 57.0,
            "max_dd": 10.1,
            "trades": 137,
        },
    },
    {
        "name": "cw_reversal",
        "label": "Reversal + Quality",
        "starting_capital": 248_000,
        "started_at": "2026-04-05",
        "key_env": "ALPACA_API_KEY_CW_REVERSAL",
        "secret_env": "ALPACA_API_SECRET_CW_REVERSAL",
        "backtest": {
            "cagr": 28.1,
            "sharpe": 1.14,
            "win_rate": 61.3,
            "max_dd": 23.5,
            "trades": 65,
        },
    },
    {
        "name": "tenb51_surprise",
        "label": "10b5-1 Surprise",
        "starting_capital": 100_000,
        "started_at": "2026-04-05",
        "key_env": "ALPACA_API_KEY_TENB51_SURPRISE",
        "secret_env": "ALPACA_API_SECRET_TENB51_SURPRISE",
        "backtest": {
            "cagr": 10.2,
            "sharpe": 0.68,
            "win_rate": 55.0,
            "max_dd": 12.0,
            "trades": 50,
        },
    },
]


# Simple in-process cache: { "dashboard": (timestamp, payload) }
_CACHE: dict = {}
_CACHE_TTL = 60  # seconds


def _expected_equity(starting_capital: float, cagr_pct: float, started_at: str) -> float:
    """Compute expected equity given a CAGR and elapsed days since start.

    Uses 252 trading days per year. Returns starting_capital if no time has
    elapsed yet.
    """
    try:
        start_dt = datetime.strptime(started_at, "%Y-%m-%d")
    except ValueError:
        return starting_capital
    days_elapsed = (datetime.utcnow() - start_dt).days
    if days_elapsed <= 0:
        return starting_capital
    cagr = cagr_pct / 100.0
    return starting_capital * ((1 + cagr) ** (days_elapsed / 252))


def _deviation_status(delta_pct: float) -> str:
    """Classify how far from backtest expectation we are."""
    if delta_pct >= -2.0:
        return "on_track"
    if delta_pct >= -10.0:
        return "below"
    return "well_below"


def _fetch_strategy_snapshot(strategy_def: dict) -> dict:
    """Fetch live Alpaca state for one strategy + compute backtest delta."""
    api_key = os.environ.get(strategy_def["key_env"], "")
    api_secret = os.environ.get(strategy_def["secret_env"], "")

    base = {
        "name": strategy_def["name"],
        "label": strategy_def["label"],
        "starting_capital": strategy_def["starting_capital"],
        "started_at": strategy_def["started_at"],
        "backtest": strategy_def["backtest"],
    }

    if not api_key or not api_secret:
        return {**base, "error": f"Missing creds: {strategy_def['key_env']} not set"}

    try:
        acc = AlpacaAccount(api_key=api_key, api_secret=api_secret, paper=True)
        snap = acc.get_snapshot()
    except Exception as e:
        logger.warning("Alpaca fetch failed for %s: %s", strategy_def["name"], e)
        return {**base, "error": f"{type(e).__name__}: {e}"}

    expected = _expected_equity(
        strategy_def["starting_capital"],
        strategy_def["backtest"]["cagr"],
        strategy_def["started_at"],
    )
    total_pnl = snap["equity"] - strategy_def["starting_capital"]
    total_pnl_pct = (total_pnl / strategy_def["starting_capital"] * 100) if strategy_def["starting_capital"] else 0
    delta_from_expected_pct = (
        (snap["equity"] - expected) / expected * 100 if expected > 0 else 0
    )

    return {
        **base,
        "current_equity": round(snap["equity"], 2),
        "cash": round(snap["cash"], 2),
        "buying_power": round(snap["buying_power"], 2),
        "status": snap["status"],
        "day_change": round(snap["day_change"], 2),
        "day_change_pct": round(snap["day_change_pct"], 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "expected_equity": round(expected, 2),
        "delta_from_expected_pct": round(delta_from_expected_pct, 2),
        "deviation_status": _deviation_status(delta_from_expected_pct),
        "position_count": snap["position_count"],
        "open_positions": snap["positions"],
    }


@router.get("/dashboard")
def paper_trading_dashboard(user: UserContext = Depends(get_current_user)) -> dict:
    """Multi-strategy paper trading snapshot. Admin-only."""
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="admin only")

    # Cache check
    cached = _CACHE.get("dashboard")
    now = time.time()
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    strategies = [_fetch_strategy_snapshot(s) for s in STRATEGIES]
    payload = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "strategies": strategies,
        "summary": {
            "total_strategies": len(strategies),
            "on_track": sum(1 for s in strategies if s.get("deviation_status") == "on_track"),
            "below": sum(1 for s in strategies if s.get("deviation_status") == "below"),
            "well_below": sum(1 for s in strategies if s.get("deviation_status") == "well_below"),
            "errored": sum(1 for s in strategies if "error" in s),
        },
    }

    _CACHE["dashboard"] = (now, payload)
    return payload

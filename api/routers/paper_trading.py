"""Paper trading dashboard — admin-only view of the 3 live Alpaca paper accounts.

$100K CONVENTION (source of truth for how strategies are reported)
================================================================================
Every strategy is treated as if it started with $100,000 at its strategy start
date (the first trade in strategy_portfolio). The real Alpaca accounts have
been funded to match the "$100K compounded through actual strategy P&L" value,
so the Alpaca equity IS the theoretical current value. No normalization needed.

Starting capital (label):  $100,000
Current equity:            float(account.equity)  — real Alpaca
Total P&L:                 current_equity - $100,000
Total P&L %:               total_pnl / $100,000

Backtest expectation:      $100,000 * (1 + CAGR) ^ years_since_start_date
Delta vs expected:         (current_equity - expected) / expected

Accounts were (re-)funded to the theoretical value on 2026-04-06 and again on
2026-04-10 after the backtest was re-run with updated exit logic. If the
backtest ever changes materially again, the Alpaca accounts will drift from
the new theoretical and need re-funding.

Cached server-side for 60s to avoid hammering Alpaca on every page load.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from api.auth import UserContext, get_current_user
from framework.execution.alpaca_account import AlpacaAccount

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/paper-trading", tags=["paper-trading"])

# Conceptual starting capital label. Real Alpaca accounts are pegged to
# $100K + theoretical strategy P&L since each strategy's first trade date.
STARTING_CAPITAL = 100_000

# Strategy registry. `started_at` is the FIRST TRADE DATE from the backtest
# (matches strategy_portfolio.entry_date) — not when the Alpaca account was
# funded. The Alpaca equity represents "continuation from that date as if we
# had started with $100K."
STRATEGIES = [
    {
        "name": "quality_momentum",
        "label": "Quality + Momentum",
        "started_at": "2020-03-06",
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
        "name": "reversal_dip",
        "label": "Deep Reversal + Dip",
        "started_at": "2020-03-04",
        "key_env": "ALPACA_API_KEY_REVERSAL_DIP",
        "secret_env": "ALPACA_API_SECRET_REVERSAL_DIP",
        "backtest": {
            "cagr": 11.3,
            "sharpe": 1.08,
            "win_rate": 55.0,
            "max_dd": 14.3,
            "trades": 132,
        },
    },
    {
        "name": "tenb51_surprise",
        "label": "10b5-1 Surprise",
        "started_at": "2023-08-15",
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
    """Compute expected equity compounding $100K at CAGR since the strategy
    start date. Uses calendar years (365.25 days) because the strategy's
    "age" is measured in real time, not trading days."""
    try:
        start_dt = datetime.strptime(started_at, "%Y-%m-%d")
    except ValueError:
        return starting_capital
    days_elapsed = (datetime.utcnow() - start_dt).days
    if days_elapsed <= 0:
        return starting_capital
    years = days_elapsed / 365.25
    return starting_capital * ((1 + cagr_pct / 100.0) ** years)


def _deviation_status(delta_pct: float) -> str:
    """Classify how far from backtest expectation we are."""
    if delta_pct >= -2.0:
        return "on_track"
    if delta_pct >= -10.0:
        return "below"
    return "well_below"


def _fetch_strategy_snapshot(strategy_def: dict) -> dict:
    """Fetch live Alpaca state. The Alpaca equity IS the current value — no
    scaling. Starting capital is labeled as $100K (conceptual)."""
    api_key = os.environ.get(strategy_def["key_env"], "")
    api_secret = os.environ.get(strategy_def["secret_env"], "")

    base = {
        "name": strategy_def["name"],
        "label": strategy_def["label"],
        "starting_capital": STARTING_CAPITAL,
        "started_at": strategy_def["started_at"],
        "backtest": strategy_def["backtest"],
    }

    if not api_key or not api_secret:
        return {**base, "error": f"Missing creds: {strategy_def['key_env']} not set"}

    try:
        acc_wrapper = AlpacaAccount(api_key=api_key, api_secret=api_secret, paper=True)
        account = acc_wrapper.get_account()
        positions_raw = acc_wrapper.get_positions()
    except Exception as e:
        logger.warning("Alpaca fetch failed for %s: %s", strategy_def["name"], e)
        return {**base, "error": f"{type(e).__name__}: {e}"}

    current_equity = float(account.get("equity", 0) or 0)
    last_equity = float(account.get("last_equity", 0) or 0)
    cash = float(account.get("cash", 0) or 0)
    buying_power = float(account.get("buying_power", 0) or 0)

    total_pnl = current_equity - STARTING_CAPITAL
    total_pnl_pct = total_pnl / STARTING_CAPITAL * 100

    expected = _expected_equity(
        STARTING_CAPITAL,
        strategy_def["backtest"]["cagr"],
        strategy_def["started_at"],
    )
    delta_from_expected_pct = (
        (current_equity - expected) / expected * 100 if expected > 0 else 0
    )

    day_change = current_equity - last_equity
    day_change_pct = (day_change / last_equity * 100) if last_equity > 0 else 0

    open_positions = []
    for p in positions_raw:
        try:
            qty = float(p.get("qty", 0) or 0)
            open_positions.append({
                "symbol": p.get("symbol"),
                "qty": qty,
                "avg_entry_price": float(p.get("avg_entry_price", 0) or 0),
                "current_price": float(p.get("current_price", 0) or 0),
                "market_value": float(p.get("market_value", 0) or 0),
                "unrealized_pl": float(p.get("unrealized_pl", 0) or 0),
                "unrealized_plpc": float(p.get("unrealized_plpc", 0) or 0) * 100,
            })
        except (TypeError, ValueError):
            continue

    return {
        **base,
        "current_equity": round(current_equity, 2),
        "cash": round(cash, 2),
        "buying_power": round(buying_power, 2),
        "status": account.get("status", "unknown"),
        "day_change": round(day_change, 2),
        "day_change_pct": round(day_change_pct, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "expected_equity": round(expected, 2),
        "delta_from_expected_pct": round(delta_from_expected_pct, 2),
        "deviation_status": _deviation_status(delta_from_expected_pct),
        "position_count": len(open_positions),
        "open_positions": open_positions,
    }


@router.get("/dashboard")
def paper_trading_dashboard(user: UserContext = Depends(get_current_user)) -> dict:
    """Multi-strategy paper trading snapshot.

    All amounts are normalized to a $100K starting baseline. See module
    docstring for why.

    Pro users see full detail (exact equity, open positions, day change).
    Free users see a summary (rounded equity, position count, overall status).
    """

    cached = _CACHE.get("dashboard")
    now = time.time()
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    strategies = [_fetch_strategy_snapshot(s) for s in STRATEGIES]
    payload = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "starting_capital": STARTING_CAPITAL,
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

    if not user.has_full_feed:
        redacted = []
        for s in strategies:
            r = {
                "name": s["name"],
                "label": s["label"],
                "starting_capital": s["starting_capital"],
                "started_at": s["started_at"],
                "backtest": s["backtest"],
                "position_count": s.get("position_count", 0),
                "deviation_status": s.get("deviation_status"),
            }
            eq = s.get("current_equity")
            if eq is not None:
                r["current_equity"] = round(eq / 1000) * 1000
                r["total_pnl"] = r["current_equity"] - STARTING_CAPITAL
                r["total_pnl_pct"] = round(r["total_pnl"] / STARTING_CAPITAL * 100, 0)
            if "error" in s:
                r["error"] = s["error"]
            redacted.append(r)
        return {**payload, "strategies": redacted, "gated": True}

    return payload

"""Paper trading dashboard — admin-only view of the 3 live Alpaca paper accounts.

All dollar amounts are NORMALIZED to a $100K starting baseline. The actual
Alpaca accounts were funded with different amounts ($301,680 / $191,766 /
$148,855), which would make side-by-side comparison meaningless. Instead, we
compute the real return % from each account and scale it onto a $100K baseline
so all 3 strategies can be compared apples-to-apples.

Cached server-side for 60s to avoid hammering Alpaca on every page load.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from api.auth import UserContext, get_current_user
from framework.execution.alpaca_account import AlpacaAccount

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/paper-trading", tags=["paper-trading"])

# Every strategy is displayed as if it started with this amount, with actual
# returns compounded on top. Keeps all 3 strategies directly comparable.
NORMALIZED_STARTING_CAPITAL = 100_000

# Strategy registry. No `starting_capital` field — all normalized to the
# constant above. `started_at` matches when each Alpaca paper account was
# actually created (verified against /v2/account.created_at).
STRATEGIES = [
    {
        "name": "quality_momentum",
        "label": "Quality + Momentum",
        "started_at": "2026-04-07",
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
        "started_at": "2026-04-07",
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
        "started_at": "2026-04-07",
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


def _compute_initial_funding(account: dict, positions: list, activities: list) -> tuple[float, float, float, float]:
    """Back out initial funding from current state + activity history.

    initial = equity - (realized_pnl + unrealized_pnl + fees)

    Realized P&L is computed via FIFO lot matching — buy orders open lots,
    sell orders close them against the oldest open lot at that price.

    Returns: (initial_funding, realized_pnl, unrealized_pnl, fees)
    """
    equity = float(account.get("equity", 0) or 0)
    unrealized = sum(float(p.get("unrealized_pl", 0) or 0) for p in positions)

    book: dict = defaultdict(list)  # symbol -> [[qty, price], ...]
    realized = 0.0
    fees = 0.0

    # Activities come back from Alpaca in reverse chronological order; reverse
    # again so we process fills oldest → newest and FIFO is correct.
    for a in reversed(activities):
        t = a.get("activity_type")
        if t == "FEE":
            try:
                fees += float(a.get("net_amount", 0) or 0)
            except (TypeError, ValueError):
                pass
        elif t == "FILL":
            try:
                sym = a.get("symbol")
                side = a.get("side")
                qty = float(a.get("qty", 0) or 0)
                price = float(a.get("price", 0) or 0)
            except (TypeError, ValueError):
                continue
            if not sym or qty <= 0:
                continue
            if side == "buy":
                book[sym].append([qty, price])
            elif side == "sell":
                remaining = qty
                while remaining > 0 and book[sym]:
                    lot = book[sym][0]
                    take = min(remaining, lot[0])
                    realized += take * (price - lot[1])
                    remaining -= take
                    lot[0] -= take
                    if lot[0] <= 0:
                        book[sym].pop(0)

    total_pnl = realized + unrealized + fees
    initial = equity - total_pnl
    return initial, realized, unrealized, fees


def _fetch_strategy_snapshot(strategy_def: dict) -> dict:
    """Fetch live Alpaca state for one strategy, normalize to $100K baseline."""
    api_key = os.environ.get(strategy_def["key_env"], "")
    api_secret = os.environ.get(strategy_def["secret_env"], "")

    base = {
        "name": strategy_def["name"],
        "label": strategy_def["label"],
        "starting_capital": NORMALIZED_STARTING_CAPITAL,
        "started_at": strategy_def["started_at"],
        "backtest": strategy_def["backtest"],
    }

    if not api_key or not api_secret:
        return {**base, "error": f"Missing creds: {strategy_def['key_env']} not set"}

    try:
        acc_wrapper = AlpacaAccount(api_key=api_key, api_secret=api_secret, paper=True)
        account = acc_wrapper.get_account()
        positions_raw = acc_wrapper.get_positions()
        activities = acc_wrapper.get_activities()
    except Exception as e:
        logger.warning("Alpaca fetch failed for %s: %s", strategy_def["name"], e)
        return {**base, "error": f"{type(e).__name__}: {e}"}

    actual_initial, realized, unrealized, fees = _compute_initial_funding(
        account, positions_raw, activities
    )
    actual_equity = float(account.get("equity", 0) or 0)
    actual_last_equity = float(account.get("last_equity", 0) or 0)

    if actual_initial <= 0:
        # Brand-new account with no trades — just mirror the $100K baseline.
        return {
            **base,
            "current_equity": float(NORMALIZED_STARTING_CAPITAL),
            "cash": float(NORMALIZED_STARTING_CAPITAL),
            "buying_power": float(NORMALIZED_STARTING_CAPITAL * 2),
            "status": account.get("status", "unknown"),
            "day_change": 0.0,
            "day_change_pct": 0.0,
            "total_pnl": 0.0,
            "total_pnl_pct": 0.0,
            "expected_equity": float(NORMALIZED_STARTING_CAPITAL),
            "delta_from_expected_pct": 0.0,
            "deviation_status": "on_track",
            "position_count": 0,
            "open_positions": [],
            "_alpaca_actual_funding": round(actual_initial, 2),
            "_alpaca_actual_equity": round(actual_equity, 2),
        }

    # The real return % — this is what the strategy actually earned.
    actual_return_pct = (actual_equity - actual_initial) / actual_initial * 100

    # Scale factor maps real Alpaca dollars into the $100K baseline world.
    scale = NORMALIZED_STARTING_CAPITAL / actual_initial

    normalized_current = NORMALIZED_STARTING_CAPITAL * (1 + actual_return_pct / 100)
    normalized_pnl = normalized_current - NORMALIZED_STARTING_CAPITAL

    expected = _expected_equity(
        NORMALIZED_STARTING_CAPITAL,
        strategy_def["backtest"]["cagr"],
        strategy_def["started_at"],
    )
    delta_from_expected_pct = (
        (normalized_current - expected) / expected * 100 if expected > 0 else 0
    )

    # Scale positions into the $100K world. Ticker / qty / entry price / current
    # price stay as-is (they're real market data), but market value and
    # unrealized P&L dollars are scaled. The unrealized P&L % is unchanged.
    normalized_positions = []
    for p in positions_raw:
        try:
            qty = float(p.get("qty", 0) or 0)
            avg_entry = float(p.get("avg_entry_price", 0) or 0)
            current = float(p.get("current_price", 0) or 0)
            mv = float(p.get("market_value", 0) or 0)
            upl = float(p.get("unrealized_pl", 0) or 0)
            uplpc = float(p.get("unrealized_plpc", 0) or 0) * 100
        except (TypeError, ValueError):
            continue
        normalized_positions.append({
            "symbol": p.get("symbol"),
            "qty": qty,
            "avg_entry_price": avg_entry,
            "current_price": current,
            "market_value": mv * scale,
            "unrealized_pl": upl * scale,
            "unrealized_plpc": uplpc,
        })

    normalized_positions_value = sum(p["market_value"] for p in normalized_positions)
    normalized_cash = normalized_current - normalized_positions_value

    day_change_actual = actual_equity - actual_last_equity
    day_change_pct = (
        (day_change_actual / actual_last_equity * 100) if actual_last_equity > 0 else 0
    )

    return {
        **base,
        "current_equity": round(normalized_current, 2),
        "cash": round(normalized_cash, 2),
        "buying_power": round(normalized_current * 2, 2),  # approximates 2x margin
        "status": account.get("status", "unknown"),
        "day_change": round(day_change_actual * scale, 2),
        "day_change_pct": round(day_change_pct, 2),
        "total_pnl": round(normalized_pnl, 2),
        "total_pnl_pct": round(actual_return_pct, 2),
        "expected_equity": round(expected, 2),
        "delta_from_expected_pct": round(delta_from_expected_pct, 2),
        "deviation_status": _deviation_status(delta_from_expected_pct),
        "position_count": len(normalized_positions),
        "open_positions": normalized_positions,
        # Debug fields — useful for verifying the normalization math
        "_alpaca_actual_funding": round(actual_initial, 2),
        "_alpaca_actual_equity": round(actual_equity, 2),
        "_alpaca_realized_pnl": round(realized, 2),
        "_alpaca_unrealized_pnl": round(unrealized, 2),
    }


@router.get("/dashboard")
def paper_trading_dashboard(user: UserContext = Depends(get_current_user)) -> dict:
    """Multi-strategy paper trading snapshot. Admin-only.

    All amounts are normalized to a $100K starting baseline. See module
    docstring for why.
    """
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="admin only")

    cached = _CACHE.get("dashboard")
    now = time.time()
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    strategies = [_fetch_strategy_snapshot(s) for s in STRATEGIES]
    payload = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "normalized_starting_capital": NORMALIZED_STARTING_CAPITAL,
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

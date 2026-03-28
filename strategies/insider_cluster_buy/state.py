"""
State persistence for Insider Cluster Buy paper trading.

Atomic JSON writes (write .tmp → rename) so crashes never corrupt state.
"""

import json
import os
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_STATE = {
    "queued_signals": [],
    "open_positions": [],
    "closed_trades": [],
    "rolling_window": {},
    "last_edgar_check": None,
    "last_seen_accession": None,
    "performance": {
        "total_pnl": 0.0,
        "trades": 0,
        "wins": 0,
        "sharpe": None,
        "max_dd": 0.0,
        "median_ar": None,
        "returns": [],
        "options_trades": 0,
        "options_wins": 0,
        "options_pnl": 0.0,
        "options_returns": [],
    },
    "circuit_breaker_active": False,
    "skipped_signals": [],
    # Solo insider follow strategy (Phase 1)
    "queued_solo_signals": [],
    "open_solo_positions": [],
    "closed_solo_trades": [],
    "solo_performance": {
        "total_pnl": 0.0,
        "trades": 0,
        "wins": 0,
        "max_dd": 0.0,
        "returns": [],
    },
}


def load_state(path: str) -> dict:
    """Load state from JSON file. Returns default state if file doesn't exist."""
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                state = json.load(f)
            # Merge in any new default keys (forward compatibility)
            for key, val in DEFAULT_STATE.items():
                if key not in state:
                    state[key] = val
            if "performance" in state:
                for pk, pv in DEFAULT_STATE["performance"].items():
                    if pk not in state["performance"]:
                        state["performance"][pk] = pv
            if "solo_performance" in state:
                for pk, pv in DEFAULT_STATE["solo_performance"].items():
                    if pk not in state["solo_performance"]:
                        state["solo_performance"][pk] = pv
            logger.info("Loaded state from %s", path)
            return state
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Corrupt state file %s: %s — starting fresh", path, e)
    logger.info("Creating fresh state at %s", path)
    return json.loads(json.dumps(DEFAULT_STATE))  # deep copy


def save_state(state: dict, path: str) -> None:
    """Atomic write: write to .tmp then rename."""
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp_path, path)
    except Exception:
        logger.exception("Failed to save state to %s", path)
        # Clean up tmp if rename failed
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def add_trading_days(start_date: date, n: int) -> date:
    """Add n trading days (skip weekends) to start_date."""
    current = start_date
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon-Fri
            added += 1
    return current


def compute_rolling_dd(returns: list, window: int = 30) -> float:
    """
    Compute max drawdown over the last `window` trades.

    Returns are portfolio-level fractional returns (pnl / equity).
    Computes multiplicative equity curve and max peak-to-trough decline.
    Returns drawdown as a positive fraction (e.g., 0.08 = 8% DD).
    """
    if not returns:
        return 0.0
    recent = returns[-window:]
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in recent:
        equity *= (1.0 + r)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd

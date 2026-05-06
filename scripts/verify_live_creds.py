#!/usr/bin/env python3
"""Read-only verification that the live Alpaca credentials work.

Usage:
    python3 scripts/verify_live_creds.py
    python3 scripts/verify_live_creds.py --strategy quality_momentum
    python3 scripts/verify_live_creds.py --min-equity 9500

Read-only: hits /v2/account and /v2/positions; does NOT submit any orders.
Returns exit 0 on success, 1 on failure (creds missing, network, low
equity). Suitable for use in the pre-launch validator.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from framework.execution.paper import PaperBackend, LIVE_API_BASE


def check_strategy(strategy: str, min_equity: float) -> dict:
    prefix = strategy.upper()
    key = os.getenv(f"ALPACA_API_KEY_{prefix}_LIVE", "")
    sec = os.getenv(f"ALPACA_API_SECRET_{prefix}_LIVE", "")
    if not key or not sec:
        return {
            "strategy": strategy, "ok": False,
            "reason": f"ALPACA_API_KEY_{prefix}_LIVE / _SECRET not set in .env",
            "equity": None, "positions": None,
        }
    try:
        backend = PaperBackend(key, sec, base_url=LIVE_API_BASE)
        account = backend.get_account()
    except Exception as exc:
        return {"strategy": strategy, "ok": False,
                "reason": f"account fetch failed: {exc}",
                "equity": None, "positions": None}

    equity = float(account.get("equity") or 0)
    portfolio_value = float(account.get("portfolio_value") or 0)
    cash = float(account.get("cash") or 0)
    raw = account.get("raw") or {}
    pdt = bool(raw.get("pattern_day_trader"))
    trading_blocked = bool(raw.get("trading_blocked"))
    account_blocked = bool(raw.get("account_blocked"))

    try:
        positions = backend.list_positions()
    except Exception as exc:
        return {"strategy": strategy, "ok": False,
                "reason": f"positions fetch failed: {exc}",
                "equity": equity, "positions": None}

    issues = []
    if equity < min_equity:
        issues.append(f"equity ${equity:,.0f} < min ${min_equity:,.0f}")
    if trading_blocked:
        issues.append("trading_blocked=true")
    if account_blocked:
        issues.append("account_blocked=true")
    if pdt:
        issues.append("pattern_day_trader=true (not strategy-relevant but flagged)")

    return {
        "strategy": strategy,
        "ok": not issues or (len(issues) == 1 and "pattern_day_trader" in issues[0]),
        "reason": "; ".join(issues) if issues else "all checks pass",
        "equity": equity,
        "portfolio_value": portfolio_value,
        "cash": cash,
        "positions": len(positions),
        "pattern_day_trader": pdt,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="quality_momentum")
    p.add_argument("--min-equity", type=float, default=9500.0)
    args = p.parse_args()

    result = check_strategy(args.strategy, args.min_equity)
    print(f"=== Live credential check: {result['strategy']} ===")
    print(f"  ok           : {result['ok']}")
    print(f"  reason       : {result['reason']}")
    print(f"  equity       : ${result['equity']:,.2f}" if result['equity'] is not None else "  equity       : —")
    if result.get("portfolio_value") is not None:
        print(f"  portfolio    : ${result['portfolio_value']:,.2f}")
    if result.get("cash") is not None:
        print(f"  cash         : ${result['cash']:,.2f}")
    if result.get("positions") is not None:
        print(f"  positions    : {result['positions']}")
    if result.get("pattern_day_trader"):
        print("  ⚠️  pattern_day_trader flag set on this account")

    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()

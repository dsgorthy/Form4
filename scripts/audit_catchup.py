#!/usr/bin/env python3
"""Post-catchup audit — verify missed trades were entered and all systems functional.

Run after 9:45 ET on catchup day. Checks:
1. Catchup log files show successful entries
2. strategy_portfolio has new rows for BW, PANW, MSTR
3. Alpaca accounts have matching positions
4. Normal runner conviction logging is working
5. No orphaned DB rows or Alpaca positions
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from config.database import get_connection
from framework.execution.paper import PaperBackend

ET = ZoneInfo("America/New_York")
LOGS_DIR = PROJECT_ROOT / "logs"

EXPECTED_ENTRIES = {
    "quality_momentum": ["BW"],
    "tenb51_surprise": ["PANW", "MSTR"],
}

STRATEGY_PREFIXES = {
    "quality_momentum": "QUALITY_MOMENTUM",
    "reversal_dip": "REVERSAL_DIP",
    "tenb51_surprise": "TENB51_SURPRISE",
}

passed = 0
failed = 0


def check(label: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {label}")
    else:
        failed += 1
        msg = f"  ❌ {label}"
        if detail:
            msg += f" — {detail}"
        print(msg)


def audit_logs():
    print("\n1. CATCHUP LOG FILES")
    for name in ["catchup-qm", "catchup-10b5"]:
        log_path = LOGS_DIR / f"{name}.log"
        if not log_path.exists():
            check(f"{name}.log exists", False, "File not found — catchup may not have fired")
            continue
        check(f"{name}.log exists", True)
        content = log_path.read_text()
        has_entries = "Opened" in content or "new positions" in content
        has_error = "ERROR" in content or "Traceback" in content
        check(f"{name} shows entries", has_entries, content[-500:] if not has_entries else "")
        check(f"{name} no errors", not has_error, content[-500:] if has_error else "")


def audit_db():
    print("\n2. DATABASE — NEW ENTRIES")
    conn = get_connection(readonly=True)
    cur = conn.cursor()
    today = datetime.now(ET).strftime("%Y-%m-%d")

    for strategy, expected_tickers in EXPECTED_ENTRIES.items():
        for ticker in expected_tickers:
            cur.execute("""
                SELECT id, entry_date, entry_price, shares, status, execution_source
                FROM strategy_portfolio
                WHERE strategy = %s AND ticker = %s AND status = 'open'
                ORDER BY entry_date DESC LIMIT 1
            """, (strategy, ticker))
            row = cur.fetchone()
            if row:
                check(
                    f"{strategy}/{ticker} in DB",
                    True,
                )
                print(f"       entry={row[1]} price=${row[2]:.2f} shares={row[4]} src={row[5]}")
            else:
                cur.execute("""
                    SELECT id, entry_date, exit_date, status, execution_source
                    FROM strategy_portfolio
                    WHERE strategy = %s AND ticker = %s
                    ORDER BY entry_date DESC LIMIT 1
                """, (strategy, ticker))
                last = cur.fetchone()
                detail = f"Last row: entry={last[1]} exit={last[2]} status={last[3]}" if last else "No rows at all"
                check(f"{strategy}/{ticker} in DB (open)", False, detail)

    # Check total open positions per strategy
    print()
    for strategy in ["quality_momentum", "reversal_dip", "tenb51_surprise"]:
        cur.execute("""
            SELECT COUNT(*), string_agg(ticker, ', ')
            FROM strategy_portfolio
            WHERE strategy = %s AND status = 'open'
        """, (strategy,))
        row = cur.fetchone()
        count, tickers = row[0], row[1] or "none"
        print(f"  {strategy}: {count} open positions ({tickers})")

    conn.close()


def audit_alpaca():
    print("\n3. ALPACA ACCOUNTS — POSITION VERIFICATION")
    for strategy, expected_tickers in EXPECTED_ENTRIES.items():
        prefix = STRATEGY_PREFIXES[strategy]
        api_key = os.getenv(f"ALPACA_API_KEY_{prefix}", "")
        api_secret = os.getenv(f"ALPACA_API_SECRET_{prefix}", "")
        if not api_key or not api_secret:
            check(f"{strategy} Alpaca credentials", False, f"ALPACA_API_KEY_{prefix} not set")
            continue

        try:
            alpaca = PaperBackend(api_key, api_secret)
            account = alpaca._request("GET", "/account")
            equity = float(account.get("equity", 0))
            check(f"{strategy} Alpaca connected", True)
            print(f"       equity=${equity:,.2f}")

            positions = alpaca._request("GET", "/positions")
            alpaca_tickers = {p["symbol"] for p in positions}

            for ticker in expected_tickers:
                if ticker in alpaca_tickers:
                    pos = next(p for p in positions if p["symbol"] == ticker)
                    check(
                        f"{strategy}/{ticker} in Alpaca",
                        True,
                    )
                    print(f"       qty={pos['qty']} avg_price=${float(pos['avg_entry_price']):.2f} unrealized=${float(pos['unrealized_pl']):,.2f}")
                else:
                    check(f"{strategy}/{ticker} in Alpaca", False, f"Alpaca has: {alpaca_tickers or 'no positions'}")

        except Exception as e:
            check(f"{strategy} Alpaca connected", False, str(e))


def audit_runner_logs():
    print("\n4. NORMAL RUNNER LOGS — CONVICTION FIX WORKING")
    for name in ["quality-momentum", "tenb51-surprise", "reversal-dip"]:
        log_path = LOGS_DIR / f"{name}.log"
        if not log_path.exists():
            check(f"{name} log exists", False)
            continue

        lines = log_path.read_text().splitlines()
        today = datetime.now(ET).strftime("%Y-%m-%d")
        today_lines = [l for l in lines if today in l]

        has_scan = any("scan_signals" in l for l in today_lines)
        has_conviction_log = any("PASS " in l or "SKIP " in l for l in today_lines)

        check(f"{name} ran today", has_scan, f"{len(today_lines)} log lines today")
        check(f"{name} conviction logging active", has_conviction_log,
              "No PASS/SKIP lines — conviction logging may not be deployed")


def audit_reconciliation():
    print("\n5. DB vs ALPACA RECONCILIATION")
    conn = get_connection(readonly=True)
    cur = conn.cursor()

    for strategy, prefix in STRATEGY_PREFIXES.items():
        api_key = os.getenv(f"ALPACA_API_KEY_{prefix}", "")
        api_secret = os.getenv(f"ALPACA_API_SECRET_{prefix}", "")
        if not api_key or not api_secret:
            continue

        try:
            alpaca = PaperBackend(api_key, api_secret)
            positions = alpaca._request("GET", "/positions")
            alpaca_tickers = {p["symbol"] for p in positions}

            cur.execute("""
                SELECT ticker FROM strategy_portfolio
                WHERE strategy = %s AND status = 'open'
            """, (strategy,))
            db_tickers = {r[0] for r in cur.fetchall()}

            in_db_not_alpaca = db_tickers - alpaca_tickers
            in_alpaca_not_db = alpaca_tickers - db_tickers

            check(f"{strategy} DB↔Alpaca match", not in_db_not_alpaca and not in_alpaca_not_db,
                  f"DB only: {in_db_not_alpaca or '∅'}, Alpaca only: {in_alpaca_not_db or '∅'}")
        except Exception as e:
            check(f"{strategy} reconciliation", False, str(e))

    conn.close()


def main():
    now = datetime.now(ET)
    print(f"═══ CATCHUP AUDIT — {now.strftime('%Y-%m-%d %H:%M ET')} ═══")

    audit_logs()
    audit_db()
    audit_alpaca()
    audit_runner_logs()
    audit_reconciliation()

    print(f"\n═══ RESULT: {passed} passed, {failed} failed ═══")
    if failed > 0:
        print("\n⚠️  FAILURES DETECTED. Review above.")
        sys.exit(1)
    else:
        print("\n✅ All checks passed.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Backfill strategy_portfolio from Alpaca broker truth.

The existing seed scripts run DB -> Alpaca (they place orders). This runs the
other direction: for every position the broker actually holds that has no open
`paper`/`live` ledger row, reconstruct the row from the real fill.

Why it matters: cw_runner's daily exit pass only sees positions via
strategy_portfolio:

    SELECT * FROM strategy_portfolio
     WHERE strategy=? AND status='open'
       AND execution_source IN ('paper','live')
       AND planned_exit_date <= today

A broker position with no ledger row is invisible to that query, so it is
never sold. The 2026-05-19 seeding placed 13 paper orders without writing
back ledger rows, which is how the drift started.

Entry data comes from the real filled BUY order (fill timestamp + Alpaca's
avg_entry_price), never synthesized. planned_exit_date uses the same
MarketCalendar().add_trading_days() call cw_runner uses, so backfilled rows
are indistinguishable from natively-written ones.

Rows with execution_source 'simulated' / 'alert' are deliberately ignored on
both sides: they are signal-only by design and cw_runner excludes them.

Usage (on Studio):
    python3.12 backfill_positions_from_alpaca.py              # dry run (default)
    python3.12 backfill_positions_from_alpaca.py --apply
    python3.12 backfill_positions_from_alpaca.py --strategy tenb51_surprise
    python3.12 backfill_positions_from_alpaca.py --apply --skip-overdue
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from strategies.cw_strategies.cw_runner import load_config, get_alpaca  # noqa: E402
from framework.data.calendar import MarketCalendar  # noqa: E402
from config.database import get_connection  # noqa: E402

CONFIG_DIR = REPO / "strategies" / "cw_strategies" / "configs"
ORDERS_AFTER = "2026-01-01T00:00:00Z"
ET = ZoneInfo("America/New_York")
DEFAULT_STOP_PCT = -0.15
LEDGER_SOURCES = ("paper", "live")


def fetch_filled_buys(backend) -> dict[str, dict]:
    """symbol -> most recent filled BUY order."""
    out, page_token, latest = [], None, {}
    while True:
        params = {"status": "all", "after": ORDERS_AFTER,
                  "limit": 500, "direction": "asc"}
        if page_token:
            params["page_token"] = page_token
        # base_url already ends in /v2 — do not repeat it in the path.
        batch = backend._request("GET", "/orders", params=params)
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < 500:
            break
        page_token = batch[-1].get("id")

    for o in out:
        if o.get("status") == "filled" and o.get("side") == "buy" and o.get("filled_at"):
            sym = o["symbol"]
            if sym not in latest or o["filled_at"] > latest[sym]["filled_at"]:
                latest[sym] = o
    return latest


def et_date(iso_utc: str) -> str:
    """Alpaca timestamps are UTC; the ledger stores ET calendar dates."""
    ts = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    return ts.astimezone(ET).date().isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Write rows. Default is a dry run.")
    ap.add_argument("--strategy", help="Limit to one strategy_name")
    ap.add_argument("--skip-overdue", action="store_true",
                    help="Do not backfill positions whose planned exit has "
                         "already passed (they would be sold at the next "
                         "15:45 ET exit pass)")
    args = ap.parse_args()

    cal = MarketCalendar()
    today = date.today().isoformat()
    conn = get_connection()
    planned_rows: list[tuple] = []
    grand_total = 0

    for cfg_path in sorted(CONFIG_DIR.glob("*.yaml")):
        cfg = load_config(str(cfg_path))
        name = cfg.get("strategy_name")
        if not name or cfg.get("live_money", False):
            continue
        if args.strategy and name != args.strategy:
            continue

        hold_days = int((cfg.get("exit") or {}).get("hold_days") or 0)
        if not hold_days:
            print(f"!! {name}: no exit.hold_days in config — skipping")
            continue
        stop_pct = (cfg.get("exit") or {}).get("stop_loss_pct")
        stop_pct = DEFAULT_STOP_PCT if stop_pct is None else float(stop_pct)

        backend = get_alpaca(cfg)
        positions = backend.list_positions()
        try:
            equity = float(backend.get_account().get("equity") or 0) or None
        except Exception:
            equity = None
        fills = fetch_filled_buys(backend)

        existing = {
            r["ticker"] for r in conn.execute(
                """SELECT ticker FROM strategy_portfolio
                    WHERE strategy = ? AND status = 'open'
                      AND execution_source IN ('paper','live')""",
                (name,),
            ).fetchall()
        }

        print(f"\n{'='*86}")
        print(f"{name}  (hold={hold_days}td, stop={stop_pct}, equity={equity})")
        print("=" * 86)

        missing = [p for p in positions if p["symbol"] not in existing]
        if not missing:
            print("  ledger already matches broker — nothing to backfill")
            continue

        print(f"  {'SYM':<7}{'QTY':>7}{'ENTRY_PX':>11}{'ENTRY_DATE':>13}"
              f"{'PLANNED_EXIT':>14}{'SIZE%':>8}  STATUS")
        for p in sorted(missing, key=lambda x: x["symbol"]):
            sym = p["symbol"]
            order = fills.get(sym)
            if not order:
                print(f"  {sym:<7}{'—':>7}{'—':>11}{'—':>13}{'—':>14}{'—':>8}  "
                      f"SKIP: no filled buy order found")
                continue

            qty = abs(float(p["qty"]))
            entry_price = float(p.get("avg_entry_price") or order.get("filled_avg_price") or 0)
            entry_date = et_date(order["filled_at"])
            planned_exit = cal.add_trading_days(entry_date, hold_days).isoformat()
            dollar_amount = round(qty * entry_price, 2)
            size_pct = round(dollar_amount / equity * 100, 4) if equity else 0.0
            overdue = planned_exit <= today

            if overdue and args.skip_overdue:
                status = "SKIPPED (overdue, --skip-overdue)"
            elif overdue:
                status = f"*** OVERDUE — exits next 15:45 ET pass ***"
            else:
                status = "ok"

            print(f"  {sym:<7}{qty:>7.0f}{entry_price:>11.3f}{entry_date:>13}"
                  f"{planned_exit:>14}{size_pct:>8.2f}  {status}")

            if overdue and args.skip_overdue:
                continue

            planned_rows.append((
                name, sym, "buy_stock", "long", entry_date, round(entry_price, 4),
                hold_days, stop_pct, size_pct, equity, "open", "paper",
                round(entry_price, 4), qty, dollar_amount, planned_exit, False,
                f"Backfilled {today} from Alpaca fill {order['id']} "
                f"({order['filled_at']}); position held at broker with no ledger row.",
            ))
            grand_total += 1

    print(f"\n{'='*86}")
    print(f"{'APPLY' if args.apply else 'DRY RUN'}: {grand_total} row(s) to insert")
    print("=" * 86)

    if not args.apply:
        print("No changes written. Re-run with --apply to commit.")
        conn.close()
        return 0

    if not planned_rows:
        conn.close()
        return 0

    for row in planned_rows:
        conn.execute(
            """INSERT INTO strategy_portfolio (
                   strategy, ticker, trade_type, direction, entry_date,
                   entry_price, target_hold, stop_pct, position_size,
                   portfolio_value, status, execution_source, actual_fill_price,
                   shares, dollar_amount, planned_exit_date, is_live,
                   entry_reasoning
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            row,
        )
    conn.commit()
    print(f"Inserted {len(planned_rows)} row(s).")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

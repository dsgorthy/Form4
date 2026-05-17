#!/usr/bin/env python3
"""Alpaca trade-updates WebSocket listener — event-driven fill confirmation.

Replaces the polling-based wait_for_fill flow for cw_runner's terminal
state detection. cw_runner still polls (now with 300s timeout per
2026-05-13 bump) as a hot path, but this listener catches fills regardless
of how slow Alpaca is and resolves the "timeout — manual verify needed"
class of false-positive critical alerts.

Architecture:
  - One asyncio task per Alpaca paper/live account (3 paper today; 1 live
    when quality_momentum_live launches 2026-06-04)
  - Each task connects to wss://(paper|api).alpaca.markets/stream
  - Subscribes to trade_updates
  - On every event, updates order_audit and strategy_portfolio by
    alpaca_order_id (the join key cw_runner already records)
  - Reconnects on disconnect with exponential backoff
  - Writes a heartbeat file every cycle so /admin/jobs can monitor freshness

Events handled:
  - fill                → mark order filled, set fill_price/qty/at, update portfolio
  - partial_fill        → accumulate filled_qty (we mostly use market orders so
                          full fills are typical, but track partials anyway)
  - canceled / expired  → mark canceled, close the strategy_portfolio row
  - rejected            → mark rejected, close the strategy_portfolio row

Run as a daemon via launchd (com.openclaw.alpaca-stream-listener).

Usage:
    python3 -m framework.oms.alpaca_stream_listener
    python3 -m framework.oms.alpaca_stream_listener --dry-run    # log only, no DB writes
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

import websockets
from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


PAPER_URL = "wss://paper-api.alpaca.markets/stream"
LIVE_URL = "wss://api.alpaca.markets/stream"

# Account registry — one entry per Alpaca account we listen on.
# `env_prefix` resolves to ALPACA_API_KEY_{prefix} for paper accounts and
# ALPACA_API_KEY_{prefix}_LIVE for live accounts (suffix added in
# listen_account based on `live`). If the credentials env vars aren't set
# the per-account task logs "credentials not in env — skipping" and exits
# cleanly; the other accounts keep listening. Safe to pre-add live entries
# before the creds are provisioned.
ACCOUNTS = [
    {"name": "quality_momentum",      "env_prefix": "QUALITY_MOMENTUM", "live": False},
    {"name": "reversal_dip",          "env_prefix": "REVERSAL_DIP",     "live": False},
    {"name": "tenb51_surprise",       "env_prefix": "TENB51_SURPRISE",  "live": False},
    {"name": "quality_momentum_live", "env_prefix": "QUALITY_MOMENTUM", "live": True},
]

HEARTBEAT_PATH = REPO / "strategies/cw_strategies/data/alpaca_stream_heartbeat.json"


# ── DB writers ───────────────────────────────────────────────────────────

def _update_order_audit_fill(conn, alpaca_order_id: str, order: dict) -> int:
    """Mark an order_audit row filled. Returns rowcount."""
    filled_qty = order.get("filled_qty")
    filled_avg_price = order.get("filled_avg_price")
    filled_at = order.get("filled_at")
    return conn.execute(
        """UPDATE order_audit
           SET fill_status = 'filled',
               fill_qty = ?,
               fill_price = ?,
               filled_at = ?
           WHERE alpaca_order_id = ?
             AND fill_status NOT IN ('filled', 'rejected')""",
        (
            float(filled_qty) if filled_qty else None,
            float(filled_avg_price) if filled_avg_price else None,
            filled_at,
            alpaca_order_id,
        ),
    ).rowcount or 0


def _update_strategy_portfolio_fill(conn, strategy: str, alpaca_order_id: str,
                                    order: dict) -> int:
    """Sync strategy_portfolio with actual fill data. Returns rowcount.

    Writes the Alpaca fill into `actual_fill_price` and updates `shares`
    to the actually-filled qty. **Does NOT overwrite `entry_price`** — the
    cw_runner contract is that entry_price is the data-API quote at
    decision time (the strategy's intent), not the broker fill (slippage-
    inclusive). P&L calculation that wants the slippage-adjusted basis
    reads `actual_fill_price`; P&L vs intent reads `entry_price`.

    Pre-2026-05-17 this function overwrote entry_price too — harmless on
    paper (paper Alpaca fills exactly at the quote ~always) but a real
    divergence on live with slippage, where the strategy's intended
    entry would be silently rewritten to the broker's actual fill.
    """
    filled_qty = order.get("filled_qty")
    filled_avg_price = order.get("filled_avg_price")
    ticker = order.get("symbol")
    if not (filled_qty and filled_avg_price and ticker):
        return 0
    return conn.execute(
        """UPDATE strategy_portfolio
           SET shares = ?,
               actual_fill_price = ?
           WHERE strategy = ? AND ticker = ?
             AND status = 'open'
             AND execution_source IN ('paper', 'live')
             AND (actual_fill_price IS NULL OR ABS(actual_fill_price - ?) > 0.0001)""",
        (
            int(float(filled_qty)),
            float(filled_avg_price),
            strategy, ticker, float(filled_avg_price),
        ),
    ).rowcount or 0


def _update_order_audit_terminal(conn, alpaca_order_id: str, status: str,
                                 reason: Optional[str] = None) -> int:
    return conn.execute(
        """UPDATE order_audit
           SET fill_status = ?,
               rejection_reason = COALESCE(?, rejection_reason)
           WHERE alpaca_order_id = ?
             AND fill_status NOT IN ('filled', 'rejected', 'canceled')""",
        (status, reason, alpaca_order_id),
    ).rowcount or 0


def _close_portfolio_on_reject(conn, strategy: str, alpaca_order_id: str) -> int:
    """For rejected/canceled orders we don't have a position; mark the
    speculative open row in strategy_portfolio as closed-cancelled."""
    # Find the matching row via order_audit
    row = conn.execute(
        """SELECT ticker FROM order_audit
           WHERE alpaca_order_id = ? AND strategy = ? AND side = 'buy'""",
        (alpaca_order_id, strategy),
    ).fetchone()
    if not row:
        return 0
    ticker = row[0] if not hasattr(row, "keys") else row["ticker"]
    return conn.execute(
        """UPDATE strategy_portfolio
           SET status = 'closed',
               exit_date = CURRENT_DATE,
               exit_reason = 'alpaca_rejected',
               pnl_pct = 0.0,
               pnl_dollar = 0.0
           WHERE strategy = ? AND ticker = ?
             AND status = 'open'
             AND execution_source IN ('paper', 'live')""",
        (strategy, ticker),
    ).rowcount or 0


# ── Event handler ────────────────────────────────────────────────────────

async def handle_event(account: dict, data: dict, dry_run: bool):
    event = data.get("event")
    order = data.get("order", {})
    alpaca_order_id = order.get("id")
    symbol = order.get("symbol", "?")
    side = order.get("side", "?")
    if not alpaca_order_id:
        return

    if event == "fill":
        filled_qty = order.get("filled_qty", "?")
        filled_avg_price = order.get("filled_avg_price", "?")
        logger.info("[%s] FILL %s %s qty=%s @ $%s order_id=%s",
                    account["name"], side, symbol, filled_qty, filled_avg_price,
                    alpaca_order_id)
        if dry_run:
            return
        with get_connection() as conn:
            n_audit = _update_order_audit_fill(conn, alpaca_order_id, order)
            n_port = _update_strategy_portfolio_fill(conn, account["name"],
                                                    alpaca_order_id, order)
            conn.commit()
        if n_audit or n_port:
            logger.info("[%s] DB: %d order_audit row + %d strategy_portfolio row updated",
                        account["name"], n_audit, n_port)

    elif event in ("rejected", "canceled", "expired"):
        reason = order.get("reject_reason") or order.get("cancel_reason") or event
        logger.warning("[%s] %s %s %s order_id=%s reason=%s",
                       account["name"], event.upper(), side, symbol,
                       alpaca_order_id, reason)
        if dry_run:
            return
        with get_connection() as conn:
            _update_order_audit_terminal(conn, alpaca_order_id, event, reason)
            if side == "buy":
                _close_portfolio_on_reject(conn, account["name"], alpaca_order_id)
            conn.commit()

    elif event == "partial_fill":
        logger.info("[%s] PARTIAL_FILL %s %s filled=%s/%s",
                    account["name"], side, symbol,
                    order.get("filled_qty"), order.get("qty"))
        # Don't write to DB on partials — wait for the full fill event

    elif event in ("new", "pending_new", "accepted", "pending_cancel",
                   "pending_replace", "replaced", "stopped", "suspended"):
        # Transitional states — log but don't act
        logger.debug("[%s] %s %s %s", account["name"], event, side, symbol)


# ── Per-account loop ─────────────────────────────────────────────────────

async def listen_account(account: dict, dry_run: bool):
    # Live accounts read ALPACA_API_KEY_{prefix}_LIVE — paper-mode reads
    # ALPACA_API_KEY_{prefix}. Mirrors cw_runner.get_alpaca's lookup.
    url = LIVE_URL if account["live"] else PAPER_URL
    suffix = "_LIVE" if account["live"] else ""
    key_var = f"ALPACA_API_KEY_{account['env_prefix']}{suffix}"
    secret_var = f"ALPACA_API_SECRET_{account['env_prefix']}{suffix}"
    key = os.environ.get(key_var)
    secret = os.environ.get(secret_var)
    if not (key and secret):
        logger.error("[%s] credentials not in env (%s) — skipping (account remains unmonitored)",
                     account["name"], key_var)
        return
    if account["live"]:
        logger.warning("[%s] LIVE account — listening on %s with real-money creds",
                       account["name"], url)

    backoff = 5
    while True:
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=15) as ws:
                # Authenticate
                await ws.send(json.dumps({
                    "action": "auth", "key": key, "secret": secret,
                }))
                auth_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                status = auth_resp.get("data", {}).get("status")
                if status != "authorized":
                    raise RuntimeError(f"auth status={status}: {auth_resp}")

                # Subscribe
                await ws.send(json.dumps({
                    "action": "listen", "data": {"streams": ["trade_updates"]},
                }))
                sub_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                logger.info("[%s] subscribed: %s", account["name"], sub_resp.get("data", {}))

                backoff = 5  # reset after a healthy connect

                async for msg in ws:
                    try:
                        envelope = json.loads(msg)
                    except json.JSONDecodeError:
                        logger.warning("[%s] non-JSON msg: %s", account["name"], msg[:200])
                        continue
                    if envelope.get("stream") != "trade_updates":
                        continue
                    try:
                        await handle_event(account, envelope.get("data", {}), dry_run)
                    except Exception:
                        logger.exception("[%s] handle_event failed", account["name"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[%s] connection lost: %s — reconnect in %ds",
                         account["name"], e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


# ── Heartbeat ────────────────────────────────────────────────────────────

async def heartbeat_loop():
    while True:
        try:
            HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
            HEARTBEAT_PATH.write_text(json.dumps({
                "service": "alpaca_stream_listener",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "accounts": [a["name"] for a in ACCOUNTS],
                "status": "active",
            }, indent=2))
        except Exception as e:
            logger.warning("heartbeat write failed: %s", e)
        await asyncio.sleep(60)


# ── Main ─────────────────────────────────────────────────────────────────

async def main_async(dry_run: bool):
    logger.info("Starting Alpaca trade_updates listener (dry_run=%s)", dry_run)
    tasks = [
        asyncio.create_task(listen_account(a, dry_run), name=f"listen-{a['name']}")
        for a in ACCOUNTS
    ]
    tasks.append(asyncio.create_task(heartbeat_loop(), name="heartbeat"))
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="Log events without writing to DB")
    args = p.parse_args()
    try:
        asyncio.run(main_async(args.dry_run))
    except KeyboardInterrupt:
        logger.info("interrupted")


if __name__ == "__main__":
    main()

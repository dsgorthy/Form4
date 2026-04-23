"""Shared helper to enrich trade items with context facts from trade_context table.

Static context types use pre-rendered context_text.
Live types (value_rank, cluster_count) render strings from metadata + fresh DB queries.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

# Context types that need live rendering (stale-risk)
LIVE_TYPES = {"value_rank", "cluster_count"}


def _ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _render_value_rank(conn, meta: dict) -> str | None:
    """Render 'Nth largest purchase, out of M' with live total count."""
    rank = meta.get("rank")
    insider_id = meta.get("insider_id")
    ticker = meta.get("ticker")
    trade_type = meta.get("trade_type")
    if rank is None or insider_id is None:
        return None

    # Fresh count of total filings (not lots) for this insider+ticker+type
    row = conn.execute(
        """SELECT COUNT(DISTINCT CASE WHEN accession IS NOT NULL THEN accession ELSE trade_date END) AS cnt
           FROM trades
           WHERE insider_id = ? AND ticker = ? AND trade_type = ?
             AND trans_code IN ('P', 'S')
             AND is_derivative = 0""",
        (insider_id, ticker, trade_type),
    ).fetchone()
    total = row["cnt"] if row else 0

    if total <= 1:
        return None

    action = "purchase" if trade_type == "buy" else "sale"
    if rank == 1:
        return f"Largest {action} ever, out of {total}"
    return f"{_ordinal(rank)} largest {action}, out of {total}"


def _render_cluster_count(conn, meta: dict) -> str | None:
    """Render 'N other insiders also purchased in last 30 days' with live count."""
    ticker = meta.get("ticker")
    trade_type = meta.get("trade_type")
    as_of = meta.get("as_of")
    if not all([ticker, trade_type, as_of]):
        return None

    row = conn.execute(
        """SELECT COUNT(DISTINCT insider_id) AS cnt FROM trades
           WHERE ticker = ? AND trade_type = ?
             AND trade_date BETWEEN date(?, '-30 days') AND ?
             AND trans_code IN ('P', 'S')
             AND is_derivative = 0""",
        (ticker, trade_type, as_of, as_of),
    ).fetchone()
    # Subtract 1 for the current insider (the count includes them)
    count = (row["cnt"] - 1) if row else 0
    if count < 2:
        return None

    action = "purchased" if trade_type == "buy" else "sold"
    return f"{count} other insiders also {action} in last 30 days"


def enrich_items_with_context(conn, items: list[dict], trade_id_key: str = "trade_id") -> None:
    """Add context list to each item in-place.

    Each item gets a 'context' key: [{type, text}, ...] ordered by sort_order.
    Gracefully handles missing trade_context table.
    """
    if not items:
        return

    # Check if table exists
    try:
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_context'"
        ).fetchone()
        if not table_check:
            return
    except Exception:
        return

    # Collect raw trade_ids
    raw_ids = []
    for item in items:
        tid = item.get("_raw_trade_id") or item.get(trade_id_key)
        if isinstance(tid, int):
            raw_ids.append(tid)
        elif isinstance(tid, str) and tid.isdigit():
            raw_ids.append(int(tid))

    if not raw_ids:
        return

    placeholders = ",".join("?" * len(raw_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT trade_id, context_type, context_text, sort_order, metadata
            FROM trade_context
            WHERE trade_id IN ({placeholders})
            ORDER BY sort_order
            """,
            raw_ids,
        ).fetchall()
    except Exception:
        return

    # Build lookup: trade_id -> list of context entries
    context_by_trade: dict[int, list[dict]] = {}
    for r in rows:
        tid = r["trade_id"]
        ctx_type = r["context_type"]
        text = r["context_text"]

        # Live rendering for stale-risk types
        if ctx_type in LIVE_TYPES and text is None:
            try:
                meta = json.loads(r["metadata"]) if r["metadata"] else {}
            except (json.JSONDecodeError, TypeError):
                meta = {}

            if ctx_type == "value_rank":
                text = _render_value_rank(conn, meta)
            elif ctx_type == "cluster_count":
                text = _render_cluster_count(conn, meta)

        if text:
            context_by_trade.setdefault(tid, []).append({
                "type": ctx_type,
                "text": text,
            })

    # Enrich items
    for item in items:
        tid = item.get("_raw_trade_id") or item.get(trade_id_key)
        if isinstance(tid, str) and tid.isdigit():
            tid = int(tid)
        item["context"] = context_by_trade.get(tid, [])

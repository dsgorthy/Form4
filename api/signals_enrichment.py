"""Shared helper to enrich trade items with signal tags from trade_signals table."""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def enrich_items_with_signals(conn, items: list[dict], trade_id_key: str = "trade_id") -> None:
    """Add signal_types string and signals list to each item in-place.

    Gracefully handles missing trade_signals table.
    Items whose trade_id is already encoded (string) are decoded first.
    """
    if not items:
        return

    # Check if table exists
    try:
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_signals'"
        ).fetchone()
        if not table_check:
            return
    except Exception:
        return

    # Collect raw trade_ids (before encoding)
    # Items may have raw int or encoded string trade_ids depending on when enrichment is called
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
            SELECT trade_id, signal_type, signal_label, signal_class, confidence, metadata
            FROM trade_signals
            WHERE trade_id IN ({placeholders})
            """,
            raw_ids,
        ).fetchall()
    except Exception:
        return

    # Build lookup: trade_id -> list of signals
    signals_by_trade: dict[int, list[dict]] = {}
    for r in rows:
        tid = r["trade_id"]
        sig = {
            "signal_type": r["signal_type"],
            "signal_label": r["signal_label"],
            "signal_class": r["signal_class"],
            "confidence": r["confidence"],
        }
        if r["metadata"]:
            try:
                sig["metadata"] = json.loads(r["metadata"])
            except (json.JSONDecodeError, TypeError):
                pass
        signals_by_trade.setdefault(tid, []).append(sig)

    # Enrich items
    for item in items:
        tid = item.get("_raw_trade_id") or item.get(trade_id_key)
        if isinstance(tid, str) and tid.isdigit():
            tid = int(tid)
        sigs = signals_by_trade.get(tid, [])
        item["signals"] = sigs
        item["signal_types"] = ",".join(s["signal_type"] for s in sigs) if sigs else None

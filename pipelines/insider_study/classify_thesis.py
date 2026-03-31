#!/usr/bin/env python3
"""
Assign a primary thesis to each P-code trade using rule-based classification.

Reads indicator columns from the trades table and cluster signals from trade_signals,
then writes thesis labels back to trade_signals as signal_type='thesis'.

Priority order (first match wins):
  1. reversal   -- is_rare_reversal=1 AND consecutive_sells_before >= 3
  2. dip_buy    -- dip_1mo <= -0.15 OR dip_3mo <= -0.25 OR dip_1yr <= -0.30
  3. cluster    -- has trade_signals row with signal_type='top_trade'
  4. momentum   -- above_sma50=1 AND above_sma200=1
  5. value      -- week52_proximity < 0.30 AND NOT matching dip_buy criteria
  6. growth     -- default for remaining P-code buys

Usage:
    python3 pipelines/insider_study/classify_thesis.py
    python3 pipelines/insider_study/classify_thesis.py --since 2024-01-01
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 10_000

# ---------------------------------------------------------------------------
# Thesis classifiers
# ---------------------------------------------------------------------------

# Each classifier returns (label, confidence, metadata_dict) or None.
# Priority order is enforced by the caller -- first match wins.

THESIS_CLASSIFIERS = []


def register_thesis(fn):
    """Register a thesis classifier in priority order."""
    THESIS_CLASSIFIERS.append(fn)
    return fn


@register_thesis
def _reversal(trade: dict, has_cluster: bool) -> tuple[str, float, dict] | None:
    """Rare reversal with sustained prior selling."""
    if trade["is_rare_reversal"] == 1 and (trade["consecutive_sells_before"] or 0) >= 3:
        return (
            "reversal",
            0.90,
            {
                "consecutive_sells_before": trade["consecutive_sells_before"],
                "is_rare_reversal": True,
            },
        )
    return None


@register_thesis
def _dip_buy(trade: dict, has_cluster: bool) -> tuple[str, float, dict] | None:
    """Buying into significant price weakness."""
    dip_1mo = trade["dip_1mo"]
    dip_3mo = trade["dip_3mo"]
    dip_1yr = trade["dip_1yr"]

    reasons = []
    if dip_1mo is not None and dip_1mo <= -0.15:
        reasons.append(f"1mo={dip_1mo:+.1%}")
    if dip_3mo is not None and dip_3mo <= -0.25:
        reasons.append(f"3mo={dip_3mo:+.1%}")
    if dip_1yr is not None and dip_1yr <= -0.30:
        reasons.append(f"1yr={dip_1yr:+.1%}")

    if reasons:
        # Confidence scales with severity of the dip
        worst_dip = min(
            d for d in [dip_1mo, dip_3mo, dip_1yr] if d is not None
        )
        confidence = min(abs(worst_dip), 1.0)
        return (
            "dip_buy",
            round(confidence, 3),
            {"dip_1mo": dip_1mo, "dip_3mo": dip_3mo, "dip_1yr": dip_1yr, "triggers": reasons},
        )
    return None


@register_thesis
def _cluster(trade: dict, has_cluster: bool) -> tuple[str, float, dict] | None:
    """Part of a multi-insider cluster event."""
    if has_cluster:
        return ("cluster", 0.85, {"source": "top_trade_signal"})
    return None


@register_thesis
def _momentum(trade: dict, has_cluster: bool) -> tuple[str, float, dict] | None:
    """Buying into price strength above both SMAs."""
    if trade["above_sma50"] == 1 and trade["above_sma200"] == 1:
        return (
            "momentum",
            0.70,
            {"above_sma50": True, "above_sma200": True},
        )
    return None


@register_thesis
def _value(trade: dict, has_cluster: bool) -> tuple[str, float, dict] | None:
    """Near 52-week low without qualifying as a dip_buy."""
    w52 = trade["week52_proximity"]
    if w52 is not None and w52 < 0.30:
        # Exclude trades that would match dip_buy criteria
        dip_1mo = trade["dip_1mo"]
        dip_3mo = trade["dip_3mo"]
        dip_1yr = trade["dip_1yr"]
        is_dip = (
            (dip_1mo is not None and dip_1mo <= -0.15)
            or (dip_3mo is not None and dip_3mo <= -0.25)
            or (dip_1yr is not None and dip_1yr <= -0.30)
        )
        if not is_dip:
            return (
                "value",
                0.65,
                {"week52_proximity": round(w52, 3)},
            )
    return None


# growth is the fallback -- no explicit classifier needed.


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def classify_trades(conn: sqlite3.Connection, since: str | None = None) -> int:
    """Classify all P-code trades and write thesis signals."""
    # Clear existing thesis signals
    if since:
        # Only delete thesis signals for trades in the date range
        conn.execute("""
            DELETE FROM trade_signals
            WHERE signal_type = 'thesis'
              AND trade_id IN (
                  SELECT trade_id FROM trades WHERE trade_date >= ? AND trans_code = 'P'
              )
        """, (since,))
    else:
        conn.execute("DELETE FROM trade_signals WHERE signal_type = 'thesis'")
    conn.commit()
    logger.info("Cleared existing thesis signals")

    # Load P-code trades with indicator columns
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    trades = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date, t.value,
               t.is_rare_reversal, t.consecutive_sells_before,
               t.dip_1mo, t.dip_3mo, t.dip_1yr,
               t.above_sma50, t.above_sma200,
               t.week52_proximity
        FROM trades t
        WHERE t.trans_code = 'P'
          {where_since}
        ORDER BY t.trade_date
    """).fetchall()
    logger.info("Loaded %d P-code trades", len(trades))

    # Prefetch set of trade_ids that have top_trade signals (for cluster detection)
    cluster_ids = set()
    cluster_rows = conn.execute(f"""
        SELECT ts.trade_id
        FROM trade_signals ts
        JOIN trades t ON ts.trade_id = t.trade_id
        WHERE ts.signal_type = 'top_trade'
          AND t.trans_code = 'P'
          {where_since}
    """).fetchall()
    for r in cluster_rows:
        cluster_ids.add(r["trade_id"])
    logger.info("Found %d trades with top_trade signals", len(cluster_ids))

    # Classify each trade
    inserts = []
    thesis_counts: dict[str, int] = {}
    t0 = time.time()

    for trade in trades:
        trade_dict = dict(trade)
        has_cluster = trade["trade_id"] in cluster_ids

        label = "growth"  # default fallback
        confidence = 0.50
        metadata = {}

        for classifier in THESIS_CLASSIFIERS:
            result = classifier(trade_dict, has_cluster)
            if result is not None:
                label, confidence, metadata = result
                break

        thesis_counts[label] = thesis_counts.get(label, 0) + 1
        inserts.append((
            trade["trade_id"],
            "thesis",
            label,
            "informational",
            confidence,
            json.dumps(metadata),
        ))

        if len(inserts) >= BATCH_SIZE:
            conn.executemany("""
                INSERT OR IGNORE INTO trade_signals
                (trade_id, signal_type, signal_label, signal_class, confidence, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, inserts)
            conn.commit()
            inserts = []

    # Flush remaining
    if inserts:
        conn.executemany("""
            INSERT OR IGNORE INTO trade_signals
            (trade_id, signal_type, signal_label, signal_class, confidence, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, inserts)
        conn.commit()

    elapsed = time.time() - t0
    total = sum(thesis_counts.values())
    logger.info("Classified %d trades in %.1fs", total, elapsed)

    # Print distribution
    print(f"\n=== Thesis Distribution ({total:,} P-code trades) ===\n")
    print(f"{'Thesis':<12} {'Count':>8} {'Pct':>7}")
    print("-" * 30)
    for label in ["reversal", "dip_buy", "cluster", "momentum", "value", "growth"]:
        cnt = thesis_counts.get(label, 0)
        pct = (cnt / total * 100) if total > 0 else 0
        print(f"{label:<12} {cnt:>8,} {pct:>6.1f}%")

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Classify P-code trades by thesis type")
    parser.add_argument("--since", help="Only process trades since this date (YYYY-MM-DD)")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to insiders.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")

    logger.info("Database: %s", args.db)

    n = classify_trades(conn, args.since)
    logger.info("Done: %d thesis labels assigned", n)
    conn.close()


if __name__ == "__main__":
    main()

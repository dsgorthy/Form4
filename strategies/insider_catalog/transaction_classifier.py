#!/usr/bin/env python3
"""
Transaction classifier: assigns signal quality and detects routine patterns.

Phase B of the Data Quality & Scoring Redesign.

Signal quality reflects how informative each transaction type is:
  - Open-market purchases (P) are strongest — cash out of pocket
  - Sales (S) are next — discretionary, but reduced if 10b5-1
  - Option exercises (M) are compensation-related
  - Tax withholding (F), grants (A), gifts (G), vesting (V), RSU (X) are noise

Routine detection identifies programmatic/scheduled selling patterns:
  - 10b5-1 plan flag on filing
  - Footnote text mentioning 10b5-1
  - Regular interval selling at similar dollar values

Usage:
    # Batch classify all trades
    python strategies/insider_catalog/transaction_classifier.py

    # Classify + print analysis
    python strategies/insider_catalog/transaction_classifier.py --analyze
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill import DB_PATH, migrate_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Signal quality weights by transaction code ──

SIGNAL_WEIGHTS = {
    "P": ("open_market_buy", 1.0),
    "S": ("open_market_sell", 0.8),
    "M": ("option_exercise", 0.3),
    "F": ("tax_withholding", 0.1),
    "A": ("award_grant", 0.05),
    "G": ("gift", 0.0),
    "V": ("vesting", 0.0),
    "X": ("rsu_exercise", 0.1),
}

# 10b5-1 discount: routine sells are less informative
ROUTINE_SELL_DISCOUNT = 0.5  # S with 10b5-1 → 0.8 * 0.5 = 0.4


def classify_transaction(
    trans_code: str | None,
    is_10b5_1: int | None,
    trade_type: str | None = None,
) -> dict:
    """
    Classify a single transaction by signal quality.

    Returns dict with:
      - signal_category: human-readable category name
      - signal_quality: 0.0-1.0 quality score
      - is_routine_flag: whether 10b5-1 flag indicates routine
    """
    if not trans_code:
        # Legacy trades without trans_code: infer from trade_type
        if trade_type == "buy":
            return {
                "signal_category": "open_market_buy",
                "signal_quality": 1.0,
                "is_routine_flag": False,
            }
        elif trade_type == "sell":
            quality = 0.8
            is_routine = bool(is_10b5_1)
            if is_routine:
                quality *= ROUTINE_SELL_DISCOUNT
            return {
                "signal_category": "open_market_sell",
                "signal_quality": quality,
                "is_routine_flag": is_routine,
            }
        return {
            "signal_category": "unknown",
            "signal_quality": 0.5,
            "is_routine_flag": False,
        }

    category, base_quality = SIGNAL_WEIGHTS.get(trans_code, ("unknown", 0.5))

    is_routine = False
    quality = base_quality

    # Apply 10b5-1 discount for sells
    if trans_code == "S" and is_10b5_1:
        quality *= ROUTINE_SELL_DISCOUNT
        is_routine = True

    return {
        "signal_category": category,
        "signal_quality": round(quality, 3),
        "is_routine_flag": is_routine,
    }


def detect_routine_pattern(
    conn: sqlite3.Connection,
    insider_id: int,
    ticker: str,
    as_of_date: str,
) -> dict:
    """
    Detect routine selling patterns for an insider at a specific ticker.

    Rules:
      1. is_10b5_1 = 1 on the filing → routine
      2. Footnote text contains "10b5-1" or "Rule 10b5-1" → routine
      3. 3+ sells at same ticker with similar $ value (within 20%)
         at regular intervals (quarterly ±15 days) → routine

    Returns dict with:
      - is_routine: bool
      - reason: str or None
    """
    # Rule 1: Check if any trades for this insider+ticker have 10b5-1 flag
    row = conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE insider_id = ? AND ticker = ? AND is_10b5_1 = 1
          AND trade_date <= ?
    """, (insider_id, ticker, as_of_date)).fetchone()
    if row and row[0] > 0:
        return {"is_routine": True, "reason": "10b5-1 flag on filing"}

    # Rule 2: Check footnotes for 10b5-1 mentions
    row = conn.execute("""
        SELECT COUNT(*) FROM filing_footnotes fn
        JOIN trades t ON fn.accession = t.accession
        WHERE t.insider_id = ? AND t.ticker = ?
          AND t.trade_date <= ?
          AND (LOWER(fn.footnote_text) LIKE '%10b5-1%'
               OR LOWER(fn.footnote_text) LIKE '%rule 10b5%')
    """, (insider_id, ticker, as_of_date)).fetchone()
    if row and row[0] > 0:
        return {"is_routine": True, "reason": "10b5-1 in footnotes"}

    # Rule 3: Regular interval selling pattern
    sells = conn.execute("""
        SELECT trade_date, value FROM trades
        WHERE insider_id = ? AND ticker = ? AND trade_type = 'sell'
          AND trade_date <= ?
        ORDER BY trade_date
    """, (insider_id, ticker, as_of_date)).fetchall()

    if len(sells) >= 3:
        # Check for similar values and regular intervals
        values = [s[1] for s in sells]
        dates = [datetime.strptime(s[0], "%Y-%m-%d") for s in sells]

        # Check value similarity (within 20% of median)
        median_val = sorted(values)[len(values) // 2]
        if median_val > 0:
            similar = sum(1 for v in values if abs(v - median_val) / median_val < 0.20)
            if similar >= 3:
                # Check interval regularity (~90 days ±15)
                intervals = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
                regular = sum(1 for iv in intervals if 75 <= iv <= 105)
                if regular >= 2:
                    return {
                        "is_routine": True,
                        "reason": f"regular interval selling ({similar} similar-value trades, {regular} quarterly intervals)",
                    }

    return {"is_routine": False, "reason": None}


def batch_classify(conn: sqlite3.Connection):
    """
    Classify all trades in the database, populating signal_quality,
    signal_category, and is_routine columns.
    """
    logger.info("Batch classifying all trades...")

    # First pass: classify by trans_code + is_10b5_1
    trades = conn.execute("""
        SELECT trade_id, trans_code, is_10b5_1, trade_type
        FROM trades
    """).fetchall()

    logger.info("Classifying %d trades by transaction code...", len(trades))

    batch = []
    for trade_id, trans_code, is_10b5_1, trade_type in trades:
        result = classify_transaction(trans_code, is_10b5_1, trade_type)
        batch.append((
            result["signal_quality"],
            result["signal_category"],
            1 if result["is_routine_flag"] else 0,
            trade_id,
        ))

    conn.executemany("""
        UPDATE trades SET
            signal_quality = ?,
            signal_category = ?,
            is_routine = ?
        WHERE trade_id = ?
    """, batch)
    conn.commit()
    logger.info("Updated signal_quality/signal_category for %d trades", len(batch))

    # Second pass: detect routine patterns for sell trades
    # Only check insiders with 3+ sells at the same ticker
    candidates = conn.execute("""
        SELECT DISTINCT insider_id, ticker, MAX(trade_date) as latest
        FROM trades
        WHERE trade_type = 'sell' AND is_routine = 0
        GROUP BY insider_id, ticker
        HAVING COUNT(*) >= 3
    """).fetchall()

    logger.info("Checking %d insider-ticker pairs for routine patterns...", len(candidates))

    routine_count = 0
    for insider_id, ticker, latest_date in candidates:
        result = detect_routine_pattern(conn, insider_id, ticker, latest_date)
        if result["is_routine"]:
            conn.execute("""
                UPDATE trades SET is_routine = 1
                WHERE insider_id = ? AND ticker = ? AND trade_type = 'sell'
            """, (insider_id, ticker))
            routine_count += 1

    conn.commit()
    logger.info("Marked %d insider-ticker pairs as routine sellers", routine_count)


def print_analysis(conn: sqlite3.Connection):
    """Print signal quality analysis."""
    print("\n" + "=" * 70)
    print("TRANSACTION CLASSIFICATION ANALYSIS")
    print("=" * 70)

    # Signal category breakdown
    rows = conn.execute("""
        SELECT signal_category, COUNT(*),
               AVG(signal_quality),
               SUM(CASE WHEN is_routine = 1 THEN 1 ELSE 0 END) as routine_count
        FROM trades
        WHERE signal_category IS NOT NULL
        GROUP BY signal_category
        ORDER BY AVG(signal_quality) DESC
    """).fetchall()

    print(f"\n{'Category':<20} {'Count':>8} {'Avg Quality':>12} {'Routine':>8}")
    print("-" * 52)
    for cat, count, avg_q, routine in rows:
        print(f"{cat:<20} {count:>8,} {avg_q:>12.3f} {routine:>8,}")

    # Trans code breakdown with forward returns
    print("\n\nSignal quality vs forward returns (trades with return data):")
    rows = conn.execute("""
        SELECT t.signal_category, COUNT(*),
               AVG(r.abnormal_7d),
               SUM(CASE WHEN r.abnormal_7d > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate
        FROM trades t
        JOIN trade_returns r ON t.trade_id = r.trade_id
        WHERE t.signal_category IS NOT NULL AND r.abnormal_7d IS NOT NULL
        GROUP BY t.signal_category
        ORDER BY AVG(r.abnormal_7d) DESC
    """).fetchall()

    print(f"\n{'Category':<20} {'N':>8} {'Avg Abn 7d':>12} {'Win Rate':>10}")
    print("-" * 54)
    for cat, n, avg_abn, wr in rows:
        print(f"{cat:<20} {n:>8,} {avg_abn:>11.4f} {wr:>9.1%}")

    # Routine vs non-routine sells
    print("\n\nRoutine vs non-routine sells:")
    rows = conn.execute("""
        SELECT
            CASE WHEN t.is_routine = 1 THEN 'routine' ELSE 'discretionary' END as type,
            COUNT(*),
            AVG(r.return_7d),
            AVG(r.abnormal_7d)
        FROM trades t
        JOIN trade_returns r ON t.trade_id = r.trade_id
        WHERE t.trade_type = 'sell' AND r.return_7d IS NOT NULL
        GROUP BY t.is_routine
    """).fetchall()

    print(f"\n{'Type':<15} {'N':>8} {'Avg Ret 7d':>12} {'Avg Abn 7d':>12}")
    print("-" * 50)
    for type_, n, avg_ret, avg_abn in rows:
        print(f"{type_:<15} {n:>8,} {avg_ret:>11.4f} {avg_abn:>11.4f}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Classify trades by signal quality")
    parser.add_argument("--analyze", action="store_true",
                        help="Print analysis after classification")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    migrate_schema(conn)
    batch_classify(conn)

    if args.analyze:
        print_analysis(conn)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()

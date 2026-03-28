#!/usr/bin/env python3
"""
Add recency-weighted scoring to insider_track_records.

Implements exponential decay weighting:
  - Trades from last 1 year:  weight 1.0
  - Trades from 1-2 years ago: weight 0.7
  - Trades from 2-3 years ago: weight 0.5
  - Trades from 3+ years ago:  weight 0.3

Adds/updates columns:
  - score_recency_weighted: recency-weighted composite score (0-3)
  - recent_win_rate_7d: win rate using only last 2 years of trades

Reports tier changes vs original scoring.

Usage:
  python recency_scoring.py
  python recency_scoring.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import statistics
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CATALOG_DIR = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog"
DB_PATH = CATALOG_DIR / "insiders.db"

REFERENCE_DATE = datetime(2026, 3, 11)  # today


def ensure_columns(conn: sqlite3.Connection):
    """Add recency columns if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(insider_track_records)").fetchall()}

    new_cols = [
        ("score_recency_weighted", "REAL"),
        ("recent_win_rate_7d", "REAL"),
        ("tier_recency", "INTEGER"),
    ]

    for col_name, col_type in new_cols:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE insider_track_records ADD COLUMN {col_name} {col_type}")
            logger.info("Added column: %s", col_name)

    conn.commit()


def get_decay_weight(trade_date_str: str) -> float:
    """Exponential decay weight based on trade recency."""
    try:
        trade_date = datetime.strptime(trade_date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return 0.3  # oldest weight for unparseable dates

    years_ago = (REFERENCE_DATE - trade_date).days / 365.25

    if years_ago <= 1.0:
        return 1.0
    elif years_ago <= 2.0:
        return 0.7
    elif years_ago <= 3.0:
        return 0.5
    else:
        return 0.3


def weighted_mean(values: list[float], weights: list[float]) -> float:
    """Weighted arithmetic mean."""
    if not values or not weights:
        return 0.0
    total_w = sum(weights)
    if total_w == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_w


def weighted_std(values: list[float], weights: list[float]) -> float:
    """Weighted standard deviation."""
    if len(values) < 2:
        return 0.0
    mu = weighted_mean(values, weights)
    total_w = sum(weights)
    if total_w == 0:
        return 0.0
    var = sum(w * (v - mu) ** 2 for v, w in zip(values, weights)) / total_w
    return var ** 0.5


def _score_window_weighted(wr: float, avg_abn: float, n_eff: float) -> float:
    """
    Recency-weighted version of _score_window.
    Uses effective N (sum of weights) instead of raw count.
    """
    if wr is None or avg_abn is None or n_eff < 2.0:
        return 0.0
    wr_part = max(0, (wr - 0.4)) * 2.5
    ret_part = max(0, min(1.0, avg_abn * 10 + 0.5))
    n_confidence = max(0, 1.0 - 2.0 / n_eff)
    return (wr_part * 0.5 + ret_part * 0.5) * n_confidence


# Annualization factors (same as walkforward_study.py)
ANNUALIZE = {"7d": (252 / 7) ** 0.5, "30d": (252 / 30) ** 0.5, "90d": (252 / 90) ** 0.5}


def main():
    parser = argparse.ArgumentParser(description="Recency-weighted scoring")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        logger.error("DB not found at %s", DB_PATH)
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    ensure_columns(conn)

    # Get all insiders with buy trades + return data
    insiders = conn.execute("""
        SELECT DISTINCT t.insider_id
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND (tr.abnormal_7d IS NOT NULL OR tr.abnormal_30d IS NOT NULL OR tr.abnormal_90d IS NOT NULL)
    """).fetchall()

    logger.info("Processing %d insiders for recency-weighted scoring", len(insiders))

    # Capture old tiers for comparison
    old_tiers = {}
    for row in conn.execute("SELECT insider_id, score_tier FROM insider_track_records WHERE score_tier IS NOT NULL"):
        old_tiers[row[0]] = row[1]

    raw_scores = []  # (insider_id, raw_score, recent_wr_7d)

    for idx, (insider_id,) in enumerate(insiders):
        if idx % 5000 == 0 and idx > 0:
            logger.info("  Progress: %d/%d (%.0f%%)", idx, len(insiders), 100 * idx / len(insiders))

        # Get all buy trades with dates and returns
        trades = conn.execute("""
            SELECT t.trade_date, t.value,
                   tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ? AND t.trade_type = 'buy'
            ORDER BY t.trade_date
        """, (insider_id,)).fetchall()

        if not trades:
            continue

        # Compute weights
        weights = [get_decay_weight(t[0]) for t in trades]

        # Per-window recency-weighted stats
        window_data = {}
        for w_idx, w in enumerate(["7d", "30d", "90d"]):
            ret_col = 2 + w_idx * 2
            abn_col = 3 + w_idx * 2

            rets = []
            abns = []
            w_weights = []

            for t, wt in zip(trades, weights):
                if t[ret_col] is not None and t[abn_col] is not None:
                    rets.append(t[ret_col])
                    abns.append(t[abn_col])
                    w_weights.append(wt)

            if not rets:
                window_data[w] = None
                continue

            n_eff = sum(w_weights)
            # Weighted win rate: proportion of positive abnormal returns, weighted
            wr = sum(wt for v, wt in zip(abns, w_weights) if v > 0) / sum(w_weights) if sum(w_weights) > 0 else 0
            avg_abn = weighted_mean(abns, w_weights)
            avg_ret = weighted_mean(rets, w_weights)

            window_data[w] = {
                "wr": wr,
                "avg_abn": avg_abn,
                "avg_ret": avg_ret,
                "n_eff": n_eff,
                "quality": _score_window_weighted(wr, avg_abn, n_eff),
            }

        # Recent win rate (last 2 years only) for 7d
        recent_trades_7d = [(t[2], t[3]) for t, wt in zip(trades, weights) if wt >= 0.7 and t[2] is not None]
        recent_wr_7d = None
        if recent_trades_7d:
            # For buy-side, win = positive return
            wins = sum(1 for ret, abn in recent_trades_7d if ret is not None and ret > 0)
            recent_wr_7d = wins / len(recent_trades_7d)

        # Composite score (same formula as _compute_scores in backfill.py, but weighted)
        valid_windows = [(w, window_data[w]) for w in ["7d", "30d", "90d"] if window_data[w] is not None]

        if not valid_windows or all(d["n_eff"] < 2 for _, d in valid_windows):
            continue

        best_w, best_data = max(valid_windows, key=lambda x: x[1]["quality"])
        best_quality = best_data["quality"]

        # Multi-window consistency
        positive_windows = sum(1 for _, d in valid_windows if d["quality"] > 0.3)
        consistency = min(1.0, positive_windows / 3.0)

        # Horizon bonus
        horizon_bonus = 0.0
        for w, d in valid_windows:
            if w == "30d" and d["avg_abn"] > 0:
                horizon_bonus += 0.4
            elif w == "90d" and d["avg_abn"] > 0:
                horizon_bonus += 0.6
        horizon_bonus = min(1.0, horizon_bonus)

        # Frequency (weighted effective N)
        total_n_eff = sum(d["n_eff"] for _, d in valid_windows)
        freq_score = min(1.0, math.log2(max(1, total_n_eff)) / 5)

        # Size
        total_val = sum(t[1] or 0 for t in trades)
        size_score = min(1.0, math.log10(max(1, total_val)) / 8)

        # Breadth
        breadth = len(valid_windows) / 3.0

        raw = (
            best_quality * 0.40 +
            horizon_bonus * 0.15 +
            consistency * 0.15 +
            freq_score * 0.15 +
            size_score * 0.05 +
            breadth * 0.10
        ) * 3.0

        raw_scores.append((insider_id, raw, recent_wr_7d))

    logger.info("Computed recency scores for %d insiders", len(raw_scores))

    # Sort and assign tiers (same percentile thresholds as original)
    raw_scores.sort(key=lambda x: x[1])
    n = len(raw_scores)

    tier_changes = {"upgraded": 0, "downgraded": 0, "unchanged": 0, "new": 0}
    tier_distribution = {0: 0, 1: 0, 2: 0, 3: 0}

    for rank, (insider_id, raw, recent_wr_7d) in enumerate(raw_scores):
        percentile = (rank + 1) / n * 100
        score = min(3.0, max(0.0, raw))

        if percentile >= 93:
            tier = 3
        elif percentile >= 80:
            tier = 2
        elif percentile >= 67:
            tier = 1
        else:
            tier = 0

        tier_distribution[tier] += 1

        # Track tier changes
        old_tier = old_tiers.get(insider_id)
        if old_tier is None:
            tier_changes["new"] += 1
        elif tier > old_tier:
            tier_changes["upgraded"] += 1
        elif tier < old_tier:
            tier_changes["downgraded"] += 1
        else:
            tier_changes["unchanged"] += 1

        if not args.dry_run:
            conn.execute("""
                UPDATE insider_track_records
                SET score_recency_weighted = ?,
                    recent_win_rate_7d = ?,
                    tier_recency = ?,
                    computed_at = datetime('now')
                WHERE insider_id = ?
            """, (
                round(score, 4),
                round(recent_wr_7d, 6) if recent_wr_7d is not None else None,
                tier,
                insider_id,
            ))

    if not args.dry_run:
        conn.commit()

    # Report
    logger.info("=" * 60)
    logger.info("RECENCY-WEIGHTED SCORING COMPLETE")
    logger.info("=" * 60)
    logger.info("  Total scored: %d", n)
    logger.info("  Tier distribution (recency-weighted):")
    for tier in [3, 2, 1, 0]:
        logger.info("    Tier %d: %d insiders", tier, tier_distribution[tier])

    logger.info("")
    logger.info("  Tier changes (recency vs original):")
    logger.info("    Upgraded:   %d", tier_changes["upgraded"])
    logger.info("    Downgraded: %d", tier_changes["downgraded"])
    logger.info("    Unchanged:  %d", tier_changes["unchanged"])
    logger.info("    New (no old tier): %d", tier_changes["new"])

    # Show some examples of tier changes
    if not args.dry_run:
        # Upgraded examples
        upgrades = conn.execute("""
            SELECT i.name, tr.score_tier, tr.tier_recency, tr.score, tr.score_recency_weighted,
                   tr.buy_count, tr.primary_title, tr.primary_ticker
            FROM insider_track_records tr
            JOIN insiders i ON tr.insider_id = i.insider_id
            WHERE tr.score_tier IS NOT NULL
              AND tr.tier_recency IS NOT NULL
              AND tr.tier_recency > tr.score_tier
            ORDER BY tr.tier_recency - tr.score_tier DESC, tr.score_recency_weighted DESC
            LIMIT 10
        """).fetchall()

        if upgrades:
            logger.info("\n  Top 10 UPGRADED insiders:")
            for name, old_t, new_t, old_s, new_s, count, title, ticker in upgrades:
                logger.info("    %s (%s @ %s) — T%d->T%d | score %.2f->%.2f | %d trades",
                             name, title or "?", ticker or "?", old_t, new_t,
                             old_s or 0, new_s or 0, count)

        # Downgraded examples
        downgrades = conn.execute("""
            SELECT i.name, tr.score_tier, tr.tier_recency, tr.score, tr.score_recency_weighted,
                   tr.buy_count, tr.primary_title, tr.primary_ticker
            FROM insider_track_records tr
            JOIN insiders i ON tr.insider_id = i.insider_id
            WHERE tr.score_tier IS NOT NULL
              AND tr.tier_recency IS NOT NULL
              AND tr.tier_recency < tr.score_tier
            ORDER BY tr.score_tier - tr.tier_recency DESC, tr.score DESC
            LIMIT 10
        """).fetchall()

        if downgrades:
            logger.info("\n  Top 10 DOWNGRADED insiders:")
            for name, old_t, new_t, old_s, new_s, count, title, ticker in downgrades:
                logger.info("    %s (%s @ %s) — T%d->T%d | score %.2f->%.2f | %d trades",
                             name, title or "?", ticker or "?", old_t, new_t,
                             old_s or 0, new_s or 0, count)

        # Recent win rate summary
        recent_stats = conn.execute("""
            SELECT COUNT(*),
                   AVG(recent_win_rate_7d) * 100,
                   AVG(CASE WHEN tier_recency >= 2 THEN recent_win_rate_7d END) * 100,
                   AVG(CASE WHEN tier_recency = 0 THEN recent_win_rate_7d END) * 100
            FROM insider_track_records
            WHERE recent_win_rate_7d IS NOT NULL
        """).fetchone()

        logger.info("\n  Recent win rate (last 2 years):")
        logger.info("    All insiders: %.1f%% (N=%d)", recent_stats[1] or 0, recent_stats[0] or 0)
        logger.info("    Tier 2+: %.1f%%", recent_stats[2] or 0)
        logger.info("    Tier 0: %.1f%%", recent_stats[3] or 0)

    conn.close()


if __name__ == "__main__":
    main()

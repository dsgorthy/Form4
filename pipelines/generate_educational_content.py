#!/usr/bin/env python3
"""Generate evergreen educational video scripts and visual assets for Form4 social media.

Pulls from validated research findings stored in the insider trading database.
Each topic produces a storyboard, data visualization card, hook card, CTA card,
and platform captions.

Usage:
    python3 pipelines/generate_educational_content.py --topic routine_filter
    python3 pipelines/generate_educational_content.py --list          # show all topics
    python3 pipelines/generate_educational_content.py --all           # generate all
    python3 pipelines/generate_educational_content.py --topic cluster_buys --audio
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
BRAND_DIR = Path(__file__).resolve().parent.parent / "brand"
OUTPUT_BASE = Path(__file__).resolve().parent / "data" / "content" / "educational"
WIDTH = 1080
HEIGHT = 1920

# Brand palette (matches render_video_assets.py)
MIDNIGHT = "#0A0A0F"
SLATE = "#12121A"
CLOUD = "#E8E8ED"
STEEL = "#8888A0"
FOG = "#55556A"
SIGNAL_BLUE = "#3B82F6"
ALPHA_GREEN = "#22C55E"
RISK_RED = "#EF4444"
AMBER = "#F59E0B"

BASE_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    width: 1080px; height: 1920px;
    background: """ + MIDNIGHT + """;
    color: """ + CLOUD + """;
    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Inter', sans-serif;
    display: flex; flex-direction: column;
    justify-content: center;
    overflow: hidden;
}
.card {
    background: """ + SLATE + """;
    border: 1px solid #2A2A3A;
    border-radius: 20px;
    padding: 44px;
    width: 100%;
}
.stat-big {
    font-size: 96px; font-weight: 800;
    font-family: monospace; line-height: 1.1;
}
.stat-label {
    font-size: 26px; color: """ + STEEL + """;
    margin-top: 12px; line-height: 1.5;
}
.divider {
    width: 100%; height: 1px;
    background: linear-gradient(90deg, transparent, #2A2A3A, transparent);
    margin: 36px 0;
}
.comparison-row {
    display: flex; justify-content: space-between;
    align-items: center; padding: 20px 0;
}
.comparison-label {
    font-size: 24px; color: """ + FOG + """;
    flex: 1;
}
.comparison-value {
    font-size: 36px; font-weight: 700;
    font-family: monospace;
}
.bar-container {
    width: 100%; height: 44px;
    background: rgba(255,255,255,0.04);
    border-radius: 10px; overflow: hidden;
    margin-top: 8px;
}
.bar-fill {
    height: 100%; border-radius: 10px;
    display: flex; align-items: center;
    padding-left: 16px; font-weight: 700;
    font-size: 18px;
}
"""


# ---------------------------------------------------------------------------
# Topic definitions
# ---------------------------------------------------------------------------

TOPICS: dict[str, dict] = {
    "routine_filter": {
        "title": "This One Filter Turns Sell Noise Into Signal",
        "hook": "90% of insider sells are meaningless. Here's the filter that changes everything.",
        "key_stat_label": "Sharpe improvement",
        "key_stat_format": "-0.03 to 0.53",
        "description": "The routine sell filter transforms the sell signal from Sharpe -0.03 to 0.53.",
    },
    "rare_reversals": {
        "title": "When Insiders Break Their Pattern",
        "hook": "When an insider who's only sold for years suddenly buys, it beats the market by 3.6% in 30 days.",
        "key_stat_label": "30-day excess return",
        "key_stat_format": "+3.6%",
        "description": "Rare reversals beat market by 3.6% within 30 days.",
    },
    "cluster_buys": {
        "title": "3 Insiders, 1 Stock, 1 Week",
        "hook": "When three or more insiders buy the same stock in the same week, pay attention.",
        "key_stat_label": "cluster events",
        "key_stat_format": "{count}",
        "description": "When 3+ insiders buy the same ticker the same week, it outperforms.",
    },
    "52week_high": {
        "title": "They're Buying at the Top — And Winning",
        "hook": "Everyone says don't buy at all-time highs. Insiders disagree — and earn 12% annual alpha doing it.",
        "key_stat_label": "annual alpha",
        "key_stat_format": "12%",
        "description": "Insiders buying at 52-week highs earn 12% annual alpha.",
    },
    "signal_grading": {
        "title": "Not All Insider Buys Are Equal",
        "hook": "We grade every insider trade from A to F. A-grade buys beat the market by 2.1% in 30 days.",
        "key_stat_label": "A-grade 30d excess return",
        "key_stat_format": "+2.1%",
        "description": "A-grade buys average 2.1% above market within 30 days.",
    },
    "sell_signal": {
        "title": "Why Most Insider Sells Are Noise",
        "hook": "An insider selling stock sounds scary. But most of the time, it means absolutely nothing.",
        "key_stat_label": "sells that are routine",
        "key_stat_format": "{pct}%",
        "description": "Only opportunistic, non-routine sells predict underperformance.",
    },
    "proven_sellers": {
        "title": "The Insiders With a Track Record",
        "hook": "Some insiders sell stock and it drops. Every. Single. Time. We track them.",
        "key_stat_label": "Sharpe (accuracy >=60%)",
        "key_stat_format": "1.75",
        "description": "Proven seller accuracy >=60% yields Sharpe 1.75 on stock returns.",
    },
    "holdings_change": {
        "title": "How Much Skin They Put in the Game",
        "hook": "A CEO buying $50K in stock sounds bullish. But what if they already own $500 million?",
        "key_stat_label": "outperformance threshold",
        "key_stat_format": ">=10%",
        "description": "Holdings increase >=10% outperforms <10%.",
    },
}


# ---------------------------------------------------------------------------
# Database queries — one per topic
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    return conn


def query_routine_filter(conn: sqlite3.Connection) -> dict:
    """Compare returns for routine vs non-routine sells."""
    row_routine = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'sell'
          AND t.cohen_routine = 1
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    row_opp = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'sell'
          AND t.cohen_routine = 0
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    return {
        "routine_count": row_routine["cnt"],
        "routine_avg_return": round(row_routine["avg_ret_30d"] * 100, 2) if row_routine["avg_ret_30d"] else 0,
        "routine_excess": round(row_routine["avg_excess_30d"] * 100, 2) if row_routine["avg_excess_30d"] else 0,
        "opportunistic_count": row_opp["cnt"],
        "opp_avg_return": round(row_opp["avg_ret_30d"] * 100, 2) if row_opp["avg_ret_30d"] else 0,
        "opp_excess": round(row_opp["avg_excess_30d"] * 100, 2) if row_opp["avg_excess_30d"] else 0,
    }


def query_rare_reversals(conn: sqlite3.Connection) -> dict:
    """Get average returns for rare reversal trades."""
    row = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.spy_return_30d) AS avg_spy_30d,
               AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.is_rare_reversal = 1
          AND t.trade_type = 'buy'
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    return {
        "count": row["cnt"],
        "avg_return_30d": round(row["avg_ret_30d"] * 100, 2) if row["avg_ret_30d"] else 0,
        "avg_spy_30d": round(row["avg_spy_30d"] * 100, 2) if row["avg_spy_30d"] else 0,
        "avg_excess_30d": round(row["avg_excess_30d"] * 100, 2) if row["avg_excess_30d"] else 0,
    }


def query_cluster_buys(conn: sqlite3.Connection) -> dict:
    """Count cluster events: 3+ insiders buying same ticker in same week."""
    rows = conn.execute("""
        SELECT ticker,
               strftime('%Y-%W', trade_date) AS trade_week,
               COUNT(DISTINCT insider_id) AS n_insiders,
               SUM(value) AS total_value
        FROM trades
        WHERE trade_type = 'buy'
          AND trans_code = 'P'
          AND (is_duplicate = 0 OR is_duplicate IS NULL)
        GROUP BY ticker, trade_week
        HAVING COUNT(DISTINCT insider_id) >= 3
        ORDER BY n_insiders DESC
    """).fetchall()

    cluster_events = [dict(r) for r in rows]
    top_clusters = cluster_events[:5]
    avg_insiders = (
        sum(c["n_insiders"] for c in cluster_events) / len(cluster_events)
        if cluster_events else 0
    )

    return {
        "total_cluster_events": len(cluster_events),
        "avg_insiders_per_cluster": round(avg_insiders, 1),
        "top_clusters": top_clusters,
    }


def query_52week_high(conn: sqlite3.Connection) -> dict:
    """Get returns for insiders buying near 52-week highs."""
    row_high = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d,
               AVG(tr.return_90d) AS avg_ret_90d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.week52_proximity >= 0.8
          AND t.trade_type = 'buy'
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    row_all = conn.execute("""
        SELECT AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.return_90d) AS avg_ret_90d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    return {
        "high_count": row_high["cnt"],
        "high_avg_return_30d": round(row_high["avg_ret_30d"] * 100, 2) if row_high["avg_ret_30d"] else 0,
        "high_excess_30d": round(row_high["avg_excess_30d"] * 100, 2) if row_high["avg_excess_30d"] else 0,
        "high_avg_return_90d": round(row_high["avg_ret_90d"] * 100, 2) if row_high["avg_ret_90d"] else 0,
        "all_buy_avg_30d": round(row_all["avg_ret_30d"] * 100, 2) if row_all["avg_ret_30d"] else 0,
        "all_buy_avg_90d": round(row_all["avg_ret_90d"] * 100, 2) if row_all["avg_ret_90d"] else 0,
    }


def query_signal_grading(conn: sqlite3.Connection) -> dict:
    """Compare returns by signal grade."""
    results = {}
    for grade in ("A", "B", "C"):
        row = conn.execute("""
            SELECT COUNT(*) AS cnt,
                   AVG(tr.return_30d) AS avg_ret_30d,
                   AVG(tr.spy_return_30d) AS avg_spy_30d,
                   AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.signal_grade = ?
              AND t.trade_type = 'buy'
              AND tr.return_30d IS NOT NULL
        """, (grade,)).fetchone()
        results[grade] = {
            "count": row["cnt"],
            "avg_return_30d": round(row["avg_ret_30d"] * 100, 2) if row["avg_ret_30d"] else 0,
            "avg_excess_30d": round(row["avg_excess_30d"] * 100, 2) if row["avg_excess_30d"] else 0,
        }

    return results


def query_sell_signal(conn: sqlite3.Connection) -> dict:
    """Break down sell trades by routine classification."""
    row_total = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM trades
        WHERE trade_type = 'sell'
          AND trans_code = 'S'
          AND (is_duplicate = 0 OR is_duplicate IS NULL)
    """).fetchone()

    row_routine = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM trades
        WHERE trade_type = 'sell'
          AND trans_code = 'S'
          AND (is_duplicate = 0 OR is_duplicate IS NULL)
          AND (is_routine = 1 OR is_10b5_1 = 1 OR cohen_routine = 1)
    """).fetchone()

    row_opp = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'sell'
          AND t.trans_code = 'S'
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND t.is_routine = 0
          AND t.is_10b5_1 = 0
          AND t.cohen_routine = 0
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    total = row_total["cnt"] or 1
    routine = row_routine["cnt"] or 0
    pct_routine = round(routine / total * 100, 1)

    return {
        "total_sells": total,
        "routine_count": routine,
        "pct_routine": pct_routine,
        "opp_count": row_opp["cnt"],
        "opp_avg_return_30d": round(row_opp["avg_ret_30d"] * 100, 2) if row_opp["avg_ret_30d"] else 0,
    }


def query_proven_sellers(conn: sqlite3.Connection) -> dict:
    """Get stats on insiders with high sell accuracy from insider_ticker_scores."""
    # Count insiders with sell accuracy >= 60% (minimum 3 scored sells)
    try:
        rows = conn.execute("""
            SELECT COUNT(*) AS cnt,
                   AVG(sell_accuracy) AS avg_accuracy
            FROM insider_ticker_scores
            WHERE sell_accuracy >= 0.6
              AND sell_count >= 3
        """).fetchone()
        proven_count = rows["cnt"] if rows["cnt"] else 0
        avg_accuracy = round(rows["avg_accuracy"] * 100, 1) if rows["avg_accuracy"] else 0
    except sqlite3.OperationalError:
        # Table may not exist — fall back to trade-level stats
        proven_count = 0
        avg_accuracy = 0

    # Also count total scored sellers for comparison
    try:
        row_all = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM insider_ticker_scores
            WHERE sell_count >= 3
        """).fetchone()
        total_scored = row_all["cnt"] if row_all["cnt"] else 0
    except sqlite3.OperationalError:
        total_scored = 0

    return {
        "proven_count": proven_count,
        "avg_accuracy": avg_accuracy,
        "total_scored_sellers": total_scored,
        "sharpe": 1.75,
    }


def query_holdings_change(conn: sqlite3.Connection) -> dict:
    """Compare returns based on holdings increase percentage."""
    # Holdings increase >= 10%
    row_big = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.shares_owned_after IS NOT NULL
          AND t.qty IS NOT NULL
          AND t.qty > 0
          AND t.shares_owned_after > t.qty
          AND CAST(t.qty AS REAL) / (t.shares_owned_after - t.qty) >= 0.10
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    # Holdings increase < 10%
    row_small = conn.execute("""
        SELECT COUNT(*) AS cnt,
               AVG(tr.return_30d) AS avg_ret_30d,
               AVG(tr.return_30d - tr.spy_return_30d) AS avg_excess_30d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.shares_owned_after IS NOT NULL
          AND t.qty IS NOT NULL
          AND t.qty > 0
          AND t.shares_owned_after > t.qty
          AND CAST(t.qty AS REAL) / (t.shares_owned_after - t.qty) < 0.10
          AND tr.return_30d IS NOT NULL
    """).fetchone()

    return {
        "big_increase_count": row_big["cnt"],
        "big_avg_return_30d": round(row_big["avg_ret_30d"] * 100, 2) if row_big["avg_ret_30d"] else 0,
        "big_excess_30d": round(row_big["avg_excess_30d"] * 100, 2) if row_big["avg_excess_30d"] else 0,
        "small_increase_count": row_small["cnt"],
        "small_avg_return_30d": round(row_small["avg_ret_30d"] * 100, 2) if row_small["avg_ret_30d"] else 0,
        "small_excess_30d": round(row_small["avg_excess_30d"] * 100, 2) if row_small["avg_excess_30d"] else 0,
    }


QUERY_FUNCS = {
    "routine_filter": query_routine_filter,
    "rare_reversals": query_rare_reversals,
    "cluster_buys": query_cluster_buys,
    "52week_high": query_52week_high,
    "signal_grading": query_signal_grading,
    "sell_signal": query_sell_signal,
    "proven_sellers": query_proven_sellers,
    "holdings_change": query_holdings_change,
}


# ---------------------------------------------------------------------------
# Video scripts (storyboard format)
# ---------------------------------------------------------------------------

def script_routine_filter(data: dict) -> str:
    return "\n".join([
        "=== FORM4 EDUCATIONAL — Routine Sell Filter ===",
        "~45 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"Ninety percent of insider sells are meaningless. Here\'s the one filter that changes everything."',
        "",
        "--- DATA (0:04-0:20) ---",
        "[SHOW: assets/data_card.png]",
        '"When we look at all insider sells — every single one — the signal is basically zero. Sharpe ratio negative 0.03. That\'s noise."',
        "",
        "[BEAT — 1 second]",
        '"But when you filter out the routine stuff — the pre-scheduled sales plans, the regular quarterly dumps — the Sharpe jumps to 0.53."',
        "",
        "--- EXPLAIN (0:20-0:35) ---",
        "[SHOW: assets/data_card.png]",
        f'"We looked at {data["routine_count"]:,} routine sells versus {data["opportunistic_count"]:,} opportunistic ones."',
        f'"Routine sells averaged {data["routine_excess"]:+.1f}% excess return. Opportunistic? {data["opp_excess"]:+.1f}%."',
        '"The difference is massive. Routine sells happen on autopilot — the insider isn\'t making a judgment call. Opportunistic sells? That\'s someone choosing to exit."',
        "",
        "--- CTA (0:35-0:45) ---",
        "[SHOW: assets/cta.png]",
        '"We flag every sell as routine or opportunistic on Form4. Link in bio — free trial, no credit card."',
    ])


def script_rare_reversals(data: dict) -> str:
    return "\n".join([
        "=== FORM4 EDUCATIONAL — Rare Reversals ===",
        "~40 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"When an insider who\'s only been selling for years suddenly buys — that\'s one of the strongest signals in the market."',
        "",
        "--- DATA (0:04-0:18) ---",
        "[SHOW: assets/data_card.png]",
        f'"We tracked {data["count"]:,} of these rare reversal buys."',
        f'"Average 30-day return: {data["avg_return_30d"]:+.1f}%. The market averaged {data["avg_spy_30d"]:+.1f}% over those same windows."',
        f'"That\'s {data["avg_excess_30d"]:+.1f}% excess return in just one month."',
        "",
        "--- EXPLAIN (0:18-0:32) ---",
        "[SHOW: assets/data_card.png]",
        '"Think about what this means. An insider who\'s been cashing out — getting their money out, diversifying, whatever — suddenly reverses and puts money back in."',
        '"Something changed. They know something the market hasn\'t priced in yet."',
        "",
        "--- CTA (0:32-0:40) ---",
        "[SHOW: assets/cta.png]",
        '"We flag every rare reversal the moment it hits the SEC filing. form4.app, link in bio."',
    ])


def script_cluster_buys(data: dict) -> str:
    top = data.get("top_clusters", [])
    example = ""
    if top:
        best = top[0]
        example = f'"{best["ticker"]} once had {best["n_insiders"]} insiders buying in the same week. That kind of coordination is rare — and it\'s a signal."'

    return "\n".join([
        "=== FORM4 EDUCATIONAL — Cluster Buys ===",
        "~45 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"When three or more insiders at the same company all buy stock the same week — pay attention."',
        "",
        "--- DATA (0:04-0:18) ---",
        "[SHOW: assets/data_card.png]",
        f'"We\'ve identified {data["total_cluster_events"]:,} cluster buy events — three or more insiders, same ticker, same week."',
        example,
        "",
        "--- EXPLAIN (0:18-0:35) ---",
        "[SHOW: assets/data_card.png]",
        '"One insider buying could mean anything. Two is interesting. But three or more? They\'re not coordinating — insider trading is illegal. They\'re each independently deciding the stock is undervalued."',
        '"That convergence of independent conviction is one of the strongest signals we track."',
        "",
        "--- CTA (0:35-0:45) ---",
        "[SHOW: assets/cta.png]",
        '"We detect cluster buys automatically and alert you in real time. form4.app, link in bio."',
    ])


def script_52week_high(data: dict) -> str:
    return "\n".join([
        "=== FORM4 EDUCATIONAL — 52-Week High Buys ===",
        "~40 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"Everyone says don\'t buy at all-time highs. Insiders disagree — and they earn 12% annual alpha doing it."',
        "",
        "--- DATA (0:04-0:18) ---",
        "[SHOW: assets/data_card.png]",
        f'"We looked at {data["high_count"]:,} insider buys that happened when the stock was within 20% of its 52-week high."',
        f'"Average 30-day return: {data["high_avg_return_30d"]:+.1f}%. All insider buys averaged {data["all_buy_avg_30d"]:+.1f}%."',
        f'"That\'s {data["high_excess_30d"]:+.1f}% excess over the market in just a month."',
        "",
        "--- EXPLAIN (0:18-0:32) ---",
        "[SHOW: assets/data_card.png]",
        '"The psychology makes sense. When an insider buys at the top, they\'re saying: I know what\'s coming, and this price is still cheap."',
        '"They have the earnings calls, the contracts, the pipeline. If they\'re buying at the high, there\'s a reason."',
        "",
        "--- CTA (0:32-0:40) ---",
        "[SHOW: assets/cta.png]",
        '"We tag every insider buy that happens near a 52-week high. form4.app, link in bio."',
    ])


def script_signal_grading(data: dict) -> str:
    a = data.get("A", {})
    b = data.get("B", {})
    c = data.get("C", {})
    return "\n".join([
        "=== FORM4 EDUCATIONAL — Signal Grading ===",
        "~45 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"Not every insider buy is a good signal. We grade them — and the spread between A and C is huge."',
        "",
        "--- DATA (0:04-0:22) ---",
        "[SHOW: assets/data_card.png]",
        f'"A-grade buys — we\'ve seen {a["count"]:,} of them — averaged {a["avg_excess_30d"]:+.1f}% above the market in 30 days."',
        f'"B-grade: {b["avg_excess_30d"]:+.1f}%. C-grade: {c["avg_excess_30d"]:+.1f}%."',
        '"The grading system looks at who\'s trading, their track record, the timing, the context — everything."',
        "",
        "--- EXPLAIN (0:22-0:36) ---",
        "[SHOW: assets/data_card.png]",
        '"An A-grade is a C-suite officer with a history of good calls, buying opportunistically, near an inflection point."',
        '"A C-grade might be a director making a small routine purchase. Same SEC filing. Completely different signal."',
        "",
        "--- CTA (0:36-0:45) ---",
        "[SHOW: assets/cta.png]",
        '"Every trade on Form4 gets an automatic signal grade. form4.app, free trial, link in bio."',
    ])


def script_sell_signal(data: dict) -> str:
    return "\n".join([
        "=== FORM4 EDUCATIONAL — Sell Signal ===",
        "~45 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"An insider sold stock. Should you panic? Probably not. Here\'s why."',
        "",
        "--- DATA (0:04-0:20) ---",
        "[SHOW: assets/data_card.png]",
        f'"Out of {data["total_sells"]:,} insider sells in our database, {data["pct_routine"]:.0f}% are routine."',
        '"Pre-planned. Automated. Scheduled months in advance under Rule 10b5-1 plans."',
        '"These tell you nothing about the insider\'s view on the stock."',
        "",
        "--- EXPLAIN (0:20-0:36) ---",
        "[SHOW: assets/data_card.png]",
        f'"But the other {100 - data["pct_routine"]:.0f}%? Opportunistic sells. The insider chose to sell, right now, at this price."',
        f'"Those {data["opp_count"]:,} opportunistic sells averaged {data["opp_avg_return_30d"]:+.1f}% over 30 days."',
        '"That\'s the real signal. The sell itself isn\'t bearish — it\'s whether the insider chose to sell or it was on autopilot."',
        "",
        "--- CTA (0:36-0:45) ---",
        "[SHOW: assets/cta.png]",
        '"We label every sell as routine or opportunistic so you don\'t have to guess. form4.app, link in bio."',
    ])


def script_proven_sellers(data: dict) -> str:
    return "\n".join([
        "=== FORM4 EDUCATIONAL — Proven Sellers ===",
        "~40 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"Some insiders sell stock — and it drops. Every. Single. Time. We track them."',
        "",
        "--- DATA (0:04-0:18) ---",
        "[SHOW: assets/data_card.png]",
        f'"We score every insider\'s track record. {data["proven_count"]:,} insiders out of {data["total_scored_sellers"]:,} have a sell accuracy of 60% or higher — meaning the stock drops after they sell, most of the time."',
        '"Following only proven sellers gives you a Sharpe ratio of 1.75 on the short side. That\'s exceptional."',
        "",
        "--- EXPLAIN (0:18-0:30) ---",
        "[SHOW: assets/data_card.png]",
        '"Not all insiders are good traders. Some sell for tax reasons, divorce settlements, estate planning. Their sells don\'t mean anything."',
        '"But the ones who consistently sell before drops? They have real edge. And now you can follow them."',
        "",
        "--- CTA (0:30-0:40) ---",
        "[SHOW: assets/cta.png]",
        '"Every insider on Form4 has a scored track record. form4.app, link in bio."',
    ])


def script_holdings_change(data: dict) -> str:
    return "\n".join([
        "=== FORM4 EDUCATIONAL — Holdings Change ===",
        "~45 sec estimated",
        "",
        "--- HOOK (0:00-0:04) ---",
        "[SHOW: assets/hook.png]",
        '"A CEO buying fifty thousand dollars in stock sounds bullish. But what if they already own 500 million?"',
        "",
        "--- DATA (0:04-0:20) ---",
        "[SHOW: assets/data_card.png]",
        f'"We split insider buys by how much they increased their holdings. {data["big_increase_count"]:,} buys increased holdings by 10% or more."',
        f'"Those averaged {data["big_excess_30d"]:+.1f}% excess return over 30 days."',
        f'"Buys that increased holdings by less than 10%? {data["small_excess_30d"]:+.1f}%."',
        "",
        "--- EXPLAIN (0:20-0:36) ---",
        "[SHOW: assets/data_card.png]",
        '"The size of the buy relative to what they already own tells you how much conviction they have."',
        '"A ten percent increase in your position is meaningful. A one percent increase? That\'s a rounding error."',
        '"Raw dollar value is misleading. It\'s the percentage change that matters."',
        "",
        "--- CTA (0:36-0:45) ---",
        "[SHOW: assets/cta.png]",
        '"We show holdings change on every trade. form4.app, link in bio."',
    ])


SCRIPT_FUNCS = {
    "routine_filter": script_routine_filter,
    "rare_reversals": script_rare_reversals,
    "cluster_buys": script_cluster_buys,
    "52week_high": script_52week_high,
    "signal_grading": script_signal_grading,
    "sell_signal": script_sell_signal,
    "proven_sellers": script_proven_sellers,
    "holdings_change": script_holdings_change,
}


# ---------------------------------------------------------------------------
# Data visualization cards (1080x1920 HTML)
# ---------------------------------------------------------------------------

def _logo_html() -> str:
    """Brand logo for cards, with fallback."""
    from pipelines.render_video_assets import _img_data_uri
    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4.png")
    if logo_uri:
        return f'<img src="{logo_uri}" style="width:40%;max-height:80px;object-fit:contain" />'
    return f'<div style="font-size:48px;font-weight:800">Form<span style="color:{SIGNAL_BLUE}">4</span></div>'


def card_hook_educational(topic: dict) -> str:
    """Render hook card for an educational topic."""
    logo = _logo_html()
    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 80px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:100px">{logo}</div>
    <div style="font-size:52px;font-weight:800;line-height:1.25;max-width:900px">{topic["title"]}</div>
    <div style="margin-top:60px">
        <div style="font-size:22px;color:{FOG};text-transform:uppercase;letter-spacing:4px;font-weight:600">Research Insight</div>
        <div style="font-size:24px;color:{STEEL};margin-top:16px">{topic["description"]}</div>
    </div>
    </body></html>"""


def card_data_routine_filter(data: dict) -> str:
    """Data card: routine vs opportunistic sell comparison."""
    bar_routine_w = min(max(abs(data["routine_excess"]) * 10, 10), 95)
    bar_opp_w = min(max(abs(data["opp_excess"]) * 10, 10), 95)
    routine_color = RISK_RED if data["routine_excess"] < 0 else ALPHA_GREEN
    opp_color = RISK_RED if data["opp_excess"] < 0 else ALPHA_GREEN

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Sell Signal Sharpe Ratio</div>
        <div class="stat-big" style="margin-top:20px">
            <span style="color:{RISK_RED}">-0.03</span>
            <span style="color:{FOG};font-size:48px;margin:0 20px">vs</span>
            <span style="color:{ALPHA_GREEN}">0.53</span>
        </div>
        <div class="stat-label">All sells vs filtered sells</div>
    </div>

    <div class="card">
        <div style="margin-bottom:36px">
            <div style="font-size:22px;color:{FOG};margin-bottom:12px">Routine Sells ({data["routine_count"]:,})</div>
            <div class="bar-container">
                <div class="bar-fill" style="width:{bar_routine_w}%;background:{routine_color}">{data["routine_excess"]:+.1f}%</div>
            </div>
        </div>
        <div>
            <div style="font-size:22px;color:{FOG};margin-bottom:12px">Opportunistic Sells ({data["opportunistic_count"]:,})</div>
            <div class="bar-container">
                <div class="bar-fill" style="width:{bar_opp_w}%;background:{opp_color}">{data["opp_excess"]:+.1f}%</div>
            </div>
        </div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_rare_reversals(data: dict) -> str:
    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Rare Reversal Buys</div>
    </div>

    <div class="card" style="text-align:center">
        <div class="stat-big" style="color:{ALPHA_GREEN}">{data["avg_excess_30d"]:+.1f}%</div>
        <div class="stat-label">30-day excess return vs S&P 500</div>
        <div class="divider"></div>
        <div style="display:flex;justify-content:space-around">
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{CLOUD}">{data["count"]:,}</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">events tracked</div>
            </div>
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{CLOUD}">{data["avg_return_30d"]:+.1f}%</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">avg 30d return</div>
            </div>
        </div>
    </div>

    <div style="margin-top:60px;max-width:800px">
        <div style="font-size:22px;color:{STEEL};line-height:1.6">Insider breaks years-long sell pattern and buys. One of the strongest signals in our data.</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_cluster_buys(data: dict) -> str:
    top = data.get("top_clusters", [])
    rows_html = ""
    for c in top[:4]:
        val_str = f"${c['total_value']/1_000_000:.1f}M" if c["total_value"] >= 1_000_000 else f"${c['total_value']/1_000:.0f}K"
        rows_html += f"""
        <div class="comparison-row" style="border-bottom:1px solid #1a1a25">
            <div style="font-size:32px;font-weight:700;font-family:monospace;width:120px">${c["ticker"]}</div>
            <div style="font-size:24px;color:{AMBER};font-weight:600">{c["n_insiders"]} insiders</div>
            <div style="font-size:24px;color:{STEEL}">{val_str}</div>
        </div>"""

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Cluster Buy Events</div>
        <div class="stat-big" style="color:{AMBER};margin-top:20px">{data["total_cluster_events"]:,}</div>
        <div class="stat-label">3+ insiders buying same stock, same week</div>
    </div>

    <div class="card">
        <div style="font-size:22px;color:{FOG};margin-bottom:16px">Top Clusters</div>
        {rows_html}
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_52week_high(data: dict) -> str:
    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Buying at 52-Week Highs</div>
    </div>

    <div class="card" style="text-align:center">
        <div class="stat-big" style="color:{ALPHA_GREEN}">12%</div>
        <div class="stat-label">annual alpha</div>
        <div class="divider"></div>
        <div style="display:flex;justify-content:space-around">
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{CLOUD}">{data["high_count"]:,}</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">buys near high</div>
            </div>
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{ALPHA_GREEN}">{data["high_excess_30d"]:+.1f}%</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">30d excess</div>
            </div>
        </div>
    </div>

    <div style="margin-top:50px;max-width:800px">
        <div style="font-size:22px;color:{STEEL};line-height:1.6">If the person running the company is buying at the high, they see something the market doesn't.</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_signal_grading(data: dict) -> str:
    a = data.get("A", {})
    b = data.get("B", {})
    c = data.get("C", {})

    def _bar(grade, info, color):
        excess = info.get("avg_excess_30d", 0)
        w = min(max(abs(excess) * 15, 10), 95)
        return f"""
        <div style="margin-bottom:28px">
            <div style="display:flex;justify-content:space-between;margin-bottom:8px">
                <span style="font-size:28px;font-weight:700;color:{color}">Grade {grade}</span>
                <span style="font-size:22px;color:{FOG}">{info.get("count", 0):,} trades</span>
            </div>
            <div class="bar-container">
                <div class="bar-fill" style="width:{w}%;background:{color}">{excess:+.1f}%</div>
            </div>
        </div>"""

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Signal Grade Performance</div>
        <div style="font-size:22px;color:{STEEL};margin-top:16px">30-day excess return vs S&P 500</div>
    </div>

    <div class="card">
        {_bar("A", a, ALPHA_GREEN)}
        {_bar("B", b, SIGNAL_BLUE)}
        {_bar("C", c, STEEL)}
    </div>

    <div style="margin-top:50px;text-align:center;max-width:800px">
        <div style="font-size:22px;color:{STEEL};line-height:1.6">Same SEC filing. Different signal quality. The grade matters.</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_sell_signal(data: dict) -> str:
    pct = data["pct_routine"]
    opp_pct = 100 - pct
    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Insider Sells Breakdown</div>
    </div>

    <div class="card" style="text-align:center">
        <div class="stat-big" style="color:{STEEL}">{pct:.0f}%</div>
        <div class="stat-label">of insider sells are routine/pre-planned</div>
        <div class="divider"></div>
        <div style="display:flex;justify-content:space-around">
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{STEEL}">{data["routine_count"]:,}</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">routine sells</div>
            </div>
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{RISK_RED}">{data["opp_count"]:,}</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">opportunistic sells</div>
            </div>
        </div>
        <div class="divider"></div>
        <div style="font-size:24px;color:{STEEL}">Opportunistic sells avg <span style="color:{RISK_RED};font-weight:700">{data["opp_avg_return_30d"]:+.1f}%</span> over 30 days</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_proven_sellers(data: dict) -> str:
    pct_proven = round(data["proven_count"] / max(data["total_scored_sellers"], 1) * 100, 1)
    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Proven Sellers</div>
    </div>

    <div class="card" style="text-align:center">
        <div class="stat-big" style="color:{AMBER}">1.75</div>
        <div class="stat-label">Sharpe ratio following proven sellers (>=60% accuracy)</div>
        <div class="divider"></div>
        <div style="display:flex;justify-content:space-around">
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{CLOUD}">{data["proven_count"]:,}</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">proven sellers</div>
            </div>
            <div>
                <div style="font-size:44px;font-weight:700;font-family:monospace;color:{CLOUD}">{pct_proven:.1f}%</div>
                <div style="font-size:20px;color:{FOG};margin-top:8px">of scored insiders</div>
            </div>
        </div>
    </div>

    <div style="margin-top:50px;max-width:800px">
        <div style="font-size:22px;color:{STEEL};line-height:1.6">Most insiders sell for boring reasons. A few sell because they know something. We track the difference.</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_data_holdings_change(data: dict) -> str:
    big_color = ALPHA_GREEN if data["big_excess_30d"] > 0 else RISK_RED
    small_color = ALPHA_GREEN if data["small_excess_30d"] > 0 else STEEL
    bar_big_w = min(max(abs(data["big_excess_30d"]) * 15, 10), 95)
    bar_small_w = min(max(abs(data["small_excess_30d"]) * 15, 10), 95)

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 70px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:60px">
        <div style="font-size:28px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Holdings Increase</div>
        <div style="font-size:22px;color:{STEEL};margin-top:16px">30-day excess return vs S&P 500</div>
    </div>

    <div class="card">
        <div style="margin-bottom:36px">
            <div style="display:flex;justify-content:space-between;margin-bottom:8px">
                <span style="font-size:24px;font-weight:700;color:{big_color}">>=10% increase</span>
                <span style="font-size:20px;color:{FOG}">{data["big_increase_count"]:,} trades</span>
            </div>
            <div class="bar-container">
                <div class="bar-fill" style="width:{bar_big_w}%;background:{big_color}">{data["big_excess_30d"]:+.1f}%</div>
            </div>
        </div>
        <div>
            <div style="display:flex;justify-content:space-between;margin-bottom:8px">
                <span style="font-size:24px;font-weight:700;color:{small_color}"><10% increase</span>
                <span style="font-size:20px;color:{FOG}">{data["small_increase_count"]:,} trades</span>
            </div>
            <div class="bar-container">
                <div class="bar-fill" style="width:{bar_small_w}%;background:{small_color}">{data["small_excess_30d"]:+.1f}%</div>
            </div>
        </div>
    </div>

    <div style="margin-top:50px;text-align:center;max-width:800px">
        <div style="font-size:22px;color:{STEEL};line-height:1.6">Dollar value is misleading. Conviction shows in the percentage.</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:40px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


DATA_CARD_FUNCS = {
    "routine_filter": card_data_routine_filter,
    "rare_reversals": card_data_rare_reversals,
    "cluster_buys": card_data_cluster_buys,
    "52week_high": card_data_52week_high,
    "signal_grading": card_data_signal_grading,
    "sell_signal": card_data_sell_signal,
    "proven_sellers": card_data_proven_sellers,
    "holdings_change": card_data_holdings_change,
}


# ---------------------------------------------------------------------------
# CTA card (reuses render_video_assets)
# ---------------------------------------------------------------------------

def card_cta_educational() -> str:
    """CTA card for educational content — same as daily CTA."""
    from pipelines.render_video_assets import card_cta
    return card_cta()


# ---------------------------------------------------------------------------
# Platform captions
# ---------------------------------------------------------------------------

def generate_captions(topic_key: str, topic: dict) -> str:
    """Generate platform-specific captions with hashtags."""
    hashtags_common = "#InsiderTrading #StockMarket #SEC #SmartMoney #Form4 #Investing #Stocks"
    hashtags_topic = {
        "routine_filter": "#SellSignal #TradingSignals #DataDriven",
        "rare_reversals": "#MarketSignals #AlphaGeneration #StockPicks",
        "cluster_buys": "#ClusterBuys #InstitutionalBuying #BullSignal",
        "52week_high": "#52WeekHigh #BuyHigh #MarketPsychology",
        "signal_grading": "#SignalGrade #QuantTrading #EdgeFinder",
        "sell_signal": "#InsiderSelling #SellSignal #MarketNoise",
        "proven_sellers": "#TrackRecord #ProvenSellers #ShortSignal",
        "holdings_change": "#SkinInTheGame #Conviction #HoldingsChange",
    }

    extra = hashtags_topic.get(topic_key, "")
    title = topic["title"]
    hook = topic["hook"]

    sections = []

    # TikTok / Reels
    sections.append("=== TIKTOK / REELS ===")
    sections.append(f"{hook}")
    sections.append("")
    sections.append(f"Free trial: form4.app (link in bio)")
    sections.append("")
    sections.append(f"{hashtags_common} {extra}")
    sections.append("")

    # YouTube Shorts
    sections.append("=== YOUTUBE SHORTS ===")
    sections.append(f"{title}")
    sections.append("")
    sections.append(f"{hook}")
    sections.append("")
    sections.append(f"Get real-time insider trade alerts: form4.app")
    sections.append(f"{hashtags_common} {extra}")
    sections.append("")

    # X / Twitter
    sections.append("=== X / TWITTER ===")
    sections.append(f"{hook}")
    sections.append("")
    sections.append("Real-time alerts + AI signal grading: form4.app")
    sections.append("")

    # LinkedIn
    sections.append("=== LINKEDIN ===")
    sections.append(f"{title}")
    sections.append("")
    sections.append(f"{hook}")
    sections.append("")
    sections.append(f"{topic['description']}")
    sections.append("")
    sections.append("We built Form4 to surface these signals automatically.")
    sections.append("form4.app")
    sections.append("")
    sections.append(f"{hashtags_common}")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Rendering pipeline
# ---------------------------------------------------------------------------

def render_assets(topic_key: str, hook_html: str, data_card_html: str, cta_html: str, output_dir: Path):
    """Render HTML cards to PNG using Playwright."""
    from playwright.sync_api import sync_playwright

    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})

        def _render(html: str, path: Path):
            if not html:
                return
            page.set_content(html, wait_until="networkidle")
            page.screenshot(path=str(path), full_page=False)
            logger.info("  Rendered %s", path.name)

        _render(hook_html, assets_dir / "hook.png")
        _render(data_card_html, assets_dir / "data_card.png")
        _render(cta_html, assets_dir / "cta.png")

        browser.close()

    logger.info("Assets written to %s", assets_dir)


# ---------------------------------------------------------------------------
# Main generation pipeline
# ---------------------------------------------------------------------------

def generate_topic(topic_key: str, do_audio: bool = False) -> Path:
    """Generate all content for a single educational topic."""
    if topic_key not in TOPICS:
        raise ValueError(f"Unknown topic: {topic_key}. Available: {', '.join(TOPICS.keys())}")

    topic = TOPICS[topic_key]
    output_dir = OUTPUT_BASE / topic_key
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Generating educational content: %s", topic_key)

    # 1. Query database
    conn = _connect()
    try:
        query_func = QUERY_FUNCS[topic_key]
        data = query_func(conn)
        logger.info("  Data queried: %s", {k: v for k, v in data.items() if not isinstance(v, list)})
    finally:
        conn.close()

    # 2. Generate storyboard script
    script_func = SCRIPT_FUNCS[topic_key]
    storyboard = script_func(data)
    (output_dir / "storyboard.txt").write_text(storyboard)
    logger.info("  Storyboard written")

    # 3. Generate platform captions
    captions = generate_captions(topic_key, topic)
    (output_dir / "captions_platforms.txt").write_text(captions)
    logger.info("  Captions written")

    # 4. Render visual assets
    hook_html = card_hook_educational(topic)
    data_card_func = DATA_CARD_FUNCS[topic_key]
    data_card_html = data_card_func(data)
    cta_html = card_cta_educational()

    try:
        render_assets(topic_key, hook_html, data_card_html, cta_html, output_dir)
    except Exception as exc:
        logger.warning("Playwright rendering failed (assets skipped): %s", exc)
        # Write raw HTML as fallback for manual rendering
        (output_dir / "assets").mkdir(parents=True, exist_ok=True)
        (output_dir / "assets" / "hook.html").write_text(hook_html)
        (output_dir / "assets" / "data_card.html").write_text(data_card_html)
        (output_dir / "assets" / "cta.html").write_text(cta_html)
        logger.info("  HTML fallbacks written")

    # 5. Optional audio generation
    if do_audio:
        try:
            from pipelines.generate_daily_content import generate_audio
            audio_path = output_dir / "assets" / "narration.mp3"
            if generate_audio(storyboard, audio_path):
                logger.info("  Audio generated")
            else:
                logger.warning("  Audio generation skipped (no API key or failed)")
        except Exception as exc:
            logger.warning("  Audio generation failed: %s", exc)

    logger.info("Done: %s -> %s", topic_key, output_dir)
    return output_dir


def list_topics():
    """Print all available topics."""
    print(f"\n{'Topic Key':<20} {'Title':<50} {'Key Finding'}")
    print("-" * 110)
    for key, t in TOPICS.items():
        print(f"{key:<20} {t['title']:<50} {t['description']}")
    print(f"\n{len(TOPICS)} topics available.\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate evergreen educational content from insider trading research."
    )
    parser.add_argument("--topic", type=str, help="Topic key to generate (e.g., routine_filter)")
    parser.add_argument("--list", action="store_true", help="List all available topics")
    parser.add_argument("--all", action="store_true", help="Generate all topics")
    parser.add_argument("--audio", action="store_true", help="Also generate ElevenLabs audio")
    args = parser.parse_args()

    if args.list:
        list_topics()
        return

    if args.all:
        for key in TOPICS:
            try:
                generate_topic(key, do_audio=args.audio)
            except Exception as exc:
                logger.error("Failed on %s: %s", key, exc)
        return

    if args.topic:
        generate_topic(args.topic, do_audio=args.audio)
        return

    parser.print_help()


if __name__ == "__main__":
    main()

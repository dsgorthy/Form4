#!/usr/bin/env python3
"""Generate daily social media content from insider trading signals.

Outputs:
  1. X/Twitter text post (copy-paste ready)
  2. Short-form video script (30-60 sec) with hook format
  3. Optional ElevenLabs audio generation

Hook format: Tease the best trade first ("One insider just dropped $X on this stock..."),
iterate through trades in reverse order (least to most interesting), save the
teased blockbuster for the end reveal.

Usage:
    python3 pipelines/generate_daily_content.py                    # today
    python3 pipelines/generate_daily_content.py --date 2026-03-25  # specific date
    python3 pipelines/generate_daily_content.py --audio             # also generate audio
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_connection

try:
    from pipelines.portfolio_simulator import compute_signal_quality
except ImportError:
    from portfolio_simulator import compute_signal_quality

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "content"


# ---------------------------------------------------------------------------
# Strategy fill detection
# ---------------------------------------------------------------------------

STRATEGY_LABELS = {
    "quality_momentum": "Quality + Momentum",
    "reversal_dip": "Deep Reversal",
    "tenb51_surprise": "10b5-1 Surprise",
}


def get_strategy_fills(conn: object, target_date: str) -> list[dict]:
    """Check if any live strategy entered or exited a position on target_date."""
    rows = conn.execute("""
        SELECT strategy, ticker, company, insider_name, entry_date, exit_date,
               exit_reason, pnl_pct, status, execution_source
        FROM strategy_portfolio
        WHERE execution_source = 'paper'
          AND (entry_date = ? OR exit_date = ?)
        ORDER BY strategy, entry_date DESC
    """, (target_date, target_date)).fetchall()
    return [dict(r) for r in rows]


def format_strategy_fill_line(fill: dict) -> str:
    """Generate a content-ready line about a strategy fill."""
    strat_label = STRATEGY_LABELS.get(fill["strategy"], fill["strategy"])
    ticker = fill["ticker"]
    company = fill.get("company") or ticker

    if fill["entry_date"] == fill.get("exit_date"):
        pnl = fill.get("pnl_pct")
        if pnl is not None:
            result = f"+{pnl*100:.1f}%" if pnl >= 0 else f"{pnl*100:.1f}%"
            return f"{strat_label} exited {company} ({ticker}) at {result}"
        return f"{strat_label} exited {company} ({ticker})"

    if fill.get("status") == "open" or fill.get("exit_date") is None:
        insider = fill.get("insider_name") or "An insider"
        return f"{strat_label} entered {company} ({ticker}) — triggered by {insider}"

    return f"{strat_label} traded {company} ({ticker})"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def get_top_trades(conn: object, target_date: str, limit: int = 8) -> list[dict]:
    """Get today's most interesting trades for content."""
    rows = conn.execute("""
        SELECT
            MIN(t.trade_id) AS trade_id,
            t.ticker,
            t.insider_id,
            MAX(t.company) AS company,
            MAX(COALESCE(i.display_name, i.name)) AS insider_name,
            MAX(t.title) AS title,
            t.trade_type,
            SUM(t.value) AS total_value,
            MAX(t.signal_grade) AS signal_grade,
            MAX(t.is_rare_reversal) AS is_rare_reversal,
            MAX(t.week52_proximity) AS week52_proximity,
            MAX(t.cohen_routine) AS cohen_routine,
            MAX(t.is_10b5_1) AS is_10b5_1,
            MAX(t.is_routine) AS is_routine,
            MAX(t.is_csuite) AS is_csuite,
            MAX(t.shares_owned_after) AS shares_after,
            SUM(t.qty) AS total_qty,
            MAX(COALESCE(t.pit_win_rate_7d, itr.buy_win_rate_7d)) AS pit_win_rate_7d,
            MAX(COALESCE(t.pit_n_trades, itr.buy_count)) AS pit_n_trades,
            MAX(t.insider_switch_rate) AS insider_switch_rate
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE t.filing_date = ?
          AND t.trans_code IN ('P', 'S')
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        GROUP BY t.insider_id, t.ticker, t.trade_type, t.filing_key
        ORDER BY SUM(t.value) DESC
        LIMIT ?
    """, (target_date, limit * 2)).fetchall()

    trades = []
    for r in rows:
        r = dict(r)
        # Skip routine/planned sells, bad tickers, pure 10% owners
        if r["is_routine"] or r["is_10b5_1"]:
            continue
        if r["ticker"] in ("NONE", "", None):
            continue
        title_lower = (r["title"] or "").lower()
        if "10%" in title_lower and not any(kw in title_lower for kw in ["ceo", "cfo", "president", "chair", "officer"]):
            continue

        # Compute V4 quality
        title = (r.get("title") or "").lower()
        csuite = any(kw in title for kw in ["ceo", "chief exec", "president", "cfo", "chief financial", "coo", "evp", "svp", "vp", "vice pres"])
        quality, _ = compute_signal_quality(
            pit_wr=r.get("pit_win_rate_7d"),
            pit_n=r.get("pit_n_trades"),
            is_csuite=csuite,
            holdings_pct_change=None,
            is_10pct_owner=False,
            title=r.get("title"),
            is_rare_reversal=bool(r.get("is_rare_reversal")),
            switch_rate=r.get("insider_switch_rate"),
        )
        r["_quality"] = quality
        if quality < 6.0:  # content floor — show Q6+ for volume
            continue

        # Compute interest score for content — signal quality > raw dollar value
        score = 0
        # Signal quality is most important for content
        if r["is_rare_reversal"]:
            score += 6
        if r["signal_grade"] == "A":
            score += 4
        elif r["signal_grade"] == "B":
            score += 1
        if r["week52_proximity"] and r["week52_proximity"] >= 0.8 and r["trade_type"] == "buy":
            score += 3
        if r["week52_proximity"] and r["week52_proximity"] <= 0.2 and r["trade_type"] == "sell":
            score += 3
        # Buys are inherently more interesting than sells for content
        if r["trade_type"] == "buy":
            score += 2
        # C-suite adds credibility
        if r["is_csuite"]:
            score += 2
        # Value matters but shouldn't dominate
        if r["total_value"] >= 5_000_000:
            score += 2
        elif r["total_value"] >= 1_000_000:
            score += 1
        elif r["total_value"] >= 500_000:
            score += 1

        r["_interest_score"] = score
        trades.append(r)

    # Sort by confidence: grade first, then track record quality, then interest score
    grade_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "F": 4}

    def _track_record_score(t: dict) -> float:
        """Higher = better track record for content. 0 if no data."""
        tr = t.get("_track_record", [])
        with_ret = [x for x in tr if x.get("return_30d") is not None]
        if len(with_ret) < 2:
            return 0  # no meaningful track record
        wins = sum(1 for x in with_ret if (x["return_30d"] > 0) == (x["trade_type"] == t["trade_type"]))
        wr = wins / len(with_ret)
        avg = sum(x["return_30d"] for x in with_ret) / len(with_ret)
        # Score: win rate matters most, average return adds flavor
        return wr + (avg / 10)  # e.g., 75% WR + 5% avg = 0.75 + 0.5 = 1.25

    trades.sort(key=lambda x: (
        grade_rank.get(x.get("signal_grade", "F"), 4),
        -_track_record_score(x),
        -x["_interest_score"],
        -x["total_value"],
    ))

    # One ticker per slot — pick the best trade per ticker, combine multiple insiders
    seen_tickers: dict[str, dict] = {}
    for t in trades:
        ticker = t["ticker"]
        if ticker not in seen_tickers:
            # First (best) trade for this ticker — use it
            seen_tickers[ticker] = t
        else:
            # Additional insider for the same ticker — note it for context
            existing = seen_tickers[ticker]
            if "_additional_insiders" not in existing:
                existing["_additional_insiders"] = []
            existing["_additional_insiders"].append({
                "insider_name": t["insider_name"],
                "title": t.get("title", ""),
                "total_value": t["total_value"],
                "trade_type": t["trade_type"],
            })
            # Combine total value
            existing["_combined_value"] = existing.get("_combined_value", existing["total_value"]) + t["total_value"]

    top = list(seen_tickers.values())[:limit]

    # Enrich with track record and stock performance
    for t in top:
        t["_track_record"] = get_insider_track_record(conn, t["insider_id"], t["ticker"], t["trade_type"])
        t["_stock_perf"] = get_stock_performance(conn, t["ticker"])

    return top


def get_insider_track_record(conn: object, insider_id: int, ticker: str, trade_type: str) -> list[dict]:
    """Get the insider's past trades on this ticker with returns."""
    rows = conn.execute("""
        SELECT MAX(t.trade_date) AS trade_date,
               MAX(t.trade_type) AS trade_type,
               SUM(t.value) AS value,
               MAX(tr.return_7d) AS return_7d,
               MAX(tr.return_30d) AS return_30d,
               MAX(tr.return_90d) AS return_90d
        FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.insider_id = ? AND t.ticker = ?
          AND t.trans_code IN ('P','S')
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        GROUP BY t.filing_key
        ORDER BY MAX(t.trade_date) DESC
        LIMIT 5
    """, (insider_id, ticker)).fetchall()
    return [dict(r) for r in rows]


def get_stock_performance(conn: object, ticker: str, days: int = 90) -> dict | None:
    """Get recent stock performance for context."""
    rows = conn.execute("""
        SELECT date, close FROM daily_prices
        WHERE ticker = ? ORDER BY date DESC LIMIT ?
    """, (ticker, days)).fetchall()
    if len(rows) < 2:
        return None
    latest = rows[0]["close"]
    oldest = rows[-1]["close"]
    pct_change = ((latest - oldest) / oldest) * 100
    return {
        "ticker": ticker,
        "current_price": latest,
        "period_days": len(rows),
        "pct_change": round(pct_change, 1),
        "direction": "up" if pct_change > 0 else "down",
    }


def get_daily_stats(conn: object, target_date: str) -> dict:
    """Get aggregate stats for the day."""
    row = conn.execute("""
        SELECT
            COUNT(DISTINCT CASE WHEN trade_type = 'buy' THEN filing_key END) AS buy_filings,
            COUNT(DISTINCT CASE WHEN trade_type = 'sell' THEN filing_key END) AS sell_filings,
            SUM(CASE WHEN trade_type = 'buy' THEN value ELSE 0 END) AS buy_value,
            SUM(CASE WHEN trade_type = 'sell' THEN value ELSE 0 END) AS sell_value,
            SUM(CASE WHEN signal_grade = 'A' THEN 1 ELSE 0 END) AS a_grade_count,
            SUM(CASE WHEN is_routine = 1 OR is_10b5_1 = 1 THEN 1 ELSE 0 END) AS routine_count
        FROM trades
        WHERE filing_date = ? AND trans_code IN ('P','S')
          AND (is_duplicate = 0 OR is_duplicate IS NULL)
    """, (target_date,)).fetchone()
    return dict(row)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_value(v: float) -> str:
    """Format value for visual display (e.g., $1.2M)."""
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"


def fmt_company_spoken(company: str | None, ticker: str) -> str:
    """Clean company name for TTS — remove Inc., Corp., etc."""
    if not company:
        return ticker
    import re
    # Strip legal suffixes
    cleaned = re.sub(
        r'\s*,?\s*\b(Inc\.?|Corp\.?|Corporation|Ltd\.?|Limited|LLC|L\.?L\.?C\.?|PLC|plc|N\.?V\.?|S\.?A\.?|Co\.?|Company|Holdings?|Group)\s*\.?\s*$',
        '', company, flags=re.IGNORECASE
    ).strip().rstrip(',').strip()
    # Title-case if all-caps (NUCOR -> Nucor, CONOCOPHILLIPS -> ConocoPhillips)
    if cleaned and cleaned == cleaned.upper() and len(cleaned) > 3:
        cleaned = cleaned.title()
    return cleaned or ticker


def fmt_insider_spoken(insider_name: str, title: str, company_spoken: str) -> str:
    """Format insider reference for TTS — use company + title, not name."""
    title_clean = fmt_title(title) if title else ""
    if title_clean:
        # Handle possessive: "Nucor's CEO" but "ConocoPhillips' CEO" (ends in s)
        possessive = f"{company_spoken}'" if company_spoken.endswith("s") else f"{company_spoken}'s"
        return f"{possessive} {title_clean}"
    return insider_name


def fmt_value_natural(v: float) -> str:
    """Format value the way a human would say it in conversation."""
    if v >= 1_000_000:
        m = v / 1_000_000
        if abs(m - round(m)) < 0.05:
            r = int(round(m))
            return "a million" if r == 1 else f"{r} million"
        return f"{m:.1f} million"
    if 400_000 <= v <= 600_000:
        return "half a million"
    if v >= 100_000:
        k = round(v / 10_000) * 10
        return f"{int(k)} thousand"
    if v >= 10_000:
        k = round(v / 1_000)
        return f"{int(k)} thousand"
    return f"{int(v):,}"


def fmt_title(title: str) -> str:
    """Clean up title for display."""
    title = title.strip()
    # Remove 10% owner suffix for cleaner display
    import re
    title = re.sub(r',?\s*10%.*$', '', title, flags=re.IGNORECASE).strip()
    # Expand abbreviations for TTS, keep full titles readable
    expansions = {
        "dir": "Director",
        "ceo": "CEO",
        "cfo": "CFO",
        "coo": "COO",
        "cco": "CCO",
        "svp": "Senior Vice President",
        "evp": "Executive Vice President",
        "vp": "Vice President",
        "pres": "President",
        "exec": "Executive",
        "sr": "Senior",
    }
    # Expand single-word abbreviations
    words = title.split()
    expanded = []
    for w in words:
        lower = w.lower().rstrip(",.")
        if lower in expansions:
            expanded.append(expansions[lower])
        else:
            expanded.append(w)
    title = " ".join(expanded)

    # Shorten very long titles for display
    replacements = {
        "Chief Executive Officer": "CEO",
        "Chief Financial Officer": "CFO",
        "Chief Operating Officer": "COO",
        "Chief Customer Officer": "CCO",
        "President and CEO": "President and CEO",
        "President and Chief Executive Officer": "President and CEO",
        "General Counsel": "General Counsel",
    }
    for long, short in replacements.items():
        if long.lower() in title.lower():
            return short
    if len(title) > 40:
        return title[:37] + "..."
    return title


def build_context_line(trade: dict) -> str:
    """Build a short context line for a trade."""
    parts = []
    if trade["is_rare_reversal"]:
        if trade["trade_type"] == "buy":
            parts.append("first buy after years of selling")
        else:
            parts.append("first sell after years of buying")
    if trade["week52_proximity"] is not None:
        w52 = trade["week52_proximity"]
        if w52 >= 0.8 and trade["trade_type"] == "buy":
            parts.append("near 52-week high")
        elif w52 <= 0.2 and trade["trade_type"] == "sell":
            parts.append("near 52-week low")
        elif w52 <= 0.2 and trade["trade_type"] == "buy":
            parts.append("near 52-week low")
    if trade["cohen_routine"] == 0:
        parts.append("opportunistic pattern")
    if trade["signal_grade"] == "A":
        parts.append("A-grade signal")

    # Holdings context
    if trade["shares_after"] and trade["total_qty"] and trade["trade_type"] == "buy":
        before = trade["shares_after"] - trade["total_qty"]
        if before > 0:
            pct = (trade["total_qty"] / before) * 100
            if pct >= 20:
                parts.append(f"increased holdings by {pct:.0f}%")

    return " · ".join(parts[:3]) if parts else ""


# ---------------------------------------------------------------------------
# Narration — varied, human-sounding video scripts
# ---------------------------------------------------------------------------

# Openers vary by position in the video to avoid repetition
_OPENERS_EARLY = [
    "Starting off.",
    "First up.",
    "Kicking it off —",
]
_OPENERS_MIDDLE = [
    "Next.",
    "Number {rank}.",
    "Moving on.",
    "Alright, number {rank}.",
]
_OPENERS_LATE = [
    "Now this one caught my eye.",
    "Here's where it gets interesting.",
    "Number {rank} — this is a good one.",
    "This next one stood out.",
]

# Action phrases — cycled to avoid repeats within a single script
_BUY_PHRASES = [
    "{insider} just bought {value}",
    "{insider} quietly picked up {value} in shares",
    "{insider} dropped {value} on the stock",
    "{insider} put up {value} of their own money",
    "{insider} went in for {value}",
    "{insider} bought {value} — all open market",
]
_SELL_PHRASES = [
    "{insider} just sold {value}",
    "{insider} unloaded {value}",
    "{insider} sold off {value} in shares",
    "{insider} cashed out {value}",
    "{insider} dumped {value}",
]

_narration_buy_idx = 0
_narration_sell_idx = 0


def _pick_opener(position: int, total: int) -> str:
    """Pick a varied opener based on position in the video."""
    if position == 0:
        pool = _OPENERS_EARLY
    elif position >= total - 2:
        pool = _OPENERS_LATE
    else:
        pool = _OPENERS_MIDDLE
    return pool[position % len(pool)]


def _pick_action(trade: dict, insider_spoken: str, value_natural: str) -> str:
    """Pick a varied action phrase. Cycles to avoid repeats."""
    global _narration_buy_idx, _narration_sell_idx
    if trade["trade_type"] == "buy":
        phrase = _BUY_PHRASES[_narration_buy_idx % len(_BUY_PHRASES)]
        _narration_buy_idx += 1
    else:
        phrase = _SELL_PHRASES[_narration_sell_idx % len(_SELL_PHRASES)]
        _narration_sell_idx += 1
    return phrase.format(insider=insider_spoken, value=value_natural)


def _weave_context(trade: dict) -> str:
    """Build context woven naturally into narration — not a bullet list."""
    parts = []
    is_buy = trade["trade_type"] == "buy"

    if trade.get("is_rare_reversal"):
        if is_buy:
            parts.append("This is their first buy after years of only selling.")
        else:
            parts.append("First sell after years of buying. That's a warning sign.")

    w52 = trade.get("week52_proximity")
    if w52 is not None:
        if w52 >= 0.8 and is_buy:
            parts.append("Stock is near its 52-week high and they're still buying. That's conviction.")
        elif w52 <= 0.2 and is_buy:
            parts.append("They're buying near the 52-week low.")
        elif w52 <= 0.2 and not is_buy:
            parts.append("Selling near the 52-week low — that's bearish.")

    perf = trade.get("_stock_perf")
    if perf and abs(perf["pct_change"]) > 10:
        direction = "up" if perf["pct_change"] > 0 else "down"
        pct = abs(perf["pct_change"])
        if is_buy and perf["pct_change"] < -15:
            parts.append(f"Stock is down {pct:.0f}% this quarter. They're buying into weakness.")
        elif not is_buy and perf["pct_change"] > 15:
            parts.append(f"Stock's up {pct:.0f}% this quarter. After a run like that, insiders cashing out is a red flag.")
        elif not parts:
            parts.append(f"Stock is {direction} {pct:.0f}% over the last three months.")

    web = trade.get("_web_context", "")
    if web and len(web) < 80 and not parts:
        parts.append(web)

    if not parts and trade.get("signal_grade") == "A":
        parts.append("This scores in our top tier for signal quality.")

    return " ".join(parts[:2])


def _asset_ref(rank: int, ticker: str, filename: str, is_reveal: bool = False) -> str:
    """Generate asset reference path for storyboard."""
    prefix = f"reveal_{ticker}" if is_reveal else f"trade_{rank}_{ticker}"
    return f"assets/{prefix}/{filename}"


# ---------------------------------------------------------------------------
# Content generators
# ---------------------------------------------------------------------------

def generate_x_post(trades: list[dict], stats: dict, target_date: str,
                    strategy_fills: list[dict] | None = None) -> str:
    """Generate a X/Twitter post."""
    date_fmt = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d")

    lines = [f"Form4 Daily Signal — {date_fmt}\n"]

    if strategy_fills:
        lines.append("Strategy Activity:")
        for fill in strategy_fills:
            lines.append(f"• {format_strategy_fill_line(fill)}")
        lines.append("")

    buys = [t for t in trades if t["trade_type"] == "buy"]
    sells = [t for t in trades if t["trade_type"] == "sell"]

    if buys:
        lines.append("Top Insider Buys:")
        for t in buys[:4]:
            ctx = build_context_line(t)
            ctx_str = f" ({ctx})" if ctx else ""
            lines.append(f"• ${t['ticker']} — {t['insider_name']} ({fmt_title(t['title'])}) bought {fmt_value(t['total_value'])}{ctx_str}")
        lines.append("")

    if sells:
        lines.append("Notable Sells:")
        for t in sells[:3]:
            ctx = build_context_line(t)
            ctx_str = f" ({ctx})" if ctx else ""
            lines.append(f"• ${t['ticker']} — {t['insider_name']} ({fmt_title(t['title'])}) sold {fmt_value(t['total_value'])}{ctx_str}")
        lines.append("")

    # Stats footer
    routine = stats.get("routine_count", 0)
    a_count = stats.get("a_grade_count", 0)
    if routine:
        lines.append(f"{routine} routine/planned trades filtered out.")
    if a_count:
        lines.append(f"{a_count} A-grade signals detected.")

    lines.append("\nReal-time alerts + signal grades: form4.app")

    return "\n".join(lines)


def generate_video_script(trades: list[dict], stats: dict, target_date: str) -> str:
    """Generate a storyboard-format video script with visual cues and human narration.

    Output format:
    - [BRACKETS] = visual/editing cues (which asset to show, timing)
    - "Quotes" = spoken narration (extracted for ElevenLabs audio)
    - Everything else = metadata/notes for the editor

    Structure: HOOK → supporting trades (weakest first) → REVEAL → CTA
    Target duration: 30-45 seconds
    """
    global _narration_buy_idx, _narration_sell_idx
    _narration_buy_idx = 0
    _narration_sell_idx = 0

    if not trades:
        return "No notable trades today."

    blockbuster = trades[0]
    supporting = list(reversed(trades[1:5]))  # weakest first, building to reveal
    all_shown = supporting + [blockbuster]

    # Date formatting
    _d = datetime.strptime(target_date, "%Y-%m-%d")
    _day = _d.day
    _suffix = "th" if 11 <= _day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(_day % 10, "th")
    date_spoken = f"{_d.strftime('%B')} {_day}{_suffix}"
    num_trades = len(all_shown)

    lines = []

    # ── HEADER ──────────────────────────────────────────────────────
    lines.append(f"=== FORM4 DAILY — {date_spoken}, {_d.year} ===")
    lines.append(f"{num_trades} trades | ~{num_trades * 8 + 10} sec estimated")
    lines.append("")

    # ── HOOK ────────────────────────────────────────────────────────
    bb_action = "bought" if blockbuster["trade_type"] == "buy" else "sold"
    bb_title = fmt_title(blockbuster.get("title", ""))
    bb_company = fmt_company_spoken(blockbuster.get("company"), blockbuster["ticker"])
    bb_value = fmt_value_natural(blockbuster.get("_combined_value", blockbuster["total_value"]))
    bb_additional = blockbuster.get("_additional_insiders", [])
    bb_n_insiders = 1 + len(bb_additional)

    # One sentence. Specific. Creates a question.
    if blockbuster["is_rare_reversal"] and blockbuster["trade_type"] == "buy":
        hook = f"An insider who hasn't bought stock in years just put up {bb_value}."
    elif blockbuster["is_rare_reversal"] and blockbuster["trade_type"] == "sell":
        hook = f"A long-time buyer just flipped — and sold {bb_value}."
    elif bb_n_insiders >= 3:
        hook = f"{bb_n_insiders} executives at the same company all {bb_action} stock on the same day."
    elif bb_n_insiders == 2:
        hook = f"Two insiders at the same company. Same day. A combined {bb_value}."
    elif blockbuster["total_value"] >= 5_000_000:
        hook = f"A {bb_title} just bet {bb_value} on his own company."
    elif blockbuster.get("week52_proximity") and blockbuster["week52_proximity"] >= 0.8 and blockbuster["trade_type"] == "buy":
        hook = f"This stock is near its all-time high — and the {bb_title} is still buying."
    elif blockbuster["signal_grade"] == "A":
        hook = "One insider just made a move that triggered our highest signal grade."
    else:
        hook = f"A {bb_title} just {bb_action} {bb_value}. Here's why it matters."

    lines.append("--- HOOK (0:00–0:03) ---")
    lines.append("[SHOW: hook_text.png]")
    lines.append(f'"{hook}"')
    lines.append("")

    # ── SUPPORTING TRADES ───────────────────────────────────────────
    for i, t in enumerate(supporting):
        rank = len(supporting) - i + 1  # counting down: #5, #4, #3, #2
        ticker = t["ticker"]
        company_spoken = fmt_company_spoken(t.get("company"), ticker)
        insider_spoken = fmt_insider_spoken(t["insider_name"], t.get("title", ""), company_spoken)
        value_natural = fmt_value_natural(t.get("_combined_value", t["total_value"]))
        additional = t.get("_additional_insiders", [])

        t_start = 3 + i * 8
        t_end = t_start + 8

        lines.append(f"--- TRADE #{rank} ({t_start // 60}:{t_start % 60:02d}–{t_end // 60}:{t_end % 60:02d}) ---")
        lines.append(f"[SHOW: {_asset_ref(rank, ticker, 'card.png')}]")
        lines.append(f"[OVERLAY: {_asset_ref(rank, ticker, 'logo.png')}]")

        opener = _pick_opener(i, len(supporting))
        if "{rank}" in opener:
            opener = opener.format(rank=rank)

        if additional:
            n_ins = 1 + len(additional)
            action_word = "bought" if t["trade_type"] == "buy" else "sold"
            lines.append(f'"{opener} {company_spoken}. {n_ins} insiders {action_word} a combined {value_natural}."')
        else:
            action_phrase = _pick_action(t, insider_spoken, value_natural)
            lines.append(f'"{opener} {company_spoken}. {action_phrase}."')

        ctx = _weave_context(t)
        if ctx:
            lines.append(f'"{ctx}"')

        web_ctx = t.get("_web_context", "")
        if web_ctx:
            lines.append(f'"{web_ctx}"')

        lines.append(f"[SHOW: {_asset_ref(rank, ticker, 'chart.png')}]")
        lines.append(f"[OVERLAY: {_asset_ref(rank, ticker, 'headshot.png')}]")
        lines.append("")

    # ── REVEAL ──────────────────────────────────────────────────────
    bb_ticker = blockbuster["ticker"]
    r_start = 3 + len(supporting) * 8
    r_end = r_start + 15

    lines.append(f"--- REVEAL #{1} ({r_start // 60}:{r_start % 60:02d}–{r_end // 60}:{r_end % 60:02d}) ---")
    lines.append(f"[SHOW: {_asset_ref(1, bb_ticker, 'mystery_card.png', is_reveal=True)}]")
    lines.append("[BEAT — 2 seconds]")

    if supporting:
        lines.append('"Now. The one you\'ve been waiting for."')
    else:
        lines.append('"Here\'s the full story."')

    lines.append(f"[SHOW: {_asset_ref(1, bb_ticker, 'card.png', is_reveal=True)}]")
    lines.append(f"[OVERLAY: {_asset_ref(1, bb_ticker, 'headshot.png', is_reveal=True)}]")

    # Reveal narration — company name withheld until the end
    bb_insider = fmt_insider_spoken(blockbuster["insider_name"], blockbuster.get("title", ""), bb_company)
    bb_val = fmt_value_natural(blockbuster.get("_combined_value", blockbuster["total_value"]))
    is_buy = blockbuster["trade_type"] == "buy"

    if bb_additional:
        # Collect clean titles, skip junk like "Unknown", "See Remarks", "10% Owner"
        _skip_titles = {"unknown", "see remarks", "none", "n/a", ""}
        titles = []
        for src in [blockbuster] + bb_additional[:2]:
            t = fmt_title(src.get("title", ""))
            if t and t.lower() not in _skip_titles and "10%" not in t.lower():
                titles.append(t)
        if titles:
            titles_str = ", ".join(titles)
            lines.append(f'"The {titles_str} — all {bb_action} on the same day. That\'s {bb_val} combined."')
        else:
            lines.append(f'"{bb_n_insiders} insiders all {bb_action} on the same day. That\'s {bb_val} combined."')
    else:
        lines.append(f'"{bb_insider} just {bb_action} {bb_val}."')

    # Stock backdrop
    lines.append(f"[SHOW: {_asset_ref(1, bb_ticker, 'chart.png', is_reveal=True)}]")
    bb_perf = blockbuster.get("_stock_perf")
    if bb_perf and abs(bb_perf["pct_change"]) > 5:
        direction = "up" if bb_perf["pct_change"] > 0 else "down"
        pct = abs(bb_perf["pct_change"])
        if is_buy and bb_perf["pct_change"] < -15:
            lines.append(f'"Stock is down {pct:.0f}% this quarter. They\'re buying into weakness — that takes conviction."')
        elif not is_buy and bb_perf["pct_change"] > 15:
            lines.append(f'"Stock\'s up {pct:.0f}% this quarter. After a run like that, insiders cashing out? Pay attention."')
        elif is_buy:
            lines.append(f'"Stock is {direction} {pct:.0f}% recently, and they\'re still putting their own money in."')
        else:
            lines.append(f'"Stock is {direction} {pct:.0f}% this quarter."')

    # Web context
    bb_web = blockbuster.get("_web_context", "")
    if bb_web:
        lines.append(f'"{bb_web}"')

    # Thesis — one punchy paragraph, not a research paper
    if blockbuster["is_rare_reversal"] and is_buy:
        lines.append('"This is a rare reversal. Years of selling, then a sudden buy. '
                     'In our data, these beat the market by 3.6% within a month."')
    elif blockbuster["is_rare_reversal"] and not is_buy:
        lines.append('"Rare reversal. Long-time buyer just switched to selling. '
                     'Historically, the stock underperforms from here."')
    elif bb_n_insiders >= 3 and not is_buy:
        lines.append(f'"{bb_n_insiders} executives selling on the same day? '
                     f'That\'s not routine. That\'s coordinated."')
    elif bb_n_insiders >= 2 and is_buy:
        lines.append('"Multiple insiders buying at once — one of the most reliable signals in our data."')
    elif blockbuster.get("week52_proximity") and blockbuster["week52_proximity"] >= 0.8 and is_buy:
        lines.append('"Buying near an all-time high. Most people think that\'s bad. '
                     'Our data says the opposite — 12% alpha over the next year."')
    elif blockbuster["signal_grade"] == "A" and is_buy:
        lines.append('"A-grade signal. When insiders with this profile buy, '
                     'they average 2% above the market within a month."')
    elif blockbuster["signal_grade"] == "A" and not is_buy:
        lines.append('"A-grade sell. The stock typically lags from here."')
    elif blockbuster["total_value"] >= 5_000_000:
        lines.append(f'"That\'s {bb_val} of their own cash. Not options. Not grants. '
                     f'Real money. That conviction is rare."')
    else:
        lines.append('"When insiders move with this level of conviction, the data says pay attention."')

    # Track record (if strong enough to mention)
    track = blockbuster.get("_track_record", [])
    with_ret = [tr for tr in track if tr.get("return_30d") is not None]
    if len(with_ret) >= 3:
        wins = sum(1 for tr in with_ret if (tr["return_30d"] > 0) == is_buy)
        wr = wins / len(with_ret)
        avg_ret = sum(tr["return_30d"] for tr in with_ret) / len(with_ret)
        if wr >= 0.6 and abs(avg_ret) >= 1:
            lines.append(f"[SHOW: {_asset_ref(1, bb_ticker, 'stats.png', is_reveal=True)}]")
            lines.append(f'"This insider\'s track record? {wins} for {len(with_ret)}. '
                         f'Average return, {abs(avg_ret):.1f}%."')

    # Company name reveal
    lines.append(f'"The company — {bb_company}. Ticker, {bb_ticker}."')

    # Product screenshot
    lines.append(f"[SHOW: {_asset_ref(1, bb_ticker, 'screenshot.png', is_reveal=True)}]")

    # ── CTA ─────────────────────────────────────────────────────────
    lines.append("")
    lines.append("--- CTA ---")
    lines.append("[SHOW: cta.png]")
    lines.append('"Get these signals before anyone else. form4.app, link in bio."')

    return "\n".join(lines)


def generate_carousel_data(trades: list[dict], target_date: str) -> list[dict]:
    """Generate data for an Instagram carousel (5 slides)."""
    date_fmt = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d, %Y")
    slides = []

    # Slide 1: Title
    slides.append({
        "type": "title",
        "text": f"Top Insider Trades\n{date_fmt}",
        "subtext": "Ranked by Form4 Signal Quality",
    })

    # Slides 2-5: Individual trades
    for t in trades[:4]:
        action = "BOUGHT" if t["trade_type"] == "buy" else "SOLD"
        ctx = build_context_line(t)
        slides.append({
            "type": "trade",
            "ticker": t["ticker"],
            "company": t["company"] or t["ticker"],
            "insider": t["insider_name"],
            "title": fmt_title(t["title"]),
            "action": action,
            "value": fmt_value(t["total_value"]),
            "grade": t["signal_grade"],
            "context": ctx,
        })

    # Last slide: CTA
    slides.append({
        "type": "cta",
        "text": "Get real-time insider alerts",
        "subtext": "7-day free trial at form4.app",
    })

    return slides


def generate_platform_captions(trades: list[dict], stats: dict, target_date: str) -> str:
    """Generate captions + hashtags for each platform."""
    _d = datetime.strptime(target_date, "%Y-%m-%d")
    _day = _d.day
    _suffix = "th" if 11 <= _day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(_day % 10, "th")
    date_fmt = f"{_d.strftime('%B')} {_day}{_suffix}"

    buys = [t for t in trades[:5] if t["trade_type"] == "buy"]
    sells = [t for t in trades[:5] if t["trade_type"] == "sell"]
    hashtags = "#InsiderTrading #StockMarket #SEC #SmartMoney #Form4"

    # Build trade summary lines
    highlights = []
    for t in trades[:3]:
        company = fmt_company_spoken(t.get("company"), t["ticker"])
        title = fmt_title(t.get("title", ""))
        action = "bought" if t["trade_type"] == "buy" else "sold"
        val = fmt_value(t["total_value"])
        highlights.append(f"{company}'s {title} {action} {val}")

    highlight_text = ". ".join(highlights)

    # X post (under 280 chars)
    x_lines = [f"📅 Top insider trades for {date_fmt}\n"]
    for t in (buys[:2] + sells[:1]):
        emoji = "🟢" if t["trade_type"] == "buy" else "🔴"
        x_lines.append(f"{emoji} ${t['ticker']} — {fmt_title(t.get('title',''))} {t['trade_type']} {fmt_value(t['total_value'])}")
    routine = stats.get("routine_count", 0)
    if routine:
        x_lines.append(f"\n{routine} routine trades filtered.")
    x_lines.append("\nform4.app")
    x_post = "\n".join(x_lines)
    # Truncate if too long
    if len(x_post) > 275:
        x_post = x_post[:272] + "..."

    lines = []
    lines.append("=== INSTAGRAM REELS ===\n")
    lines.append(f"📅 Top insider trades for {date_fmt}\n")
    lines.append(f"{highlight_text}.")
    lines.append(f"\nFollow for daily insider trade alerts 📊")
    lines.append(f"\n{hashtags}")

    lines.append("\n\n=== TIKTOK ===\n")
    lines.append(f"📅 Top insider trades for {date_fmt}\n")
    lines.append(f"{highlight_text}.")
    lines.append(f"\nFollow for daily insider trade alerts 📊")
    lines.append(f"\n{hashtags}")

    lines.append("\n\n=== FACEBOOK ===\n")
    lines.append(f"📅 Top insider trades for {date_fmt}\n")
    lines.append(f"{highlight_text}.\n")
    lines.append("We filter out routine and pre-planned trades so you only see the ones that matter.")
    lines.append(f"\nFree 7-day trial at form4.app")
    lines.append(f"\n{hashtags}")

    lines.append(f"\n\n=== X/TWITTER ===\n")
    lines.append(x_post)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ElevenLabs audio generation
# ---------------------------------------------------------------------------

def generate_audio(script: str, output_path: Path) -> bool:
    """Generate audio from script using ElevenLabs API."""
    api_key = os.getenv("ELEVENLABS_API_KEY", "")
    if not api_key:
        logger.warning("ELEVENLABS_API_KEY not set, skipping audio generation")
        return False

    try:
        import requests
        voice_id = os.getenv("ELEVENLABS_VOICE_ID", "IKne3meq5aSn9XLyUdCD")  # Charlie — energetic

        # Extract spoken lines (in quotes) and pauses (BEAT markers)
        spoken = []
        for line in script.split("\n"):
            line = line.strip()
            if line.startswith('"') and line.endswith('"'):
                spoken.append(line[1:-1])
            elif "[BEAT" in line:
                spoken.append("...")  # ElevenLabs natural pause
        narration = " ".join(spoken)

        resp = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={
                "xi-api-key": api_key,
                "Content-Type": "application/json",
            },
            json={
                "text": narration,
                "model_id": "eleven_turbo_v2_5",
                "voice_settings": {
                    "stability": 0.4,
                    "similarity_boost": 0.8,
                    "style": 0.5,
                    "speed": 1.15,
                },
            },
            timeout=60,
        )

        if resp.status_code == 200:
            output_path.write_bytes(resp.content)
            logger.info("Audio saved to %s (%d bytes)", output_path, len(resp.content))
            return True
        else:
            logger.warning("ElevenLabs API error %d: %s", resp.status_code, resp.text[:200])
            return False

    except Exception as exc:
        logger.error("Audio generation failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# SRT caption generation
# ---------------------------------------------------------------------------

def _extract_spoken_lines(script: str) -> list[str]:
    """Extract spoken lines from storyboard script."""
    lines = []
    for line in script.split("\n"):
        line = line.strip()
        if line.startswith('"') and line.endswith('"'):
            lines.append(line[1:-1])
    return lines


def _split_subtitle_chunks(text: str, max_words: int = 8) -> list[str]:
    """Split text into subtitle-friendly chunks at natural break points."""
    import re
    # Split on natural pauses: commas, periods, dashes, semicolons
    # Ensure spaces around break points for clean subtitles
    parts = re.split(r'([.!?,;]\s+|—\s*|\s+-\s+|\.\s*)', text)

    chunks: list[str] = []
    current = ""
    for part in parts:
        if not part.strip():
            continue
        test = (current + part).strip() if current else part.strip()
        if len(test.split()) <= max_words:
            current = test
        else:
            if current.strip():
                chunks.append(current.strip())
            current = part.strip()
    if current.strip():
        chunks.append(current.strip())

    # Split any remaining long chunks at the midpoint
    final = []
    for chunk in chunks:
        words = chunk.split()
        if len(words) > max_words + 2:
            mid = len(words) // 2
            final.append(" ".join(words[:mid]))
            final.append(" ".join(words[mid:]))
        else:
            final.append(chunk)

    return final if final else [text]


def _fmt_srt_time(seconds: float) -> str:
    """Format seconds as SRT timestamp (HH:MM:SS,mmm)."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def generate_srt(script: str, words_per_minute: float = 170) -> str:
    """Generate approximate SRT captions from storyboard script.

    Uses word-count estimation at the configured speaking rate (170 wpm ≈ 1.15x speed).
    Good enough for CapCut import — Derek can nudge timing if needed.
    """
    spoken_lines = _extract_spoken_lines(script)
    if not spoken_lines:
        return ""

    entries: list[tuple[int, float, float, str]] = []
    current_time = 0.5  # small lead-in

    for line in spoken_lines:
        words = len(line.split())
        duration = (words / words_per_minute) * 60

        chunks = _split_subtitle_chunks(line)
        chunk_dur = duration / max(len(chunks), 1)

        for chunk in chunks:
            start = current_time
            end = current_time + chunk_dur
            entries.append((len(entries) + 1, start, end, chunk))
            current_time = end

        # Small gap between spoken sections
        current_time += 0.3

    srt_lines = []
    for idx, start, end, text in entries:
        srt_lines.append(str(idx))
        srt_lines.append(f"{_fmt_srt_time(start)} --> {_fmt_srt_time(end)}")
        srt_lines.append(text)
        srt_lines.append("")

    return "\n".join(srt_lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily social media content")
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Target date (default: today)")
    parser.add_argument("--audio", action="store_true",
                        help="Generate ElevenLabs audio for the video script")
    parser.add_argument("--no-assets", action="store_true",
                        help="Skip visual asset rendering (text content only)")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON instead of text")
    args = parser.parse_args()

    conn = get_connection(readonly=True)

    trades = get_top_trades(conn, args.date)
    stats = get_daily_stats(conn, args.date)
    strategy_fills = get_strategy_fills(conn, args.date)
    conn.close()

    if strategy_fills:
        logger.info("Strategy fills detected: %d entries/exits", len(strategy_fills))
        for fill in strategy_fills:
            logger.info("  %s", format_strategy_fill_line(fill))

    if not trades and not strategy_fills:
        print(f"No notable trades or strategy fills for {args.date}")
        return

    if not trades:
        trades = []

    # Scrape web context for each trade
    if trades:
        from pipelines.scrape_trade_context import scrape_all_trades
        logger.info("Scraping web context for %d trades...", len(trades))
        web_contexts = scrape_all_trades(trades)
        for t, ctx in zip(trades, web_contexts):
            t["_web_context"] = ctx.get("spoken_context", "")
            t["_web_sources"] = ctx.get("sources", [])

    # Generate text content
    x_post = generate_x_post(trades, stats, args.date, strategy_fills=strategy_fills)
    storyboard = generate_video_script(trades, stats, args.date)
    carousel = generate_carousel_data(trades, args.date)
    captions = generate_platform_captions(trades, stats, args.date)
    srt = generate_srt(storyboard)

    # Nested output: content/YYYYMMDD/
    date_slug = args.date.replace("-", "")
    day_dir = OUTPUT_DIR / date_slug
    day_dir.mkdir(parents=True, exist_ok=True)

    (day_dir / "storyboard.txt").write_text(storyboard)
    (day_dir / "x_post.txt").write_text(x_post)
    (day_dir / "captions.srt").write_text(srt)
    (day_dir / "captions_platforms.txt").write_text(captions)
    (day_dir / "carousel.json").write_text(json.dumps(carousel, indent=2))

    # Save sources for reference
    all_sources = []
    for t in trades:
        if t.get("_web_sources"):
            all_sources.append({
                "ticker": t["ticker"],
                "company": t.get("company", ""),
                "sources": t["_web_sources"],
            })
    (day_dir / "sources.json").write_text(json.dumps(all_sources, indent=2))

    logger.info("Text content saved to %s", day_dir)

    # Audio generation
    if args.audio:
        audio_path = day_dir / "narration.mp3"
        generate_audio(storyboard, audio_path)

    # Visual asset rendering
    if not args.no_assets:
        try:
            from pipelines.render_video_assets import render_all_assets
            logger.info("Rendering visual assets...")
            render_all_assets(trades, args.date, storyboard)
            logger.info("Visual assets complete")
        except ImportError:
            logger.warning("render_video_assets not available, skipping asset rendering")
        except Exception as exc:
            logger.error("Asset rendering failed: %s", exc)

    # Also write flat copies for backward compat with existing launchd pipeline
    (OUTPUT_DIR / f"{date_slug}_video_script.txt").write_text(storyboard)
    (OUTPUT_DIR / f"{date_slug}_x_post.txt").write_text(x_post)
    (OUTPUT_DIR / f"{date_slug}_captions.txt").write_text(captions)


if __name__ == "__main__":
    main()

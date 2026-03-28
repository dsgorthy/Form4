"""
Insider catalog lookup — used by edgar_monitor.py and solo_insider strategy.

Provides:
  - get_insider_record(name) -> track record dict or None
  - get_pit_score_for_insider(name, ticker, as_of_date) -> PIT score dict or None
  - enrich_signal(signal) -> signal dict with insider history added
  - check_solo_trigger(trade) -> signal dict if insider qualifies for solo follow
  - format_insider_card(record) -> formatted string for Telegram
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"

# Solo insider signal thresholds
MIN_BUY_COUNT = 5          # need 5+ historical buys to trust the track record
MIN_WIN_RATE = 0.55        # >55% win rate on 7d returns
MIN_AVG_RETURN = 0.01      # >1% avg 7d return
MIN_TRADE_VALUE = 100_000  # current trade must be $100K+
MIN_SCORE_TIER = 2         # must be tier 2 (top 20%) or tier 3 (top 7%)
MIN_PIT_SCORE = 1.0        # minimum PIT blended_score for solo follow
MIN_PIT_SCORE_OPTIONS = 1.5  # minimum PIT score for options overlay


def _get_conn() -> sqlite3.Connection:
    """Get a read-only connection to the catalog DB."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_pit_score_for_insider(
    name: str, ticker: str, as_of_date: str = None
) -> Optional[dict]:
    """
    Look up the most recent PIT score for an insider at a specific ticker.

    Returns dict with blended_score, global_score, ticker_score, etc., or None.
    """
    if not DB_PATH.exists():
        return None

    if as_of_date is None:
        as_of_date = date.today().isoformat()

    import re
    name_norm = name.lower().strip()
    name_norm = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv|v)\s*$', '', name_norm)
    name_norm = re.sub(r'\s+', ' ', name_norm)

    try:
        conn = _get_conn()
        row = conn.execute("""
            SELECT its.*
            FROM insider_ticker_scores its
            JOIN insiders i ON its.insider_id = i.insider_id
            WHERE i.name_normalized = ? AND its.ticker = ? AND its.as_of_date <= ?
            ORDER BY its.as_of_date DESC LIMIT 1
        """, (name_norm, ticker, as_of_date)).fetchone()
        conn.close()

        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error("PIT score lookup failed for '%s' @ %s: %s", name, ticker, e)
        return None


def pit_confidence_multiplier(pit_score: float) -> float:
    """
    Map PIT blended_score to a position sizing multiplier.
      2.0+ → 1.0 (full size)
      1.5-2.0 → 0.8
      1.0-1.5 → 0.6
      <1.0 → 0.0 (don't trade)
    """
    if pit_score >= 2.0:
        return 1.0
    elif pit_score >= 1.5:
        return 0.8
    elif pit_score >= 1.0:
        return 0.6
    else:
        return 0.0


def get_insider_record(name: str, cik: str = None) -> Optional[dict]:
    """
    Look up an insider's track record by name.
    Returns dict with all track record fields, or None if not found.
    """
    if not DB_PATH.exists():
        logger.warning("Insider catalog DB not found at %s", DB_PATH)
        return None

    import re
    name_norm = name.lower().strip()
    name_norm = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv|v)\s*$', '', name_norm)
    name_norm = re.sub(r'\s+', ' ', name_norm)

    try:
        conn = _get_conn()
        row = conn.execute("""
            SELECT i.insider_id, i.name, tr.*
            FROM insiders i
            JOIN insider_track_records tr ON i.insider_id = tr.insider_id
            WHERE i.name_normalized = ?
            LIMIT 1
        """, (name_norm,)).fetchone()
        conn.close()

        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error("Insider lookup failed for '%s': %s", name, e)
        return None


def get_insider_companies(name: str) -> list[dict]:
    """Get all companies an insider has traded at."""
    if not DB_PATH.exists():
        return []

    import re
    name_norm = name.lower().strip()
    name_norm = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv|v)\s*$', '', name_norm)
    name_norm = re.sub(r'\s+', ' ', name_norm)

    try:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT ic.* FROM insider_companies ic
            JOIN insiders i ON ic.insider_id = i.insider_id
            WHERE i.name_normalized = ?
            ORDER BY ic.last_trade DESC
        """, (name_norm,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_insider_trades(name: str, limit: int = 20) -> list[dict]:
    """Get an insider's recent trades."""
    if not DB_PATH.exists():
        return []

    import re
    name_norm = name.lower().strip()
    name_norm = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv|v)\s*$', '', name_norm)
    name_norm = re.sub(r'\s+', ' ', name_norm)

    try:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT t.*, tr.return_7d, tr.abnormal_7d
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE i.name_normalized = ?
            ORDER BY t.trade_date DESC
            LIMIT ?
        """, (name_norm, limit)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def enrich_signal(signal: dict) -> dict:
    """
    Enrich a cluster buy signal with insider track records and PIT scores.
    Adds 'insider_records' list, 'best_insider_score', and 'best_pit_score' to the signal.
    """
    insider_names = signal.get("insiders", [])
    ticker = signal.get("ticker", "")
    records = []

    for name in insider_names:
        rec = get_insider_record(name)
        pit = get_pit_score_for_insider(name, ticker)
        if rec:
            records.append({
                "name": name,
                "insider_id": rec.get("insider_id"),
                "buy_count": rec.get("buy_count", 0),
                "win_rate_7d": rec.get("buy_win_rate_7d"),
                "avg_return_7d": rec.get("buy_avg_return_7d"),
                "avg_abnormal_7d": rec.get("buy_avg_abnormal_7d"),
                "score": rec.get("score"),
                "score_tier": rec.get("score_tier"),
                "primary_title": rec.get("primary_title"),
                "total_value": rec.get("buy_total_value", 0),
                "pit_blended_score": pit.get("blended_score") if pit else None,
                "pit_sufficient_data": pit.get("sufficient_data") if pit else None,
            })
        else:
            records.append({
                "name": name,
                "insider_id": None,
                "buy_count": 0,
                "win_rate_7d": None,
                "avg_return_7d": None,
                "avg_abnormal_7d": None,
                "score": None,
                "score_tier": None,
                "primary_title": None,
                "total_value": 0,
                "pit_blended_score": pit.get("blended_score") if pit else None,
                "pit_sufficient_data": pit.get("sufficient_data") if pit else None,
            })

    signal["insider_records"] = records

    scored = [r for r in records if r["score"] is not None]
    signal["best_insider_score"] = max((r["score"] for r in scored), default=None)
    signal["best_insider_tier"] = max((r["score_tier"] for r in scored), default=None)
    signal["has_proven_insider"] = any(
        r["score_tier"] is not None and r["score_tier"] >= MIN_SCORE_TIER
        for r in records
    )

    # PIT scoring — ticker-specific, point-in-time
    pit_scores = [r["pit_blended_score"] for r in records if r["pit_blended_score"] is not None]
    signal["best_pit_score"] = max(pit_scores, default=None)
    signal["pit_confidence_mult"] = pit_confidence_multiplier(max(pit_scores, default=0.0))

    return signal


def check_solo_trigger(trade: dict) -> Optional[dict]:
    """
    Check if a single insider's new buy qualifies for a solo follow trade.
    This is a NEW strategy: trade alongside proven insiders even without a cluster.

    Uses PIT scores (point-in-time, per-ticker) as the primary quality gate.
    Falls back to global track record if no PIT score exists.

    Args:
        trade: dict with keys from edgar_monitor parse_form4_xml output
               (insider_name, ticker, value, title, filing_date, etc.)

    Returns:
        Signal dict if qualified, None otherwise.
    """
    name = trade.get("insider_name", "")
    ticker = trade.get("ticker", "")
    value = trade.get("value", 0)

    if value < MIN_TRADE_VALUE:
        return None

    rec = get_insider_record(name)
    if not rec:
        return None

    buy_count = rec.get("buy_count", 0)
    win_rate = rec.get("buy_win_rate_7d")
    avg_return = rec.get("buy_avg_return_7d")
    score_tier = rec.get("score_tier")
    score = rec.get("score")

    # Basic track record checks
    if buy_count < MIN_BUY_COUNT:
        return None
    if win_rate is None or win_rate < MIN_WIN_RATE:
        return None
    if avg_return is None or avg_return < MIN_AVG_RETURN:
        return None

    # PIT score check (primary gate — replaces global tier check)
    pit = get_pit_score_for_insider(name, ticker)
    pit_score = pit.get("blended_score", 0.0) if pit else None

    if pit_score is not None and pit.get("sufficient_data"):
        # PIT score available — use it as the gate
        if pit_score < MIN_PIT_SCORE:
            return None
    else:
        # No PIT score — fall back to global tier
        if score_tier is None or score_tier < MIN_SCORE_TIER:
            return None

    return {
        "type": "solo_insider",
        "ticker": ticker,
        "company": trade.get("company", ""),
        "insider_name": name,
        "insider_id": rec.get("insider_id"),
        "trade_value": value,
        "title": trade.get("title", ""),
        "filing_date": trade.get("filing_date", ""),
        "insider_score": score,
        "insider_tier": score_tier,
        "pit_blended_score": pit_score,
        "pit_confidence_mult": pit_confidence_multiplier(pit_score) if pit_score else 0.6,
        "buy_count": buy_count,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "avg_abnormal": rec.get("buy_avg_abnormal_7d"),
        "primary_title": rec.get("primary_title"),
        "n_tickers": rec.get("n_tickers", 0),
    }


def format_insider_card(name: str) -> str:
    """
    Format a rich Telegram card for an insider's track record.
    Used in enriched signal alerts and /insider command.
    """
    rec = get_insider_record(name)
    if not rec:
        return f"No track record found for: {name}"

    tier = rec.get("score_tier", 0) or 0
    score = rec.get("score") or 0
    best_window = rec.get("best_window") or "7d"

    buy_count = rec.get("buy_count", 0)
    sell_count = rec.get("sell_count", 0)
    total_val = rec.get("buy_total_value", 0)
    title = rec.get("primary_title") or "Unknown"
    ticker = rec.get("primary_ticker") or "?"
    n_tickers = rec.get("n_tickers", 0)

    first = rec.get("buy_first_date") or "?"
    last = rec.get("buy_last_date") or "?"

    def _fmt_window(wr_key, ret_key, abn_key=None):
        wr = rec.get(wr_key)
        ret = rec.get(ret_key)
        abn = rec.get(abn_key) if abn_key else None
        wr_s = f"WR {wr*100:.0f}%" if wr is not None else "WR -"
        ret_s = f"ret {ret*100:+.1f}%" if ret is not None else "ret -"
        abn_s = f"alpha {abn*100:+.1f}%" if abn is not None else ""
        return f"{wr_s}, {ret_s}" + (f", {abn_s}" if abn_s else "")

    lines = [
        f"*{rec.get('name', name)}*  {'*' * tier} (Score {score:.2f})",
        f"Title: {title} | Primary: {ticker} ({n_tickers} companies)",
        f"Best window: {best_window}",
        f"",
        f"Buy Record ({buy_count} trades, {first} - {last}):",
        f"  7d:  {_fmt_window('buy_win_rate_7d', 'buy_avg_return_7d', 'buy_avg_abnormal_7d')}",
        f"  30d: {_fmt_window('buy_win_rate_30d', 'buy_avg_return_30d', 'buy_avg_abnormal_30d')}",
        f"  90d: {_fmt_window('buy_win_rate_90d', 'buy_avg_return_90d', 'buy_avg_abnormal_90d')}",
        f"  Total $ Bought: ${total_val:,.0f}",
    ]

    if sell_count > 0:
        lines.append(f"  Sell trades: {sell_count}")

    return "\n".join(lines)


def format_signal_enriched(signal: dict) -> str:
    """
    Format an enriched cluster signal for Telegram, including per-insider cards.
    """
    ticker = signal.get("ticker", "?")
    company = signal.get("company", "N/A")
    total_value = signal.get("total_value", 0)
    confidence = signal.get("confidence", 0)
    n_insiders = signal.get("n_insiders", 0)

    best_tier = signal.get("best_insider_tier")
    tier_label = {0: "", 1: "Top 33%", 2: "Top 20%", 3: "Top 7%"}.get(best_tier, "")
    has_proven = signal.get("has_proven_insider", False)

    lines = [
        f"*SIGNAL DETECTED*: {ticker}",
        f"Company: {company}",
        f"Total Value: ${total_value:,.0f} | Confidence: {confidence:.1f}",
        f"Insiders: {n_insiders}" + (f" | Best: {tier_label}" if tier_label else ""),
        "",
    ]

    for rec in signal.get("insider_records", []):
        name = rec["name"]
        count = rec.get("buy_count", 0)
        wr = rec.get("win_rate_7d")
        avg = rec.get("avg_return_7d")
        tier = rec.get("score_tier")

        if count == 0:
            lines.append(f"  {name}: No prior trades on record")
        else:
            wr_str = f"{wr*100:.0f}%" if wr is not None else "?"
            avg_str = f"{avg*100:+.1f}%" if avg is not None else "?"
            stars = "⭐" * (tier or 0)
            lines.append(f"  {name} ({rec.get('primary_title', '?')}): "
                         f"{count} trades, WR {wr_str}, avg {avg_str} {stars}")

    if has_proven:
        lines.append("")
        lines.append("✅ *Proven insider in cluster*")
    else:
        lines.append("")
        lines.append("⚠️ No proven insiders (new/unscored)")

    lines.append("_Queued for next market open_")

    return "\n".join(lines)


def format_solo_signal(signal: dict) -> str:
    """Format a solo insider signal for Telegram."""
    name = signal.get("insider_name", "?")
    ticker = signal.get("ticker", "?")
    company = signal.get("company", "?")
    value = signal.get("trade_value", 0)
    tier = signal.get("insider_tier", 0)
    score = signal.get("insider_score", 0)
    count = signal.get("buy_count", 0)
    wr = signal.get("win_rate", 0)
    avg = signal.get("avg_return", 0)
    title = signal.get("primary_title") or signal.get("title", "?")

    stars = "⭐" * tier

    return "\n".join([
        f"*SOLO INSIDER SIGNAL*: {ticker}",
        f"Company: {company}",
        f"Trade Value: ${value:,.0f}",
        f"",
        f"*{name}* {stars} (Score {score:.2f})",
        f"Title: {title}",
        f"Track Record: {count} prior buys",
        f"  Win Rate (7d): {wr*100:.0f}%",
        f"  Avg Return (7d): {avg*100:+.1f}%",
        f"",
        f"_Queued for next market open_",
    ])

"""
EDGAR monitor for Insider V3.3 strategy — both buy and sell signals.

V3.3 filters (from board-approved backtest):
  BUY:  Tier 2+ insider (point-in-time), $2M+ value, quality >= 1.5
  SELL: 2+ insiders selling in 30-day window, $5M+ total value, quality >= 2.14

Reuses V1 utilities: title weights, confidence scoring, EDGAR polling, XML parsing.
"""

from __future__ import annotations

import logging
import math
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

# Reuse V1 utilities
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from insider_cluster_buy.edgar_monitor import (
    poll_edgar_rss,
    fetch_form4_xml,
    parse_form4_xml,
    compute_confidence_score,
    get_title_weight,
    is_csuite,
    DEFAULT_TITLE_WEIGHT,
)

# Insider catalog DB for tier lookup
CATALOG_DIR = Path(__file__).resolve().parent.parent / "insider_catalog"
DB_PATH = CATALOG_DIR / "insiders.db"

logger = logging.getLogger(__name__)

# ── V3.3 Thresholds ──────────────────────────────────────────────────────

# Buy leg: Tier 2+ point-in-time scoring, $2M+, quality >= 1.5
BUY_MIN_INSIDERS = 1
BUY_MIN_VALUE = 2_000_000     # V3: raised from $1M to $2M
BUY_MIN_QUALITY = 1.5         # V3: quality filter
BUY_REQUIRE_TIER2 = True      # V3: point-in-time tier 2+ required

# Sell leg: tighter filters from V3.3 backtest
SELL_MIN_INSIDERS = 2         # Cluster selling is the signal
SELL_MIN_VALUE = 5_000_000    # V3: raised from $1M to $5M
SELL_MIN_QUALITY = 2.14       # V3: quality filter for put leg


# ── Insider Tier Lookup (point-in-time) ──────────────────────────────────

# Cache: loaded once from insiders.db, maps normalized_name -> score_tier
_insider_tier_cache: dict[str, int] = {}
_tier_cache_loaded = False


def _normalize_name(name: str) -> str:
    """Normalize insider name for tier lookup (mirrors backtest_v3.py)."""
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"\b(jr\.?|sr\.?|iii|ii|iv|v|esq\.?|phd|md)\b", "", n)
    n = re.sub(r"[^a-z\s]", "", n)
    return " ".join(n.split())


def load_insider_tiers(db_path: Path = DB_PATH) -> dict[str, int]:
    """
    Load insider tier mapping from insiders.db.

    Uses pre-computed score_tier from insider_track_records table.
    For live trading this is acceptable — tiers are recomputed periodically
    during catalog backfill and the population of Tier 2+ insiders is stable.
    """
    global _insider_tier_cache, _tier_cache_loaded

    if _tier_cache_loaded:
        return _insider_tier_cache

    if not db_path.exists():
        logger.warning("insiders.db not found at %s — tier filtering disabled", db_path)
        _tier_cache_loaded = True
        return _insider_tier_cache

    try:
        from config.database import get_connection
        conn = get_connection(readonly=True)
        rows = conn.execute("""
            SELECT i.name_normalized, COALESCE(tr.score_tier, 0)
            FROM insiders i
            LEFT JOIN insider_track_records tr ON i.insider_id = tr.insider_id
            WHERE i.name_normalized IS NOT NULL AND i.name_normalized != ''
        """).fetchall()
        conn.close()

        for name_norm, tier in rows:
            # Keep highest tier if name appears multiple times
            if name_norm not in _insider_tier_cache or tier > _insider_tier_cache[name_norm]:
                _insider_tier_cache[name_norm] = tier

        tier_counts = defaultdict(int)
        for t in _insider_tier_cache.values():
            tier_counts[t] += 1
        logger.info(
            "Loaded insider tiers: %d insiders (T0=%d, T1=%d, T2=%d, T3=%d)",
            len(_insider_tier_cache),
            tier_counts[0], tier_counts[1], tier_counts[2], tier_counts[3],
        )
    except Exception as e:
        logger.error("Failed to load insider tiers: %s", e)

    _tier_cache_loaded = True
    return _insider_tier_cache


def get_insider_tier(insider_name: str) -> int:
    """Get tier (0-3) for an insider by name. Returns 0 if unknown."""
    tiers = load_insider_tiers()
    norm = _normalize_name(insider_name)
    return tiers.get(norm, 0)


def parse_form4_xml_v2(xml_text: str, cik: str, filing_date: str, company: str) -> list[dict]:
    """
    Parse Form 4 XML and extract BOTH purchase (P) and sale (S) transactions.

    Returns list of trade dicts with additional 'direction' field: 'buy' or 'sell'.
    """
    import xml.etree.ElementTree as ET

    trades = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return trades

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    def find(element, path):
        return element.find(f"{ns}{path}" if ns else path)

    def findtext(element, path, default=""):
        el = find(element, path)
        return el.text.strip() if el is not None and el.text else default

    # Issuer info
    issuer = find(root, "issuer")
    ticker = findtext(issuer, "issuerTradingSymbol") if issuer is not None else ""
    issuer_name = findtext(issuer, "issuerName") if issuer is not None else company

    if not ticker:
        return trades
    ticker = ticker.upper().strip()

    # Reporting owner info
    person = find(root, "reportingOwner")
    insider_name = ""
    title = ""
    if person is not None:
        person_id = find(person, "reportingOwnerId")
        if person_id is not None:
            insider_name = findtext(person_id, "rptOwnerName")

        relationship = find(person, "reportingOwnerRelationship")
        if relationship is not None:
            parts = []
            is_officer = findtext(relationship, "isOfficer")
            officer_title = findtext(relationship, "officerTitle")
            is_director = findtext(relationship, "isDirector")
            is_ten_pct = findtext(relationship, "isTenPercentOwner")

            def is_true(val):
                return val in ("1", "true", "True", "TRUE")

            if is_true(is_officer) and officer_title:
                parts.append(officer_title)
            elif is_true(is_director):
                parts.append("Dir")
            if is_true(is_ten_pct):
                parts.append("10%")
            title = ", ".join(parts) if parts else "Unknown"

    # Check 10b5-1 flag (submission-level)
    is_10b5_1 = False
    aff_el = find(root, "aff10b5One")
    if aff_el is not None and aff_el.text and aff_el.text.strip() in ("1", "true", "True"):
        is_10b5_1 = True

    # Parse non-derivative transactions — P (purchase) and S (sale)
    nd_table = find(root, "nonDerivativeTable")
    if nd_table is None:
        return trades

    tag = f"{ns}nonDerivativeTransaction" if ns else "nonDerivativeTransaction"
    for txn in nd_table.findall(tag):
        txn_code = findtext(txn, "transactionCoding/transactionCode")
        if txn_code not in ("P", "S"):
            continue

        trade_date_raw = findtext(txn, "transactionDate/value", filing_date)
        trade_date = trade_date_raw[:10] if trade_date_raw else filing_date

        price_str = findtext(txn, "transactionAmounts/transactionPricePerShare/value", "0")
        qty_str = findtext(txn, "transactionAmounts/transactionShares/value", "0")

        try:
            price = float(price_str) if price_str else 0.0
            qty = float(qty_str) if qty_str else 0.0
        except ValueError:
            continue

        if price <= 0 or qty <= 0:
            continue

        value = price * qty

        trades.append({
            "ticker": ticker,
            "insider_name": insider_name,
            "title": title,
            "trade_date": trade_date,
            "filing_date": filing_date,
            "price": price,
            "qty": int(qty),
            "value": value,
            "cik": cik,
            "company": issuer_name,
            "is_csuite": is_csuite(title),
            "title_weight": get_title_weight(title),
            "direction": "buy" if txn_code == "P" else "sell",
            "is_10b5_1": is_10b5_1,
        })

    return trades


# ── Rolling Window (separate buy/sell windows) ───────────────────────────


def update_rolling_windows(
    trades: list[dict],
    buy_window: dict,
    sell_window: dict,
    window_days: int = 30,
) -> None:
    """
    Add parsed trades to the appropriate rolling window (buy or sell).
    Prunes entries older than window_days.
    """
    cutoff = (date.today() - timedelta(days=window_days)).isoformat()

    for trade in trades:
        ticker = trade["ticker"]
        direction = trade.get("direction", "buy")

        window = buy_window if direction == "buy" else sell_window
        if ticker not in window:
            window[ticker] = []
        window[ticker].append(trade)

    # Prune old entries in both windows
    for window in (buy_window, sell_window):
        for ticker in list(window.keys()):
            window[ticker] = [
                t for t in window[ticker]
                if t.get("filing_date", "") >= cutoff
            ]
            if not window[ticker]:
                del window[ticker]


# ── V2 Trigger Checks ────────────────────────────────────────────────────


def check_buy_trigger(ticker: str, buy_window: dict) -> Optional[dict]:
    """
    Check if a ticker's buy filings form a qualifying V3.3 buy signal.

    V3.3 buy trigger:
      - 1+ insider purchasing
      - $2M+ total purchase value
      - At least one Tier 2+ insider (point-in-time scoring)
      - Quality score >= 1.5
    """
    filings = buy_window.get(ticker, [])
    if not filings:
        return None

    distinct_insiders = set()
    total_value = 0.0
    title_weights = []
    max_single_value = 0.0
    insider_values = defaultdict(float)
    insider_tiers = {}

    for f in filings:
        name = f.get("insider_name", "Unknown")
        distinct_insiders.add(name)
        val = f.get("value", 0)
        total_value += val
        insider_values[name] += val
        title_weights.append(f.get("title_weight", DEFAULT_TITLE_WEIGHT))
        # Look up tier for each insider
        if name not in insider_tiers:
            insider_tiers[name] = get_insider_tier(name)

    n_insiders = len(distinct_insiders)
    max_single_value = max(insider_values.values()) if insider_values else 0

    # V3.3 buy filters
    if n_insiders < BUY_MIN_INSIDERS:
        return None
    if total_value < BUY_MIN_VALUE:
        return None

    # Tier 2+ check: at least one insider must be Tier 2 or 3
    max_tier = max(insider_tiers.values()) if insider_tiers else 0
    if BUY_REQUIRE_TIER2 and max_tier < 2:
        logger.debug(
            "Buy trigger SKIPPED %s: no Tier 2+ insider (best tier=%d)",
            ticker, max_tier,
        )
        return None

    conf = compute_confidence_score(
        total_value=total_value,
        n_distinct_insiders=n_insiders,
        title_weights=title_weights,
        max_single_value=max_single_value,
    )

    # Quality filter
    if conf["quality_score"] < BUY_MIN_QUALITY:
        logger.debug(
            "Buy trigger SKIPPED %s: quality=%.2f < %.2f",
            ticker, conf["quality_score"], BUY_MIN_QUALITY,
        )
        return None

    latest_filing = max(filings, key=lambda f: f.get("filing_date", ""))

    signal = {
        "direction": "buy",
        "ticker": ticker,
        "company": latest_filing.get("company", ticker),
        "n_insiders": n_insiders,
        "insiders": list(distinct_insiders),
        "total_value": total_value,
        "confidence": conf["confidence_score"],
        "quality_score": conf["quality_score"],
        "max_insider_tier": max_tier,
        "insider_tiers": insider_tiers,
        "trigger_date": latest_filing["filing_date"],
        "entry_date": date.today().isoformat(),
        "filings": filings,
    }

    logger.info(
        "BUY SIGNAL: %s — %d insider(s), $%.0f, tier=%d, quality=%.2f",
        ticker, n_insiders, total_value, max_tier, conf["quality_score"],
    )
    return signal


def _get_insider_sell_accuracy(name_normalized: str) -> float | None:
    """Look up sell accuracy from insider_track_records by normalized name."""
    try:
        from config.database import get_connection
        conn = get_connection(readonly=True)
        row = conn.execute("""
            SELECT itr.sell_win_rate_7d
            FROM insiders i
            JOIN insider_track_records itr ON i.insider_id = itr.insider_id
            WHERE i.name_normalized = ?
            LIMIT 1
        """, (name_normalized,)).fetchone()
        conn.close()
        return row["sell_win_rate_7d"] if row and row["sell_win_rate_7d"] is not None else None
    except Exception:
        return None


def check_sell_trigger(ticker: str, sell_window: dict) -> Optional[dict]:
    """
    Check if a ticker's sell filings form a qualifying V3.5 sell signal.

    V3.5 sell trigger (discretionary + proven seller quality):
      - Exclude 10b5-1 planned sales
      - 2+ distinct insiders selling within 30 days (cluster)
      - $5M+ total sale value
      - Quality tier: if avg sell accuracy >= 65% AND C-suite present → high_conviction
    """
    filings = sell_window.get(ticker, [])
    if not filings:
        return None

    # V3.4: Filter out 10b5-1 planned sales — these are noise
    discretionary = [f for f in filings if not f.get("is_10b5_1", False)]
    if not discretionary:
        return None

    distinct_insiders = set()
    total_value = 0.0
    title_weights = []
    max_single_value = 0.0
    insider_values = defaultdict(float)

    for f in discretionary:
        name = f.get("insider_name", "Unknown")
        distinct_insiders.add(name)
        val = f.get("value", 0)
        total_value += val
        insider_values[name] += val
        title_weights.append(f.get("title_weight", DEFAULT_TITLE_WEIGHT))

    n_insiders = len(distinct_insiders)
    max_single_value = max(insider_values.values()) if insider_values else 0

    # V3.3 sell cluster filters
    if n_insiders < SELL_MIN_INSIDERS:
        return None
    if total_value < SELL_MIN_VALUE:
        return None

    conf = compute_confidence_score(
        total_value=total_value,
        n_distinct_insiders=n_insiders,
        title_weights=title_weights,
        max_single_value=max_single_value,
    )

    # Quality filter
    if conf["quality_score"] < SELL_MIN_QUALITY:
        logger.debug(
            "Sell trigger SKIPPED %s: quality=%.2f < %.2f",
            ticker, conf["quality_score"], SELL_MIN_QUALITY,
        )
        return None

    latest_filing = max(filings, key=lambda f: f.get("filing_date", ""))

    # V3.5: Compute proven-seller quality tier
    sell_accuracies = []
    has_csuite = False
    for f in discretionary:
        name = f.get("insider_name", "")
        norm = _normalize_name(name) if name else ""
        if norm:
            acc = _get_insider_sell_accuracy(norm)
            if acc is not None:
                sell_accuracies.append(acc)
        if f.get("is_csuite"):
            has_csuite = True

    avg_sell_accuracy = float(np.mean(sell_accuracies)) if sell_accuracies else 0.0
    is_high_conviction = (
        avg_sell_accuracy >= 0.65
        and has_csuite
        and total_value >= 5_000_000
    )

    signal = {
        "direction": "sell",
        "ticker": ticker,
        "company": latest_filing.get("company", ticker),
        "n_insiders": n_insiders,
        "insiders": list(distinct_insiders),
        "total_value": total_value,
        "confidence": conf["confidence_score"],
        "quality_score": conf["quality_score"],
        "trigger_date": latest_filing["filing_date"],
        "entry_date": date.today().isoformat(),
        "filings": filings,
        "avg_sell_accuracy": round(avg_sell_accuracy, 3),
        "has_csuite": has_csuite,
        "is_high_conviction": is_high_conviction,
    }

    tier = "HIGH CONVICTION" if is_high_conviction else "standard"
    logger.info(
        "SELL SIGNAL [%s]: %s — %d sellers, $%.0f, quality=%.2f, sell_acc=%.1f%%",
        tier, ticker, n_insiders, total_value, conf["quality_score"],
        avg_sell_accuracy * 100,
    )
    return signal

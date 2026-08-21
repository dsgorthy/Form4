"""
Signal Quality Score — composite quality indicator for insider trades.

Combines validated research factors into a 0-10 score:
- Cohen routine classification (opportunistic vs routine)
- Holdings % change (≥10% = high signal, <1% = noise)
- 10% Owner penalty (with activist exception)
- C-suite bonus
- Insider track record tier
- Market cap tier
- Sell-specific: routine/10b5-1 penalty, proven seller accuracy bonus

Displayed as a letter grade: A (8+), B (6-7), C (5), D (3-4), F (0-2)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Market cap cache
_mcaps: dict[str, int] | None = None


def _load_mcaps() -> dict[str, int]:
    global _mcaps
    if _mcaps is not None:
        return _mcaps
    # Try container mount path first, then local dev path
    candidates = [
        Path("/data/app_data/market_caps.json"),
        Path(__file__).resolve().parent.parent / "data" / "market_caps.json",
    ]
    for mcap_path in candidates:
        try:
            _mcaps = json.loads(mcap_path.read_text())
            logger.info("Loaded %d market caps from %s", len(_mcaps), mcap_path)
            return _mcaps
        except Exception:
            continue
    _mcaps = {}
    return _mcaps


def compute_signal_quality(item: dict) -> dict | None:
    """Compute signal quality score for a trade item (dict with trade fields).

    Returns {score: float, grade: str, factors: list[str]} or None if insufficient data.
    """
    score = 5.0
    factors = []
    trade_type = item.get("trade_type", "buy")
    is_buy = trade_type == "buy"

    # 1. Cohen routine
    cohen = item.get("cohen_routine")
    if cohen == 0:
        score += 1.5
        factors.append("Opportunistic trade pattern")
    elif cohen == 1:
        score -= 1.0
        factors.append("Routine trade pattern")

    # 2. Holdings % change (buys)
    if is_buy:
        qty = item.get("qty") or 0
        shares_after = item.get("shares_owned_after") or 0
        if shares_after > 0 and qty > 0:
            before = shares_after - qty
            if before > 0:
                pct = (qty / before) * 100
                if pct >= 10:
                    score += 1.5
                    factors.append(f"Large holdings increase ({pct:.0f}%)")
                elif pct < 1:
                    score -= 1.5
                    factors.append(f"Minimal holdings increase ({pct:.2f}%)")

    # 3. 10% Owner
    title = (item.get("title") or "").lower()
    is_10pct = "10%" in title or "tenpercent" in title
    is_csuite = item.get("is_csuite")
    if is_10pct and not is_csuite:
        # Check if activist (from signal metadata)
        signals = item.get("signals") or []
        is_activist = any(
            s.get("signal_type") == "ten_pct_owner_buy" and s.get("signal_class") == "bullish"
            for s in signals
        )
        if is_activist:
            score += 0.5
            factors.append("Activist investor")
        else:
            score -= 1.5
            factors.append("10% Owner (typically low signal)")

    # 4. C-suite
    if is_csuite:
        score += 0.5
        factors.append("C-suite executive")

    # 5. Insider PIT grade (point-in-time, not full-history)
    pit_grade = item.get("pit_grade")
    if is_buy and pit_grade:
        if pit_grade == "A":
            score += 1.5
            factors.append("PIT A-grade insider")
        elif pit_grade == "B":
            score += 0.75
            factors.append("PIT B-grade insider")

    # 6. Market cap
    mcaps = _load_mcaps()
    ticker = item.get("ticker", "")
    mc = mcaps.get(ticker)
    if mc:
        if mc < 2e9:
            score += 0.5
            factors.append("Small-cap (stronger insider signal)")
        elif mc > 1e11:
            score -= 0.5
            factors.append("Mega-cap (weaker insider signal)")

    # 7. Rare reversal (Akbas 2018)
    is_rare_reversal = item.get("is_rare_reversal")
    if is_rare_reversal == 1:
        score += 1.5
        if is_buy:
            factors.append("Rare reversal (persistent seller now buying)")
        else:
            factors.append("Rare reversal (persistent buyer now selling)")

    # 8. 52-week proximity (Lasfer 2024)
    w52 = item.get("week52_proximity")
    if w52 is not None:
        if is_buy and w52 >= 0.8:
            score += 0.5
            factors.append(f"Buying near 52-week high ({w52*100:.0f}%)")
        elif is_buy and w52 <= 0.2:
            score -= 0.5
            factors.append(f"Buying near 52-week low ({w52*100:.0f}%)")
        elif not is_buy and w52 <= 0.2:
            score += 0.5
            factors.append(f"Selling near 52-week low ({w52*100:.0f}%)")

    # 9. Sell-specific
    if not is_buy:
        is_routine = item.get("is_routine") == 1
        is_planned = item.get("is_10b5_1") == 1
        if is_routine or is_planned:
            score -= 2.0
            if is_planned:
                factors.append("Pre-planned (10b5-1)")
            else:
                factors.append("Routine selling pattern")

        # Proven seller accuracy — check from track record if available
        # This comes through as part of the filing_stats or track_record
        sell_acc = item.get("_sell_accuracy")
        if sell_acc and sell_acc >= 0.60:
            score += 2.0
            factors.append(f"Proven seller ({sell_acc*100:.0f}% accuracy)")
        elif sell_acc and sell_acc >= 0.55:
            score += 1.0
            factors.append(f"Above-average seller ({sell_acc*100:.0f}% accuracy)")

    # Clamp and grade
    score = max(0, min(10, round(score, 1)))

    if score >= 8:
        grade = "A"
    elif score >= 6:
        grade = "B"
    elif score >= 5:
        grade = "C"
    elif score >= 3:
        grade = "D"
    else:
        grade = "F"

    return {"score": score, "grade": grade, "factors": factors}


def enrich_items_with_quality(conn, items: list[dict]) -> None:
    """Add signal_quality to each item in-place.

    Uses fields already present on items from the query (cohen_routine,
    shares_owned_after, sell_win_rate_7d) to avoid DB round-trips.
    Falls back to DB lookup only when fields are missing.
    """
    if not items:
        return

    # Check if items already have the fields we need (from the SQL query).
    # If so, skip the DB lookups entirely.
    needs_db = False
    for item in items:
        if "cohen_routine" not in item or "shares_owned_after" not in item:
            needs_db = True
            break

    cohen_map: dict = {}
    if needs_db:
        trade_ids = []
        for item in items:
            tid = item.get("_raw_trade_id") or item.get("trade_id")
            if isinstance(tid, int):
                trade_ids.append(tid)
            elif isinstance(tid, str) and tid.isdigit():
                trade_ids.append(int(tid))
        if trade_ids:
            placeholders = ",".join("?" * len(trade_ids))
            try:
                rows = conn.execute(
                    f"SELECT trade_id, cohen_routine, shares_owned_after, qty, title, is_csuite, is_rare_reversal, week52_proximity FROM trades WHERE trade_id IN ({placeholders})",
                    trade_ids,
                ).fetchall()
                for r in rows:
                    cohen_map[r["trade_id"]] = dict(r)
            except Exception:
                pass

    for item in items:
        tid = item.get("_raw_trade_id") or item.get("trade_id")
        if isinstance(tid, str) and tid.isdigit():
            tid = int(tid)

        # Use fields from query if present, otherwise merge from DB
        if needs_db:
            extra = cohen_map.get(tid, {})
            merged = {**item, **extra}
        else:
            merged = item

        # Sell accuracy: use sell_win_rate_7d from the query join
        if item.get("trade_type") == "sell":
            swr = item.get("sell_win_rate_7d")
            if swr is not None:
                merged["_sell_accuracy"] = swr

        quality = compute_signal_quality(merged)
        if quality:
            item["signal_quality"] = quality

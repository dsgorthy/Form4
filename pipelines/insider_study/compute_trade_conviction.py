"""
Single source of truth for trade conviction computation.

Used by BOTH grid_search_reversal.py AND backfill_cw_portfolio.py.
All inputs are PIT-safe (verified in audit 2026-04-01).
"""

from __future__ import annotations

import sqlite3

from pipelines.insider_study.conviction_score import compute_conviction, pit_score_to_grade


# Cache for PIT grade lookups to avoid redundant DB queries
_pit_cache: dict[tuple[int, str, str], str] = {}


def compute_full_conviction(
    event: dict,
    conn: sqlite3.Connection,
    thesis: str,
) -> float:
    """Compute conviction with ALL available PIT-safe inputs.

    Every input used here has been audited for PIT safety:
    - pit_grade: insider_ticker_scores WHERE as_of_date <= filing_date
    - consecutive_sells_before: counts sells before this buy only
    - dip_1mo/dip_3mo: price change ending at trade_date
    - above_sma50/above_sma200: SMA at or before trade_date
    - is_largest_ever: compares against prior trades only
    - insider_title/is_csuite: static from Form 4 filing
    - holdings_pct_change: computed from qty + shares_owned_after (filing data)
    - cluster_size: pit_cluster_size (backward-looking 30-day window)
    - is_opportunistic: cohen_routine == 0 (PIT: 3+ prior years)
    - trade_value: from filing (static)
    """
    insider_id = event.get("insider_id")
    ticker = event.get("ticker")
    filing_date = event.get("filing_date")

    # --- PIT grade lookup (cached) ---
    cache_key = (insider_id, ticker, filing_date)
    if cache_key not in _pit_cache:
        pit_row = conn.execute("""
            SELECT blended_score FROM insider_ticker_scores
            WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
            ORDER BY as_of_date DESC LIMIT 1
        """, (insider_id, ticker, filing_date)).fetchone()
        _pit_cache[cache_key] = pit_score_to_grade(pit_row[0] if pit_row else None) or "C"
    pit_grade = _pit_cache[cache_key]

    # --- Holdings % change ---
    after = event.get("shares_owned_after") or 0
    qty = event.get("qty") or 0
    if after > 0 and qty > 0 and after > qty:
        holdings_pct = qty / (after - qty)
    else:
        holdings_pct = None

    # --- Cluster size from pit_cluster_size ---
    cluster = event.get("pit_cluster_size") or 0

    # --- Opportunistic (cohen_routine == 0) ---
    cohen = event.get("cohen_routine")
    is_opp = cohen == 0 if cohen is not None else False

    # --- Trade value ---
    trade_val = event.get("value")

    return compute_conviction(
        thesis=thesis,
        signal_grade=pit_grade,
        consecutive_sells=event.get("consecutive_sells_before"),
        dip_1mo=event.get("dip_1mo"),
        dip_3mo=event.get("dip_3mo"),
        is_largest_ever=bool(event.get("is_largest_ever")),
        above_sma50=bool(event.get("above_sma50")),
        above_sma200=bool(event.get("above_sma200")),
        insider_title=event.get("title"),
        is_csuite=bool(event.get("is_csuite")),
        holdings_pct_change=holdings_pct,
        cluster_size=cluster,
        is_opportunistic=is_opp,
        trade_value=trade_val,
    )


def clear_cache():
    """Clear PIT grade cache between strategy runs."""
    _pit_cache.clear()

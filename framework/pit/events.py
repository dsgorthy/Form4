"""Immutable event/decision/score types used by PITDataView and strategies.

These dataclasses are deliberately narrow: they expose only fields that are
PIT-safe by construction. A `TradeEvent` cannot carry a forward return or
a future filing_date — if you find yourself wanting to add such a field,
add a NEW accessor method to `PITDataView` instead so the PIT check happens
at the boundary.

All `__post_init__` validators run cheap structural checks. They are NOT a
substitute for the clock's `assert_known` — that runs at the DB boundary.
These checks catch programming errors (typos, wrong field order) at
construction time.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class TradeEvent:
    """A single insider trade as it became knowable at `filing_date`.

    Only fields here that are derivable from PIT-safe inputs. Forward
    returns / future-filed amendments / live prices are NOT carried — they
    are queried separately through the view and must pass through the clock.
    """
    trade_id: int
    insider_id: int
    ticker: str
    trade_date: str            # transaction time
    filing_date: str           # knowledge time — the only date the engine sees
    trade_type: str            # 'buy' | 'sell'
    insider_name: Optional[str] = None
    insider_title: Optional[str] = None
    is_csuite: Optional[bool] = None

    # Pre-computed CW indicators (themselves PIT — computed from trade_date
    # back, validated in compute_cw_indicators.py).
    consecutive_sells_before: Optional[int] = None
    dip_1mo: Optional[float] = None
    dip_3mo: Optional[float] = None
    above_sma50: Optional[int] = None
    above_sma200: Optional[int] = None
    is_largest_ever: Optional[int] = None
    is_rare_reversal: Optional[int] = None
    is_10b5_1: Optional[int] = None
    is_recurring: Optional[int] = None
    is_tax_sale: Optional[int] = None
    cohen_routine: Optional[int] = None

    # Static grade columns — these are PIT-stamped at filing_date by
    # backfill_pit_grades.py / backfill_v3_missing_trades.py
    pit_grade: Optional[str] = None
    career_grade: Optional[str] = None
    pit_blended_score: Optional[float] = None

    # For the audit trail
    company: Optional[str] = None


@dataclass(frozen=True)
class InsiderScore:
    """Result of `PITDataView.get_insider_score(insider_id, ticker)`.

    All fields are as-of the clock's `as_of_date`. The `as_of_date` carried
    on this dataclass is the **score's** as_of_date — i.e., the most-recent
    score row whose as_of_date <= clock.as_of_date. It may be earlier than
    clock.as_of_date if the insider has no scoring event on that day.
    """
    insider_id: int
    ticker: str
    as_of_date: str
    blended_score: Optional[float] = None         # V2 / Recent Form
    career_blended_score: Optional[float] = None  # V3 / Career
    pit_grade: Optional[str] = None
    career_grade: Optional[str] = None
    ticker_trade_count: int = 0
    global_trade_count: int = 0
    sufficient_data: bool = False


@dataclass
class Decision:
    """A strategy's evaluation of a TradeEvent. The engine consumes this.

    Mutable on purpose — strategies may emit a Decision and the engine may
    augment it (e.g., add fill price, position size) before persisting.
    """
    trade_id: int
    ticker: str
    filing_date: str
    strategy: str
    action: str                   # 'enter' | 'skip' | 'rotate'
    stage: str                    # 'dedup' | 'filter' | 'conviction' | 'capacity'
    passed: bool
    reason: str
    conviction: Optional[float] = None
    pit_grade: Optional[str] = None
    career_grade: Optional[str] = None
    snapshot: Dict[str, Any] = field(default_factory=dict)

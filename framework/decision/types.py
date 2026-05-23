"""Dataclasses that flow through the decision engine.

These are the wire format between the runner (which fetches facts and
emits intents) and the decision engine (which transforms facts into
intents). They are deliberately plain dataclasses — no DB columns, no
Alpaca-isms — so the engine stays pure.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class CandidateFact:
    """One P-trade filing being considered for entry on a given date.

    Sourced from `trades` joined with `insiders` and any precomputed
    feature columns. Both sim and live populate the same fields so the
    engine can't tell the difference.
    """
    trade_id: int
    insider_id: int
    ticker: str
    filing_date: str
    trade_date: str
    insider_name: Optional[str] = None
    insider_title: Optional[str] = None
    company: Optional[str] = None
    is_csuite: bool = False
    is_duplicate: bool = False
    is_rare_reversal: bool = False
    consecutive_sells_before: Optional[int] = None
    dip_1mo: Optional[float] = None
    dip_3mo: Optional[float] = None
    above_sma50: bool = False
    above_sma200: bool = False
    is_largest_ever: bool = False
    is_10b5_1: bool = False
    is_recurring: bool = False
    is_tax_sale: bool = False
    cohen_routine: bool = False
    pit_grade: Optional[str] = None
    career_grade: Optional[str] = None
    # Decision engine attaches conviction during processing; runners
    # should not pre-populate it.
    conviction: Optional[float] = None


@dataclass
class PositionState:
    """A currently-held position from the perspective of the decision engine.

    Used both for capacity checks (don't re-enter held tickers) and for
    exit decisions (compare today's price to stop / target / hold-days).
    """
    strategy: str
    ticker: str
    trade_id: int
    entry_date: str
    entry_price: float
    capital_at_entry: float
    target_exit_idx: int           # calendar index when hold_td expires
    stop_price: float
    conviction: float
    pit_grade: Optional[str] = None
    career_grade: Optional[str] = None
    days_held: int = 0
    # Stale-price tracking (Stage 0 addition — see simulate_strategy_portfolio)
    last_seen_close: Optional[float] = None
    last_seen_date: Optional[str] = None


@dataclass
class StrategyConfig:
    """Engine-relevant subset of a strategy's YAML config.

    The runners load the full YAML (which also has e.g. alpaca_env_prefix
    that is not the engine's business); this is the slice the engine
    actually uses. Keeps the engine signature stable as YAML evolves.
    """
    name: str
    hold_td: int
    position_size_pct: float
    max_concurrent: int
    min_conviction: float
    stop_loss_pct: float
    thesis_filters: dict
    # Capacity behavior at max_concurrent: 'skip' (default) drops new
    # candidates; 'replace_weakest' rotates out the lowest-conviction
    # current holding if the new candidate beats it by replacement_advantage.
    at_capacity: str = "skip"
    soft_cap: Optional[int] = None
    min_conviction_above_soft: Optional[float] = None
    replacement_advantage: float = 0.0


@dataclass
class TradeIntent:
    """An entry decision emitted by decide_entries(). The runner (live or
    sim) is responsible for actually placing the trade — but at this point
    the decision is final, the engine has signed off, and the only
    remaining work is execution + persistence.
    """
    strategy: str
    ticker: str
    trade_id: int
    side: str                       # "buy" (entry) — sell intents use ExitIntent
    qty_dollars: float              # $ allocated; runner converts to share count
    conviction: float
    reason: str                     # "entry"
    insider_name: Optional[str] = None
    insider_title: Optional[str] = None
    company: Optional[str] = None
    pit_grade: Optional[str] = None
    career_grade: Optional[str] = None
    is_csuite: bool = False
    is_rare_reversal: bool = False
    # Extra context the runner can forward to audit logs / DB
    metadata: dict = field(default_factory=dict)


@dataclass
class ExitIntent:
    """An exit decision emitted by decide_exits()."""
    strategy: str
    ticker: str
    trade_id: int
    side: str                       # "sell"
    reason: str                     # "time" | "stop" | "time_stale" | "stop_stale" | "replacement"
    exit_price: float
    pnl_pct: float
    pnl_dollar: float
    hold_days: int
    stale_price: bool = False       # true if exit_price was a fallback from last_seen_close
    metadata: dict = field(default_factory=dict)

"""Minimal PIT backtest engine — proves the rails work end-to-end.

Scope (Phase 1+2 in `docs/pit_backtest_design.md`):
  - Walk a date range forward
  - Build a per-date PITClock and PITDataView
  - For each filing_date, present events to the strategy
  - Collect decisions into a result object
  - NO portfolio / capacity / exit logic yet (those live in Phase 3 alongside
    the strategy-level rewrite). For now the engine returns the per-event
    decision stream — equivalent to what `simulate_decision_audit.py` writes
    to `trade_decision_audit` table.

The point of this thin engine is to prove:
  1. PITStrategy + PITDataView compose cleanly
  2. The engine itself can be tested for PIT correctness (read tape audit)
  3. Decisions reproduce what cw_runner / simulate_decision_audit would
     compute (equivalence test exists in tests/integration)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from framework.pit.clock import PITClock
from framework.pit.events import Decision
from framework.pit.strategy import PITStrategy
from framework.pit.view import PITDataView

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Aggregate output of a backtest run."""
    strategy: str
    start_date: str
    end_date: str
    decisions: List[Decision] = field(default_factory=list)
    days_processed: int = 0
    events_evaluated: int = 0
    n_enter: int = 0
    n_skip: int = 0
    max_knowledge_date_seen: Optional[str] = None  # for PIT audit

    def by_action(self) -> dict:
        return {
            "enter": sum(1 for d in self.decisions if d.action == "enter"),
            "skip": sum(1 for d in self.decisions if d.action == "skip"),
            "rotate": sum(1 for d in self.decisions if d.action == "rotate"),
        }

    def by_stage(self) -> dict:
        from collections import Counter
        return dict(Counter(d.stage for d in self.decisions))


class PITBacktestEngine:
    """Walks dates forward, presents events to a strategy.

    Parameters
    ----------
    conn : DBAPI connection — typically a `config.database.get_connection()`.
           The engine forwards the connection to PITDataView; strategies
           never see it directly.

    Notes
    -----
    The engine ASSERTS its read tape after each day. Any accidental
    lookahead by an accessor surfaces as `LookaheadError` immediately;
    we don't need a separate "post-hoc audit" pass.
    """

    def __init__(self, conn) -> None:
        self.conn = conn

    def run(self, strategy: PITStrategy, start_date: str, end_date: str,
            trading_day_loader=None) -> BacktestResult:
        """Execute the backtest.

        `trading_day_loader`: optional callable that returns the list of
        trading days in [start, end]. Defaults to "every calendar day with
        a row in prices.daily_prices for any ticker." For pure-event
        backtests (insider strategies), this is good enough.
        """
        result = BacktestResult(
            strategy=strategy.name, start_date=start_date, end_date=end_date,
        )
        days = (trading_day_loader or self._default_trading_days)(start_date, end_date)
        logger.info("PIT backtest [%s] %s → %s — %d trading days",
                    strategy.name, start_date, end_date, len(days))

        for d in days:
            clock = PITClock(as_of_date=d)
            view = PITDataView(clock, self.conn)
            events = view.events_filed_on(d)

            for event in events:
                result.events_evaluated += 1
                decision = strategy.evaluate(view, event)
                result.decisions.append(decision)
                if decision.action == "enter":
                    result.n_enter += 1
                elif decision.action == "skip":
                    result.n_skip += 1

            # After-day audit: every read on this clock must have a
            # knowledge_date ≤ d. If not, LookaheadError already raised mid-day.
            max_kd = clock.tape.max_knowledge_date()
            if max_kd:
                if (result.max_knowledge_date_seen is None
                        or max_kd > result.max_knowledge_date_seen):
                    result.max_knowledge_date_seen = max_kd
            result.days_processed += 1

        logger.info("PIT backtest done: %d events, %d enter, %d skip; "
                    "max knowledge_date seen = %s",
                    result.events_evaluated, result.n_enter, result.n_skip,
                    result.max_knowledge_date_seen)
        return result

    def _default_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """Default: dates with any price row in [start, end]."""
        rows = self.conn.execute(
            """
            SELECT DISTINCT date::text AS d
            FROM prices.daily_prices
            WHERE date >= ? AND date <= ?
            ORDER BY d
            """,
            (start_date, end_date),
        ).fetchall()
        return [r[0] for r in rows]

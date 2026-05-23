"""Pure decision engine — single source of truth for "should we trade?".

The goal of this module is to make sim/live drift IMPOSSIBLE BY CONSTRUCTION.
Both the live runner (cw_runner) and the walk-forward simulator
(simulate_strategy_portfolio) must call the same `decide_entries()` and
`decide_exits()` functions. No I/O, no DB, no Alpaca — the runners are
responsible for fetching facts and emitting intents; this module just
maps (facts, state, config) → intents.

Migration plan (Stage 3 — in progress):
    Phase A (now): filter evaluation extracted to shared module.
                   simulate_strategy_portfolio uses it. cw_runner's
                   SQL-based _build_thesis_query() still owns live
                   filter logic — drift surface remains.
    Phase B:       extract conviction + capacity + soft-cap + replacement
                   into shared functions; cw_runner refactored to call
                   them instead of inline SQL/Python.
    Phase C:       both sim and live driven by `decide_entries(date,
                   candidates, open_positions, config) -> List[TradeIntent]`
                   and `decide_exits(date, open_positions, prices, config)
                   -> List[ExitIntent]`. Property-based tests verify
                   semantic equivalence across both runners by replaying
                   trade_decision_audit fixtures.
"""
from framework.decision.filters import evaluate_filters
from framework.decision.types import (
    CandidateFact,
    PositionState,
    StrategyConfig,
    TradeIntent,
    ExitIntent,
)

__all__ = [
    "evaluate_filters",
    "CandidateFact",
    "PositionState",
    "StrategyConfig",
    "TradeIntent",
    "ExitIntent",
]

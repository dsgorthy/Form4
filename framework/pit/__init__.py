"""PIT-honest backtest/runtime primitives.

See `docs/pit_backtest_design.md` for the architecture rationale.

Public surface — anything below is the only legal way to access PIT data
from strategies:

    from framework.pit import PITClock, PITDataView, TradeEvent, Decision
    from framework.pit import LookaheadError

Strategies must NOT import from `config.database` directly. The point of
this module is that PIT enforcement is structural, not conventional.
"""

from framework.pit.clock import PITClock, LookaheadError
from framework.pit.events import TradeEvent, Decision, InsiderScore
from framework.pit.view import PITDataView

__all__ = [
    "PITClock",
    "LookaheadError",
    "PITDataView",
    "TradeEvent",
    "Decision",
    "InsiderScore",
]

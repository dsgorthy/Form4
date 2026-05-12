"""PITStrategy — the interface backtests and live cw_runner share.

A PITStrategy is a PURE function of (view, event) → Decision. Strategies do
NOT take DB connections, do NOT compute returns, and do NOT mutate state.
They observe the world through the `PITDataView` and emit decisions.

The engine (or live runner) is responsible for:
  - Walking time forward
  - Building per-date views
  - Persisting decisions
  - Managing portfolio state, capacity, exits

This separation is exactly what makes the architecture testable: a strategy
can be exercised at any historical as_of_date by constructing the right
clock/view pair, with no production side effects.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from framework.pit.events import Decision, TradeEvent
from framework.pit.view import PITDataView


class PITStrategy(ABC):
    """Abstract base class. Subclasses implement `evaluate`."""

    def __init__(self, config: dict) -> None:
        self.config = config

    @property
    def name(self) -> str:
        return self.config["strategy_name"]

    @abstractmethod
    def evaluate(self, view: PITDataView, event: TradeEvent) -> Decision:
        """Decide whether `event` should trigger an entry.

        The strategy may consult `view` for prior trades, scores, prices, etc.
        Every read is PIT-enforced. The strategy returns a `Decision` whose
        `action` is one of:

          'enter' — pre-capacity admission; the engine still applies dedup
                    and capacity rules before opening the position.
          'skip'  — explicit rejection (filter failed, conviction too low).
          'rotate'— at-capacity admission that requires replacing a held
                    position. (Engine handles the actual swap.)

        The Decision carries `stage` indicating where the decision was made
        (`'filter' | 'conviction' | 'capacity'`) so the audit table can
        reconstruct the full per-stage trail without re-running.
        """
        ...

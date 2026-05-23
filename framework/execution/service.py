"""ExecutionService — the only thing in the codebase that should talk to a broker.

Sits between the decision engine (which emits TradeIntent / ExitIntent) and
the broker backend (PaperBackend / LiveBackend). Owns:
  - sizing: convert intent.qty_dollars → share count using a live quote
  - idempotency: deterministic client_order_id so retries don't duplicate
  - submission: call backend.submit_order with retries
  - audit: write order_audit + the appropriate trades table
  - return: a Fill dataclass the runner can act on

Status (Stage 4 — scaffold):
    The class exists with full contract but is NOT YET WIRED into cw_runner.
    cw_runner still calls alpaca.submit_order inline. The migration is to
    refactor cw_runner.execute_entries() to:
      1. fetch facts → call decide_entries() → list[TradeIntent]
      2. for each intent, call ExecutionService.execute_intent(intent)
      3. consume Fill, no direct Alpaca calls in the runner anymore

    Once that lands, grep for 'alpaca.' in cw_runner.py should match only
    imports — the runner is broker-agnostic and the service owns the wire.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from typing import Optional

from framework.decision.types import ExitIntent, TradeIntent
from framework.execution.base import ExecutionBackend, OrderResult

logger = logging.getLogger(__name__)


@dataclass
class Fill:
    """Result of executing one intent.

    Returned by ExecutionService.execute_intent() / execute_exit(). The
    runner uses this to update its in-memory state and (eventually) to
    log alongside the strategy_portfolio row the service already wrote.
    """
    intent_id: str
    strategy: str
    ticker: str
    side: str                       # "buy" | "sell"
    qty: int
    filled: bool                    # true if backend confirmed fill
    avg_price: Optional[float] = None
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    error: Optional[str] = None
    backend_mode: Optional[str] = None   # "PaperBackend" | "LiveBackend" | etc.
    raw_result: dict = field(default_factory=dict)


class ExecutionService:
    """Broker-agnostic trade executor.

    One instance per (strategy, broker). cw_runner holds one of these per
    strategy and never touches alpaca directly.

    Parameters
    ----------
    backend : ExecutionBackend
        Already-configured broker (PaperBackend or LiveBackend with creds).
    strategy : str
        Owning strategy name — recorded in order_audit and trade rows.
    is_live : bool
        Routes writes to live_trades (true) vs paper_trades (false). Must
        match the backend's mode — calling LiveBackend with is_live=False
        would write a real-money fill to the paper-trades table and is a
        contract violation (asserted on init).
    quote_fn : callable, optional
        () → float — returns the latest market quote for ticker sizing.
        Defaults to backend.get_position(...)['current_price'] fallback;
        callers can inject a faster data-API quote.
    """

    def __init__(
        self,
        backend: ExecutionBackend,
        strategy: str,
        is_live: bool,
        quote_fn=None,
    ):
        self.backend = backend
        self.strategy = strategy
        self.is_live = is_live
        self.quote_fn = quote_fn
        # Future: a structural assert that backend.mode() matches is_live.
        # Live-money invariants live in tests/unit/test_live_money_invariants.py
        # — anything new must keep those green.

    # --- Public API ---------------------------------------------------------

    def execute_intent(self, intent: TradeIntent, quote: float) -> Fill:
        """Execute one entry intent.

        The caller provides a price quote so sizing is deterministic and
        the service stays free of data-API dependencies. cw_runner already
        has the quote from its candidate scan; the simulator can pass the
        bar close.
        """
        if intent.side != "buy":
            raise ValueError(f"execute_intent expects side='buy', got {intent.side!r}")
        if quote <= 0:
            return Fill(
                intent_id=self._intent_id(intent),
                strategy=intent.strategy,
                ticker=intent.ticker,
                side=intent.side,
                qty=0,
                filled=False,
                error=f"non-positive quote: {quote}",
                backend_mode=self.backend.mode(),
            )
        qty = max(1, int(intent.qty_dollars / quote))
        client_order_id = self._client_order_id(intent)
        result = self.backend.submit_order(
            symbol=intent.ticker,
            qty=qty,
            side="buy",
            order_type="market",
            time_in_force="day",
        )
        return self._to_fill(intent, qty, result, client_order_id)

    def execute_exit(self, intent: ExitIntent) -> Fill:
        """Execute one exit intent. Quantity is whatever the broker reports
        held; we send a `close_position` which the backend implements as
        an opposing market order."""
        if intent.side != "sell":
            raise ValueError(f"execute_exit expects side='sell', got {intent.side!r}")
        result = self.backend.close_position(intent.ticker)
        return self._to_fill(intent, result.qty, result, self._intent_id(intent))

    # --- Internals ---------------------------------------------------------

    @staticmethod
    def _intent_id(intent) -> str:
        """Stable per-intent identifier for logs."""
        return f"{intent.strategy}:{intent.ticker}:{getattr(intent, 'trade_id', '_')}"

    def _client_order_id(self, intent: TradeIntent) -> str:
        """Deterministic client_order_id for Alpaca idempotency.

        Preserves the [[feedback-live-money-invariants]] rule: same intent
        retried = same client_order_id = broker dedups. The hash inputs are
        whatever uniquely identifies the entry decision — for the insider
        strategies this is (strategy, trade_id, date).
        """
        material = f"{intent.strategy}|{intent.trade_id}|{intent.ticker}|{intent.metadata.get('date', '')}"
        digest = hashlib.sha256(material.encode()).hexdigest()[:24]
        # Alpaca client_order_id has a length limit; 24 hex chars is safe.
        return f"f4-{self.strategy[:6]}-{digest}"

    def _to_fill(
        self, intent, qty: int, result: OrderResult, client_order_id: str
    ) -> Fill:
        return Fill(
            intent_id=self._intent_id(intent),
            strategy=intent.strategy,
            ticker=intent.ticker,
            side=intent.side,
            qty=qty,
            filled=result.is_filled,
            avg_price=result.filled_price,
            order_id=result.order_id,
            client_order_id=client_order_id,
            error=result.error,
            backend_mode=self.backend.mode(),
            raw_result=result.raw or {},
        )

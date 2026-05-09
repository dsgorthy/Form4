"""OrderIntent + Order state machine.

After a strategy emits Decision(action='enter'), the OMS turns it into
an OrderIntent (sized, instrumented), runs pre-trade risk checks against
it, and submits an Order to the broker. The Order tracks broker-side
lifecycle through an explicit state machine.

Why explicit states: today cw_runner.py treats orders as fire-and-forget,
with reconciliation as the only catch. The states make it possible to
detect "submitted but never accepted in 5 min" anomalies, which today
silently leak into reconciliation drift.

Why deterministic client_order_id: same Decision + same retry count
produces the same client_order_id. Alpaca dedups server-side on
client_order_id, so a network blip retry returns the existing order
rather than creating a duplicate. Solves the dual-runner risk that
prompted the 2026-04-19 Mini cleanup.

Schema mirrors pipelines/migrations/2026-05-02_002_order_audit.sql.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Optional


OrderSide = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]
TimeInForce = Literal["day", "gtc", "opg", "cls", "ioc", "fok"]


# ── State machine ───────────────────────────────────────────────────────────


class OrderState(Enum):
    """Order lifecycle states.

    Transitions:
        PENDING ─→ SUBMITTED ─→ ACCEPTED ─┬→ FILLED
                ↘            ↘            ├→ PARTIALLY_FILLED ─→ FILLED
                  REJECTED     REJECTED   │                    └→ CANCELLED
                ↘            ↘            ├→ REJECTED
                  CANCELLED    CANCELLED  └→ CANCELLED

    Terminal states (FILLED, REJECTED, CANCELLED) have no outgoing edges.
    """

    PENDING = "pending"                      # OrderIntent created; not yet submitted
    SUBMITTED = "submitted"                  # Sent to broker; awaiting accept
    ACCEPTED = "accepted"                    # Broker accepted; awaiting fill
    PARTIALLY_FILLED = "partially_filled"    # Some shares filled; still working
    FILLED = "filled"                        # Terminal: all shares filled
    REJECTED = "rejected"                    # Terminal: broker or risk-check rejected
    CANCELLED = "cancelled"                  # Terminal: cancelled before fully filling

    @classmethod
    def terminal_states(cls) -> set["OrderState"]:
        return {cls.FILLED, cls.REJECTED, cls.CANCELLED}

    def is_terminal(self) -> bool:
        return self in self.terminal_states()


# Allowed forward transitions per state. Validation is enforced in
# Order.transition() — invalid transitions raise InvalidStateTransition.
_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    OrderState.PENDING:           frozenset({OrderState.SUBMITTED, OrderState.REJECTED, OrderState.CANCELLED}),
    OrderState.SUBMITTED:         frozenset({OrderState.ACCEPTED, OrderState.REJECTED, OrderState.CANCELLED}),
    OrderState.ACCEPTED:          frozenset({OrderState.PARTIALLY_FILLED, OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELLED}),
    OrderState.PARTIALLY_FILLED:  frozenset({OrderState.FILLED, OrderState.CANCELLED}),
    OrderState.FILLED:            frozenset(),
    OrderState.REJECTED:          frozenset(),
    OrderState.CANCELLED:         frozenset(),
}


class InvalidStateTransition(Exception):
    """Raised when Order.transition() is called with a disallowed target state."""


# ── OrderIntent ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OrderIntent:
    """Sized, instrumented order — pre-broker.

    Frozen because the intent IS the immutable contract. The Order
    wrapper carries mutable broker-side state.

    Created from a Decision(action='enter') by:
      1. Sizing (Kelly / fixed-pct / unit) — produces qty
      2. Instrumenting (equity vs. option strike) — sets ticker, side, type
      3. (No DB writes here — risk checks happen later)
    """

    intent_id: str

    # Origin
    decision_id: str         # FK to Decision.decision_id
    strategy: str
    strategy_version: str

    # Trade specification
    ticker: str
    side: OrderSide
    qty: float               # shares (or contracts for options)
    order_type: OrderType = "market"
    limit_price: Optional[float] = None   # required for order_type='limit'
    time_in_force: TimeInForce = "day"

    # Metadata
    is_live: bool = False
    estimated_value_usd: Optional[float] = None
    pit_grade: Optional[str] = None
    conviction_score: Optional[float] = None
    decision_rationale: Optional[str] = None  # short human explanation

    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self):
        if self.qty <= 0:
            raise ValueError(f"OrderIntent.qty must be > 0, got {self.qty}")
        if self.side not in ("buy", "sell"):
            raise ValueError(f"OrderIntent.side must be buy|sell, got {self.side!r}")
        if self.order_type == "limit" and self.limit_price is None:
            raise ValueError("limit order requires limit_price")
        if self.order_type == "market" and self.limit_price is not None:
            raise ValueError("market order must not set limit_price")


# ── Deterministic client_order_id ───────────────────────────────────────────


CLIENT_ORDER_ID_PREFIX = "f4_"  # Form4 prefix — visible in Alpaca dashboard


def deterministic_client_order_id(decision_id: str, retry: int = 0) -> str:
    """Hash-based client_order_id stable across retries.

    Same (decision_id, retry) → same client_order_id. Alpaca dedups
    server-side, so a retry-with-same-retry returns the existing order
    instead of creating a duplicate.

    Bumping `retry` is for cases where the original order was cancelled
    or rejected and the operator wants to re-submit a fresh attempt
    (e.g., after fixing a margin issue). The new ID is distinct.

    Format: f4_<24 hex chars>. Alpaca's client_order_id limit is 48 chars;
    we use 27 to leave room for prefix + future extension.
    """
    payload = f"{decision_id}|{retry}".encode()
    h = hashlib.sha256(payload).hexdigest()[:24]
    return f"{CLIENT_ORDER_ID_PREFIX}{h}"


# ── Order (mutable, with state machine) ─────────────────────────────────────


@dataclass
class Order:
    """Broker-bound order with state machine.

    The Order carries:
      - The originating OrderIntent (immutable)
      - The broker-side identifiers and state (mutable)
      - A history of state transitions (append-only)

    Construction: Order.from_intent(intent) — produces PENDING.
    Submission: state transitions PENDING → SUBMITTED, populates submitted_at.
    Fill: ACCEPTED → FILLED with fill_qty + fill_price.
    Etc.
    """

    order_id: str            # = client_order_id (deterministic from decision_id)
    intent: OrderIntent
    state: OrderState = OrderState.PENDING
    state_history: list[tuple[OrderState, datetime]] = field(default_factory=list)

    # Broker side, populated during lifecycle
    alpaca_order_id: Optional[str] = None
    submitted_at: Optional[datetime] = None
    fill_qty: float = 0.0
    fill_price: Optional[float] = None       # average fill price
    filled_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None

    @classmethod
    def from_intent(cls, intent: OrderIntent, *, retry: int = 0) -> "Order":
        order_id = deterministic_client_order_id(intent.decision_id, retry=retry)
        return cls(order_id=order_id, intent=intent)

    def transition(
        self,
        new_state: OrderState,
        *,
        at: Optional[datetime] = None,
    ) -> None:
        """Move to `new_state`; raise InvalidStateTransition if not allowed.

        Records the previous state in state_history with the transition
        timestamp. Idempotent in the sense that transition(SAME_STATE)
        is a no-op (some brokers send duplicate state events).
        """
        if new_state == self.state:
            return  # idempotent no-op
        allowed = _TRANSITIONS.get(self.state, frozenset())
        if new_state not in allowed:
            raise InvalidStateTransition(
                f"{self.state.value} → {new_state.value} not allowed; "
                f"allowed from {self.state.value}: "
                f"{sorted(s.value for s in allowed)}"
            )
        ts = at or datetime.now(timezone.utc)
        self.state_history.append((self.state, ts))
        self.state = new_state

    def mark_submitted(self, alpaca_order_id: str, *, at: Optional[datetime] = None) -> None:
        """PENDING → SUBMITTED, capture broker ID."""
        self.transition(OrderState.SUBMITTED, at=at)
        self.alpaca_order_id = alpaca_order_id
        self.submitted_at = at or datetime.now(timezone.utc)

    def mark_accepted(self, *, at: Optional[datetime] = None) -> None:
        """SUBMITTED → ACCEPTED."""
        self.transition(OrderState.ACCEPTED, at=at)

    def mark_partial_fill(
        self,
        *,
        fill_qty: float,
        avg_price: float,
        at: Optional[datetime] = None,
    ) -> None:
        """ACCEPTED → PARTIALLY_FILLED (or stays PARTIALLY_FILLED)."""
        if self.state == OrderState.ACCEPTED:
            self.transition(OrderState.PARTIALLY_FILLED, at=at)
        elif self.state != OrderState.PARTIALLY_FILLED:
            raise InvalidStateTransition(
                f"partial fill requires ACCEPTED or PARTIALLY_FILLED, "
                f"got {self.state.value}"
            )
        self.fill_qty = fill_qty
        self.fill_price = avg_price

    def mark_filled(
        self,
        *,
        fill_qty: float,
        avg_price: float,
        at: Optional[datetime] = None,
    ) -> None:
        """ACCEPTED or PARTIALLY_FILLED → FILLED (terminal)."""
        self.transition(OrderState.FILLED, at=at)
        self.fill_qty = fill_qty
        self.fill_price = avg_price
        self.filled_at = at or datetime.now(timezone.utc)

    def mark_rejected(self, reason: str, *, at: Optional[datetime] = None) -> None:
        """Any non-terminal → REJECTED (terminal). Captures reason."""
        self.transition(OrderState.REJECTED, at=at)
        self.rejection_reason = reason

    def mark_cancelled(self, *, at: Optional[datetime] = None) -> None:
        """Any non-terminal → CANCELLED (terminal)."""
        self.transition(OrderState.CANCELLED, at=at)

    @property
    def is_terminal(self) -> bool:
        return self.state.is_terminal()

    @property
    def fill_status_audit_str(self) -> str:
        """String form for order_audit.fill_status column."""
        return self.state.value

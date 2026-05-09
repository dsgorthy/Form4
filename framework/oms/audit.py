"""Atomic audit-table writers for OMS.

write_decision() inserts into trade_decision_audit.
write_order() upserts into order_audit (so state transitions overwrite).

Caller controls the transaction. Both writers expect the connection to
be open and call conn.execute() — no commits here, no connection
ownership. Same convention as framework.contracts.freshness_writer.

Why upsert for order_audit: an Order's state advances over time
(SUBMITTED → ACCEPTED → FILLED). Each transition calls write_order()
to persist the latest state. ON CONFLICT (order_id) DO UPDATE keeps
one row per order with the latest state.
"""
from __future__ import annotations

import json
from typing import Optional

from framework.oms.decision import Decision
from framework.oms.order_manager import Order


# ── Decision audit (append-only) ────────────────────────────────────────────


def write_decision(conn, decision: Decision) -> None:
    """Insert one row into trade_decision_audit. Does not commit.

    Schema columns (from 2026-05-02_003_trade_decision_audit.sql):
      id, ts, run_id, strategy, ticker, trade_id, filing_date, thesis,
      stage, passed, reason, pit_grade, conviction, feature_snapshot

    The `passed` boolean maps from action: 'enter' → true, 'reject' → false.
    'exit' is also recorded as passed=true (the exit decision succeeded).
    """
    passed = decision.action != "reject"
    feature_json = json.dumps(decision.feature_snapshot) if decision.feature_snapshot else None
    conn.execute(
        """
        INSERT INTO trade_decision_audit
            (ts, run_id, strategy, ticker, trade_id, filing_date, thesis,
             stage, passed, reason, pit_grade, conviction, feature_snapshot)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            decision.decided_at,
            decision.run_id,
            decision.strategy,
            decision.ticker,
            decision.trade_id,
            decision.filing_date,
            decision.thesis,
            decision.stage,
            passed,
            decision.reason,
            decision.pit_grade,
            decision.conviction,
            feature_json,
        ),
    )


def write_decisions(conn, decisions: list[Decision]) -> int:
    """Bulk-insert. Returns count written. Caller commits."""
    n = 0
    for d in decisions:
        write_decision(conn, d)
        n += 1
    return n


# ── Order audit (upsert; one row per order_id) ──────────────────────────────


def write_order(
    conn,
    order: Order,
    *,
    config_version_sha: str = "",
    config_yaml_sha: str = "",
    signal_inputs: Optional[dict] = None,
) -> None:
    """Upsert one row into order_audit. Does not commit.

    On insert: writes full intent + decision context.
    On conflict (order_id): updates only the lifecycle fields that have
    advanced (alpaca_order_id, fill_status, fill_price, etc.) — the
    immutable intent fields (ticker, side, qty, decision_id) are NOT
    touched, since they're set once and shouldn't change.

    Args:
      order: the Order with current state.
      config_version_sha: git SHA of the runner code at decision time.
      config_yaml_sha: sha256 of the strategy yaml at decision time.
      signal_inputs: dict snapshot of feature inputs that drove the decision.
                     Persisted to signal_inputs_json for replay.

    The order_audit schema requires config_version_sha + config_yaml_sha
    NOT NULL. Caller must provide them; an empty string is valid for
    backfill scenarios but should never be used at write-time in prod.
    """
    intent = order.intent
    inputs_json = json.dumps(signal_inputs) if signal_inputs else "{}"
    conn.execute(
        """
        INSERT INTO order_audit
            (order_id, strategy, alpaca_order_id, ticker, side, qty,
             order_type, conviction_score, pit_grade, signal_inputs_json,
             decision_rationale, config_version_sha, config_yaml_sha,
             decided_at, submitted_at, fill_status, fill_price, fill_qty,
             filled_at, rejection_reason, is_live)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            alpaca_order_id  = COALESCE(EXCLUDED.alpaca_order_id, order_audit.alpaca_order_id),
            submitted_at     = COALESCE(order_audit.submitted_at, EXCLUDED.submitted_at),
            fill_status      = EXCLUDED.fill_status,
            fill_price       = COALESCE(EXCLUDED.fill_price, order_audit.fill_price),
            fill_qty         = GREATEST(order_audit.fill_qty, EXCLUDED.fill_qty),
            filled_at        = COALESCE(EXCLUDED.filled_at, order_audit.filled_at),
            rejection_reason = COALESCE(EXCLUDED.rejection_reason, order_audit.rejection_reason)
        """,
        (
            order.order_id,
            intent.strategy,
            order.alpaca_order_id,
            intent.ticker,
            intent.side,
            float(intent.qty),
            intent.order_type,
            intent.conviction_score,
            intent.pit_grade,
            inputs_json,
            intent.decision_rationale,
            config_version_sha,
            config_yaml_sha,
            intent.created_at,
            order.submitted_at,
            order.fill_status_audit_str,
            order.fill_price,
            float(order.fill_qty),
            order.filled_at,
            order.rejection_reason,
            intent.is_live,
        ),
    )

"""Order Management System (OMS) — Phase 2 P2.

Decouples the four concerns conflated in cw_runner.py today:

    Decision      — strategy: pure fn (signals, portfolio_state) → Decision[]
    OrderIntent   — sized + instrumented version of an enter Decision
    Order         — broker-submitted, with explicit state machine
    Audit         — every Decision and Order persisted, replay-ready

Day 1-2 deliverable: standalone modules + tests, NO cw_runner integration.
Day 3-5 wires this into cw_runner behind an OMS_V2 feature flag.

Public surface (each module documents its own contract):

    framework.oms.decision        Decision dataclass
    framework.oms.order_manager   OrderIntent / Order / OrderState / client_order_id
    framework.oms.risk_checks     RiskCheck ABC + concrete checks + Pipeline
    framework.oms.audit           write_decision() / write_order() helpers

The OMS does NOT own DB connections — the caller passes a connection so
that audit writes commit atomically with strategy_portfolio writes. This
matches the Phase 2 P0 freshness_writer pattern.
"""

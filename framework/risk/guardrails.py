"""Hard guardrails on order execution.

The strategy code computes what it WANTS to trade. These guardrails are the
last line of defense before any order is submitted to a broker (paper or
live). They exist so that a bug in the strategy logic — wrong qty, wrong
size, weird ticker price, runaway loop submitting the same order, etc. —
gets caught here instead of at the broker (where it would either fill in a
way that loses real money or get rejected after we've already updated our
internal canonical state).

Everything in this module is intentionally simple, parameterized, and
auditable. Defaults err on the conservative side; strategy yamls can widen
specific limits via the `guardrails:` block (not narrow them — `min_*`
defaults stay floor; `max_*` defaults stay ceiling).

Usage:

    from framework.risk.guardrails import validate_entry_order
    ok, reason = validate_entry_order(
        conn, strategy="quality_momentum", side="buy",
        qty=qty, dollar_amount=dollar_amount,
        current_price=current_price, equity=equity,
        guardrails_cfg=config.get("guardrails", {}),
    )
    if not ok:
        alert.critical("cw_runner.guardrails", reason, ...)
        continue
"""
from __future__ import annotations

from typing import Any

# ── Defaults ────────────────────────────────────────────────────────────────
# These are floors/ceilings — the strategy yaml can override per-strategy.
DEFAULT_GUARDRAILS = {
    # Per-trade size
    "min_dollar_amount": 100.0,        # below this, the trade isn't material
    "max_dollar_amount": 50_000.0,     # absolute hard cap regardless of equity
    # Per-trade share count
    "min_qty": 1,
    "max_qty": 10_000,                 # huge for a single insider position
    # Per-trade price
    "min_price": 0.50,                 # below = penny stock / data error
    "max_price": 5_000.0,              # above = BRK.A class; rarely a real signal
    # Per-day order counts (per strategy, per side)
    "max_daily_buys": 10,
    "max_daily_sells": 20,
    # Equity sanity
    "min_equity": 100.0,               # below = something is very wrong
}


def _merge(defaults: dict, overrides: dict) -> dict:
    out = dict(defaults)
    out.update(overrides or {})
    return out


def validate_entry_order(
    conn,
    *,
    strategy: str,
    side: str,
    qty: int,
    dollar_amount: float,
    current_price: float,
    equity: float,
    guardrails_cfg: dict | None = None,
) -> tuple[bool, str]:
    """Return (ok, reason). reason is empty string on ok=True."""
    cfg = _merge(DEFAULT_GUARDRAILS, guardrails_cfg or {})

    if equity < cfg["min_equity"]:
        return False, f"equity ${equity:,.0f} < min ${cfg['min_equity']:,.0f}"

    if qty < cfg["min_qty"]:
        return False, f"qty={qty} < min {cfg['min_qty']}"
    if qty > cfg["max_qty"]:
        return False, f"qty={qty} > max {cfg['max_qty']}"

    if dollar_amount < cfg["min_dollar_amount"]:
        return False, (f"dollar_amount=${dollar_amount:,.0f} < "
                       f"min ${cfg['min_dollar_amount']:,.0f}")
    if dollar_amount > cfg["max_dollar_amount"]:
        return False, (f"dollar_amount=${dollar_amount:,.0f} > "
                       f"max ${cfg['max_dollar_amount']:,.0f} (defense in depth)")

    if current_price < cfg["min_price"]:
        return False, (f"price=${current_price:.2f} < min "
                       f"${cfg['min_price']:.2f} (penny stock / bad quote)")
    if current_price > cfg["max_price"]:
        return False, (f"price=${current_price:.2f} > max "
                       f"${cfg['max_price']:.2f} (suspicious quote)")

    # Daily order count (per strategy, per side). Counts only orders submitted
    # today (decided_at >= today midnight). Uses order_audit because that's
    # the canonical record of "we tried to place an order".
    side = side.lower()
    if side not in ("buy", "sell"):
        return False, f"unknown side {side!r}"
    max_key = "max_daily_buys" if side == "buy" else "max_daily_sells"
    max_today = cfg[max_key]

    today_count = _count_orders_today(conn, strategy, side)
    if today_count >= max_today:
        return False, (f"already {today_count} {side} orders today for "
                       f"{strategy}, max {max_today}")

    return True, ""


def _count_orders_today(conn, strategy: str, side: str) -> int:
    """Count today's order activity for (strategy, side).

    Defense in depth: takes MAX of two sources because each is incomplete on
    its own:

      - `order_audit` is the canonical decision-time log, but only populated
        since 2026-05-02 (migration `2026-05-02_002_order_audit.sql`) and
        depends on `_record_order_decision` / `write_order` actually firing —
        both are wrapped in try/except so a silent write failure would
        otherwise reduce the count to 0 and make the daily cap unenforceable.

      - `strategy_portfolio` is the canonical position state — every
        successful entry lands here even when order_audit write fails. Used
        as the fallback floor. Only counts paper/live (not simulated) for
        the `buy` side; `sell` has no equivalent same-day-fired counter on
        strategy_portfolio, so falls back to order_audit alone.
    """
    audit_count = 0
    portfolio_count = 0
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS n FROM order_audit
                WHERE strategy = ?
                  AND LOWER(side) = ?
                  AND decided_at::date = CURRENT_DATE""",
            (strategy, side),
        ).fetchone()
        if row:
            audit_count = int((row["n"] if hasattr(row, "keys") else row[0]) or 0)
    except Exception:
        pass

    if side == "buy":
        try:
            row = conn.execute(
                """SELECT COUNT(*) AS n FROM strategy_portfolio
                    WHERE strategy = ?
                      AND entry_date = CURRENT_DATE::text
                      AND execution_source IN ('paper', 'live')""",
                (strategy,),
            ).fetchone()
            if row:
                portfolio_count = int((row["n"] if hasattr(row, "keys") else row[0]) or 0)
        except Exception:
            pass

    return max(audit_count, portfolio_count)

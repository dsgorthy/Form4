"""Composable pre-trade risk checks.

A RiskCheck takes an OrderIntent + RiskContext and returns Pass or
Reject(reason). Multiple checks compose via RiskCheckPipeline; the
pipeline short-circuits on the first failure.

Adding a new check:
    1. Subclass RiskCheck, implement evaluate().
    2. Add an instance to the strategy's RiskCheckPipeline.

A failed check writes a Decision(reject, reason='risk:<check_name>')
via the audit module — the candidate is NOT lost, it's recorded for
later analysis ("we rejected 47 candidates last month for sector
concentration — was that the right call?").

Phase 2 P2 Day 1-2 implements the 6 checks called out in the design
doc. Live launch readiness gates on all of these being unit-tested
and integration-tested under paper.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from framework.oms.order_manager import OrderIntent


# ── Context + result ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OpenPosition:
    """Snapshot of one open position for risk-check evaluation."""
    ticker: str
    qty: float
    entry_price: float
    sector: Optional[str] = None
    market_value: Optional[float] = None  # current; defaults to qty*entry_price


@dataclass(frozen=True)
class RiskContext:
    """Account + portfolio state at risk-check time. Snapshot — pure data."""
    portfolio_value: float       # cash + positions market value
    cash_balance: float
    open_positions: tuple[OpenPosition, ...]
    daily_pnl: float = 0.0       # since today's open
    is_live: bool = False
    blocklist: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class RiskResult:
    """Outcome of one or more checks. Carries the check name on failure."""
    passed: bool
    reason: Optional[str] = None
    check_name: Optional[str] = None
    metadata: Optional[dict] = None

    @classmethod
    def pass_(cls, check_name: Optional[str] = None) -> "RiskResult":
        return cls(passed=True, check_name=check_name)

    @classmethod
    def reject(cls, check_name: str, reason: str, **metadata) -> "RiskResult":
        return cls(
            passed=False,
            reason=reason,
            check_name=check_name,
            metadata=metadata or None,
        )


# ── Base ────────────────────────────────────────────────────────────────────


class RiskCheck(ABC):
    """ABC for pre-trade risk checks. Subclasses set name + implement evaluate."""

    name: str = "abstract"

    @abstractmethod
    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        """Return RiskResult.pass_() or RiskResult.reject(...). Pure function."""


# ── Concrete checks ─────────────────────────────────────────────────────────


class MaxPositionSizeCheck(RiskCheck):
    """Reject if this single position would exceed `pct`% of portfolio value."""

    def __init__(self, pct: float):
        if pct <= 0 or pct > 100:
            raise ValueError(f"pct must be in (0, 100], got {pct}")
        self.pct = pct
        self.name = f"max_position_size_{pct}pct"

    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        if ctx.portfolio_value <= 0:
            return RiskResult.reject(
                self.name,
                f"portfolio_value={ctx.portfolio_value} — cannot size",
            )
        position_value = (
            intent.estimated_value_usd
            if intent.estimated_value_usd is not None
            else (intent.qty * (intent.limit_price or 0))
        )
        if position_value <= 0:
            # Can't evaluate without a price; defer to runtime size enforcement
            return RiskResult.pass_(self.name)
        position_pct = position_value / ctx.portfolio_value * 100
        if position_pct > self.pct:
            return RiskResult.reject(
                self.name,
                f"position_pct={position_pct:.2f}% > limit={self.pct}%",
                position_value=position_value,
                portfolio_value=ctx.portfolio_value,
            )
        return RiskResult.pass_(self.name)


class MaxConcurrentCheck(RiskCheck):
    """Reject if opening this position would exceed `n` concurrent positions.

    Counts strictly OTHER tickers — adding to an existing position
    (same ticker) is still allowed up to MaxPositionSizeCheck limits.
    """

    def __init__(self, n: int):
        if n < 0:
            raise ValueError(f"n must be >= 0, got {n}")
        self.n = n
        self.name = f"max_concurrent_{n}"

    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        # If the intent's ticker is already held, we're not opening a new slot
        held_tickers = {p.ticker for p in ctx.open_positions}
        if intent.ticker in held_tickers:
            return RiskResult.pass_(self.name)
        if len(held_tickers) >= self.n:
            return RiskResult.reject(
                self.name,
                f"open_positions={len(held_tickers)} >= limit={self.n}",
                held_tickers=sorted(held_tickers),
            )
        return RiskResult.pass_(self.name)


class DailyLossLimitCheck(RiskCheck):
    """Reject new entries when today's loss exceeds `pct`% of portfolio value.

    Loss is measured as min(daily_pnl, 0). Only new entries are gated;
    existing positions can still exit (the runner handles exits separately).
    """

    def __init__(self, pct: float):
        if pct <= 0:
            raise ValueError(f"pct must be > 0, got {pct}")
        self.pct = pct
        self.name = f"daily_loss_limit_{pct}pct"

    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        if intent.side != "buy":
            # Sells can be exits; don't block. Buy-side blocking is the goal.
            return RiskResult.pass_(self.name)
        if ctx.portfolio_value <= 0:
            return RiskResult.pass_(self.name)
        loss_pct = abs(min(ctx.daily_pnl, 0.0)) / ctx.portfolio_value * 100
        if loss_pct >= self.pct:
            return RiskResult.reject(
                self.name,
                f"daily_loss={loss_pct:.2f}% >= limit={self.pct}%",
                daily_pnl=ctx.daily_pnl,
            )
        return RiskResult.pass_(self.name)


class SectorConcentrationCheck(RiskCheck):
    """Reject if adding this position would push sector exposure over `max_pct`%.

    Requires open_positions to have `sector` populated. Positions without
    a sector are bucketed as 'unknown' and counted; this is conservative
    (an unknown sector can't deplete other sectors' headroom).

    The intent's sector must be passed via intent.estimated_value_usd's
    metadata or via a sector_lookup function — for v1, we accept an
    optional `sector_lookup: Callable[[str], str]` constructor param.
    """

    def __init__(self, max_pct: float, sector_lookup=None):
        if max_pct <= 0 or max_pct > 100:
            raise ValueError(f"max_pct must be in (0, 100], got {max_pct}")
        self.max_pct = max_pct
        self.sector_lookup = sector_lookup or (lambda ticker: "unknown")
        self.name = f"sector_concentration_{max_pct}pct"

    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        if ctx.portfolio_value <= 0:
            return RiskResult.pass_(self.name)

        intent_sector = self.sector_lookup(intent.ticker)
        intent_value = (
            intent.estimated_value_usd
            if intent.estimated_value_usd is not None
            else (intent.qty * (intent.limit_price or 0))
        )
        if intent_value <= 0:
            return RiskResult.pass_(self.name)  # can't evaluate

        # Sum existing exposure in this sector
        existing_sector_value = sum(
            (p.market_value if p.market_value is not None else p.qty * p.entry_price)
            for p in ctx.open_positions
            if (p.sector or self.sector_lookup(p.ticker)) == intent_sector
        )
        new_sector_value = existing_sector_value + intent_value
        sector_pct = new_sector_value / ctx.portfolio_value * 100

        if sector_pct > self.max_pct:
            return RiskResult.reject(
                self.name,
                f"sector={intent_sector} would be {sector_pct:.2f}% > {self.max_pct}%",
                intent_sector=intent_sector,
                existing_sector_value=existing_sector_value,
            )
        return RiskResult.pass_(self.name)


class MarginCheck(RiskCheck):
    """Reject if intent's notional exceeds available cash + margin headroom.

    For paper trading we assume no margin (cash-only). For live, callers
    can pass margin_multiplier to allow up to N× cash.
    """

    def __init__(self, margin_multiplier: float = 1.0):
        if margin_multiplier < 1.0:
            raise ValueError(f"margin_multiplier must be >= 1.0, got {margin_multiplier}")
        self.margin_multiplier = margin_multiplier
        self.name = f"margin_{margin_multiplier}x"

    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        if intent.side != "buy":
            return RiskResult.pass_(self.name)  # sells don't require buying power
        notional = (
            intent.estimated_value_usd
            if intent.estimated_value_usd is not None
            else (intent.qty * (intent.limit_price or 0))
        )
        if notional <= 0:
            return RiskResult.pass_(self.name)
        buying_power = ctx.cash_balance * self.margin_multiplier
        if notional > buying_power:
            return RiskResult.reject(
                self.name,
                f"notional=${notional:.0f} > buying_power=${buying_power:.0f} "
                f"(cash=${ctx.cash_balance:.0f} × {self.margin_multiplier}x)",
                notional=notional,
                buying_power=buying_power,
            )
        return RiskResult.pass_(self.name)


class SymbolBlocklistCheck(RiskCheck):
    """Reject if intent.ticker is in the configured blocklist.

    Use for: delisted symbols, halted tickers, manipulated penny stocks,
    symbols under regulatory review. Blocklist comes from RiskContext
    so it can update at runtime without redeploying.
    """

    name = "symbol_blocklist"

    def evaluate(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        if intent.ticker in ctx.blocklist:
            return RiskResult.reject(
                self.name,
                f"ticker={intent.ticker} is in blocklist",
                blocklist_size=len(ctx.blocklist),
            )
        return RiskResult.pass_(self.name)


# ── Pipeline ────────────────────────────────────────────────────────────────


class RiskCheckPipeline:
    """Compose multiple RiskChecks. Short-circuits on first failure.

    Usage:
        pipeline = RiskCheckPipeline([
            MaxPositionSizeCheck(pct=10),
            MaxConcurrentCheck(n=10),
            DailyLossLimitCheck(pct=5),
            MarginCheck(),
            SymbolBlocklistCheck(),
        ])
        result = pipeline.run(intent, ctx)
        if not result.passed:
            log_decision(reject, reason=f"risk:{result.check_name}: {result.reason}")
    """

    def __init__(self, checks: list[RiskCheck]):
        if not checks:
            raise ValueError("RiskCheckPipeline requires at least one check")
        names = [c.name for c in checks]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate check names: {names}")
        self.checks = list(checks)

    def run(self, intent: OrderIntent, ctx: RiskContext) -> RiskResult:
        """Run all checks in order. Return first failure or aggregate pass."""
        for check in self.checks:
            result = check.evaluate(intent, ctx)
            if not result.passed:
                return result
        return RiskResult(
            passed=True,
            metadata={"checks_run": [c.name for c in self.checks]},
        )

    def run_all(self, intent: OrderIntent, ctx: RiskContext) -> list[RiskResult]:
        """Run every check (no short-circuit). Useful for diagnostics."""
        return [check.evaluate(intent, ctx) for check in self.checks]

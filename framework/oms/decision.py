"""Decision dataclass — what a strategy outputs for each candidate.

A strategy's evaluate(candidate, portfolio_state) returns a list of
Decisions. Each Decision is a fully-resolved verdict: enter (this candidate
should become an order), reject (this candidate failed at stage X), or
exit (close an existing position). Per-stage rejections produce one
Decision per stage so the audit trail can answer "why didn't strategy X
trade ticker Y?" with sub-stage granularity.

Decisions are pure data — no side effects, no DB, no broker calls. They
serialize 1:1 to trade_decision_audit rows via framework.oms.audit.

Schema mirrors pipelines/migrations/2026-05-02_003_trade_decision_audit.sql.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Optional

DecisionAction = Literal["enter", "reject", "exit"]


# Standard stage names. Strategies should use these for consistency; the
# audit query "SELECT stage, COUNT(*) FROM trade_decision_audit GROUP BY 1"
# is a primary diagnostic.
STAGES = (
    "dedup",          # already in portfolio
    "recency",        # filing too old/new
    "filter",         # min value, trans_code, role
    "pit_lookup",     # pit_grade required but missing
    "min_10b5_1",     # 10b5-1 surprise condition
    "feature",        # dip_3mo, above_sma50, consecutive_sells_before
    "conviction",     # conviction_score threshold
    "capacity",       # max_concurrent positions
    "risk",           # pre-trade risk check (RiskCheckPipeline)
    "final",          # passed everything; will become an OrderIntent
    "exit",           # closing an existing position (action='exit')
)


@dataclass(frozen=True)
class Decision:
    """A single per-stage verdict for one candidate.

    Multiple Decisions per (run_id, candidate) — one per stage evaluated.
    The first stage that fails has action='reject'; the candidate stops
    being evaluated. If all stages pass, the final stage has action='enter'.

    Frozen because Decisions are append-only audit records. Mutating one
    after writing it to trade_decision_audit would desync the row.
    """

    decision_id: str
    run_id: str
    strategy: str
    strategy_version: str    # f"{yaml_sha}:{git_sha}" — set once per scan

    # Candidate identity
    trade_id: Optional[int]  # FK to trades.trade_id (None for synthetic candidates)
    ticker: str
    filing_date: Optional[str]  # YYYY-MM-DD; None for non-trade-driven decisions

    # Verdict
    action: DecisionAction
    stage: str
    reason: Optional[str]    # human-readable; e.g., "pit_grade=C not in [A,A+]"

    # Per-decision context for replay
    confidence: Optional[float]
    pit_grade: Optional[str]
    conviction: Optional[float]
    feature_snapshot: dict   # full feature vector seen at decision time

    # Optional thesis name (for strategies with multiple theses in one yaml)
    thesis: Optional[str] = None

    decided_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @classmethod
    def reject(
        cls,
        *,
        run_id: str,
        strategy: str,
        strategy_version: str,
        trade_id: Optional[int],
        ticker: str,
        filing_date: Optional[str],
        stage: str,
        reason: str,
        feature_snapshot: Optional[dict] = None,
        thesis: Optional[str] = None,
        pit_grade: Optional[str] = None,
        conviction: Optional[float] = None,
    ) -> "Decision":
        """Construct a reject Decision. Convenience for the common case."""
        return cls(
            decision_id=str(uuid.uuid4()),
            run_id=run_id,
            strategy=strategy,
            strategy_version=strategy_version,
            trade_id=trade_id,
            ticker=ticker,
            filing_date=filing_date,
            action="reject",
            stage=stage,
            reason=reason,
            confidence=None,
            pit_grade=pit_grade,
            conviction=conviction,
            feature_snapshot=feature_snapshot or {},
            thesis=thesis,
        )

    @classmethod
    def enter(
        cls,
        *,
        run_id: str,
        strategy: str,
        strategy_version: str,
        trade_id: Optional[int],
        ticker: str,
        filing_date: Optional[str],
        confidence: float,
        feature_snapshot: dict,
        pit_grade: Optional[str] = None,
        conviction: Optional[float] = None,
        thesis: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> "Decision":
        """Construct an enter Decision (passed every filter stage).

        action='enter' implies the candidate will be promoted to an
        OrderIntent and submitted (subject to pre-trade risk checks).
        """
        return cls(
            decision_id=str(uuid.uuid4()),
            run_id=run_id,
            strategy=strategy,
            strategy_version=strategy_version,
            trade_id=trade_id,
            ticker=ticker,
            filing_date=filing_date,
            action="enter",
            stage="final",
            reason=reason,
            confidence=confidence,
            pit_grade=pit_grade,
            conviction=conviction,
            feature_snapshot=feature_snapshot,
            thesis=thesis,
        )

    @classmethod
    def exit(
        cls,
        *,
        run_id: str,
        strategy: str,
        strategy_version: str,
        trade_id: Optional[int],
        ticker: str,
        reason: str,
        feature_snapshot: Optional[dict] = None,
    ) -> "Decision":
        """Construct an exit Decision (close an existing position)."""
        return cls(
            decision_id=str(uuid.uuid4()),
            run_id=run_id,
            strategy=strategy,
            strategy_version=strategy_version,
            trade_id=trade_id,
            ticker=ticker,
            filing_date=None,
            action="exit",
            stage="exit",
            reason=reason,
            confidence=None,
            pit_grade=None,
            conviction=None,
            feature_snapshot=feature_snapshot or {},
        )

    def __post_init__(self):
        if self.action not in ("enter", "reject", "exit"):
            raise ValueError(
                f"Decision.action must be enter|reject|exit, got {self.action!r}"
            )
        if self.action == "reject" and self.reason is None:
            raise ValueError("reject Decision requires a reason")
        if self.stage not in STAGES:
            # Don't reject — strategies might add custom stages — but warn.
            # If you see this, add the stage to STAGES.
            import warnings
            warnings.warn(
                f"Decision.stage={self.stage!r} not in STAGES — add it to "
                f"framework.oms.decision.STAGES for consistency.",
                stacklevel=2,
            )

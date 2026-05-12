"""PITLiveEngine — live-mode entry pipeline on the PIT rails.

Replaces `cw_runner.scan_signals` + the entry portion of
`cw_runner.execute_entries`. Reuses the same `PITStrategy` implementations
that the backtest engine uses, so the strategy's decision logic runs
identically in both contexts.

Scope (Phase 3 part 2):
  - Scan today's filings → PITStrategy decisions
  - Apply capacity rules (skip / replace_weakest / replace_oldest)
  - In `dry_run=True`: log what would happen, no writes
  - In `dry_run=False`: submit Alpaca orders, persist strategy_portfolio
    + trade_decision_audit rows

NOT in scope here:
  - Exit processing (stays in cw_runner.check_exits / check_scheduled_exits)
  - Order-state machine v2 (already handled by framework/oms)
  - Heartbeat / weekly digest / freshness preflight (stays in cw_runner.run_daily)

The architecture is: cw_runner's run_daily can be progressively cut over by
calling `PITLiveEngine.scan_and_decide` instead of `scan_signals`. The first
cut keeps the same downstream (`execute_entries`) and just swaps the scan;
that's an isolated, low-risk migration.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from framework.pit.clock import PITClock
from framework.pit.engine import PITBacktestEngine
from framework.pit.events import Decision
from framework.pit.strategy import PITStrategy
from framework.pit.view import PITDataView

logger = logging.getLogger(__name__)


@dataclass
class LiveRunResult:
    """Aggregate of one live-mode run. Comparable to cw_runner's return dict."""
    strategy: str
    as_of_date: str
    n_events_today: int = 0
    n_filter_pass: int = 0
    n_conviction_pass: int = 0
    n_admitted: int = 0          # passed capacity gate
    n_entered: int = 0           # actually ordered (live) or would-enter (dry)
    n_swapped_out: int = 0       # replaced by rotation
    decisions: List[Decision] = field(default_factory=list)
    capacity_audits: List[Dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "strategy": self.strategy,
            "as_of_date": self.as_of_date,
            "n_events_today": self.n_events_today,
            "n_filter_pass": self.n_filter_pass,
            "n_conviction_pass": self.n_conviction_pass,
            "n_admitted": self.n_admitted,
            "n_entered": self.n_entered,
            "n_swapped_out": self.n_swapped_out,
        }


class PITLiveEngine:
    """Live-mode entry engine.

    Parameters
    ----------
    conn : DBAPI connection — must NOT be read-only for live runs.
    strategy : a PITStrategy implementation (QualityMomentumStrategy etc.).
    config : strategy yaml dict (same as cw_runner gets).
    alpaca : optional PaperBackend; required when dry_run=False.
    """

    def __init__(self, conn, strategy: PITStrategy, config: dict,
                 alpaca=None) -> None:
        self.conn = conn
        self.strategy = strategy
        self.config = config
        self.alpaca = alpaca
        self.strategy_name = config["strategy_name"]
        self.max_concurrent = int(config["max_concurrent"])
        self.position_size_pct = float(config["position_size_pct"])
        self.starting_capital = float(config.get("starting_capital", 100_000))
        # Capacity rule and rotation params
        self.at_capacity = config.get("at_capacity", "skip")
        self.replacement_advantage = float(config.get("replacement_advantage", 0.5))
        self.min_conv_at_hard = float(config.get("min_conviction_at_hard",
                                                 config.get("min_conviction", 1.5)))

    # ── Public entry points ──────────────────────────────────────────────

    def scan_and_decide(self, as_of_date: Optional[str] = None
                        ) -> List[Decision]:
        """Scan today's filings, return per-event decisions from the
        strategy. PIT-enforced via PITDataView."""
        as_of_date = as_of_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        clock = PITClock(as_of_date=as_of_date)
        view = PITDataView(clock, self.conn)
        events = view.events_filed_on(as_of_date)
        decisions = [self.strategy.evaluate(view, e) for e in events]
        return decisions

    def run_daily_cycle(self, as_of_date: Optional[str] = None,
                        dry_run: bool = True) -> LiveRunResult:
        """Full entry-side daily cycle. Returns a structured result.

        When `dry_run=True` (DEFAULT — safe), no Alpaca orders are submitted
        and no DB writes happen. We log what *would* happen so the cw_runner
        cutover can be validated side-by-side.
        """
        as_of_date = as_of_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = LiveRunResult(strategy=self.strategy_name, as_of_date=as_of_date)

        # 1) Scan + per-event decisions
        decisions = self.scan_and_decide(as_of_date)
        result.decisions = decisions
        result.n_events_today = len(decisions)
        result.n_filter_pass = sum(
            1 for d in decisions
            if d.stage != "filter" or d.passed
        )
        result.n_conviction_pass = sum(
            1 for d in decisions if d.action == "enter"
        )

        # 2) Capacity gate
        admitted = self._apply_capacity_gate(decisions, dry_run=dry_run, result=result)
        result.n_admitted = len(admitted)

        # 3) Execute (or log for dry-run)
        if dry_run:
            for d in admitted:
                logger.info("[%s/DRY] WOULD ENTER %s conv=%.2f filing_date=%s",
                            self.strategy_name, d.ticker, d.conviction or 0,
                            d.filing_date)
            result.n_entered = len(admitted)
        else:
            result.n_entered = self._submit_admitted(admitted, result)

        return result

    # ── Capacity logic — mirror cw_runner.execute_entries ───────────────

    def _apply_capacity_gate(self, decisions: List[Decision], dry_run: bool,
                             result: LiveRunResult) -> List[Decision]:
        """Returns the subset of `enter` decisions that pass capacity.
        Mirrors cw_runner.execute_entries (lines ~880–1060) but operates
        on Decision objects instead of dict candidates."""
        # Live positions (paper/live only — backfill rows excluded)
        held_tickers = {
            r["ticker"]
            for r in self.conn.execute(
                "SELECT ticker FROM strategy_portfolio WHERE strategy = ? "
                "AND status = 'open' AND execution_source IN ('paper', 'live')",
                (self.strategy_name,),
            ).fetchall()
        }
        n_open_initial = len(held_tickers)

        # Process enter-decisions in conviction DESC order
        enter_decs = sorted(
            [d for d in decisions if d.action == "enter"],
            key=lambda d: -(d.conviction or 0),
        )

        admitted: List[Decision] = []
        n_open = n_open_initial

        for d in enter_decs:
            # 1) Same-day same-ticker dedup
            if d.ticker in held_tickers:
                result.capacity_audits.append({
                    "trade_id": d.trade_id, "ticker": d.ticker,
                    "passed": False, "reason": "dedup_held_ticker",
                })
                continue

            # 2) Slot check
            if n_open < self.max_concurrent:
                admitted.append(d)
                held_tickers.add(d.ticker)
                n_open += 1
                result.capacity_audits.append({
                    "trade_id": d.trade_id, "ticker": d.ticker,
                    "passed": True, "reason": f"slot_{n_open}/{self.max_concurrent}",
                })
                continue

            # 3) At capacity — apply rule
            if self.at_capacity == "skip":
                result.capacity_audits.append({
                    "trade_id": d.trade_id, "ticker": d.ticker,
                    "passed": False,
                    "reason": f"at_max_concurrent={self.max_concurrent}, rule=skip",
                })
                continue

            if self.at_capacity in ("replace_weakest", "replace_oldest"):
                # Conviction must clear hard floor
                conv = d.conviction or 0
                if conv < self.min_conv_at_hard:
                    result.capacity_audits.append({
                        "trade_id": d.trade_id, "ticker": d.ticker,
                        "passed": False,
                        "reason": (f"at_max_concurrent={self.max_concurrent}, "
                                   f"rule={self.at_capacity}, conv={conv:.2f} "
                                   f"< hard_floor={self.min_conv_at_hard}"),
                    })
                    continue

                victim = self._find_victim()
                if victim is None:
                    continue
                v_id, v_ticker, v_conv = victim

                if self.at_capacity == "replace_weakest":
                    if conv < v_conv + self.replacement_advantage:
                        result.capacity_audits.append({
                            "trade_id": d.trade_id, "ticker": d.ticker,
                            "passed": False,
                            "reason": (f"replace_weakest: conv={conv:.2f} not "
                                       f">= weakest({v_conv:.2f}) + adv"
                                       f"({self.replacement_advantage:.2f})"),
                        })
                        continue
                # replace_oldest — no conviction gate beyond hard floor
                # Mark this as a rotation
                admitted.append(d)
                held_tickers.discard(v_ticker)
                held_tickers.add(d.ticker)
                result.n_swapped_out += 1
                result.capacity_audits.append({
                    "trade_id": d.trade_id, "ticker": d.ticker,
                    "passed": True,
                    "reason": (f"{self.at_capacity}: swap victim={v_ticker} "
                               f"(conv {v_conv:.2f} → {conv:.2f})"),
                    "victim_id": v_id, "victim_ticker": v_ticker,
                })
                continue

            # Unknown rule — fail-safe skip
            result.capacity_audits.append({
                "trade_id": d.trade_id, "ticker": d.ticker,
                "passed": False,
                "reason": f"unknown_at_capacity={self.at_capacity}",
            })

        return admitted

    def _find_victim(self) -> Optional[tuple]:
        """For rotation: find the open live position with lowest conviction
        (replace_weakest) or oldest entry_date (replace_oldest)."""
        order = "entry_date ASC" if self.at_capacity == "replace_oldest" else "id"
        rows = self.conn.execute(
            f"""SELECT id, ticker, entry_reasoning FROM strategy_portfolio
                WHERE strategy = ? AND status = 'open'
                  AND execution_source IN ('paper', 'live')
                ORDER BY {order}""",
            (self.strategy_name,),
        ).fetchall()
        if not rows:
            return None

        if self.at_capacity == "replace_oldest":
            r = rows[0]
            try:
                rc = float(
                    (json.loads(r["entry_reasoning"]) if r["entry_reasoning"] else {}).get("conviction", 999)
                )
            except Exception:
                rc = 999.0
            return (r["id"], r["ticker"], rc)

        # replace_weakest — scan all
        weakest = None
        weakest_conv = 999.0
        for r in rows:
            try:
                rc = float(
                    (json.loads(r["entry_reasoning"]) if r["entry_reasoning"] else {}).get("conviction", 999)
                )
            except Exception:
                rc = 999.0
            if rc < weakest_conv:
                weakest_conv = rc
                weakest = (r["id"], r["ticker"], rc)
        return weakest

    # ── Order submission — live mode only ───────────────────────────────

    def _submit_admitted(self, admitted: List[Decision],
                         result: LiveRunResult) -> int:
        """LIVE PATH — submit Alpaca orders, persist DB rows.

        NOT IMPLEMENTED in Phase 3 part 2 MVP. The current MVP runs in
        dry_run mode only; full live submission requires mirroring
        cw_runner.execute_entries lines 1100–1300 (Alpaca order, retry,
        strategy_portfolio insert, trade_decision_audit insert,
        order_audit insert). That's the cutover work, intentionally
        deferred until after parallel-run validation.

        Raising here makes any accidental dry_run=False call fail loudly
        instead of silently doing nothing.
        """
        raise NotImplementedError(
            "PITLiveEngine submission is not yet wired. Use dry_run=True for "
            "validation. The submission path lands in the cw_runner cutover PR."
        )

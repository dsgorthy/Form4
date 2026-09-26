"""PITStrategy implementation of Quality + Momentum.

Mirrors the entry-decision logic in `cw_runner.scan_signals` for QM, but
written as a pure function of (view, event). This is the reference
implementation that the engine and any future grid-search code can share.

Yaml inputs (from `strategies/cw_strategies/configs/quality_momentum.yaml`):
  - filters.career_grade: ['A+', 'A']
  - filters.above_sma50: 1
  - filters.above_sma200: 1
  - filters.exclude_recurring: true
  - filters.exclude_tax_sales: true
  - min_conviction: 1.5
"""
from __future__ import annotations

from typing import Optional

from framework.decision.filters import evaluate_filters
from framework.pit.events import Decision, TradeEvent
from framework.pit.strategy import PITStrategy
from framework.pit.view import PITDataView


class QualityMomentumStrategy(PITStrategy):
    """Quality + Momentum: A+/A career-graded insiders buying in an uptrend.

    Decision pipeline (each stage emits an audit row):
      1. filter — career_grade ∈ {A+, A}, above_sma50 == 1, above_sma200 == 1,
                  not recurring, not tax-sale.
      2. conviction — compute via compute_conviction; require >= min_conv.

    The engine appends dedup / capacity stages after. This class focuses
    only on the strategy-specific filter + conviction.
    """

    def evaluate(self, view: PITDataView, event: TradeEvent) -> Decision:
        filters = self.config.get("filters", {})
        min_conv = float(self.config.get("min_conviction", 1.5))

        # ── Stage 1: filter — DELEGATED, never hand-coded ────────────
        #
        # This block used to check five conditions by hand: career_grade,
        # above_sma50, above_sma200, exclude_recurring, exclude_tax_sales. Any
        # OTHER filter a yaml declared was silently ignored on the live path
        # while the simulator applied it, which is the drift that
        # framework/decision/filters.py exists to close and that its own
        # docstring listed cw_runner as still owing. It became load-bearing on
        # 2026-09-25 when A-List adopted min_value_pct_of_adv: hand-coded, the
        # published book would have gated on trade size and the alerts would
        # not have.
        #
        # evaluate_filters is a superset of the five and applies each only when
        # the yaml declares it, so quality_momentum's behaviour is unchanged
        # (it declares exactly those five). The one addition is an
        # unconditional is_duplicate check, which events_filed_on already
        # enforces in SQL.
        ok, failures = evaluate_filters(filters, event)

        if not ok:
            return Decision(
                trade_id=event.trade_id, ticker=event.ticker,
                filing_date=event.filing_date, strategy=self.name,
                action="skip", stage="filter", passed=False,
                reason="; ".join(failures),
                pit_grade=event.pit_grade, career_grade=event.career_grade,
            )

        # ── Stage 2: conviction (delegates to existing impl) ───────────
        # Import here to avoid creating a hard dep at module-load time
        from pipelines.insider_study.conviction_score import (
            compute_conviction, _categorize_insider,
        )
        # NOTE: cw_runner passes pit_grade (V2) as signal_grade to conviction,
        # NOT career_grade. We mirror that for byte-equivalence. (Confirmed
        # by reading cw_runner.py:684.) Switching conviction to career_grade
        # is a separate decision tracked in design doc §8.
        signal_grade = event.pit_grade or "C"
        conv = compute_conviction(
            thesis=self.name,
            signal_grade=signal_grade,
            consecutive_sells=event.consecutive_sells_before,
            dip_1mo=event.dip_1mo,
            is_largest_ever=bool(event.is_largest_ever),
            above_sma50=bool(event.above_sma50),
            above_sma200=bool(event.above_sma200),
            insider_title=event.insider_title,
            is_csuite=bool(event.is_csuite),
        )
        role = _categorize_insider(event.insider_title, bool(event.is_csuite))

        snapshot = {
            "consecutive_sells_before": event.consecutive_sells_before,
            "dip_1mo": event.dip_1mo,
            "above_sma50": bool(event.above_sma50),
            "above_sma200": bool(event.above_sma200),
            "is_largest_ever": bool(event.is_largest_ever),
            "insider_title": event.insider_title,
            "is_csuite": bool(event.is_csuite),
            "insider_name": event.insider_name,
            "company": event.company,
            "role": role,
            "career_grade": event.career_grade,
            "pit_grade": event.pit_grade,
        }
        if conv < min_conv:
            return Decision(
                trade_id=event.trade_id, ticker=event.ticker,
                filing_date=event.filing_date, strategy=self.name,
                action="skip", stage="conviction", passed=False,
                reason=f"conv={conv:.2f} < threshold {min_conv:.2f}",
                conviction=conv,
                pit_grade=event.pit_grade, career_grade=event.career_grade,
                snapshot=snapshot,
            )
        return Decision(
            trade_id=event.trade_id, ticker=event.ticker,
            filing_date=event.filing_date, strategy=self.name,
            action="enter", stage="conviction", passed=True,
            reason=f"conv={conv:.2f} >= {min_conv:.2f}",
            conviction=conv,
            pit_grade=event.pit_grade, career_grade=event.career_grade,
            snapshot=snapshot,
        )

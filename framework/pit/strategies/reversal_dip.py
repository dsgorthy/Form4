"""PITStrategy implementation of Reversal Dip.

Mirrors `cw_runner.scan_signals` for RD. Filters: rare-reversal + N consecutive
sells + deep 3-month dip + exclude routine/recurring/tax/10b5-1.

Yaml inputs (from `strategies/cw_strategies/configs/reversal_dip.yaml`):
  filters:
    is_rare_reversal: 1
    min_consecutive_sells: 10
    min_dip_3mo: -0.25
    exclude_recurring: true
    exclude_tax_sales: true
    exclude_routine: true
    exclude_10b5_1: true
  min_conviction: 3.0
"""
from __future__ import annotations

from framework.decision.filters import evaluate_filters
from framework.pit.events import Decision, TradeEvent
from framework.pit.strategy import PITStrategy
from framework.pit.view import PITDataView


class ReversalDipStrategy(PITStrategy):
    def evaluate(self, view: PITDataView, event: TradeEvent) -> Decision:
        filters = self.config.get("filters", {})
        min_conv = float(self.config.get("min_conviction", 3.0))

        # Stage 1: filter — DELEGATED to the shared evaluator, not hand-coded.
        # Same reasoning as quality_momentum: seven conditions by hand meant any
        # other filter the yaml declared was applied by the simulator and
        # ignored by the live runner. evaluate_filters covers all seven.
        ok, failures = evaluate_filters(filters, event)

        if not ok:
            return Decision(
                trade_id=event.trade_id, ticker=event.ticker,
                filing_date=event.filing_date, strategy=self.name,
                action="skip", stage="filter", passed=False,
                reason="; ".join(failures),
                pit_grade=event.pit_grade, career_grade=event.career_grade,
            )

        # Stage 2: conviction
        from pipelines.insider_study.conviction_score import (
            compute_conviction, _categorize_insider,
        )
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
            "dip_3mo": event.dip_3mo,
            "is_rare_reversal": bool(event.is_rare_reversal),
            "insider_title": event.insider_title,
            "is_csuite": bool(event.is_csuite),
            "insider_name": event.insider_name,
            "company": event.company,
            "role": role,
            "pit_grade": event.pit_grade,
        }
        if conv < min_conv:
            return Decision(
                trade_id=event.trade_id, ticker=event.ticker,
                filing_date=event.filing_date, strategy=self.name,
                action="skip", stage="conviction", passed=False,
                reason=f"conv={conv:.2f} < {min_conv:.2f}",
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

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

from framework.pit.events import Decision, TradeEvent
from framework.pit.strategy import PITStrategy
from framework.pit.view import PITDataView


class ReversalDipStrategy(PITStrategy):
    def evaluate(self, view: PITDataView, event: TradeEvent) -> Decision:
        filters = self.config.get("filters", {})
        min_conv = float(self.config.get("min_conviction", 3.0))

        # Stage 1: filter
        failures = []
        if filters.get("is_rare_reversal") and not event.is_rare_reversal:
            failures.append("is_rare_reversal != 1")
        min_csb = filters.get("min_consecutive_sells")
        if min_csb is not None:
            csb = event.consecutive_sells_before
            if csb is None or csb < int(min_csb):
                failures.append(f"consecutive_sells_before={csb} < {min_csb}")
        min_dip = filters.get("min_dip_3mo")
        if min_dip is not None:
            d3 = event.dip_3mo
            if d3 is None or d3 > float(min_dip):
                failures.append(f"dip_3mo={d3} > {min_dip}")
        if filters.get("exclude_recurring") and event.is_recurring:
            failures.append("is_recurring=1")
        if filters.get("exclude_tax_sales") and event.is_tax_sale:
            failures.append("is_tax_sale=1")
        if filters.get("exclude_routine") and event.cohen_routine:
            failures.append("cohen_routine=1")
        if filters.get("exclude_10b5_1") and event.is_10b5_1:
            failures.append("is_10b5_1=1")

        if failures:
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

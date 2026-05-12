"""PITStrategy implementation of 10b5-1 Surprise.

Mirrors `cw_runner.scan_signals` for tenb51_surprise. Filters: prior 10b5-1
sells + exclude recurring/tax. Conviction also gated by min_prior_10b5_1_sells.

Yaml inputs (from `strategies/cw_strategies/configs/tenb51_surprise.yaml`):
  filters:
    min_prior_10b5_1_sells: 5
    exclude_recurring: true
    exclude_tax_sales: true
  min_conviction: 0.5
"""
from __future__ import annotations

from framework.pit.events import Decision, TradeEvent
from framework.pit.strategy import PITStrategy
from framework.pit.view import PITDataView


class Tenb51SurpriseStrategy(PITStrategy):
    def evaluate(self, view: PITDataView, event: TradeEvent) -> Decision:
        filters = self.config.get("filters", {})
        min_conv = float(self.config.get("min_conviction", 0.5))

        # Stage 1: simple filters first
        failures = []
        if filters.get("exclude_recurring") and event.is_recurring:
            failures.append("is_recurring=1")
        if filters.get("exclude_tax_sales") and event.is_tax_sale:
            failures.append("is_tax_sale=1")
        if failures:
            return Decision(
                trade_id=event.trade_id, ticker=event.ticker,
                filing_date=event.filing_date, strategy=self.name,
                action="skip", stage="filter", passed=False,
                reason="; ".join(failures),
                pit_grade=event.pit_grade, career_grade=event.career_grade,
            )

        # Stage 2: min_prior_10b5_1_sells — count of prior 10b5-1 SELL trades
        # by this insider on this ticker, filed STRICTLY BEFORE this event.
        #
        # `get_prior_trades` filters filing_date <= clock.as_of_date, which
        # for events_filed_on(today) INCLUDES the current event itself.
        # We must additionally filter `p.filing_date < event.filing_date`
        # to match cw_runner.count_prior_10b5_1_sells (which uses `<`).
        min_10b5_1 = filters.get("min_prior_10b5_1_sells")
        if min_10b5_1 is not None:
            priors = view.get_prior_trades(
                insider_id=event.insider_id, ticker=event.ticker,
                trade_type="sell",
            )
            n_prior_10b5_1 = sum(
                1 for p in priors
                if p.is_10b5_1 and p.filing_date < event.filing_date
            )
            if n_prior_10b5_1 < int(min_10b5_1):
                return Decision(
                    trade_id=event.trade_id, ticker=event.ticker,
                    filing_date=event.filing_date, strategy=self.name,
                    action="skip", stage="min_10b5_1", passed=False,
                    reason=f"prior_10b5_1_sells={n_prior_10b5_1} < {min_10b5_1}",
                    pit_grade=event.pit_grade, career_grade=event.career_grade,
                )

        # Stage 3: conviction
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

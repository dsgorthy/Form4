"""Pure filter evaluation — shared between live runner and walk-forward sim.

evaluate_filters() was previously duplicated as a Python evaluator in
simulate_strategy_portfolio.evaluate_filters() and as a SQL-clause builder
in cw_runner._build_thesis_query(). Today only the simulator's path lives
here. The cw_runner path is on the migration list: once it also calls
this function (after fetching candidate rows), one of the highest-impact
drift surfaces between sim and live closes.

The fact source can be either a dict (legacy, what simulator passes) or
a CandidateFact dataclass — both shapes are supported during migration.
"""
from __future__ import annotations

from typing import Any, Tuple


def _get(trade: Any, key: str, default: Any = None) -> Any:
    """Read `key` from a dict or a dataclass-like object uniformly.

    During Stage 3 migration both shapes flow through this module: legacy
    callers pass dicts; new callers pass CandidateFact instances. We don't
    convert at the boundary because that would add a round of allocations
    per candidate; instead we treat both shapes as duck-typed records.
    """
    if isinstance(trade, dict):
        return trade.get(key, default)
    return getattr(trade, key, default)


def evaluate_filters(thesis_filters: dict, trade: Any) -> Tuple[bool, list]:
    """Return (passed, failure_reasons).

    Filter semantics are the contract between sim and live — changing
    any condition here is a breaking change to BOTH. Tests in
    tests/unit/test_decision_filters.py pin the expected behavior.
    """
    failures = []

    if _get(trade, "is_duplicate"):
        failures.append("is_duplicate=1")

    if thesis_filters.get("is_rare_reversal") and not _get(trade, "is_rare_reversal"):
        failures.append("is_rare_reversal != 1")

    if "min_consecutive_sells" in thesis_filters:
        v = _get(trade, "consecutive_sells_before")
        threshold = int(thesis_filters["min_consecutive_sells"])
        if v is None or v < threshold:
            failures.append(f"consec_sells={v} < {threshold}")

    if "max_dip_1mo" in thesis_filters:
        v = _get(trade, "dip_1mo")
        threshold = float(thesis_filters["max_dip_1mo"])
        if v is None or v > threshold:
            failures.append(f"dip_1mo={v} > {threshold}")

    # ── Signals added 2026-08-29 ───────────────────────────────────────────
    #
    # min_value_pct_of_adv — trade size as a multiple of 20-day dollar volume.
    # Nearly monotone by decile on graded episodes: bottom two deciles return
    # 0.25-0.36%, the top 5.09%. RAW dollar value does not do this (t=1.90 vs
    # t=3.99), because a $100k purchase means something different in a name
    # trading $50k a day than in one trading $50m.
    if "min_value_pct_of_adv" in thesis_filters:
        v = _get(trade, "value_pct_of_adv")
        threshold = float(thesis_filters["min_value_pct_of_adv"])
        if v is None or v < threshold:
            failures.append(f"value_pct_of_adv={v} < {threshold}")

    # min_filing_lag_days — days between transaction and disclosure. The SEC
    # allows two; filings past twenty are unusual and, in this corpus, strongly
    # predictive: 466 episodes across 357 tickers, mean +13.1%, MEDIAN +7.7%,
    # 76% win rate, and it survives dropping the twenty best (+9.5%).
    #
    # Checked for the obvious confounds and it is none of them: it holds inside
    # every liquidity quintile (+7.4 to +16.4pp over prompt filings) and late
    # filers are not penny stocks (median price $13.32 against $13.25 prompt,
    # 24.0% under $5 against 23.8%).
    #
    # TREAT WITH CAUTION ANYWAY. A signal this large on a population this small
    # is exactly the shape of a data artefact, and it has not yet been through
    # the simulator. It is exposed as a filter so it CAN be, not because it is
    # settled.
    if "min_filing_lag_days" in thesis_filters:
        v = _get(trade, "filing_lag_days")
        threshold = int(thesis_filters["min_filing_lag_days"])
        if v is None or v < threshold:
            failures.append(f"filing_lag_days={v} < {threshold}")

    # max_pct_off_52w_high — how far BELOW the 52-week high, as a negative
    # fraction. -0.30 means "at least 30% off the high".
    #
    # This is the contrarian half of an interaction, not a standalone trend
    # filter. Split by both, graded episodes:
    #
    #                    near high   -10..-30%   deep (<-30%)
    #   below SMA50        1.06%       1.04%        2.49%
    #   above SMA50        1.85%       2.31%        4.61%
    #
    # The best cell is SHORT-TERM STRENGTH INSIDE A LONG-TERM DRAWDOWN, which
    # is why above_sma50 reads positive and pct_off_52w_high reads negative in
    # the same screen. They are not in conflict; they are one setup.
    if "max_pct_off_52w_high" in thesis_filters:
        v = _get(trade, "pct_off_52w_high")
        threshold = float(thesis_filters["max_pct_off_52w_high"])
        if v is None or v > threshold:
            failures.append(f"pct_off_52w_high={v} > {threshold}")

    if thesis_filters.get("above_sma50") and _get(trade, "above_sma50") != 1:
        failures.append("above_sma50 != 1")

    if thesis_filters.get("above_sma200") and _get(trade, "above_sma200") != 1:
        failures.append("above_sma200 != 1")

    if thesis_filters.get("is_largest_ever") and _get(trade, "is_largest_ever") != 1:
        failures.append("is_largest_ever != 1")

    # pit_grade and career_grade are both supported filter keys
    for grade_key in ("pit_grade", "career_grade"):
        if grade_key in thesis_filters:
            wanted = thesis_filters[grade_key]
            if isinstance(wanted, str):
                wanted = [wanted]
            if _get(trade, grade_key) not in wanted:
                failures.append(f"{grade_key}={_get(trade, grade_key)!r} not in {wanted}")

    if "min_dip_3mo" in thesis_filters:
        v = _get(trade, "dip_3mo")
        threshold = float(thesis_filters["min_dip_3mo"])
        if v is None or v > threshold:
            failures.append(f"dip_3mo={v} > {threshold}")

    if thesis_filters.get("exclude_10b5_1") and _get(trade, "is_10b5_1"):
        failures.append("is_10b5_1=1")
    if thesis_filters.get("exclude_recurring") and _get(trade, "is_recurring"):
        failures.append("is_recurring=1")
    if thesis_filters.get("exclude_tax_sales") and _get(trade, "is_tax_sale"):
        failures.append("is_tax_sale=1")
    if thesis_filters.get("exclude_routine") and _get(trade, "cohen_routine"):
        failures.append("cohen_routine=1")

    return len(failures) == 0, failures

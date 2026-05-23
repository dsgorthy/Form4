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

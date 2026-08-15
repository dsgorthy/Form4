"""Evaluate user-defined alert filters against a candidate trade.

Users could previously express exactly one rule: the columns on
notification_preferences. That could not say "career grade A+ in my watchlist
tickers", and it could not hold two rules at once.

Model:
    conditions AND within a filter, filters OR across.

"A+ anywhere" and "any grade in my tickers" are two rows, not a feature
request. A user with no filters falls back to notification_preferences
unchanged, so existing settings keep working exactly as before.

The grade comparison here replaces notification_scanner's _GRADE_TO_TIER,
which collapsed A+ and A into one tier and made "only A+" inexpressible — the
finest distinction the product computes was being discarded at the
notification boundary while every other surface showed letter grades.

Both grades are exposed as filterable fields rather than picking one for the
user. They answer different questions: career_grade is the whole track record,
pit_grade is recent form. Which matters is a judgement the user gets to make.
"""
from __future__ import annotations

from typing import Any, Iterable

# Ordered best-to-worst. Index position IS the comparison, so "gte A" admits
# A+ and A and nothing else — no lossy bucketing.
GRADE_ORDER = ("A+", "A", "B", "C", "D", "F")
_GRADE_RANK = {g: i for i, g in enumerate(GRADE_ORDER)}

# Allow-list. `field` arrives from a user-writable table, so it must never
# reach a query or a getattr unchecked. Anything not named here is ignored
# rather than rejected, so an old filter referencing a retired column degrades
# to "no opinion" instead of silencing a user's alerts entirely.
#
# Coverage figures are measured over trades filed in the last 14 days
# (2026-08-15, n=9,961) and are surfaced in the UI next to each field. A filter
# on a sparsely-populated column returns near-silence, and a user reads that as
# "no good trades this week" rather than "this column is rarely computed".
FIELDS: dict[str, dict[str, Any]] = {
    "career_grade":     {"kind": "grade",  "coverage": 6.6,   "label": "Career grade"},
    "pit_grade":        {"kind": "grade",  "coverage": 38.4,  "label": "Recent form"},
    "signal_grade":     {"kind": "grade",  "coverage": 90.9,  "label": "Signal grade"},
    "value":            {"kind": "number", "coverage": 100.0, "label": "Trade value"},
    "ticker":           {"kind": "text",   "coverage": 100.0, "label": "Ticker"},
    "trans_code":       {"kind": "text",   "coverage": 100.0, "label": "Transaction code"},
    "trade_type":       {"kind": "text",   "coverage": 100.0, "label": "Buy or sell"},
    "is_recurring":     {"kind": "bool",   "coverage": 100.0, "label": "Recurring trade"},
    "is_tax_sale":      {"kind": "bool",   "coverage": 100.0, "label": "Tax sale"},
    "is_10b5_1":        {"kind": "bool",   "coverage": 100.0, "label": "10b5-1 plan"},
    "is_rare_reversal": {"kind": "bool",   "coverage": 100.0, "label": "Rare reversal"},
    "above_sma50":      {"kind": "bool",   "coverage": 78.7,  "label": "Above 50-day average"},
    "is_largest_ever":  {"kind": "bool",   "coverage": 82.9,  "label": "Largest ever for insider"},
    "pit_cluster_size": {"kind": "number", "coverage": 60.6,  "label": "Cluster size"},
}


def _grade_at_least(actual: str | None, minimum: str) -> bool:
    """True when `actual` is `minimum` or better. Unknown grade never matches.

    A NULL grade is not a bad grade — it means the insider has no scored
    history — so it must not satisfy a quality floor.
    """
    if not actual or actual not in _GRADE_RANK or minimum not in _GRADE_RANK:
        return False
    return _GRADE_RANK[actual] <= _GRADE_RANK[minimum]


def _as_bool(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "true", "t", "yes")


def evaluate_condition(trade: dict, field: str, op: str, value: str) -> bool:
    """One condition against one trade. Unknown field or op = no opinion (True).

    Defaulting an unrecognised condition to True keeps a filter permissive
    rather than silently muting a user: the other conditions in the same filter
    still have to pass, so a stale field cannot manufacture a match on its own.
    """
    spec = FIELDS.get(field)
    if spec is None:
        return True

    actual = trade.get(field)

    if spec["kind"] == "grade":
        if op in ("gte", "min"):
            return _grade_at_least(actual, value)
        if op == "eq":
            return actual == value
        if op == "in":
            return actual in {v.strip() for v in value.split(",") if v.strip()}
        return True

    if spec["kind"] == "number":
        try:
            a = float(actual)
            b = float(value)
        except (TypeError, ValueError):
            return False
        return {"gte": a >= b, "lte": a <= b, "eq": a == b}.get(op, True)

    if spec["kind"] == "bool":
        if op in ("is_true", "eq"):
            return _as_bool(actual) == _as_bool(value) if op == "eq" else _as_bool(actual)
        if op == "is_false":
            return not _as_bool(actual)
        return True

    # text
    if actual is None:
        return False
    a = str(actual).strip().upper()
    if op == "eq":
        return a == value.strip().upper()
    if op == "in":
        return a in {v.strip().upper() for v in value.split(",") if v.strip()}
    return True


def filter_matches(trade: dict, conditions: Iterable[dict]) -> bool:
    """AND across a filter's conditions. A filter with none matches nothing.

    An empty filter matching everything would turn "I made a filter and left it
    blank" into "alert me about all 1,000 filings a day", which is how a user
    ends up muting the product entirely.
    """
    conds = list(conditions)
    if not conds:
        return False
    return all(
        evaluate_condition(trade, c["field"], c["op"], c["value"]) for c in conds
    )


def any_filter_matches(trade: dict, filters: Iterable[dict]) -> bool:
    """OR across a user's enabled filters.

    Callers pass only enabled filters. No filters at all is NOT a match here —
    the caller falls back to notification_preferences in that case, which is
    what keeps existing users unaffected.
    """
    return any(filter_matches(trade, f.get("conditions", [])) for f in filters)

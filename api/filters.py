"""Shared query filter helpers for trade endpoints."""

from __future__ import annotations

from typing import Sequence

from api.classification import KIND_META, _KIND_OF


def deduplicate_filers(
    rows: list[dict],
    value_key: str = "value",
    date_key: str = "trade_date",
    score_key: str = "score",
    identity_keys: Sequence[str] = ("insider_id", "insider_name", "cik", "score", "score_tier", "title"),
) -> list[dict]:
    """Merge multiple filers reporting the same economic event.

    Keeps the highest-scored insider per (rounded_value, date) signature.
    Adds ``n_filers`` count to each surviving row.

    Args:
        rows: list of dicts to deduplicate (mutated in place).
        value_key: dict key containing the dollar value for signature.
        date_key: dict key containing the date for signature.  Falls back
            to ``"trade_date"`` if the primary key is missing.
        score_key: dict key used to pick the best insider per group.
        identity_keys: keys copied from a higher-scored duplicate onto
            the surviving row.
    """
    seen: dict[str, dict] = {}
    deduped: list[dict] = []
    for item in rows:
        val = item.get(value_key, 0) or 0
        dt = item.get(date_key) or item.get("trade_date", "")
        sig = f"{round(val, 0)}|{dt}"
        if sig in seen:
            seen[sig]["n_filers"] = seen[sig].get("n_filers", 1) + 1
            if (item.get(score_key) or 0) > (seen[sig].get(score_key) or 0):
                seen[sig].update(
                    {k: item[k] for k in identity_keys if k in item}
                )
        else:
            item["n_filers"] = 1
            seen[sig] = item
            deduped.append(item)
    return deduped

# Group trades by filing — uses pre-computed filing_key column.
# filing_key = accession when available, else trade_date.
# One Form 4 filing = one row, even if it spans multiple trade dates.


def filing_group_by(alias: str = "t") -> str:
    """Return the GROUP BY expression for filing-level aggregation."""
    return f"{alias}.filing_key"


# ---------------------------------------------------------------------------
# signal_class — see migrations/2026-08-17_trades_signal_class.sql
#
# The column is maintained by a DB trigger from form4_signal_class(); this
# module never reimplements the mapping, it only names the sets. Duplicating
# the classification in Python is the exact failure this column exists to
# prevent: `trans_code IN ('P','S')` was hand-written on ~20 browsing surfaces
# and omitted from the scoring path, which is how option exercises and grants
# ended up supplying 75% of the evidence behind a career grade.
# ---------------------------------------------------------------------------

#: Classes that carry directional information. Measured 2016-2026, 30d
#: SPY-adjusted: discretionary_buy +1.06%, discretionary_sell -0.39%.
#:
#: DERIVED, NOT TYPED. This was a hand-written tuple identical to
#: classification.DISCRETIONARY_CLASSES, with nothing keeping the two in step —
#: the same "one concept, two definitions" shape behind every defect found in
#: August 2026. It now comes from KIND_META, where each kind already declares
#: whether it is a signal, so a change to the vocabulary propagates here by
#: construction instead of by memory.
MEANINGFUL_CLASSES = tuple(
    cls for cls, kind in _KIND_OF.items()
    if kind is not None and KIND_META[kind]["signal"]
)

#: Everything retained but excluded from the meaningful default. planned_sell
#: is here rather than merged into the sell signal because at +0.86% over 30
#: days it points the OPPOSITE way from a discretionary sale — averaging the
#: two does not dilute the signal, it cancels it.
NON_MEANINGFUL_CLASSES = (
    "planned_buy", "planned_sell", "option_exercise", "compensation",
    "tax_withholding", "gift", "derivative", "inconsistent", "other",
)

ALL_CLASSES = MEANINGFUL_CLASSES + NON_MEANINGFUL_CLASSES


def add_signal_class_filter(conditions: list, params: list,
                            signal_classes: str | None, alias: str = "t") -> None:
    """Append a signal_class filter to the SQL conditions/params lists.

    Args:
        conditions: mutable list of SQL WHERE fragments
        params: mutable list of bind parameters
        signal_classes: comma-separated class names, or the literal
            ``"meaningful"`` for :data:`MEANINGFUL_CLASSES`, or ``"all"`` /
            ``None`` for no filter at all. Unknown names are dropped rather
            than passed through, so a typo narrows to nothing visible instead
            of silently matching every row.
        alias: table alias for the trades table
    """
    raw = (signal_classes or "").strip().lower()
    # Blank and "all" both mean "caller did not narrow"; only a non-empty value
    # that resolves to nothing is treated as a typo below.
    if not raw or raw == "all":
        return
    if raw == "meaningful":
        wanted = list(MEANINGFUL_CLASSES)
    else:
        wanted = [c.strip().lower() for c in raw.split(",") if c.strip()]
        wanted = [c for c in wanted if c in ALL_CLASSES]
    if not wanted:
        # Every requested name was unrecognized. Match nothing — an empty
        # result is a visible failure; ignoring the filter is a silent one.
        conditions.append("1 = 0")
        return
    placeholders = ",".join("?" * len(wanted))
    conditions.append(f"{alias}.signal_class IN ({placeholders})")
    params.extend(wanted)


def add_trans_code_filter(conditions: list, params: list, trans_codes: str, alias: str = "t") -> None:
    """Append a trans_code filter to the SQL conditions/params lists.

    Args:
        conditions: mutable list of SQL WHERE fragments
        params: mutable list of bind parameters
        trans_codes: comma-separated trans codes, e.g. "P,S" or "P,S,A,M,F,G,X"
        alias: table alias for the trades table
    """
    codes = [c.strip().upper() for c in trans_codes.split(",") if c.strip()]
    if not codes:
        return
    placeholders = ",".join("?" * len(codes))
    conditions.append(f"{alias}.trans_code IN ({placeholders})")
    params.extend(codes)

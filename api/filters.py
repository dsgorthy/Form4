"""Shared query filter helpers for trade endpoints."""

from __future__ import annotations

from typing import Sequence


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

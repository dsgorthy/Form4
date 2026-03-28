"""Shared query filter helpers for trade endpoints."""

from __future__ import annotations

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

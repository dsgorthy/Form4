"""Parity comparator — diff two signals' observations for a date range.

The use case driving the API: confirming the dataplane-native EDGAR
ingestor (`insider.filings.raw.v1`) matches the form4-bridge ingestor
(`insider.trades.raw.v1`) before retiring the bridge.

Join key by default is the canonical insider-trade fingerprint:
    (ticker, accession, trade_date, qty, trans_code)

Reports:
  - rows on each side
  - matched rows (full join key agreement)
  - rows only in A
  - rows only in B
  - first N sample mismatches
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import List, Sequence, Tuple

import psycopg2


DEFAULT_KEY_FIELDS = ("ticker", "accession", "trade_date", "qty", "trans_code")


@contextmanager
def _conn():
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


@dataclass
class ParityResult:
    signal_a: str
    signal_b: str
    from_date: str
    to_date: str
    key_fields: Tuple[str, ...]
    count_a: int = 0
    count_b: int = 0
    matched: int = 0
    only_in_a: int = 0
    only_in_b: int = 0
    sample_only_in_a: List[tuple] = field(default_factory=list)
    sample_only_in_b: List[tuple] = field(default_factory=list)


def _fingerprints(conn, signal_id: str, from_date: str, to_date: str,
                  key_fields: Sequence[str]) -> List[tuple]:
    """Return (key_tuple,) rows for a signal in the date range."""
    extract = ", ".join(f"value->>{repr(k)}" for k in key_fields)
    sql = f"""
        SELECT ticker, {extract}
          FROM signal_observations
         WHERE signal_id LIKE %s
           AND as_of_date >= %s::date
           AND as_of_date <  (%s::date + INTERVAL '1 day')
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (f"{signal_id}%", from_date, to_date))
        # Reduce each row to a homogenous tuple. Convert null -> "" to make
        # set membership deterministic.
        rows = cur.fetchall()
    finally:
        cur.close()
    return [tuple((v if v is not None else "") for v in r) for r in rows]


def compare(
    signal_a: str,
    signal_b: str,
    from_date: str,
    to_date: str,
    key_fields: Sequence[str] = DEFAULT_KEY_FIELDS,
    sample_size: int = 5,
) -> ParityResult:
    result = ParityResult(
        signal_a=signal_a,
        signal_b=signal_b,
        from_date=from_date,
        to_date=to_date,
        key_fields=tuple(key_fields),
    )

    with _conn() as conn:
        a_keys = _fingerprints(conn, signal_a, from_date, to_date, key_fields)
        b_keys = _fingerprints(conn, signal_b, from_date, to_date, key_fields)

    result.count_a = len(a_keys)
    result.count_b = len(b_keys)

    a_set = set(a_keys)
    b_set = set(b_keys)
    matched_set = a_set & b_set
    only_a = a_set - b_set
    only_b = b_set - a_set

    result.matched = len(matched_set)
    result.only_in_a = len(only_a)
    result.only_in_b = len(only_b)
    result.sample_only_in_a = list(only_a)[:sample_size]
    result.sample_only_in_b = list(only_b)[:sample_size]

    return result

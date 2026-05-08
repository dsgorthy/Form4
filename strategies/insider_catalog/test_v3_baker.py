#!/usr/bin/env python3
"""Smoke test V3 scorer against G. Leonard Baker (insider_id=3329) on CORT.

Expected: V3 should produce A or A+ given Baker's strong historical track record
(32+ observable buys, average abnormal_7d ~+10%, multiple +40% outliers).

V2 currently produces D (blended_score=0) at as_of_date=2026-03-19 due to
1.5y recency decay collapsing his 5+ year old trade weights.

Usage (on Studio, where form4 DB lives):
    python3 -m strategies.insider_catalog.test_v3_baker

This is a read-only test. Writes nothing.
"""
from __future__ import annotations

from config.database import get_connection
from strategies.insider_catalog.pit_scoring import (
    DEFAULT_SCORER,
    SCORER_V3,
    compute_insider_ticker_score,
)


BAKER_ID = 3329
TICKER = "CORT"
AS_OF = "2026-03-19"


def main():
    with get_connection() as conn:
        v2 = compute_insider_ticker_score(conn, BAKER_ID, TICKER, AS_OF)
        v3 = compute_insider_ticker_score(conn, BAKER_ID, TICKER, AS_OF, scorer=SCORER_V3)

    print("=" * 72)
    print(f"Baker (insider_id={BAKER_ID}) at {TICKER}, as_of={AS_OF}")
    print("=" * 72)
    print(f"{'metric':<35} {'V2':<15} {'V3':<15}")
    print("-" * 72)
    rows = [
        ("blended_score", v2.blended_score, v3.blended_score),
        ("grade", v2.grade, v3.grade),
        ("ticker_score", v2.ticker_score, v3.ticker_score),
        ("global_score", v2.global_score, v3.global_score),
        ("ticker_trade_count", v2.ticker_trade_count, v3.ticker_trade_count),
        ("global_trade_count", v2.global_trade_count, v3.global_trade_count),
        ("ticker_win_rate_7d", v2.ticker_win_rate_7d, v3.ticker_win_rate_7d),
        ("ticker_avg_abnormal_7d", v2.ticker_avg_abnormal_7d, v3.ticker_avg_abnormal_7d),
        ("score_7d", v2.score_7d, v3.score_7d),
        ("score_30d", v2.score_30d, v3.score_30d),
        ("score_90d", v2.score_90d, v3.score_90d),
        ("ticker_weight", v2.ticker_weight, v3.ticker_weight),
        ("role_weight", v2.role_weight, v3.role_weight),
        ("sufficient_data", v2.sufficient_data, v3.sufficient_data),
    ]
    for name, a, b in rows:
        print(f"{name:<35} {str(a):<15} {str(b):<15}")
    print("=" * 72)
    print()
    print(f"V2 method: {v2.method}")
    print(f"V3 method: {v3.method}")
    print()
    if v3.grade in ("A+", "A", "B"):
        print(f"OK: V3 produces a non-collapsed grade ({v3.grade}).")
    else:
        print(f"FLAG: V3 still produces {v3.grade}. Investigate.")


if __name__ == "__main__":
    main()

"""A filed price in the wrong currency must not become a post.

2026-09-16: UMC's CFO filed 1.6M Taiwan-listed shares at NT$142-145. Against
our $22.54 ADR close the generator wrote "sold $228.8M ... already 84% below
their fill". The sale was about US$7M. The day before, AXIA3 (a B3 listing in
BRL, no US series) went out as a $5.5M buy under a cashtag StockTwits cannot
resolve. Both were caught by a human reading the file; this makes the
generator catch them.
"""
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "generate_stocktwits_posts",
    Path(__file__).resolve().parents[2] / "pipelines" / "generate_stocktwits_posts.py",
)
gen = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gen)


def test_umc_in_twd_against_a_usd_adr_is_foreign():
    assert gen.price_is_foreign(142.333, 22.54)


def test_a_real_price_near_our_close_is_not():
    assert not gen.price_is_foreign(47.84, 48.40)
    assert not gen.price_is_foreign(10.50, 8.90)       # a bad week, not a currency


def test_a_two_and_a_half_fold_move_is_still_a_move():
    assert not gen.price_is_foreign(25.0, 10.0)
    assert not gen.price_is_foreign(10.0, 25.0)


def test_threefold_either_way_is_foreign():
    assert gen.price_is_foreign(30.0, 10.0)
    assert gen.price_is_foreign(10.0, 30.0)


def test_no_series_is_a_gap_not_a_currency():
    assert not gen.price_is_foreign(142.333, None)
    assert not gen.price_is_foreign(None, 22.54)
    assert not gen.price_is_foreign(0, 22.54)


class _Conn:
    def __init__(self, closes):
        self.closes = closes
    def execute(self, sql, params):
        tickers, _day = params
        rows = [{"ticker": t, "close": c} for t, c in self.closes.items() if t in tickers]
        class R:
            def __init__(s, rows): s.rows = rows
            def fetchall(s): return s.rows
        return R(rows)


def test_drop_foreign_priced_keeps_the_domestic_rows_and_names_the_dropped():
    rows = [
        {"ticker": "UMC", "price": 142.333},
        {"ticker": "GOLD", "price": 47.84},
        {"ticker": "AXIA3", "price": 10.5},
        {"ticker": "NEWCO", "price": 12.0},    # no series yet
    ]
    kept = gen.drop_foreign_priced(_Conn({"UMC": 22.54, "GOLD": 48.40}), rows, "2026-09-17")
    assert [r["ticker"] for r in kept] == ["GOLD", "NEWCO"]

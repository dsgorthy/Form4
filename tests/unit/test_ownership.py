"""Ownership-line reconciliation.

The two fixtures below are the filings that shipped as wrong public posts on
2026-08-17. They are kept verbatim because the failure was not a rounding
error — it was a 73% claim on a 3% trim — and a regression here goes straight
to Stocktwits.
"""

from __future__ import annotations

import pytest

from api.ownership import position_change, split_ownership_lines


def _lot(trade_id, date, qty, after, nature="", di="I"):
    return {
        "trade_id": trade_id, "trade_date": date, "qty": qty,
        "shares_owned_after": after, "nature_of_ownership": nature,
        "direct_indirect": di,
    }


# DST Global sold CHYM through seven named partnerships. Every line is
# separable from `nature_of_ownership` alone.
CHYM_LOTS = [
    _lot(1812229, "2026-08-13", 326492, 22299509, "By DST Global VI, L.P."),
    _lot(1812230, "2026-08-13", 167739, 11456614, "By DST Investments XXI, L.P."),
    _lot(1812231, "2026-08-13", 28156, 1923104, "By DSTG VI Investments, L.P."),
    _lot(1812232, "2026-08-13", 34025, 2323914, "By DSTG VI Investments-A, L.P."),
    _lot(1812233, "2026-08-13", 98821, 6749486, "By DST Global VII, L.P."),
    _lot(1812234, "2026-08-13", 51387, 3509733, "By DSTG VII Investments-1, L.P."),
    _lot(1812235, "2026-08-13", 6669, 455545, "By DSTG VII Investments-4, L.P."),
    _lot(1812236, "2026-08-14", 240152, 22059357, "By DST Global VI, L.P."),
    _lot(1812237, "2026-08-14", 123381, 11333233, "By DST Investments XXI, L.P."),
    _lot(1812238, "2026-08-14", 20710, 1902394, "By DSTG VI Investments, L.P."),
    _lot(1812239, "2026-08-14", 25027, 2298887, "By DSTG VI Investments-A, L.P."),
    _lot(1812240, "2026-08-14", 72688, 6676798, "By DST Global VII, L.P."),
    _lot(1812241, "2026-08-14", 37798, 3471935, "By DSTG VII Investments-1, L.P."),
    _lot(1812242, "2026-08-14", 4905, 450640, "By DSTG VII Investments-4, L.P."),
]

# Larry Robbins filed every BFLY line as "See footnotes", so only the balance
# chain separates them: 11,743,530 -> 4,371,387 on a 175,300-share sale.
BFLY_LOTS = [
    _lot(1812453, "2026-08-13", 215233, 12243530, "See footnotes"),
    _lot(1812454, "2026-08-13", 500000, 11743530, "See footnotes"),
    _lot(1812455, "2026-08-13", 175300, 4371387, "See footnotes"),
    _lot(1812456, "2026-08-14", 824700, 3546687, "See footnotes"),
    _lot(1812457, "2026-08-17", 582500, 2964187, "See footnotes"),
]


def test_named_partnerships_split_by_nature():
    lines = split_ownership_lines(CHYM_LOTS, is_buy=False)
    assert len(lines) == 7
    assert all(len(line) == 2 for line in lines)


def test_chym_trim_is_three_percent_not_seventy_three():
    pc = position_change(CHYM_LOTS, is_buy=False)
    assert pc is not None
    assert pc.qty == pytest.approx(1_237_950)
    assert pc.before == pytest.approx(49_431_194)
    assert pc.after == pytest.approx(48_193_244)
    assert pc.fraction == pytest.approx(0.025, abs=0.001)


def test_footnote_lines_split_on_balance_break():
    lines = split_ownership_lines(BFLY_LOTS, is_buy=False)
    assert len(lines) == 2
    assert [len(line) for line in lines] == [2, 3]


def test_bfly_cut_is_fourteen_percent_not_forty_four():
    pc = position_change(BFLY_LOTS, is_buy=False)
    assert pc is not None
    assert pc.before == pytest.approx(17_005_450)
    assert pc.fraction == pytest.approx(0.135, abs=0.001)


def test_single_line_buy_is_unchanged():
    """The common case must not regress: one line, one balance chain."""
    lots = [
        _lot(1, "2026-08-14", 13627, 33627, "", "D"),
        _lot(2, "2026-08-14", 12073, 45700, "", "D"),
    ]
    pc = position_change(lots, is_buy=True)
    assert pc is not None
    assert pc.before == pytest.approx(20000)
    assert pc.after == pytest.approx(45700)
    assert pc.fraction == pytest.approx(1.285, abs=0.001)


def test_missing_balance_suppresses_rather_than_guesses():
    lots = [_lot(1, "2026-08-14", 1000, None)]
    assert position_change(lots, is_buy=True) is None
    assert split_ownership_lines(lots, is_buy=True) is None


def test_sell_larger_than_holding_suppresses():
    """A stake cannot fall by more than 100%; refuse to say it did."""
    lots = [_lot(1, "2026-08-14", 5000, 0)]
    # before = 0 + 5000 = 5000, fraction = 1.0 -> a full exit is legitimate.
    assert position_change(lots, is_buy=False).is_full_exit

    # A balance that implies selling more than was held is not.
    bad = [
        _lot(1, "2026-08-14", 100, 900),
        _lot(2, "2026-08-14", 5000, 800),   # reconciles as a new line: before 5800
    ]
    pc = position_change(bad, is_buy=False)
    assert pc is None or pc.fraction <= 1.0


def test_zero_prior_position_suppresses():
    """A first-ever purchase has no prior stake to express a change against."""
    lots = [_lot(1, "2026-08-14", 6739, 6739, "", "D")]
    assert position_change(lots, is_buy=True) is None

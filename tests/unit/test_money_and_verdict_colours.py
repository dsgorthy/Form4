"""A loss is never green, and a negative amount is never "$-715".

WHAT WENT WRONG

Found by screenshotting /filing/zqkwh7 — a 10b5-1 sale by an EVP of Chicago
Bridge & Iron — instead of reading its markup. The "What if you followed this
trade?" table rendered:

    7d    Stock -7.15%   SPY -5.82%   Alpha -1.33%   P&L  $-715     <- GREEN
    30d   Stock +2.85%   SPY +0.26%   Alpha +2.58%   P&L  +$285     <- RED

Two separate defects stacked:

1. THE P&L WAS COLOURED BY `isGood`, which for a sell is true when the stock
   FELL. So a $715 loss came out green and a $285 gain came out red. Stock and
   Alpha may legitimately use isGood — they answer "was the insider right to
   sell" — but P&L answers "what happened to the money", and those are
   different questions with different signs.

2. formatCurrency INTERPOLATED THE NEGATIVE AFTER THE SYMBOL, producing
   "$-715". The minus belongs outside.

And the framing was incoherent for a sell: pnl_10k is 10000 * stock_return, a
LONG position, under a heading reading "what if you followed this trade". You
do not follow a sale by buying.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FMT = (REPO / "frontend" / "src" / "lib" / "format.ts").read_text(encoding="utf-8")
WHATIF = (REPO / "frontend" / "src" / "components" / "what-if-simulator.tsx").read_text(encoding="utf-8")


def test_negative_currency_puts_the_sign_outside_the_symbol():
    body = FMT[FMT.index("export function formatCurrency"):]
    body = body[:body.index("\n}")]
    assert "const sign" in body, "the sign handling is gone from formatCurrency"
    # Every branch must format the ABSOLUTE value; interpolating the signed
    # value straight after "$" is the defect.
    for m in re.finditer(r"return `([^`]+)`", body):
        tpl = m.group(1)
        assert "${value" not in tpl, (
            f'formatCurrency returns `{tpl}` — interpolating the signed value '
            'after "$" is what produced "$-715"'
        )


def test_the_pnl_column_is_coloured_by_its_own_sign():
    # Isolate the <td> that renders pnl_10k. Searching for the bare name finds
    # the TypeScript interface first, 500 lines from anything that renders.
    cells = re.findall(r"<td\b[\s\S]*?</td>", WHATIF)
    pnl_cells = [c for c in cells if "pnl_10k" in c]
    assert pnl_cells, "no <td> renders pnl_10k any more"
    pnl_cell = pnl_cells[0]
    assert "h.pnl_10k >= 0 ?" in pnl_cell or "h.pnl_10k > 0 ?" in pnl_cell, (
        "the P&L cell no longer colours on its own sign"
    )
    assert "isGood" not in pnl_cell, (
        "the P&L cell is coloured by isGood again. On a sell that renders a "
        "loss green — the reader sees green beside a minus sign."
    )


def test_stock_and_alpha_may_still_use_the_verdict_colouring():
    """Not everything should change. 'Did the stock fall after they sold' is a
    real question and green is the right answer to it."""
    assert "const isGood = isSell" in WHATIF, (
        "isGood is gone entirely; the Stock and Alpha columns legitimately "
        "answer whether the insider was right"
    )


def test_the_heading_is_honest_for_a_sell():
    """pnl_10k is 10000 * stock_return — a LONG position. You do not follow a
    sale by buying, so the heading must not claim you did."""
    assert 'isSell ? "What If You Had Bought Instead?"' in WHATIF, (
        "the sell heading is back to 'What if you followed this trade?' over a "
        "long position's P&L"
    )

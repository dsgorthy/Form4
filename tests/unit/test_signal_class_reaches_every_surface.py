"""Every surface that judges a filing must receive signal_class.

WHAT WENT WRONG

/insiders/{id}/trades did not select signal_class, so it arrived at the client
as null. insider-trades-table derives:

    isRoutineSell = trade_type === "sell" && !isDiscretionary(signal_class)

and isDiscretionary(null) is false — so EVERY sell on EVERY insider page was
labelled "SELL · Routine", including plain discretionary sales. The filing page
and /explore had the field, judged the same trade differently, and that
disagreement is how it surfaced: one STX sale read "Routine" on the insider
page and "Notable" everywhere else.

A field that is absent does not render as absent. It renders as whatever the
default branch says, and here the default was the most misleading answer.

THE AGGREGATION RULE

These endpoints group lots into filings, so signal_class has to be aggregated.
It is NOT MAX(): 723 filing groups in 90 days hold more than one class even
after grouping by trade_type — `discretionary_sell + gift + tax_withholding` is
common — and MAX() sorts alphabetically, returning `tax_withholding` and hiding
a genuine open-market sale behind the vesting paperwork filed beside it.

A filing containing a real decision IS a real decision.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
INSIDERS = REPO / "api/routers/insiders.py"
TABLE = REPO / "frontend/src/components/insider-trades-table.tsx"


def _trades_query() -> str:
    src = INSIDERS.read_text()
    i = src.index('@router.get("/{identifier}/trades")')
    return src[i:src.index("@router.get", i + 10)]


def test_the_trades_endpoint_selects_signal_class():
    q = _trades_query()
    assert "signal_class" in q, (
        "/insiders/{id}/trades does not select signal_class. It arrives null, "
        "and the client labels every sell 'Routine'."
    )


def test_it_is_carried_through_the_outer_select():
    """The inner aggregate having the column is not enough — it was added
    there once and still arrived null because the outer SELECT dropped it."""
    q = _trades_query()
    assert "agg.signal_class" in q, (
        "signal_class is aggregated but not projected by the outer SELECT, so "
        "it still reaches the client as null"
    )


def test_the_aggregation_prefers_discretionary_over_mechanical():
    """MAX() alphabetically returns tax_withholding over discretionary_sell."""
    q = _trades_query()
    block = q[q.index("AS signal_class") - 700:q.index("AS signal_class")]
    assert "FILTER" in block and "discretionary" in block, (
        "signal_class is aggregated with a plain MAX(). 723 filing groups in "
        "90 days are mixed; MAX picks alphabetically and would report a real "
        "sale as tax_withholding."
    )


def test_the_client_default_is_still_the_dangerous_one():
    """Guards the premise. If the component ever stops defaulting null to
    'Routine', this file's urgency drops — but until then the API contract
    above is load-bearing."""
    src = TABLE.read_text()
    assert "isDiscretionary(t.signal_class)" in src, (
        "insider-trades-table no longer derives its label from signal_class — "
        "re-check what it falls back to when the field is missing"
    )


@pytest.mark.parametrize("endpoint", [
    '@router.get("/{identifier}/trades")',
])
def test_no_surface_infers_routine_from_trade_type_alone(endpoint):
    """`trade_type == 'sell'` says nothing about whether a sale was a decision.
    184k compensation grants and 221k option exercises are stored as buys; the
    sell side is just as mixed."""
    src = TABLE.read_text()
    bad = re.findall(r'trade_type\s*===\s*"sell"\s*&&\s*(?!.*signal_class)', src)
    assert not bad, "a sell is being called routine without consulting signal_class"

"""A 10b5-1 planned purchase is not a signal, and must not enter a book.

THE ASYMMETRY

"Meaningful" has always excluded `planned_sell` — a sale scheduled months in
advance says nothing about what the insider thinks today. `planned_buy` is the
same object pointing the other way, and it was admitted, because the strategy
candidate query gates on `trans_code = 'P'` and a planned purchase is still a
P-code purchase. Nobody decided to include it; nobody excluded it either.

WHAT IT MEASURES, over the full history:

    discretionary_buy   n=112,831   abnormal 30d  +1.71%   47.3% beat SPY
    planned_buy         n=  1,020   abnormal 30d  -2.22%   43.0% beat SPY

Not weaker. Negative, by 3.93 points.

WHY THIS IS NOT CURVE-FITTING

Exactly one planned_buy ever reached a published book — COE in A-List on
2026-05-19, which closed -43.2%, one of that book's two worst positions.
Removing it moves A-List from 64.8% to 70.0% CAGR, and a five-point improvement
from deleting a single losing trade deserves suspicion.

The justification is not that number. It is that the rule is (a) symmetric with
what we already do for sells, (b) measured on 1,020 trades across seven years
rather than on the one position, and (c) true ex ante: "planned" means the
decision was made before the information existed. The CAGR move is a
consequence of the rule, not the reason for it.

If the effect had gone the other way we would still exclude them.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SIM = REPO / "pipelines/insider_study/simulate_strategy_portfolio.py"
RUNNER = REPO / "strategies/cw_strategies/cw_runner.py"


@pytest.mark.parametrize("path,name", [(SIM, "simulator"), (RUNNER, "live runner")])
def test_both_surfaces_exclude_planned_buys(path, name):
    """Sim and live must agree. A gate on one surface only is the -30% stop
    defect: two surfaces, one rule, two answers."""
    code = "\n".join(l for l in path.read_text().splitlines()
                     if not l.strip().startswith(("#", "--")))
    assert "planned_buy" in code, (
        f"the {name} no longer excludes planned_buy — a pre-scheduled purchase "
        "can enter the book, and that class averages -2.22% abnormal at 30d"
    )


@pytest.mark.parametrize("path", [SIM, RUNNER])
def test_the_exclusion_survives_a_null_signal_class(path):
    """A row with no class must still be admitted; excluding on `<> 'planned_buy'`
    alone silently drops every NULL, because NULL <> x is NULL, not true."""
    # Comments stripped first. The explanation above the clause mentions
    # planned_buy several times, and matching that instead of the SQL is how
    # this test passed against a broken gate on its first run.
    code = "\n".join(l for l in path.read_text().splitlines()
                     if not l.strip().startswith(("#", "--")))
    i = code.index("planned_buy")
    window = code[max(0, i - 300):i + 100]
    assert "IS NULL" in window, (
        "the planned_buy exclusion does not handle a NULL signal_class. "
        "`t.signal_class <> 'planned_buy'` evaluates to NULL for a NULL class, "
        "which is not true, so every unclassified candidate would vanish."
    )


def test_the_trans_code_gate_is_still_there():
    """planned_buy exclusion is additional to, not a replacement for, the
    open-market gate. 184k compensation grants carry trade_type='buy'."""
    for path in (SIM, RUNNER):
        assert "trans_code = 'P'" in path.read_text(), (
            f"{path.name} no longer restricts to open-market purchases"
        )

"""A simulator rebuild must not erase the live alert record.

cw_runner writes alert-only entries with execution_source = 'alert'. Those are
the record of what a subscriber was actually told — they are not derived, they
cannot be recomputed, and a nightly rebuild that drops them destroys the only
evidence of what the product sent.

wipe_strategy() deleted every row for a strategy regardless of source. It was
harmless only because no 'alert' row has ever existed; the notifier could not
see that source, so the runner's entries never mattered. Both of those are
being fixed at once, which is exactly when this would have bitten.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SIM = REPO / "pipelines/insider_study/simulate_strategy_portfolio.py"

#: Sources the simulator owns and may recreate. Anything else is a record of
#: something that happened once.
REBUILDABLE = {"simulated"}


def _deletes():
    src = SIM.read_text()
    return re.findall(r"DELETE FROM strategy_portfolio(.{0,220})", src, flags=re.S)


def test_every_delete_scopes_to_a_rebuildable_source():
    stmts = _deletes()
    assert stmts, "no DELETE against strategy_portfolio found — did it move?"
    for stmt in stmts:
        assert "execution_source" in stmt, (
            "a DELETE against strategy_portfolio does not scope by "
            f"execution_source, so it can erase live alert history:\n{stmt.strip()[:160]}"
        )
        for src in REBUILDABLE:
            if f"'{src}'" in stmt:
                break
        else:
            raise AssertionError(
                f"DELETE scopes to a source the simulator does not own:\n{stmt.strip()[:160]}"
            )


def test_alert_is_not_a_rebuildable_source():
    """Guards the premise of the test above."""
    assert "alert" not in REBUILDABLE

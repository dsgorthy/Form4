"""The conviction gate lives in the yaml, and the two surfaces default apart.

WHAT IS WRONG TODAY

Nothing — and that is exactly the state the -30% stop was in for three months
before it cost us five CAGR points.

    simulate_strategy_portfolio.py:311   config.get("min_conviction", 1.5)
    cw_runner.py:973                     config.get("min_conviction", 5.0)

Both read the yaml, so while every active strategy declares the value the two
agree. The moment one does not, the published book simulates a gate of 1.5
while subscribers' alerts apply 5.0 — a 3.5 gap on the single parameter
CLAUDE.md describes as "doing more work than any individual signal", against a
score built from ~12 half-point components.

That is the -30% stop defect precisely: two surfaces, one rule, two sources,
neither wrong on its own terms.

WHAT THESE TESTS PIN

1. Every published strategy declares min_conviction explicitly, which makes the
   divergent defaults unreachable.
2. The simulator and the live runner resolve the SAME value from the SAME yaml.
3. The gate is NOT one global number — reversal_dip is 3.0 where the other two
   are 1.5. Copy that names a single figure is wrong for at least one book.

These deliberately do not change either default. Making them agree would move
the live gate for an undeclared strategy, which is a behaviour change and
Derek's call; requiring declaration removes the hazard without touching
behaviour.

Related: tests/unit/test_stop_is_config_driven.py — same defect class, same
shape, already paid for once.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CONFIGS = REPO / "strategies/cw_strategies/configs"
SIMULATOR = REPO / "pipelines/insider_study/simulate_strategy_portfolio.py"
RUNNER = REPO / "strategies/cw_strategies/cw_runner.py"

ACTIVE = ["quality_notrend", "quality_momentum", "reversal_dip"]


def _config(strategy: str) -> dict:
    return yaml.safe_load((CONFIGS / f"{strategy}.yaml").read_text())


def _default_in(path: Path) -> float:
    """The literal default each surface falls back to."""
    m = re.search(r'min_conv\w*\s*=\s*(?:float\()?config\.get\(\s*"min_conviction"\s*,\s*([\d.]+)',
                  path.read_text())
    assert m, f"{path.name} no longer reads min_conviction from config with a default"
    return float(m.group(1))


@pytest.mark.parametrize("strategy", ACTIVE)
def test_published_strategies_declare_the_gate_explicitly(strategy):
    """This is the assertion that makes the divergent defaults unreachable."""
    cfg = _config(strategy)
    assert "min_conviction" in cfg, (
        f"{strategy}.yaml does not declare min_conviction, so the simulator "
        f"would gate at {_default_in(SIMULATOR)} and the live runner at "
        f"{_default_in(RUNNER)}. Declare it."
    )
    assert isinstance(cfg["min_conviction"], (int, float))


@pytest.mark.parametrize("strategy", ACTIVE)
def test_both_surfaces_resolve_the_same_gate(strategy):
    """A gate change is fine. A gate change that lands on one surface is not."""
    cfg = _config(strategy)
    sim = float(cfg.get("min_conviction", _default_in(SIMULATOR)))
    live = float(cfg.get("min_conviction", _default_in(RUNNER)))
    assert sim == live, (
        f"{strategy}: simulator gates at {sim}, live runner at {live}"
    )


def test_the_two_defaults_are_known_and_divergent():
    """Guards the premise. If someone reconciles the defaults this fails and
    should simply be deleted along with the rest of this file's hazard framing —
    but it must not be reconciled silently in one direction."""
    sim, live = _default_in(SIMULATOR), _default_in(RUNNER)
    assert (sim, live) == (1.5, 5.0), (
        f"the min_conviction defaults changed to sim={sim}, live={live}. If "
        "they were deliberately reconciled, delete this test. If one moved on "
        "its own, that is the bug this file exists to catch."
    )


def test_the_gate_is_not_one_global_number():
    """CLAUDE.md said 'min_conviction is 1.5'. reversal_dip is 3.0."""
    gates = {s: _config(s)["min_conviction"] for s in ACTIVE}
    assert len(set(gates.values())) > 1, (
        f"all books now share one gate ({gates}); copy naming a single figure "
        "may be simplified, but as a deliberate edit"
    )
    assert gates["reversal_dip"] != gates["quality_notrend"]

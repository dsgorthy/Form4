"""The published blended CAGR has ONE implementation.

`summary.blended_cagr` is the headline number on /portfolio and the quantity a
parameter sweep must score, or the sweep chooses a config for a number the site
does not publish. Those were two implementations until 2026-09-25 and the two
had already disagreed once: a scratch harness annualised over a 252-day trading
year while the API used 365.25, and the published annual returns were
re-litigated for a day because of it. The methodology doc's conclusion was
"Quote the API, not a harness" — this test is the structural version of that.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
API = REPO / "api" / "routers" / "portfolio.py"
SHARED = REPO / "framework" / "analysis" / "blended.py"


def test_the_shared_definition_exists_and_takes_a_table():
    src = SHARED.read_text()
    tree = ast.parse(src)
    fn = next((n for n in ast.walk(tree)
               if isinstance(n, ast.FunctionDef)
               and n.name == "blended_and_benchmark"), None)
    assert fn, "blended_and_benchmark is gone from framework/analysis/blended.py"
    args = [a.arg for a in fn.args.args]
    for expected in ("conn", "strategy", "starting", "years", "table"):
        assert expected in args, f"{expected} is no longer a parameter"
    assert "end" in args, (
        "the `end` parameter is gone; a walk-forward fold would then let idle "
        "cash compound in SPY to the present and annualise it over the fold"
    )


def test_the_api_delegates_rather_than_reimplementing():
    src = API.read_text()
    assert "from framework.analysis.blended import blended_and_benchmark" in src, (
        "portfolio.py no longer imports the shared definition"
    )
    # The give-away lines of the blend walk. If any reappear in the router, a
    # second implementation is back.
    for marker in ("idle *= 1 +", "peak, daily_dd =", "yr[y][1] = equity"):
        assert marker not in src, (
            f"portfolio.py contains {marker!r} again — the blended walk has been "
            "re-implemented in the router alongside the shared one"
        )


def test_the_sweep_scores_the_published_quantity():
    sweep = (REPO / "scripts" / "strategy_sweep.py").read_text()
    assert "from framework.analysis.blended import blended_and_benchmark" in sweep, (
        "strategy_sweep no longer scores with the published definition"
    )
    for key in ("blended_cagr_pct", "spy_cagr_pct", "excess_pct",
                "max_dd_daily_pct"):
        assert key in sweep, f"the sweep stopped reporting {key}"

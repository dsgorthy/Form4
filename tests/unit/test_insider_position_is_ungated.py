"""What an insider OWNS is not a paid feature.

Derek's call, 2026-08-24: holdings stay ungated. The reasoning is that
`shares_owned_after` is a number the insider themselves reported on the Form 4
-- it is not something this product computes, scores or ranks, so there is
nothing there for a paywall to be protecting. The same goes for the
twelve-month rollup: it is arithmetic over public filings.

The gate in `get_insider` is a deny-list -- it strips named fields from
`result` and sets `gated = True`. That shape is right, but it means a future
"strip everything except X" refactor could take these with it silently, and
nobody would notice because an anonymous visitor is exactly who nobody tests
as. Hence this file.

The second half pins the counting unit. TTM is counted in FILINGS, not
execution lots. A purchase filled in five tranches is one decision, and
counting it as five was what made every seller on this page look like they
were unloading in a flurry.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROUTER = REPO / "api/routers/insiders.py"
PAGE = REPO / "frontend/src/app/insider/[id]/page.tsx"


def _gate_body() -> str:
    """The `if not user.is_pro:` block inside get_insider."""
    tree = ast.parse(ROUTER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "get_insider")
    gate = next(n for n in ast.walk(fn)
                if isinstance(n, ast.If)
                and "not user.is_pro" in ast.unparse(n.test))
    return "\n".join(ast.unparse(s) for s in gate.body)


@pytest.mark.parametrize("field", ["holdings", "ttm"])
def test_the_anonymous_gate_does_not_strip_it(field):
    body = _gate_body()
    assert field not in body, (
        f"`{field}` is touched inside the not-is_pro branch, so anonymous "
        f"visitors no longer see it:\n{body}")


@pytest.mark.parametrize("field", ["holdings", "ttm"])
def test_it_is_set_before_the_gate_runs(field):
    """Order matters: set after the gate would work today and break the moment
    someone moves the gate to the end of the function."""
    src = ROUTER.read_text()
    fn_start = src.index("def get_insider(")
    body = src[fn_start:src.index("\n@router", fn_start)]
    assert f'result["{field}"]' in body, f"{field} is never set"
    assert body.index(f'result["{field}"]') < body.index("if not user.is_pro:"), (
        f"{field} is assigned after the gate")


def test_the_page_renders_position_outside_any_gate_check():
    """Ungated in the API is worthless if the page hides it anyway."""
    page = PAGE.read_text()
    start = page.index("{/* Position & Last 12 Months.")
    end = page.index("{/* Score */}", start)
    block = page[start:end]
    # Strip the comment that explains the decision -- it says the word
    # "gating", and matching that instead of the JSX is how this test would
    # pass against a block wrapped in `!isGated`.
    block = re.sub(r"/\*.*?\*/", "", block, flags=re.S)
    assert "isGated" not in block, (
        "the Position / Last 12 Months block is gated in the page")
    assert "profile.holdings" in block and "profile.ttm" in block


def test_ttm_counts_filings_not_execution_lots():
    src = ROUTER.read_text()
    i = src.index("# ── holdings and trailing-twelve-months")
    block = src[i:src.index("if not user.is_pro:", i)]
    ttm_sql = block[block.index("ttm"):]
    assert re.search(r"COUNT\(DISTINCT[^)]*filing_key", ttm_sql, re.I), (
        "TTM must count DISTINCT filings via filing_key -- counting rows "
        "counts execution tranches:\n" + ttm_sql[:600])


def test_ttm_counts_only_discretionary_classes():
    """Otherwise "Bought" reports shares the insider was HANDED.

    184k compensation grants and 221k option exercises carry
    trade_type='buy'. An unfiltered rollup turns a vesting event into a
    purchase on the most prominent number on the page -- which is the same
    mistake, in a different place, as the one that put twelve P&G executives
    on Stocktwits as discretionary sellers.
    """
    src = ROUTER.read_text()
    i = src.index("# ── holdings and trailing-twelve-months")
    ttm_sql = src[i:src.index("if not user.is_pro:", i)]
    ttm_sql = ttm_sql[ttm_sql.index("ttm = {}"):]
    for cls in ("discretionary_buy", "discretionary_sell"):
        assert cls in ttm_sql, (
            f"TTM does not restrict to {cls}; grants and exercises will be "
            f"counted as purchases:\n{ttm_sql[:700]}")

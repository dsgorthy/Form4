"""&ldquo;Meaningful&rdquo; has exactly one definition, and it is derived, not typed.

THE DEFINITION

A filing is meaningful when it records an actual decision to buy or sell on the
open market: signal_class IN ('discretionary_buy', 'discretionary_sell').
Everything else is mechanical or pre-committed — a 10b5-1 plan set up months
ago, a compensation grant, tax withheld on vesting, an option exercise, a gift.

It is 28.4% of filings. The default hides the other 71.6%.

WHY THIS FILE

The set was written out twice — api.filters.MEANINGFUL_CLASSES and
api.classification.DISCRETIONARY_CLASSES — as two literal tuples that happened
to be equal, with nothing keeping them so. That is the same "one concept, two
definitions" shape behind every defect found in August 2026: the -30% stop, the
lot-vs-filing grouping, the conviction gate defaults, the notifier reading a
different execution_source than the runner writes.

Both now derive from KIND_META, which already records per kind whether it is a
signal. These tests fail if either is typed out again, if they disagree, or if
the frontend mirror drifts.

DO NOT re-derive this from the boolean columns. is_tax_sale is set on 2,025
rows against 470,417 filings classified as tax withholding, and 184,121
compensation grants plus 220,692 option exercises are stored with
trade_type = 'buy'.
"""
from __future__ import annotations

import re
from pathlib import Path

from api.classification import DISCRETIONARY_CLASSES, KIND_META, _KIND_OF
from api.filters import ALL_CLASSES, MEANINGFUL_CLASSES, NON_MEANINGFUL_CLASSES

REPO = Path(__file__).resolve().parents[2]
MIRROR = REPO / "frontend/src/lib/classification.ts"


def test_the_definition_is_discretionary_buys_and_sells():
    assert set(MEANINGFUL_CLASSES) == {"discretionary_buy", "discretionary_sell"}


def test_both_names_resolve_to_the_same_set():
    assert tuple(MEANINGFUL_CLASSES) == tuple(DISCRETIONARY_CLASSES), (
        "api.filters.MEANINGFUL_CLASSES and "
        "api.classification.DISCRETIONARY_CLASSES disagree. They are the same "
        "concept and must come from the same place."
    )


def test_neither_is_a_typed_literal():
    """A literal tuple can drift; a derivation cannot."""
    for rel in ("api/filters.py", "api/classification.py"):
        src = (REPO / rel).read_text()
        for name in ("MEANINGFUL_CLASSES", "DISCRETIONARY_CLASSES"):
            m = re.search(rf"^{name} = (.+)$", src, flags=re.M)
            if not m:
                continue
            assert "discretionary_buy" not in m.group(1), (
                f"{rel}: {name} is a hand-written tuple again. Derive it from "
                "KIND_META so a vocabulary change propagates by construction."
            )


def test_exactly_one_kind_is_flagged_as_signal():
    flagged = [k for k, v in KIND_META.items() if v["signal"]]
    assert flagged == ["Discretionary"], (
        f"kinds flagged signal=True are {flagged}. Adding one silently widens "
        "the default for every alert and every browse surface — do it "
        "deliberately and update this test."
    )


def test_the_two_sets_partition_the_vocabulary():
    """No class may be in both, and none may be in neither — an unclassified
    class would silently vanish from both the default and the opt-out."""
    assert not set(MEANINGFUL_CLASSES) & set(NON_MEANINGFUL_CLASSES)
    known = {c for c in _KIND_OF}
    covered = set(ALL_CLASSES)
    assert known <= covered, f"signal_class values with no bucket: {known - covered}"


def test_the_frontend_mirror_agrees():
    src = MIRROR.read_text()
    block = src[src.index("export const DISCRETIONARY_CLASSES"):]
    block = block[:block.index("]")]
    mirrored = set(re.findall(r'"([a-z0-9_]+)"', block))
    assert mirrored == set(MEANINGFUL_CLASSES), (
        f"frontend mirror has {sorted(mirrored)}, backend has "
        f"{sorted(MEANINGFUL_CLASSES)}"
    )


# ── the default ─────────────────────────────────────────────────────────────


import pytest

from pipelines.notification_scanner import should_notify_watchlist


@pytest.mark.parametrize("signal_class,expected", [
    ("discretionary_buy", True),
    ("discretionary_sell", True),
    ("planned_sell", False),     # the 10b5-1 case Derek named
    ("planned_buy", False),
    ("compensation", False),
    ("tax_withholding", False),
    ("option_exercise", False),
    ("gift", False),
])
def test_the_default_delivers_only_meaningful_filings(signal_class, expected):
    assert should_notify_watchlist(signal_class, user_wants_all=False) is expected


@pytest.mark.parametrize("signal_class", [
    "discretionary_buy", "planned_sell", "compensation", "tax_withholding",
])
def test_opting_out_delivers_everything(signal_class):
    assert should_notify_watchlist(signal_class, user_wants_all=True) is True


@pytest.mark.parametrize("signal_class", [None, "", "   "])
def test_an_unclassifiable_filing_is_delivered(signal_class):
    """Fail open. One extra alert beats silently dropping a filing a user
    asked to see, which is the failure this whole rebuild exists to end.
    Currently costs nothing — zero filings in 60 days lack a class."""
    assert should_notify_watchlist(signal_class, user_wants_all=False) is True


def test_the_scanner_actually_calls_the_gate():
    """Behavioural tests on the helper prove nothing if the loop stops calling
    it. Checked on the AST, not by grepping for the name — an earlier version
    of this test passed while the call had been deleted, because the name
    survived in a comment and in an unrelated query.
    """
    import ast
    src = (REPO / "pipelines/notification_scanner.py").read_text()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "scan_watchlist_activity")
    calls = [n for n in ast.walk(fn)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "should_notify_watchlist"]
    assert calls, (
        "scan_watchlist_activity no longer calls should_notify_watchlist — "
        "every filing goes to every watcher, mechanical ones included"
    )


def test_the_default_does_not_read_the_dead_boolean_columns():
    """is_tax_sale covers 0.4% of tax withholdings and is NULL on 86% of the
    table. Anything gating 'meaningful' on it silently passes the rest."""
    import inspect
    src = inspect.getsource(should_notify_watchlist)
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    body = code.split('"""')[-1]
    assert "is_tax_sale" not in body and "is_10b5_1" not in body, (
        "the gate reads a boolean flag instead of signal_class"
    )

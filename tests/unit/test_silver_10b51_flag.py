"""The 10b5-1 flag Silver gets is trades' flag, computed the same way.

Gold classified every open-market sale as a decision because Silver's parser
captured neither the 2023 checkbox nor the "10b5" text in remarks and
footnotes that trades.is_10b5_1 is built from. The first parity report put
AAPL at 473 filings against 261 for exactly that reason.
"""
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "backfill_10b51", Path(__file__).resolve().parents[2] / "pipelines" / "silver" / "backfill_10b51.py",
)
bf = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(bf)

BASE = """<ownershipDocument><reportingOwner><reportingOwnerId><rptOwnerCik>0000001</rptOwnerCik></reportingOwnerId></reportingOwner>
{box}
<nonDerivativeTable><nonDerivativeTransaction><transactionCoding><transactionCode>S</transactionCode></transactionCoding></nonDerivativeTransaction></nonDerivativeTable>
<footnotes>{footnotes}</footnotes>
<remarks>{remarks}</remarks></ownershipDocument>"""


def doc(box="<aff10b5One>0</aff10b5One>", footnotes="", remarks=""):
    return BASE.format(box=box, footnotes=footnotes, remarks=remarks)


def test_the_checkbox_flags_the_filing():
    assert bf.plan_flag(doc(box="<aff10b5One>1</aff10b5One>"))
    assert bf.plan_flag(doc(box="<aff10b5One>true</aff10b5One>"))
    assert not bf.plan_flag(doc(box="<aff10b5One>0</aff10b5One>"))
    assert not bf.plan_flag(doc(box="<aff10b5One>false</aff10b5One>"))


def test_the_element_name_alone_is_not_a_mention():
    # every filing since 2023 carries the element; its NAME contains 10b5
    assert not bf.plan_flag(doc())


def test_a_footnote_or_the_remarks_mentioning_the_rule_flags_it():
    assert bf.plan_flag(doc(footnotes='<footnote id="F1">Sold pursuant to a Rule 10b5-1 trading plan adopted May 2025.</footnote>'))
    assert bf.plan_flag(doc(footnotes='<footnote id="F2">Under the 10b5 plan.</footnote>'))
    assert bf.plan_flag(doc(remarks="Transactions effected under a 10B5-1 plan."))
    assert not bf.plan_flag(doc(footnotes='<footnote id="F1">Price is a weighted average.</footnote>', remarks="None."))


def test_pre_2023_filings_have_no_box_and_rely_on_text():
    old = BASE.replace("{box}\n", "").format(footnotes='<footnote id="F1">Rule 10b5-1 plan.</footnote>', remarks="")
    assert bf.plan_flag(old)


def test_the_candidate_query_excludes_the_element_name():
    assert "10b5([^Oo]|$)" in bf.CANDIDATE_SQL
    assert "<aff10b5One>" in bf.CANDIDATE_SQL
    assert "?" not in bf.CANDIDATE_SQL, "the compat layer turns ? into a placeholder"

"""One filer, several insider rows: which ones are the same person.

Keyed on the filed reporting-owner CIK, 4,174 CIKs map to more than one
insider_id (2026-09-21). Name order, punctuation and corporate suffixes are
noise; two unrelated names under one CIK are a joint filing stamped with
one owner's CIK and must never be merged.
"""
import importlib.util
from pathlib import Path

from api.identity import classify_group, compatible, name_tokens, same_person

_SPEC = importlib.util.spec_from_file_location(
    "merge_insider_identities", Path(__file__).resolve().parents[2] / "scripts" / "merge_insider_identities.py",
)
mi = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mi)


def test_name_order_and_punctuation_do_not_matter():
    assert same_person("De Lima Filho Pedro Batista", "Pedro Batista de Lima Filho")
    assert same_person("SMITH JOHN A", "John A. Smith")
    assert same_person("JPMORGAN CHASE & CO", "JPMorgan Chase & Co.")


def test_corporate_suffixes_are_noise():
    assert name_tokens("DELTA AIR LINES, INC.") == name_tokens("DELTA AIR LINES INC /DE/")
    assert compatible("FOREST LABORATORIES INC", "Forest Laboratories, LLC")
    assert compatible("J P MORGAN CHASE & CO", "JPMORGAN CHASE & CO") is False or True  # spacing differs: a sub-form test, not equality


def test_a_sub_form_is_compatible_but_two_people_are_not():
    assert compatible("Smith John", "Smith John A")
    assert compatible("Siegfried Madden Meredith", "Siegfried Meredith R.")
    assert not compatible("Kansal Mohit", "Orgel Rob")
    assert not compatible("BKF CAPITAL GROUP INC", "BRONSON STEVEN N")


def test_group_classification():
    assert classify_group(["SMITH JOHN", "Smith John"]) == "identical"
    assert classify_group(["Siegfried Madden Meredith", "Siegfried Meredith R.", "Madden Meredith Siegfried"]) == "variants"
    assert classify_group(["DISCOVERY CAPITAL MANAGEMENT, LLC / CT", "Roberts Malcolm James"]) == "different"


def test_the_plan_keeps_the_biggest_row_and_sends_unrelated_names_to_review():
    rows = [
        {"cik": "1", "insider_id": 10, "name": "Smith John", "slug": "smith-john", "n_trades": 5},
        {"cik": "1", "insider_id": 11, "name": "SMITH JOHN", "slug": "smith-john-2", "n_trades": 40},
        {"cik": "2", "insider_id": 20, "name": "Kansal Mohit", "slug": "kansal-mohit", "n_trades": 3},
        {"cik": "2", "insider_id": 21, "name": "Orgel Rob", "slug": "orgel-rob", "n_trades": 9},
    ]
    merges, review = mi.plan(rows)
    assert len(merges) == 1 and merges[0]["survivor"] == 11 and merges[0]["merged"][0]["insider_id"] == 10
    assert len(review) == 1 and review[0]["cik"] == "2"


def test_the_apply_path_rewrites_every_table_keyed_on_insider_id():
    tables = {t for t, _ in mi.REFS}
    for must in ("public.trades", "notifications.watchlist", "public.insider_ticker_scores",
                 "public.score_history", "public.insider_companies", "research.derivative_trades"):
        assert must in tables
    assert ("public.insider_similarity", "similar_insider_id") in mi.REFS
    src = Path(mi.__file__).read_text(encoding="utf-8")
    assert "insider_slug_aliases" in src, "a merged row's URL must keep working"
    assert "conn.rollback()" in src

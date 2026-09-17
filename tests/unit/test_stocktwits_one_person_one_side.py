"""Two ranking defects from the 2026-09-16 schedule, fixed at the source.

AXIA3: one director filed under several insider_ids because the same name
was filed in different orders. He was posted as his own co-buyer and the
window cluster counted him as four people. USO: HRT Financial, a market
maker, sold $12M of the ETF on the 11th and "bought $16.6M" on the 14th,
and led a post as the first insider purchase on record.
"""
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "generate_stocktwits_posts",
    Path(__file__).resolve().parents[2] / "pipelines" / "generate_stocktwits_posts.py",
)
gen = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gen)


def test_the_same_name_in_a_different_order_is_the_same_person():
    assert gen.same_person("De Lima Filho Pedro Batista", "Pedro Batista de Lima Filho")
    assert gen.same_person("Smith, John A.", "John A. Smith")
    assert not gen.same_person("Milena Maria Pappa", "Raffaele Zagari")
    assert not gen.same_person("", "")
    assert not gen.same_person(None, "Anyone")


def test_distinct_people_collapses_name_order_variants():
    names = ["De Lima Filho Pedro Batista", "Pedro Batista de Lima Filho", "PEDRO BATISTA DE LIMA FILHO", "Raffaele Zagari"]
    assert gen.distinct_people(names) == 2


def test_a_filer_on_both_sides_is_dropped():
    rows = [
        {"ticker": "USO", "insider_id": 13459, "two_sided": True},
        {"ticker": "RWT", "insider_id": 1, "two_sided": False},
        {"ticker": "ADC", "insider_id": 2},
    ]
    assert [r["ticker"] for r in gen.drop_two_sided(rows)] == ["RWT", "ADC"]


def test_the_two_sided_query_counts_both_classes_per_filer():
    assert "HAVING COUNT(DISTINCT x.signal_class) = 2" in gen.CTX_TWO_SIDED
    assert gen.LOOKBACK_TWO_SIDED_DAYS == 5

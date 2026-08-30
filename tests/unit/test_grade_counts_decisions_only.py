"""A grade is a claim about decisions. Only decisions may enter it.

WHY THIS FILE

Until 2026-08-25 `pit_scoring._get_returns` filtered on `trade_type = 'buy'`
and nothing else. Every PIT and career grade this product had ever published
was therefore computed over a population that is **42.5% compensation grants,
39.3% option exercises and 18.0% actual purchases**. 76.5% of the 64,650
graded insiders were scored mostly on stock a board handed them on a date they
did not choose.

Randal Kirk (insider 279) was the case that exposed it: 75% accurate with
+27.2% alpha across his 44 real purchases, graded **D** off 102 filings of
which 57 were compensation grants. With the population corrected he grades B,
and G. Leonard Baker goes D to A.

`trade_type` cannot do this job — 184,121 compensation grants and 220,692
option exercises are stored with `trade_type = 'buy'`. `signal_class` can.

These tests fail if the class filter is dropped, typed out instead of derived,
or if the hygiene predicates every other reader of `trades` applies go missing
from the one query that decides who gets an A.
"""
from __future__ import annotations

from pathlib import Path

from api.filters import MEANINGFUL_CLASSES
from strategies.insider_catalog.pit_scoring import MEANINGFUL_BUY_CLASSES

SCORER = Path(__file__).resolve().parents[2] / "strategies/insider_catalog/pit_scoring.py"


def _returns_query() -> str:
    """The `_get_returns` body, comments stripped."""
    src = SCORER.read_text()
    start = src.index("cls_ph = ", src.index("def compute_insider_ticker_score"))
    end = src.index("return [(r[0], r[1])", start)
    return "\n".join(line.split("--")[0] for line in src[start:end].splitlines())


def test_the_buy_classes_are_derived_not_typed():
    assert MEANINGFUL_BUY_CLASSES, "no buy class resolved"
    assert set(MEANINGFUL_BUY_CLASSES) <= set(MEANINGFUL_CLASSES)
    assert all(c.endswith("_buy") for c in MEANINGFUL_BUY_CLASSES)
    sql = _returns_query()
    for cls in MEANINGFUL_CLASSES:
        assert cls not in sql, f"{cls!r} is typed into the scorer; derive it instead"


def test_the_scorer_filters_on_signal_class():
    sql = _returns_query()
    assert "t.signal_class IN" in sql, (
        "the scorer no longer filters signal_class — it is grading compensation "
        "grants and option exercises as though they were decisions"
    )


def test_trade_type_alone_never_gates_the_population():
    """`trade_type='buy'` is not evidence that anyone bought anything."""
    sql = _returns_query()
    if "t.trade_type" in sql:
        assert "t.signal_class IN" in sql, (
            "trade_type is back as the only population filter; 184k grants and "
            "221k option exercises carry trade_type='buy'"
        )


def test_the_hygiene_predicates_are_present():
    sql = _returns_query()
    for clause in ("t.superseded_by IS NULL",
                   "t.is_derivative = 0",
                   "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)"):
        assert clause in sql, f"scorer is missing {clause!r}"


def test_the_pit_guards_survive():
    """Both PIT constraints must stay: known AND observable."""
    sql = _returns_query()
    assert "t.trade_date <= ?" in sql, "observability lag guard is gone"
    # STRICT. This asserted "t.filing_date <= ?" until 2026-08-30, when the
    # non-strict form turned out to admit the trade being graded into its own
    # track record: the score is stamped as_of that trade's filing_date, so the
    # comparison held with EQUALITY. A trade's own 90d abnormal return, by the
    # grade it received, was 36.59% for A+/A/B against -6.94% for C/D on
    # late-filed rows — a 43.53pp gap where clean rows show -0.96pp.
    #
    # `<` is strictly stronger than `<=`, so the knowledge guard this test was
    # written to protect is intact; it just no longer lets a trade grade itself.
    assert "t.filing_date < ?" in sql, "knowledge guard is gone — late filings would leak"
    assert "t.filing_date <= ?" not in sql, (
        "the non-strict knowledge guard is back; a trade filed long after "
        "execution will grade itself again"
    )
    assert "GROUP BY t.ticker" in sql, "observations must be one row per filing"


def test_bound_parameter_order_matches_the_sql():
    """The IN list is bound before the two dates; params must follow suit."""
    src = SCORER.read_text()
    start = src.index("cls_ph = ", src.index("def compute_insider_ticker_score"))
    body = src[start:src.index("return [(r[0], r[1])", start)]
    assert "params = [insider_id_val, *MEANINGFUL_BUY_CLASSES, cutoff, as_of_date]" in body, (
        "parameter order no longer matches the placeholder order in the SQL"
    )

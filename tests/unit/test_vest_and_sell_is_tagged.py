"""Selling shares you were handed last week must be labelled as such.

WHAT WENT WRONG

On 2026-08-24 twelve Procter & Gamble executives filed sales dated 08-20. Every
one of them had an Award (trans_code A) on 08-19. That is a vesting cycle, not
twelve people deciding to reduce exposure to P&G — and signal_class called all
twelve `discretionary_sell`, because the classifier reads a row in isolation
and cannot see the grant that landed the day before.

The product ALREADY has a label for this: the `post_vest_dump` tag. It fired on
none of them. The detector had been raising on every run since the Postgres
migration, because its window was written in SQLite:

    date(a.trade_date, '+30 days')

Postgres has no two-argument date(), and the compat layer only rewrites the
`date(?, ...)` form with a literal placeholder — this passes a column. main()
catches per-detector exceptions and logs them, so the job kept exiting 0 while
the tag silently stopped being written. `exercise_and_sell` carried the same
defect with a 3-day window.

Those twelve filings reached a public Stocktwits post as discretionary selling,
tagged `opportunistic_trade`, which is close to the opposite of the truth.

SCALE: 23.1% of all discretionary sells in 180 days (5,876 of 25,490) follow an
A or M by the same insider on the same ticker within five days.

THE DESIGN. This is a TAG, not a filter and not a signal_class change. In SEC
terms it genuinely is a discretionary sale; the tag adds the context. Tags are
1-to-many and never rate, so a filing can be both `post_vest_dump` and whatever
else is true of it. Whether any given surface filters on it is that surface's
decision — but every surface must be able to SEE it, and they must all mean the
same thing by it.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DETECTORS = REPO / "pipelines/insider_study/compute_signals.py"
STOCKTWITS = REPO / "pipelines/generate_stocktwits_posts.py"
SCANNER = REPO / "pipelines/notification_scanner.py"


def _watchlist_query() -> str:
    """The SQL inside scan_watchlist_activity.

    Sliced to the whole function body -- an earlier version cut at the
    first `_set_watermark`, which appears in the empty-subscriber early
    return ABOVE the query, so the test inspected nothing and raised.
    """
    src = SCANNER.read_text()
    start = src.index("def scan_watchlist_activity")
    end = src.index("\ndef ", start + 1)
    body = src[start:end]
    q = body[body.index("SELECT MIN(t.trade_id)"):]
    return q[:q.index('"""')]


def test_the_tag_is_in_the_published_vocabulary():
    from api.ratings import TAG_KINDS
    assert TAG_KINDS.get("post_vest_dump") == "pattern"
    assert TAG_KINDS.get("exercise_and_sell") == "pattern"


def _detector_sql(name: str) -> str:
    """The SQL a detector actually executes — docstring and comments removed.

    Stripping by line prefix is not enough: the docstrings here QUOTE the
    broken SQLite expression in order to explain it, and a naive scan flags the
    explanation as the defect. Parse it instead.
    """
    import ast
    tree = ast.parse(DETECTORS.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == name)
    body = fn.body[1:] if (fn.body and isinstance(fn.body[0], ast.Expr)
                           and isinstance(fn.body[0].value, ast.Constant)) else fn.body
    sql = []
    for node in body:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                sql.append(sub.value)
            elif isinstance(sub, ast.JoinedStr):
                sql.append(ast.unparse(sub))
    text = "\n".join(sql)
    # Drop SQL comments too — the fix carries its own explanation inline.
    text = re.sub(r"--[^\n]*", "", text)
    return text


@pytest.mark.parametrize("detector", ["post_vest_dump", "exercise_and_sell"])
def test_no_detector_uses_sqlite_date_arithmetic(detector):
    """`date(column, '+N days')` reaches Postgres untranslated and raises.
    The compat layer only rewrites the `date(?, ...)` placeholder form."""
    bad = re.findall(r"date\(\s*[a-z_]+\.[a-z_]+\s*,", _detector_sql(detector))
    assert not bad, (
        f"{detector} uses SQLite two-argument date() on a column: {bad}. "
        "Postgres raises, main() swallows it, and the tag silently stops "
        "being written while the job still exits 0."
    )


@pytest.mark.parametrize("detector", ["post_vest_dump", "exercise_and_sell"])
def test_the_window_is_expressed_in_postgres(detector):
    src = DETECTORS.read_text()
    body = src[src.index(f"def {detector}("):]
    body = body[:body.index("logger.info")]
    assert "INTERVAL" in body.upper(), (
        f"{detector} no longer expresses its window as a Postgres INTERVAL"
    )


def test_the_stocktwits_generator_reads_the_tag_not_its_own_rule():
    """One definition. A post and a filing page must not disagree about
    whether the same trade was a vest-and-sell."""
    src = STOCKTWITS.read_text()
    assert "post_vest_dump" in src, (
        "the generator no longer consults the vest tag"
    )
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("--"))
    assert "trans_code IN ('A', 'M')" not in code, (
        "the generator re-derives the vest window inline instead of reading "
        "the tag — two definitions of one concept, which is how they drift"
    )


# ---------------------------------------------------------------------------
# The tag has to reach the alert gate, or tagging it changed nothing.
# ---------------------------------------------------------------------------

import pipelines.notification_scanner as NS


@pytest.mark.parametrize("tag", NS.MECHANICAL_TAGS)
def test_a_mechanically_tagged_sell_does_not_alert_by_default(tag):
    """signal_class says discretionary. The tag says otherwise, and wins.

    Twelve P&G executives were granted shares on 2026-08-19 and sold them on
    08-20. Every one was coded discretionary_sell, because the classifier reads
    a row in isolation and the grant is a different row.
    """
    assert NS.should_notify_watchlist("discretionary_sell", False) is True, (
        "premise: an untagged discretionary sell must still alert, or this "
        "test would pass for the wrong reason")
    assert NS.should_notify_watchlist("discretionary_sell", False, (tag,)) is False


@pytest.mark.parametrize("tag", NS.MECHANICAL_TAGS)
def test_unfiltering_still_overrides_the_tag(tag):
    """"Show me everything" means everything, including the mechanical."""
    assert NS.should_notify_watchlist("discretionary_sell", True, (tag,)) is True


def test_an_unrelated_tag_does_not_suppress():
    assert NS.should_notify_watchlist(
        "discretionary_sell", False, ("cluster_buy", "largest_ever")) is True


def test_empty_and_missing_tags_are_the_same_as_none():
    for tags in (None, (), [], ("",)):
        assert NS.should_notify_watchlist("discretionary_sell", False, tags) is True


def test_the_scanner_scopes_tags_to_the_filing_not_one_lot():
    """The tag is written per LOT. A filing is a decision.

    An award-then-sell filed in three tranches may carry the tag on only one
    of them, so reading MIN(trade_id) alone silently un-suppresses it. The
    first version of this query did exactly that.
    """
    q = _watchlist_query()
    sub = q[q.index("trade_signals"):]
    sub = sub[:sub.index("AS tags")]
    for col in ("insider_id", "ticker", "trade_type", "trade_date"):
        assert f"t2.{col}" in sub and f"t.{col}" in sub, (
            f"tags subquery must correlate on {col} so it covers the whole "
            f"filing group; got:\n{sub}")
    assert "MIN(t.trade_id)" not in sub, (
        "tags must not be read from a single lot of a multi-lot filing")


def test_the_scanner_does_not_join_trade_signals_into_the_main_query():
    """A direct join would double SUM(t.value).

    trade_signals is unique on (trade_id, signal_type), not trade_id -- 21
    filing groups in the last three months carry BOTH mechanical tags, and
    each would contribute its value twice.
    """
    q = _watchlist_query()
    main = q[q.index("FROM trades t"):q.index("GROUP BY")]
    assert "trade_signals" not in main, (
        "trade_signals joined into the grouped query would fan out and "
        "inflate total_value")

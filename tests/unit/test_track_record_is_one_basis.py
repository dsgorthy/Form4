"""The Buy/Sell Track Record block has ONE basis, and it is not a stored column.

THE DEFINITION

Every figure in that block -- 7d, 30d and 90d, buy side and sell side -- is
computed the same way:

  * one row per FILING, never per execution lot;
  * discretionary filings only, derived from MEANINGFUL_CLASSES;
  * the same duplicate / derivative / superseded exclusions as the filing
    counts rendered directly above it;
  * suppressed entirely below MIN_SCORED_FILINGS.

WHY THIS FILE

Until 2026-08-25 the block took 7d from a filing-grouped API query and 30d/90d
straight from `insider_track_records`, which counts lots. One table row
therefore carried two denominators: Romano Gianluca (insider 27782) rendered
"Filings 19" beside an accuracy computed over 154 lots, and 140 of those lots
were 10b5-1 plan sales -- mechanical executions scored as timing decisions.
His published record was 31% accuracy and +6.6% average move; on the corrected
basis it is 50% over 6 discretionary filings, and the move flips to -0.6%.

Across 31,165 comparable insiders the aggregate barely moves (-0.26pt) but
22.9% flip sign, because the error is per-page, not systematic.

The stored columns are retired (migrations/2026-08-25_retire_track_record_win_
rates.sql). Nothing had refreshed them since February 2026 in any case: the
daily writer `pit_scoring.sync_to_track_records` writes score, counts and
dates and never the win rates.

These tests fail if the class filter is typed out again, if the query stops
grouping by filing, if the floor goes away, or if the page starts reading the
retired columns.
"""
from __future__ import annotations

import re
from pathlib import Path

from api.filters import MEANINGFUL_CLASSES
from api.routers.insiders import MIN_SCORED_FILINGS, apply_scoring_floor

REPO = Path(__file__).resolve().parents[2]
ROUTER = REPO / "api/routers/insiders.py"
PAGE = REPO / "frontend/src/app/insider/[id]/page.tsx"

WINDOWS = ("7", "30", "90")


def _win_rate_query() -> str:
    """The filing_win_rates SQL and its bound parameters, isolated.

    Starts at the class binding, not the execute() call -- the binding is
    the part these tests care most about.
    """
    src = ROUTER.read_text()
    start = src.index("_cls = tuple(MEANINGFUL_CLASSES)")
    end = src.index(".fetchall()", start)
    return src[start:end]


def _sql_only() -> str:
    """The same slice with `--` comments stripped.

    The comments explain the defects being fixed and necessarily quote the
    wrong code; assertions about what the query DOES must not read them.
    """
    return "\n".join(line.split("--")[0] for line in _win_rate_query().splitlines())


def test_the_class_filter_is_derived_not_typed():
    """The query must bind MEANINGFUL_CLASSES, not spell the classes out.

    A literal here is how the two definitions of "meaningful" drifted apart
    the first time.
    """
    assert "MEANINGFUL_CLASSES" in _win_rate_query(), \
        "win-rate query must derive its class filter"
    sql = _sql_only()
    for cls in MEANINGFUL_CLASSES:
        assert cls not in sql, f"{cls!r} is typed out in the win-rate query; bind it instead"


def test_every_window_is_grouped_by_filing():
    """One row per filing, for all three windows, or the denominators diverge."""
    sql = _sql_only()
    # Grouped on ticker too, so an accession covering two issuers stays two
    # observations rather than being averaged into one.
    assert "GROUP BY t.trade_type, t.ticker," in sql
    assert "COALESCE(t.filing_key, t.accession, t.trade_date)" in sql
    for w in WINDOWS:
        assert f"return_{w}d" in sql, f"{w}d is missing from the filing-grouped query"


def test_all_three_windows_share_the_exclusions_of_the_filing_count():
    """The header count and the denominator must describe the same population."""
    sql = _sql_only()
    for clause in (
        "t.superseded_by IS NULL",
        "t.is_derivative = 0",
        "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)",
    ):
        assert clause in sql, f"win-rate query is missing {clause!r}"


def test_a_sell_wins_when_the_stock_falls():
    """Direction is per trade_type, and counted explicitly at every window.

    The old code derived sell wins as ``total - wins`` where wins was
    ``ret > 0``, which scored a flat 0.00% return as a good sale.
    """
    sql = _sql_only()
    for w in WINDOWS:
        assert re.search(rf"trade_type = 'sell'\s+AND ret{w}\s*< 0", sql), \
            f"sell direction missing for the {w}d window"
        assert re.search(rf"trade_type = 'buy'\s+AND ret{w}\s*> 0", sql), \
            f"buy direction missing for the {w}d window"
    assert "total - " not in sql, "sell wins must be counted, not subtracted"


def test_the_page_never_reads_a_retired_column():
    """The insider page renders filing_stats and nothing else."""
    page = PAGE.read_text()
    stale = re.findall(r"tr\.(?:buy|sell)_(?:win_rate|avg_return|avg_abnormal|median_return)_\d+d", page)
    assert not stale, f"insider page still reads retired track-record columns: {sorted(set(stale))}"


def test_the_denominator_is_always_published():
    """Every window publishes the count it was computed on, floor or no floor.

    An invisible denominator is the whole defect this block was fixed for.
    """
    src = ROUTER.read_text()
    assert 'filing_stats[f"{tt}_scored_filings_{window}"] = n' in src
    page = PAGE.read_text()
    for side in ("buy", "sell"):
        assert f"{side}_scored_filings_7d" in page, f"{side} side does not render its basis"


def test_the_floor_suppresses_a_thin_basis():
    assert MIN_SCORED_FILINGS >= 3, "a percentage over one or two filings is an artifact"

    # Below the floor: nothing is published but the count.
    for n in range(MIN_SCORED_FILINGS):
        assert apply_scoring_floor(n, n, 0.5, 0.5) == (None, None, None), \
            f"n={n} cleared a floor of {MIN_SCORED_FILINGS}"

    # At and above it: the real figures.
    rate, ret, abn = apply_scoring_floor(MIN_SCORED_FILINGS, MIN_SCORED_FILINGS, 0.25, 0.125)
    assert rate == 1.0 and ret == 0.25 and abn == 0.125

    # A missing average is still None; a zero win count is a real 0.0, not None.
    assert apply_scoring_floor(10, 0, None, None) == (0.0, None, None)


def test_the_floor_is_never_bypassed_by_the_loop():
    """The router applies the floor through the helper, not inline."""
    src = ROUTER.read_text()
    assert "apply_scoring_floor(" in src
    body = src[src.index("filing_stats = {}"):src.index('result["filing_stats"] = filing_stats')]
    assert f"/ n" not in body, "win rate computed inline; it must go through the floor helper"


def test_a_sell_must_earn_its_score_but_a_buy_need_not():
    """Buys are scored as they come; sells must carry a signal.

    Measured over 291,033 discretionary sell filings the median abnormal 30d
    is -0.58% against a -0.58% baseline -- exactly nothing. Only two
    conditions separate a sale that predicts: it is the first sale after
    buying this ticker, or the seller is an entity/fund or 10% owner. Both
    together give -2.83%.
    """
    sql = _sql_only()
    assert "trade_type = 'buy'" in sql, "buys must stay unconditionally scoreable"
    assert "prior_sells = 0 AND prior_buys >= 1" in sql, "first-sell-after-buying flag is gone"
    assert "OR speculative" in sql, "speculative-holder flag is gone"
    # The sequence must look only backwards, or the flag peeks at the future.
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING" in sql


def test_the_rejected_sell_hypotheses_stay_rejected():
    """Three plausible signals were measured and found empty. Keep them out.

    - fraction of the stake sold: flat at every slice, -0.54% to -0.75%, and
      a >90% near-total exit is -0.57%. Selling everything says nothing.
    - sale size vs the insider's own history: non-monotonic across z-buckets.
    - gap regularity between sales: flat.

    Each is intuitive enough to be re-added by someone reasoning from first
    principles, which is why this test names them.
    """
    sql = _sql_only()
    for banned, why in [
        ("shares_owned_after", "fraction-of-stake sold was measured and is flat"),
        ("stddev", "size-vs-own-history was measured and is non-monotonic"),
        ("LAG(", "gap regularity was measured and is flat"),
    ]:
        assert banned not in sql, f"{banned} is back in the win-rate query; {why}"


def test_percent_patterns_are_bound_not_inlined():
    """A literal % would be eaten by the ? -> %s compat layer."""
    sql = _sql_only()
    assert "ILIKE ?" in sql, "title patterns must be bound parameters"
    assert "'%" not in sql, "a literal % pattern reached the SQL; bind it instead"

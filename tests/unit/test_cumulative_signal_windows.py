"""A signal computed from an insider's PRIOR TRADES may not be windowed.

THE CLASS OF BUG

`compute_cw_indicators.py` has one module-level `MIN_DATE` doing two
incompatible jobs: it bounds which rows get WRITTEN (cheap incremental
refresh, correct) and it bounds which rows get LOADED for comparison
(catastrophic for anything cumulative). `fetch_latest` runs the script every
five minutes as `--since <7 days ago>`, and `--since` overrides MIN_DATE, so
the comparison history collapses to a week.

It has now bitten three times, in three sibling functions in one file:

  compute_consecutive_sells      found first. `consecutive_sells_before` max
                                 fell from 176 to 4 and silenced reversal_dip
                                 for eight weeks. Fixed in place.
  compute_purchase_size_metrics  each insider's first purchase inside the
                                 seven-day window was crowned their career
                                 largest. 23.7% of is_largest_ever flags were
                                 wrong; correcting them moved the three books
                                 by +$79,930.
  compute_recurring_purchase     worst of the three. Detecting a cadence needs
                                 3+ instances, which a week cannot hold, and
                                 the loop writes 0 below that threshold — so
                                 the incremental run could never SET the flag
                                 and actively ERASED it. 42 survivors across
                                 66,711 trades filed this year.

The first fix was applied pointwise. Its two siblings, in the same file, with
the same shape, were left broken for months. That is the actual failure: a bug
was treated as an incident rather than as a class.

THE RULE

  Derived from the trade's own row, or from the full price series
      -> windowing is fine. dip_*, above_sma*, is_tax_sale, week52_proximity
         all load prices in full and restrict only which trades they write.

  Derived from the insider's prior trades
      -> the load MUST be unwindowed; only the WRITE may be filtered.

This test enforces the second half by reading the source, because the failure
is invisible at runtime: the pipeline completes, reports success, and writes
confident wrong answers.
"""
import re
from pathlib import Path

import pytest

SRC = (Path(__file__).resolve().parents[2]
       / "pipelines/insider_study/compute_cw_indicators.py")

#: Functions whose output depends on an insider's prior trades.
CUMULATIVE = [
    "compute_purchase_size_metrics",
    "compute_consecutive_sells",
    "compute_recurring_purchase",
]

#: Functions that derive from prices or the row alone. Windowing is correct.
WINDOWABLE = [
    "compute_dip_indicators",
    "compute_sma_context",
    "compute_tax_sale_flag",
]


def _body(name: str) -> str:
    src = SRC.read_text()
    start = src.index(f"def {name}(")
    nxt = src.find("\ndef ", start + 1)
    return src[start: nxt if nxt != -1 else len(src)]


def _load_queries(body: str) -> list[str]:
    """The conn.execute(...) blocks that pull trade rows."""
    return [m.group(0) for m in re.finditer(
        r"conn\.execute\(\s*\"\"\".*?\"\"\"(?:\s*,\s*\([^)]*\))?\s*\)", body, re.S)
        if "FROM trades" in m.group(0)]


@pytest.mark.parametrize("fn", CUMULATIVE)
def test_cumulative_signals_load_full_history(fn):
    body = _body(fn)
    for q in _load_queries(body):
        assert "trade_date >= ?" not in q and "MIN_DATE" not in q, (
            f"{fn} bounds its comparison history by MIN_DATE. Under "
            f"`--since 7d` — which fetch_latest uses every five minutes — that "
            f"makes a week look like the insider's whole career. Load the full "
            f"history and filter the WRITE instead."
        )


@pytest.mark.parametrize("fn", CUMULATIVE)
def test_cumulative_signals_still_filter_their_writes(fn):
    """The other half. Loading everything and writing everything would make the
    five-minute job rewrite a decade on every run."""
    body = _body(fn)
    assert re.search(r"(trade_date|_td|td)\s*[<>]=?\s*MIN_DATE", body), (
        f"{fn} loads full history but does not restrict which rows it writes"
    )


@pytest.mark.parametrize("fn", WINDOWABLE)
def test_price_derived_signals_may_stay_windowed(fn):
    """Documents the distinction so nobody 'fixes' these into full scans.
    They load the price series in full; the trade filter is a write window."""
    body = _body(fn)
    assert "MIN_DATE" in body, f"{fn} unexpectedly stopped windowing"


def test_min_date_is_still_overridable_by_since():
    """If --since stopped overriding MIN_DATE the tests above would pass
    vacuously, because the incremental path would no longer narrow anything."""
    src = SRC.read_text()
    assert "MIN_DATE = args.since" in src


def test_no_new_cumulative_function_escapes_this_list():
    """A grouping by (insider_id, ticker) is the signature of a cumulative
    signal. Any new one must be classified here deliberately."""
    src = SRC.read_text()
    grouped = {m for m in re.findall(r"def (compute_\w+)\(", src)
               if "groups[(insider_id, ticker" in _body(m)
               or "groups[(insider_id, ticker, trans_code" in _body(m)}
    unclassified = grouped - set(CUMULATIVE)
    assert not unclassified, (
        f"{sorted(unclassified)} group by insider history but are not listed "
        f"in CUMULATIVE — classify them and check their load query"
    )

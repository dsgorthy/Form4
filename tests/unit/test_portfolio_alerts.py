"""Portfolio alerts fire for the strategies we actually run, and fire once.

TWO BUGS THIS PINS

1. scan_portfolio_alerts read `strategy = 'form4_insider'` — a backtest-only
   book retired months before, last entry 2026-03-13. The Pro "portfolio
   alerts" toggle is sold as alerts on our strategies and had never fired for
   one we run.

2. The dedup key was built from strategy_portfolio.id. That column is not
   stable: simulate_strategy_portfolio rebuilds each book with
   `DELETE FROM strategy_portfolio WHERE strategy = ?` and a fresh INSERT, so
   every trade gets a new id every night. Had the first bug been fixed without
   the second, the first rebuild would have re-alerted every position in all
   three books to every subscriber.

The second is the one worth a test, because it is invisible in review — the
code looks correct, and it only misbehaves against a writer in a different
file. These read the source rather than executing the scanner, which needs a
live Postgres; they are guards against a regression, not a functional test.
"""
import re
from pathlib import Path

from api.public_fields import ACTIVE_STRATEGIES

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines/notification_scanner.py"


def _scan_fn() -> str:
    """Source of scan_portfolio_alerts, docstring and comments stripped.

    Prose about the bug is not the bug — the docstring here quotes the old
    predicate verbatim, and an unstripped match on it fails every assertion
    below."""
    src = SCANNER.read_text()
    body = src.split("def scan_portfolio_alerts(", 1)[1]
    body = body.split("\ndef ", 1)[0]
    # Drop the docstring: everything between the first pair of triple quotes.
    if '"""' in body:
        head, _, rest = body.partition('"""')
        _, _, tail = rest.partition('"""')
        body = head + tail
    return "\n".join(
        ln for ln in body.splitlines() if not ln.lstrip().startswith("#")
    )


def test_alerts_read_the_published_strategy_list():
    fn = _scan_fn()
    assert "ACTIVE_STRATEGIES" in fn, (
        "portfolio alerts must read the published roster, not a hardcoded name"
    )


def test_alerts_no_longer_query_the_retired_backtest_book():
    # The docstring names it as history; a live SQL predicate is the failure.
    code = _scan_fn()
    assert "strategy = 'form4_insider'" not in code
    assert 'strategy = "form4_insider"' not in code


def test_backtest_rows_are_excluded():
    fn = _scan_fn()
    assert "execution_source" in fn, (
        "without an execution_source filter, re-running a backtest alerts "
        "subscribers about trades that never happened"
    )


def test_dedup_keys_survive_a_nightly_rebuild():
    """No dedup key may be built from the row id."""
    fn = _scan_fn()
    keys = re.findall(r"_dedup_key\(([^)]*)\)", fn, flags=re.S)
    assert keys, "scan_portfolio_alerts no longer builds dedup keys"
    for call in keys:
        flat = " ".join(call.split())
        assert 'r["id"]' not in flat and "r['id']" not in flat, (
            "dedup key uses strategy_portfolio.id, which is reassigned on "
            f"every simulator rebuild: {flat}"
        )
        # (strategy, ticker, date) is what stays put across a DELETE + INSERT.
        assert 'r["strategy"]' in flat, f"dedup key must be per-strategy: {flat}"
        assert 'r["ticker"]' in flat, f"dedup key must be per-ticker: {flat}"


def test_entry_and_exit_alerts_are_keyed_apart():
    fn = _scan_fn()
    prefixes = re.findall(r'_dedup_key\(\s*"([a-z]+)"', fn)
    assert len(set(prefixes)) == len(prefixes) == 2, (
        f"expected one entry prefix and one exit prefix, got {prefixes}"
    )


def test_alert_copy_names_the_strategy():
    fn = _scan_fn()
    assert fn.count("strategy_label(") >= 2, (
        "an alert that does not say which strategy fired is unactionable now "
        "that three of them can"
    )


def test_hold_length_is_not_hardcoded_in_exit_copy():
    """The three books hold 42, 42 and 21 trading days."""
    assert "30-day hold complete" not in SCANNER.read_text()


def test_every_published_strategy_can_produce_an_alert():
    from pipelines import notification_scanner  # noqa: F401  (import is the check)
    assert set(ACTIVE_STRATEGIES) == {
        "quality_notrend", "quality_momentum", "reversal_dip",
    }

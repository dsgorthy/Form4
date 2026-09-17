"""A new account follows the book it chose, so the free alert has somewhere
to come from.

Every signup since August landed on /portfolio, picked a book in onboarding,
and then heard nothing: the books' own alerts are Pro, and the account
followed nobody. Onboarding now follows the book's open positions (newest
first, five at most) for an account that follows nothing, and the welcome
email shows a real alert instead of describing one.
"""
import importlib.util
from pathlib import Path

from api import email_templates as t

_SPEC = importlib.util.spec_from_file_location(
    "onboarding", Path(__file__).resolve().parents[2] / "api" / "routers" / "onboarding.py",
)
try:
    ob = importlib.util.module_from_spec(_SPEC); _SPEC.loader.exec_module(ob)
except ModuleNotFoundError as e:   # fastapi is not on the Mini's bare python
    import pytest
    pytest.skip(f"router import needs {e.name}", allow_module_level=True)


def test_open_positions_newest_first_deduplicated():
    rows = [
        {"ticker": "PRTS", "status": "open", "entry_date": "2026-09-11"},
        {"ticker": "PRTS", "status": "open", "entry_date": "2026-09-10"},   # same position, other source
        {"ticker": "FGBI", "status": "open", "entry_date": "2026-09-10"},
        {"ticker": "AMR", "status": "open", "entry_date": "2026-09-09"},
        {"ticker": "OLD", "status": "closed", "entry_date": "2026-01-01"},
    ]
    assert ob.pick_seed_tickers(rows) == ["PRTS", "FGBI", "AMR"]


def test_a_book_holding_nothing_seeds_its_last_three_entries():
    rows = [{"ticker": f"T{i}", "status": "closed", "entry_date": f"2026-0{i}-01"} for i in range(1, 6)]
    assert ob.pick_seed_tickers(rows) == ["T5", "T4", "T3"]


def test_the_seed_stays_under_the_free_cap():
    rows = [{"ticker": f"O{i}", "status": "open", "entry_date": f"2026-09-{i:02d}"} for i in range(1, 9)]
    assert len(ob.pick_seed_tickers(rows)) == ob.SEED_FOLLOWS == 5
    assert ob.SEED_FOLLOWS < 10


def test_the_endpoint_seeds_and_reports_what_it_followed():
    src = Path(ob.__file__).read_text(encoding="utf-8")
    assert "seed_follows(conn, iconn, user.user_id, strategy)" in src
    assert '"followed": followed' in src


def test_welcome_shows_a_real_alert_of_either_kind():
    _, html = t.welcome_email(["PRTS"], "Insider Breakout", example={
        "kind": "filing", "ticker": "PRTS", "insider_name": "Jane Doe", "trade_type": "buy",
        "value": 250000, "filing_date": "2026-09-16"})
    assert "What an alert looks like" in html and "Jane Doe bought $250,000" in html
    _, html = t.welcome_email([], "A-List Buys", example={
        "kind": "book", "label": "A-List Buys", "ticker": "FOSL", "entry_date": "2026-08-24", "entry_price": 3.21})
    assert "A-List Buys bought" in html and "FOSL" in html and "after 24 hours" in html
    _, html = t.welcome_email([], None)
    assert "What an alert looks like" not in html

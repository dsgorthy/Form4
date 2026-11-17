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


# --- "Skip for now" (2026-11-16) ---------------------------------------------
#
# A skip never reached the endpoint: no profile row, no alert settings row, no
# follows. notification_scanner selects recipients FROM notification_preferences,
# so a skipped account could follow nothing and be told nothing. Every signup
# tracked since October skipped, including the one on 2026-11-16 who had looked
# at pricing twice.

from contextlib import contextmanager

FORM = Path(__file__).resolve().parents[2] / "frontend/src/app/onboarding/onboarding-form.tsx"


class _Rows:
    def __init__(self, one=None, many=()):
        self._one, self._many = one, list(many)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _Notifs:
    def __init__(self, following=0):
        self.following, self.calls = following, []

    def execute(self, sql, params=()):
        flat = " ".join(sql.split())
        self.calls.append((flat, tuple(params)))
        if flat.startswith("SELECT COUNT(*) AS n FROM watchlist"):
            return _Rows(one={"n": self.following})
        return _Rows()

    def commit(self):
        pass

    def inserts(self, table):
        return [p for s, p in self.calls if s.startswith(f"INSERT INTO {table}") or
                s.startswith(f"INSERT OR IGNORE INTO {table}")]


class _Book:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=()):
        return _Rows(many=self.rows)


BREAKOUT = [{"ticker": "EAF", "status": "open", "entry_date": "2026-11-10"},
            {"ticker": "GME", "status": "open", "entry_date": "2026-10-02"},
            {"ticker": "FGBI", "status": "closed", "entry_date": "2026-09-10"}]


def _submit(monkeypatch, *, skipped, strategy="quality_momentum", following=0, referral=None):
    notifs, book = _Notifs(following), _Book(BREAKOUT)

    @contextmanager
    def _cm(obj):
        yield obj

    # Imported normally here: pydantic resolves the model's annotations through
    # sys.modules, which the file-path load at the top of this module skips.
    import api.routers.onboarding as router
    monkeypatch.setattr(router, "get_notifications_db", lambda: _cm(notifs))
    monkeypatch.setattr(router, "get_db", lambda: _cm(book))
    body = router.OnboardingRequest(default_strategy=strategy, skipped=skipped, referral_source=referral)
    out = router.submit_onboarding(body, router.UserContext(user_id="user_new"))
    return out, notifs


def test_a_skip_still_sets_up_alerts_and_follows(monkeypatch):
    out, n = _submit(monkeypatch, skipped=True)
    assert n.inserts("notification_preferences"), "a skipped account has no alert settings, so nothing can reach it"
    assert [p[1] for p in n.inserts("watchlist")] == ["EAF", "GME"]
    assert out == {"ok": True, "followed": ["EAF", "GME"]}


def test_a_skip_is_recorded_as_a_skip_not_a_choice(monkeypatch):
    _, n = _submit(monkeypatch, skipped=True)
    (profile,) = n.inserts("user_profiles")
    default_strategy, skipped_flag = profile[5], profile[6]
    assert default_strategy is None, "a skip chose no book; the welcome email must not say it did"
    assert skipped_flag == 1


def test_a_choice_is_recorded_as_one(monkeypatch):
    out, n = _submit(monkeypatch, skipped=False)
    (profile,) = n.inserts("user_profiles")
    assert (profile[5], profile[6]) == ("quality_momentum", 0)
    assert out["followed"] == ["EAF", "GME"]


def test_an_account_that_already_follows_gets_settings_but_no_new_follows(monkeypatch):
    out, n = _submit(monkeypatch, skipped=True, following=3)
    assert n.inserts("notification_preferences")
    assert not n.inserts("watchlist") and out["followed"] == []


def test_an_unknown_book_still_gets_alert_settings(monkeypatch):
    out, n = _submit(monkeypatch, skipped=True, strategy="not_a_book")
    assert n.inserts("notification_preferences")
    assert out["followed"] == []


def test_the_form_posts_on_skip_and_says_so():
    src = FORM.read_text(encoding="utf-8")
    fn = src.split("async function submit(", 1)[1].split("\n  return (", 1)[0]
    code = "\n".join(ln for ln in fn.splitlines() if not ln.strip().startswith("//"))
    assert "if (!skipped)" not in code, "the POST is gated on not skipping again"
    assert "/onboarding`" in code and "skipped," in code
    assert "default_strategy: selectedStrategy" in code, "the API needs the book the redirect opens"

"""The Stripe webhook must handle what the Stripe SDK actually hands it.

WHAT BROKE, TWICE

stripe-python 15.x stopped making StripeObject a dict subclass, so `.get()` on
one raises:

    AttributeError: 'get' is a dict method, but a Session is not a dict.

2026-08-24: every branch called `.get()` on the event payload, so the whole
webhook returned 500. A paying customer was billed for months, never got
stripe_customer_id, and could not cancel.

2026-11-11: the August fix converted the payload but converted the re-fetched
checkout Session with `to_dict_recursive()` only, and 15.x has no such method.
So the first real Form4 checkout 500'd on `session.get("line_items")`, Stripe
gave up after three days, and the customer never got Pro. Still shown as
free, they checked out again and ended up with a trial AND a full-price
subscription, and still no access.

WHY THE TESTS DID NOT CATCH IT

They ran against a hand-written fake that HAD `to_dict_recursive()`. The real
SDK does not. They also asserted that the word appeared in the source, which
it did. These tests drive the real handler with real SDK objects from the
pinned SDK version, and fail if the venv's SDK is not the one production runs.

AND: ending one subscription must not downgrade an account that still pays
through another. The handler used to downgrade on any
customer.subscription.deleted, so cancelling that customer's duplicate would
have cut off the subscription they keep.
"""
from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

# API tests run in the form4-tests venv (CLAUDE.md). Where fastapi is present,
# a missing stripe is an ERROR, never a skip: the August tests passed for
# months against a fake because nothing here ever imported the real SDK.
pytest.importorskip("fastapi")
import stripe  # noqa: E402

from api.routers import webhooks  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
WEBHOOKS = REPO / "api/routers/webhooks.py"
REQUIREMENTS = REPO / "api/requirements.txt"

PRO = "price_pro_test"
KEY = "sk_test_unused"


def test_the_sdk_under_test_is_the_one_production_pins():
    pinned = re.search(r"^stripe==(\S+)", REQUIREMENTS.read_text(), re.M)
    assert pinned, "stripe is unpinned in api/requirements.txt; an unpinned upgrade is what broke this in August"
    assert stripe.VERSION == pinned.group(1), (
        f"testing against stripe {stripe.VERSION} but production pins {pinned.group(1)}; "
        f"run: /Users/derekg/.venvs/form4-tests/bin/python -m pip install 'stripe=={pinned.group(1)}'"
    )


def _walk(obj):
    yield obj
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def _session(customer="cus_1", user="user_1", price=PRO):
    return stripe.checkout.Session.construct_from({
        "id": "cs_test_1", "object": "checkout.session",
        "client_reference_id": user, "customer": customer,
        "subscription": "sub_new", "customer_details": {"email": "buyer@example.com"},
        "line_items": {"object": "list", "data": [
            {"id": "li_1", "object": "item", "price": {"id": price, "object": "price"}},
        ]},
    }, KEY)


def _subscription(sub_id, status, price=PRO):
    return {
        "id": sub_id, "object": "subscription", "status": status, "customer": "cus_1",
        "items": {"object": "list", "data": [
            {"id": f"si_{sub_id}", "object": "subscription_item", "price": {"id": price, "object": "price"}},
        ]},
    }


def test_plain_leaves_no_sdk_object_anywhere():
    out = webhooks._plain(_session())
    leftovers = [type(o).__name__ for o in _walk(out) if isinstance(o, stripe.StripeObject)]
    assert not leftovers, f"SDK objects survived the conversion: {leftovers}"
    assert out["line_items"]["data"][0]["price"]["id"] == PRO


class _Request:
    async def body(self):
        return b"{}"


@pytest.fixture
def hook(monkeypatch):
    """The real handler, with the network edges replaced: Clerk writes are
    recorded, the signature check returns the event it is handed."""
    writes, downgrades = [], []

    async def update(user_id, metadata):
        writes.append((user_id, metadata))

    async def downgrade(user_id):
        downgrades.append(user_id)

    async def find(customer_id):
        return "user_1"

    welcomes = []
    monkeypatch.setattr(webhooks, "_update_clerk_metadata", update)
    monkeypatch.setattr(webhooks, "_downgrade_unless_comped", downgrade)
    monkeypatch.setattr(webhooks, "_find_clerk_user_by_customer", find)
    monkeypatch.setattr(webhooks, "_send_pro_welcome", lambda user_id, session: welcomes.append(user_id))
    monkeypatch.setattr(webhooks, "PRO_PRICE_IDS", {PRO})
    monkeypatch.setattr(stripe.checkout.Session, "retrieve", lambda *a, **k: _session())

    def subscriptions(*subs):
        monkeypatch.setattr(stripe.Subscription, "list", lambda **k: stripe.ListObject.construct_from(
            {"object": "list", "data": list(subs)}, KEY))

    def deliver(event_type, obj):
        event = stripe.Event.construct_from(
            {"id": "evt_test", "object": "event", "type": event_type, "data": {"object": obj}}, KEY)
        monkeypatch.setattr(stripe.Webhook, "construct_event", lambda *a, **k: event)
        return asyncio.run(webhooks.stripe_webhook(_Request(), stripe_signature="test"))

    return SimpleNamespace(writes=writes, downgrades=downgrades, deliver=deliver,
                           subscriptions=subscriptions, welcomes=welcomes)


def test_a_paid_checkout_grants_pro(hook):
    """The 2026-11-11 incident: this raised AttributeError and returned 500."""
    hook.deliver("checkout.session.completed", {
        "id": "cs_test_1", "object": "checkout.session",
        "client_reference_id": "user_1", "customer": "cus_1",
    })
    assert hook.writes == [("user_1", {"stripe_customer_id": "cus_1", "tier": "pro"})]


_REAL_SEND_WELCOME = webhooks._send_pro_welcome
_CHECKOUT = {"id": "cs_test_1", "object": "checkout.session", "client_reference_id": "user_1", "customer": "cus_1"}


@pytest.fixture
def welcome(hook, monkeypatch):
    """The real welcome path (subscription read, claim, send) with the claim
    store and the mailer in memory."""
    from datetime import datetime, timezone
    claimed, mail = set(), []
    monkeypatch.setattr(webhooks, "_send_pro_welcome", _REAL_SEND_WELCOME)

    def claim(user_id, name):
        if (user_id, name) in claimed:
            return False
        claimed.add((user_id, name))
        return True
    monkeypatch.setattr(webhooks, "_claim_email", claim)
    monkeypatch.setattr(webhooks, "_release_email", lambda u, n: claimed.discard((u, n)))
    monkeypatch.setattr(webhooks, "send_email",
                        lambda to, subject, html, **kw: mail.append((to, subject, html, kw)) or True)
    # The shape of the real first trial: ends Sat 2026-11-21 16:39 PT, which is
    # already Sunday in UTC. The email must say Saturday.
    trial_end = int(datetime(2026, 11, 22, 0, 39, 26, tzinfo=timezone.utc).timestamp())
    monkeypatch.setattr(stripe.Subscription, "retrieve", lambda *a, **k: stripe.Subscription.construct_from({
        "id": "sub_new", "object": "subscription", "status": "trialing", "trial_end": trial_end,
        "items": {"object": "list", "data": [{
            "id": "si_1", "object": "subscription_item", "current_period_end": trial_end,
            "price": {"id": PRO, "object": "price", "unit_amount": 1900, "recurring": {"interval": "month"}},
        }]},
    }, KEY))
    return SimpleNamespace(claimed=claimed, mail=mail)


def test_a_paid_checkout_sends_one_welcome_with_the_real_terms(hook, welcome):
    """The first customer paid and heard nothing. Stripe retries; they get one."""
    hook.deliver("checkout.session.completed", dict(_CHECKOUT))
    hook.deliver("checkout.session.completed", dict(_CHECKOUT))
    assert len(welcome.mail) == 1, f"{len(welcome.mail)} welcomes for one checkout"
    to, subject, html, kw = welcome.mail[0]
    assert (to, subject) == ("buyer@example.com", "You're on Form4 Pro")
    assert "Saturday, November 21" in html, "the trial end is shown in the wrong timezone or not at all"
    assert "$19 a month" in html, "the price shown is not the price on the subscription"
    assert kw.get("campaign") == "pro_welcome" and kw.get("reply_to")


def test_a_failed_welcome_send_frees_its_claim(hook, welcome, monkeypatch):
    monkeypatch.setattr(webhooks, "send_email", lambda *a, **k: False)
    hook.deliver("checkout.session.completed", dict(_CHECKOUT))
    assert not welcome.claimed, "a failed send kept its claim, so no later attempt could ever send it"


def test_the_welcome_can_never_fail_the_checkout(hook, welcome, monkeypatch):
    """A 500 makes Stripe replay a checkout that already granted access."""
    def down(*a, **k):
        raise stripe.error.APIConnectionError("stripe unreachable")
    monkeypatch.setattr(stripe.Subscription, "retrieve", down)
    assert hook.deliver("checkout.session.completed", dict(_CHECKOUT)) == {"status": "ok"}
    assert hook.writes == [("user_1", {"stripe_customer_id": "cus_1", "tier": "pro"})]


def test_the_pro_welcome_reads_plainly():
    from datetime import datetime
    import re as _re
    from api.email_templates import pro_welcome_email
    for trial_end, renews in ((datetime(2026, 11, 21), None), (None, datetime(2026, 12, 11))):
        subject, html = pro_welcome_email(1900, "month", trial_end, renews)
        body = _re.sub(r"<[^>]+>", " ", html[html.index("<h2"):html.index("Questions?")])
        for banned in ("!", "—", "Unlock"):
            assert banned not in body, f"{banned!r} in the Pro welcome"
        assert "Manage billing" in body and "$19 a month" in body


@pytest.mark.parametrize("event_type, status", [
    ("customer.subscription.deleted", "canceled"),
    ("customer.subscription.updated", "canceled"),
    ("customer.subscription.updated", "unpaid"),
])
def test_ending_one_of_two_subscriptions_keeps_pro(hook, event_type, status):
    ended = _subscription("sub_dup", status)
    hook.subscriptions(ended, _subscription("sub_kept", "active"))
    hook.deliver(event_type, ended)
    assert hook.downgrades == [], "downgraded an account that still pays through another subscription"
    assert hook.writes == [("user_1", {"tier": "pro", "api_access": False})]


class _FakeClerk:
    """Stands in for httpx.AsyncClient: every Clerk write answers `status`."""

    def __init__(self, status, users=()):
        self.status, self.users = status, list(users)

    def __call__(self, *a, **k):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def patch(self, url, **k):
        return SimpleNamespace(status_code=self.status, text="clerk unavailable")

    async def get(self, url, **k):
        return SimpleNamespace(status_code=200, json=lambda: self.users)


_REAL_UPDATE = webhooks._update_clerk_metadata


def test_a_failed_clerk_write_answers_500_so_stripe_retries(hook, monkeypatch):
    """A 200 tells Stripe the event landed. If the Clerk write failed, the
    paying customer is left on free and nothing ever retries."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    monkeypatch.setattr(webhooks, "_update_clerk_metadata", _REAL_UPDATE)
    monkeypatch.setattr(webhooks.httpx, "AsyncClient", _FakeClerk(503))
    event = stripe.Event.construct_from({
        "id": "evt_test", "object": "event", "type": "checkout.session.completed",
        "data": {"object": {"id": "cs_test_1", "object": "checkout.session",
                            "client_reference_id": "user_1", "customer": "cus_1"}},
    }, KEY)
    monkeypatch.setattr(stripe.Webhook, "construct_event", lambda *a, **k: event)
    app = FastAPI()
    app.include_router(webhooks.router)
    resp = TestClient(app, raise_server_exceptions=False).post(
        "/api/v1/webhooks/stripe", content=b"{}", headers={"Stripe-Signature": "test"})
    assert resp.status_code == 500, f"Stripe was told {resp.status_code}; it will not retry"


def test_a_failed_link_repair_still_finds_the_account(monkeypatch):
    """The email fallback FOUND the account; failing to cache the link must
    not turn that into 'nobody', which drops the event with a 200."""
    monkeypatch.setattr(stripe.Customer, "retrieve", lambda *a, **k: stripe.Customer.construct_from(
        {"id": "cus_1", "object": "customer", "email": "buyer@example.com"}, KEY))
    monkeypatch.setattr(webhooks.httpx, "AsyncClient", _FakeClerk(503, users=[{"id": "user_1"}]))
    assert asyncio.run(webhooks._find_clerk_user_by_customer_email("cus_1")) == "user_1"


class _ClerkStore:
    """Clerk's real semantics, in memory: PATCH /users/{id} REPLACES
    public_metadata; PATCH /users/{id}/metadata merges it (null removes)."""

    def __init__(self, meta):
        self.meta = dict(meta)

    def __call__(self, *a, **k):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **k):
        if url.rstrip("/").endswith("/users"):
            return SimpleNamespace(status_code=200, json=lambda: [{"id": "user_1"}])
        return SimpleNamespace(status_code=200, json=lambda: {"public_metadata": dict(self.meta)})

    async def patch(self, url, json=None, **k):
        new = (json or {}).get("public_metadata")
        if new is not None:
            if url.endswith("/metadata"):
                for key, value in new.items():
                    if value is None:
                        self.meta.pop(key, None)
                    else:
                        self.meta[key] = value
            else:
                self.meta = dict(new)
        return SimpleNamespace(status_code=200, text="")


_REAL_DOWNGRADE = webhooks._downgrade_unless_comped


@pytest.fixture
def real_clerk_writes(hook, monkeypatch):
    """The real Clerk write path against a store with Clerk's semantics."""
    monkeypatch.setattr(webhooks, "_update_clerk_metadata", _REAL_UPDATE)
    monkeypatch.setattr(webhooks, "_downgrade_unless_comped", _REAL_DOWNGRADE)

    def install(meta):
        store = _ClerkStore(meta)
        monkeypatch.setattr(webhooks.httpx, "AsyncClient", store)
        return store
    return install


def test_a_subscription_update_keeps_the_customer_link(hook, real_clerk_writes):
    """Seen live 2026-11-14: a subscription.updated wrote {tier, api_access}
    with a REPLACING endpoint and erased the paying customer's
    stripe_customer_id, which the billing portal and every later event use."""
    store = real_clerk_writes({"stripe_customer_id": "cus_1", "tier": "pro"})
    hook.deliver("customer.subscription.updated", _subscription("sub_kept", "active"))
    assert store.meta == {"stripe_customer_id": "cus_1", "tier": "pro", "api_access": False}


def test_a_downgrade_keeps_the_customer_link(hook, real_clerk_writes):
    store = real_clerk_writes({"stripe_customer_id": "cus_1", "tier": "pro"})
    ended = _subscription("sub_only", "canceled")
    hook.subscriptions(ended)
    hook.deliver("customer.subscription.deleted", ended)
    assert store.meta == {"stripe_customer_id": "cus_1", "tier": "free", "api_access": False}


def test_the_email_repair_keeps_the_tier(real_clerk_writes, monkeypatch):
    """Repairing the link wrote {stripe_customer_id} alone, so a replacing
    write erased tier: the repair that keeps a customer in locked them out."""
    store = real_clerk_writes({"tier": "pro"})
    monkeypatch.setattr(stripe.Customer, "retrieve", lambda *a, **k: stripe.Customer.construct_from(
        {"id": "cus_1", "object": "customer", "email": "buyer@example.com"}, KEY))
    assert asyncio.run(webhooks._find_clerk_user_by_customer_email("cus_1")) == "user_1"
    assert store.meta == {"tier": "pro", "stripe_customer_id": "cus_1"}


def test_ending_the_last_subscription_downgrades(hook):
    ended = _subscription("sub_only", "canceled")
    hook.subscriptions(ended, _subscription("sub_old", "canceled"))
    hook.deliver("customer.subscription.deleted", ended)
    assert hook.downgrades == ["user_1"]
    assert hook.writes == []


def test_an_unreadable_stripe_does_not_downgrade(hook, monkeypatch):
    """Unknown means leave it alone, the same rule as an unreadable Clerk."""
    def down(**k):
        raise stripe.error.APIConnectionError("stripe unreachable")
    monkeypatch.setattr(stripe.Subscription, "list", down)
    hook.deliver("customer.subscription.deleted", _subscription("sub_only", "canceled"))
    assert hook.downgrades == [] and hook.writes == []


# ── a comp must survive the end of a subscription ───────────────────────────


def test_downgrade_paths_go_through_the_comp_check():
    """subscription.deleted and a canceled subscription.updated must not write
    tier=free directly.

    Checked by walking the AST for a literal {"tier": "free"} argument rather
    than substring-matching ast.unparse output, which renders single quotes and
    silently matched nothing.
    """
    tree = ast.parse(WEBHOOKS.read_text())
    direct = []
    for fn in ast.walk(tree):
        if not (isinstance(fn, ast.AsyncFunctionDef) and fn.name in ("stripe_webhook", "_end_subscription")):
            continue
        for n in ast.walk(fn):
            if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                    and n.func.id == "_update_clerk_metadata"):
                continue
            for arg in n.args:
                if not isinstance(arg, ast.Dict):
                    continue
                pairs = {
                    k.value: v.value
                    for k, v in zip(arg.keys, arg.values)
                    if isinstance(k, ast.Constant) and isinstance(v, ast.Constant)
                }
                if pairs.get("tier") == "free":
                    direct.append(n.lineno)
    assert not direct, (
        f"tier=free is written directly at line(s) {direct}. A comp is granted "
        "precisely to accounts with no subscription; downgrading on "
        "subscription.deleted revokes it the moment it is given."
    )
    assert "_downgrade_unless_comped" in WEBHOOKS.read_text()


def test_an_unreadable_clerk_account_is_not_downgraded():
    """Fail safe. Wrongly keeping Pro for a cycle is recoverable; wrongly
    revoking a paid or comped account is not."""
    body = WEBHOOKS.read_text()
    src = body[body.index("async def _get_clerk_metadata("):]
    src = src[:src.index("\nasync def ")]
    assert "9999" in src, (
        "a failed Clerk read no longer returns a far-future pro_until, so an "
        "API blip would downgrade real accounts"
    )


def test_comp_lapsed_is_reused_not_reimplemented():
    src = WEBHOOKS.read_text()
    assert "from api.comp import comp_lapsed" in src, (
        "date handling for comps must come from api.comp, which the API and "
        "trial_emails also use"
    )

"""The Stripe webhook must handle what the Stripe SDK actually hands it.

WHAT BROKE

stripe-python 15.x stopped making StripeObject a dict subclass. Every branch of
the handler called `.get()` on the event payload, so every branch raised:

    AttributeError: 'get' is a dict method, but a Subscription is not a dict.

The webhook returned 500 to Stripe from the moment the library was upgraded.
Stripe retried and gave up. Nothing alerted, because a webhook nobody calls
looks identical to a webhook nobody needs.

THE COST. checkout.session.completed is the ONLY place
public_metadata.stripe_customer_id is written. Without it:

  * the billing portal returns "No subscription found" — the user cannot cancel
    (frontend/src/app/api/billing-portal/route.ts:28)
  * _find_clerk_user_by_customer finds nobody, so every later subscription
    event is logged and ignored
  * Stripe keeps billing

A paying customer hit all three, was charged for months, could not cancel, and
had to email support.

AND: fixing it made Stripe replay the queued customer.subscription.deleted,
which would have set tier=free on an account that had just been comped three
months of Pro by hand. Clerk merges public_metadata, so pro_until would have
survived while the access it grants vanished — a silent revocation of something
promised in writing.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
WEBHOOKS = REPO / "api/routers/webhooks.py"


class FakeStripeObject:
    """Mimics stripe-python 15.x: attribute access, and .get() raises."""
    def __init__(self, data):
        self._data = data
    def __getitem__(self, k):
        return self._data[k]
    def __getattr__(self, k):
        if k in ("_data",):
            raise AttributeError(k)
        if k in ("get", "keys", "items", "values"):
            raise AttributeError(
                f"'{k}' is a dict method, but a Subscription is not a dict. "
                "Use .to_dict() to convert it."
            )
        try:
            return self._data[k]
        except KeyError as e:
            raise AttributeError(k) from e
    def to_dict_recursive(self):
        return {k: (v.to_dict_recursive() if isinstance(v, FakeStripeObject) else v)
                for k, v in self._data.items()}


def test_the_fake_reproduces_the_real_failure():
    """Guards the test itself: if .get() stops raising, this proves nothing."""
    obj = FakeStripeObject({"customer": "cus_1"})
    with pytest.raises(AttributeError, match="not a dict"):
        obj.get("customer")


def _code_only(text: str) -> str:
    """Strip comments. An earlier version of the test below matched the COMMENT
    that explains to_dict_recursive and passed while the actual conversion had
    been deleted — the same way a comment made a test pass for the wrong reason
    earlier in this codebase."""
    out = []
    for line in text.splitlines():
        st = line.lstrip()
        if st.startswith("#"):
            continue
        if "  # " in line:
            line = line.split("  # ", 1)[0]
        out.append(line)
    return "\n".join(out)


def test_the_handler_converts_before_using_dict_methods():
    """Structural: the payload must be converted before any .get() on it."""
    src = WEBHOOKS.read_text()
    body = src[src.index("async def stripe_webhook("):]
    body = _code_only(body[:body.index("\nasync def ")])
    assert "to_dict_recursive" in body, (
        "the payload is never converted — every .get() on it raises "
        "AttributeError and the webhook 500s, which is how a paying customer "
        "went months unable to cancel"
    )
    assert body.index("to_dict_recursive") < body.index("data_obj.get("), (
        "data_obj.get() is called before the conversion"
    )


def test_conversion_is_recursive():
    """Nested line items and subscription items are StripeObjects too; a
    shallow to_dict() just moves the crash one level down."""
    src = WEBHOOKS.read_text()
    assert "to_dict_recursive" in src, "conversion is shallow again"
    obj = FakeStripeObject({"items": FakeStripeObject({"data": [1, 2]})})
    assert obj.to_dict_recursive()["items"]["data"] == [1, 2]


# ── a comp must survive the end of a subscription ───────────────────────────


def test_downgrade_paths_go_through_the_comp_check():
    """subscription.deleted and a canceled subscription.updated must not write
    tier=free directly.

    Checked by walking the AST for a literal {"tier": "free"} argument rather
    than substring-matching ast.unparse output, which renders single quotes and
    silently matched nothing.
    """
    tree = ast.parse(WEBHOOKS.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "stripe_webhook")
    direct = []
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
        f"stripe_webhook writes tier=free directly at line(s) {direct}. A comp "
        "is granted precisely to accounts with no subscription; downgrading on "
        "subscription.deleted revokes it the moment it is given."
    )
    assert "_downgrade_unless_comped" in WEBHOOKS.read_text()


def test_an_unreadable_clerk_account_is_not_downgraded():
    """Fail safe. Wrongly keeping Pro for a cycle is recoverable; wrongly
    revoking a paid or comped account is not."""
    # Source-inspected rather than imported: fastapi is not installed on the
    # dev machine, and the rest of this suite reads the file for the same
    # reason.
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

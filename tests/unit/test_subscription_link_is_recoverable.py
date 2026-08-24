"""A user must never be locked out of their own subscription.

THE FAILURE THIS PREVENTS

`publicMetadata.stripe_customer_id` was treated as the source of truth for
"does this person have a subscription". It is a CACHE, written in exactly one
place — the checkout.session.completed webhook — with no fallback and no
repair.

When that webhook was returning 500 (stripe-python 15.x made StripeObject
non-dict, so every .get() raised), the cache was never written. A paying
customer then saw "No subscription found" in the billing portal while Stripe
charged him monthly. He could not cancel, could not see anything, and had to
email support. Silent, permanent, invisible.

THE RULE

Stripe is the source of truth for billing. Clerk metadata is a cache, and every
read path must be able to rebuild it. A missed webhook may cost one slower
lookup. It may never cost a user access to their own subscription.

Four paths have to hold that line, and these tests pin each one.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
RESOLVER = REPO / "frontend/src/lib/stripe-customer.ts"
PORTAL = REPO / "frontend/src/app/api/billing-portal/route.ts"
CHECKOUT = REPO / "frontend/src/app/api/checkout/route.ts"
WEBHOOKS = REPO / "api/routers/webhooks.py"


def test_the_resolver_exists():
    assert RESOLVER.exists(), "the shared customer resolver is gone"


def test_the_resolver_falls_back_to_stripe():
    src = RESOLVER.read_text()
    assert "stripe.customers.list" in src, (
        "the resolver no longer asks Stripe when the Clerk cache misses — a "
        "missed webhook goes back to locking the user out"
    )


def test_the_resolver_repairs_the_cache():
    src = RESOLVER.read_text()
    assert "updateUser" in src, (
        "a recovered customer id is not written back to Clerk, so the slow "
        "path would run on every request forever"
    )


def test_caching_failure_does_not_fail_the_request():
    """Writing the cache is an optimisation. It must never break the user."""
    src = RESOLVER.read_text()
    fn = src[src.index("async function cacheOnClerk"):]
    fn = fn[:fn.index("\nexport type")]
    assert "try" in fn and "catch" in fn, (
        "cacheOnClerk can throw, which would fail a billing-portal request "
        "because we could not write an optimisation"
    )


@pytest.mark.parametrize("path,name", [
    (PORTAL, "billing portal"), (CHECKOUT, "checkout"),
])
def test_no_route_reads_the_cache_directly(path, name):
    """Both routes must go through the resolver. Reading the raw field is the
    bug: checkout created a DUPLICATE Stripe customer whenever the cache was
    empty, and the portal declared the user had no subscription."""
    src = path.read_text()
    code = "\n".join(l for l in src.splitlines()
                     if not l.strip().startswith(("//", "*", "/*")))
    assert "publicMetadata?.stripe_customer_id" not in code, (
        f"{name} reads publicMetadata.stripe_customer_id directly instead of "
        "resolveStripeCustomer()"
    )
    assert "resolveStripeCustomer" in code, f"{name} does not use the resolver"


def test_the_portal_no_longer_says_no_subscription_found():
    """The literal string a paying customer was shown."""
    code = "\n".join(l for l in PORTAL.read_text().splitlines()
                     if not l.strip().startswith(("//", "*", "/*")))
    assert '"No subscription found"' not in code, (
        "the portal can still tell a paying customer they have no "
        "subscription without ever asking Stripe"
    )


def test_checkout_pins_the_customer_email():
    """Without customer_email, Stripe records whatever address is typed at
    checkout, and email-based recovery cannot match it to the account."""
    src = CHECKOUT.read_text()
    assert "customer_email" in src, (
        "checkout does not pin the Clerk email onto the Stripe customer, so a "
        "future cache miss may be unrecoverable"
    )


def test_the_webhook_has_a_reverse_fallback():
    """Stripe customer -> email -> Clerk account, for when we never wrote the
    link in the first place."""
    import ast
    src = WEBHOOKS.read_text()
    assert "_find_clerk_user_by_customer_email" in src, (
        "the webhook only finds users via metadata we may never have written; "
        "when that is missing every subsequent event is silently dropped"
    )
    fn = src[src.index("async def _find_clerk_user_by_customer_email"):]
    assert "email_address" in fn, "the fallback does not look up Clerk by email"

    # Existing is not enough — it has to be REACHED. A first version of this
    # test passed while the call had been deleted, because the function
    # definition survived.
    tree = ast.parse(src)
    primary = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef)
        and n.name == "_find_clerk_user_by_customer"
    )
    called = any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        and n.func.id == "_find_clerk_user_by_customer_email"
        for n in ast.walk(primary)
    )
    assert called, (
        "_find_clerk_user_by_customer never calls the email fallback, so a "
        "customer we have no metadata link for is still dropped silently"
    )


def test_the_email_fallback_refuses_to_guess():
    """Two accounts on one email must not cause a stranger's subscription to
    be attached to someone else."""
    src = WEBHOOKS.read_text()
    fn = src[src.index("async def _find_clerk_user_by_customer_email"):]
    assert "len(users) != 1" in fn, (
        "the email fallback no longer requires exactly one match — an "
        "ambiguous email would attach billing to the wrong account"
    )

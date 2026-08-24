from __future__ import annotations

import logging

import httpx
import stripe
from fastapi import APIRouter, Header, HTTPException, Request

from api.config import (
    CLERK_SECRET_KEY,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    PRO_PRICE_IDS,
    PRO_PLUS_PRICE_IDS,
    API_PRICE_IDS,
)

from api.rate_limit import limiter

logger = logging.getLogger(__name__)

from api.comp import comp_lapsed

router = APIRouter(prefix="/api/v1/webhooks", tags=["webhooks"])

stripe.api_key = STRIPE_SECRET_KEY


async def _update_clerk_metadata(user_id: str, public_metadata: dict) -> None:
    """Update a Clerk user's public metadata via the Backend API."""
    async with httpx.AsyncClient() as client:
        resp = await client.patch(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={
                "Authorization": f"Bearer {CLERK_SECRET_KEY}",
                "Content-Type": "application/json",
            },
            json={"public_metadata": public_metadata},
        )
        if resp.status_code != 200:
            logger.error("Clerk metadata update failed: %s %s", resp.status_code, resp.text)
        else:
            logger.info("Updated Clerk metadata for %s: %s", user_id, public_metadata)


def _determine_tier_from_items(line_items: list) -> tuple[str, bool]:
    """Determine tier and API access from Stripe line items.

    Pro+ includes API access by default. Pro + API addon also grants API.
    """
    tier = "free"
    api_access = False
    for item in line_items:
        price_id = item.get("price", {}).get("id", "")
        if price_id in PRO_PLUS_PRICE_IDS:
            tier = "pro_plus"
            api_access = True
        elif price_id in PRO_PRICE_IDS and tier != "pro_plus":
            tier = "pro"
        if price_id in API_PRICE_IDS:
            api_access = True
    return tier, api_access


@router.post("/stripe")
@limiter.exempt
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(alias="Stripe-Signature"),
) -> dict:
    """Handle Stripe webhook events to sync subscription state to Clerk."""
    payload = await request.body()

    try:
        event = stripe.Webhook.construct_event(
            payload, stripe_signature, STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    # PLAIN DICTS FROM HERE DOWN.
    #
    # stripe-python 15.x stopped making StripeObject a dict subclass, so every
    # `.get()` in this handler began raising:
    #
    #   AttributeError: 'get' is a dict method, but a Subscription is not a
    #   dict. Use .to_dict() to convert it.
    #
    # Every branch used .get(), so the ENTIRE webhook returned 500 from the
    # moment the library was upgraded. Stripe retried, got 500, gave up. The
    # visible symptom was a paying customer whose Clerk account never received
    # stripe_customer_id, so the billing portal told him "No subscription
    # found" while Stripe kept charging him, and he could not self-cancel.
    #
    # to_dict_recursive() because nested objects (line items, subscription
    # items) are StripeObjects too and a shallow conversion just moves the
    # crash one level down.
    event_type = event["type"]
    data_obj = event["data"]["object"]
    if hasattr(data_obj, "to_dict_recursive"):
        data_obj = data_obj.to_dict_recursive()
    elif hasattr(data_obj, "to_dict"):
        data_obj = data_obj.to_dict()

    if event_type == "checkout.session.completed":
        user_id = data_obj.get("client_reference_id")
        if not user_id:
            logger.warning("checkout.session.completed without client_reference_id")
            return {"status": "ignored"}

        # Fetch line items to determine what was purchased
        session = stripe.checkout.Session.retrieve(
            data_obj["id"], expand=["line_items"]
        )
        if hasattr(session, "to_dict_recursive"):
            session = session.to_dict_recursive()
        items = (session.get("line_items") or {}).get("data", [])
        tier, api_access = _determine_tier_from_items(items)

        # If this checkout only added API access (no Pro price), preserve existing tier
        metadata: dict = {"stripe_customer_id": data_obj.get("customer")}
        if tier != "free":
            metadata["tier"] = tier
        if api_access:
            metadata["api_access"] = True

        await _update_clerk_metadata(user_id, metadata)

    elif event_type == "customer.subscription.updated":
        customer_id = data_obj.get("customer")
        status = data_obj.get("status")
        items = data_obj.get("items", {}).get("data", [])

        # Find Clerk user by stripe_customer_id
        user_id = await _find_clerk_user_by_customer(customer_id)
        if not user_id:
            logger.warning("No Clerk user found for Stripe customer %s", customer_id)
            return {"status": "ignored"}

        if status in ("active", "trialing"):
            price_items = [{"price": item.get("price", {})} for item in items]
            tier, api_access = _determine_tier_from_items(price_items)
            metadata = {"tier": tier}
            if api_access:
                metadata["api_access"] = True
            else:
                metadata["api_access"] = False
            await _update_clerk_metadata(user_id, metadata)
        elif status in ("canceled", "unpaid", "past_due"):
            await _downgrade_unless_comped(user_id)

    elif event_type == "customer.subscription.deleted":
        customer_id = data_obj.get("customer")
        user_id = await _find_clerk_user_by_customer(customer_id)
        if user_id:
            await _downgrade_unless_comped(user_id)

    return {"status": "ok"}


async def _downgrade_unless_comped(user_id: str) -> None:
    """Drop to free — unless the account carries an unexpired hand-granted comp.

    A comp is given precisely to people who do NOT have a subscription: a
    refunded customer, a goodwill grant, an early supporter. Letting
    subscription.deleted set tier=free would revoke the comp the moment it was
    granted, which is the opposite of what comping means.

    Clerk merges public_metadata on PATCH, so `pro_until` survives a tier
    write — the flag stays but the access it was meant to grant disappears.
    That is a silent revocation, and the user was told in writing they had it.

    comp_lapsed() is the same predicate the API and trial_emails use; do not
    reimplement the date handling here.
    """
    meta = await _get_clerk_metadata(user_id)
    if meta.get("pro_until") and not comp_lapsed(meta):
        logger.info(
            "%s: subscription ended but comped until %s — leaving tier alone",
            user_id, meta.get("pro_until"),
        )
        return
    await _update_clerk_metadata(user_id, {"tier": "free", "api_access": False})


async def _get_clerk_metadata(user_id: str) -> dict:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.clerk.com/v1/users/{user_id}",
                headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
            )
            if resp.status_code == 200:
                return resp.json().get("public_metadata") or {}
    except Exception as exc:
        logger.error("could not read Clerk metadata for %s: %s", user_id, exc)
    # Unknown -> do not downgrade. Wrongly keeping Pro for a cycle is
    # recoverable; wrongly revoking a paid or comped account is not.
    return {"pro_until": "9999-12-31"}


async def _find_clerk_user_by_customer(customer_id: str) -> str | None:
    """Clerk user for a Stripe customer, with a fallback that does not depend
    on us having written the link.

    The metadata scan below only finds users whose
    `public_metadata.stripe_customer_id` we successfully wrote — which is
    written by exactly one webhook. When that webhook was broken, every later
    event for the affected customer landed here, found nothing, and was
    dropped: the failure compounded itself.

    So if the scan misses, ask STRIPE for the customer's email and look the
    account up by email instead. That path works even if we have never written
    anything, and it repairs the cache on the way through.
    """
    if not customer_id:
        return None
    try:
        async with httpx.AsyncClient() as client:
            offset = 0
            limit = 100
            while True:
                resp = await client.get(
                    "https://api.clerk.com/v1/users",
                    headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
                    params={"limit": limit, "offset": offset},
                )
                if resp.status_code != 200:
                    return None
                body = resp.json()
                users = body if isinstance(body, list) else body.get("data", [])
                for user in users:
                    meta = user.get("public_metadata", {})
                    if meta.get("stripe_customer_id") == customer_id:
                        return user["id"]
                # Stop when we get fewer results than the page size
                if len(users) < limit:
                    break
                offset += limit
    except Exception as e:
        logger.error("Error finding Clerk user: %s", e)

    return await _find_clerk_user_by_customer_email(customer_id)


async def _find_clerk_user_by_customer_email(customer_id: str) -> str | None:
    """Stripe customer -> email -> Clerk account, repairing the cache."""
    try:
        cust = stripe.Customer.retrieve(customer_id)
        email = getattr(cust, "email", None)
        if not email:
            return None
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.clerk.com/v1/users",
                headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
                params={"email_address": email, "limit": 2},
            )
            if resp.status_code != 200:
                return None
            body = resp.json()
            users = body if isinstance(body, list) else body.get("data", [])
            if len(users) != 1:
                # Zero means no account; more than one is ambiguous and
                # guessing would attach a stranger's subscription to someone.
                logger.warning(
                    "customer %s email matched %d Clerk accounts — not guessing",
                    customer_id, len(users),
                )
                return None
            user_id = users[0]["id"]
            logger.warning(
                "recovered %s for customer %s by email — the metadata link was "
                "missing, repairing it", user_id, customer_id,
            )
            await _update_clerk_metadata(user_id, {"stripe_customer_id": customer_id})
            return user_id
    except Exception as e:
        logger.error("email fallback failed for %s: %s", customer_id, e)
    return None

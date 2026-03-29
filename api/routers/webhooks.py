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
    API_PRICE_IDS,
)

from api.rate_limit import limiter

logger = logging.getLogger(__name__)

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
    """Determine tier and API access from Stripe line items."""
    tier = "free"
    api_access = False
    for item in line_items:
        price_id = item.get("price", {}).get("id", "")
        if price_id in PRO_PRICE_IDS:
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

    event_type = event["type"]
    data_obj = event["data"]["object"]

    if event_type == "checkout.session.completed":
        user_id = data_obj.get("client_reference_id")
        if not user_id:
            logger.warning("checkout.session.completed without client_reference_id")
            return {"status": "ignored"}

        # Fetch line items to determine what was purchased
        session = stripe.checkout.Session.retrieve(
            data_obj["id"], expand=["line_items"]
        )
        items = session.get("line_items", {}).get("data", [])
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
            await _update_clerk_metadata(user_id, {"tier": "free", "api_access": False})

    elif event_type == "customer.subscription.deleted":
        customer_id = data_obj.get("customer")
        user_id = await _find_clerk_user_by_customer(customer_id)
        if user_id:
            await _update_clerk_metadata(user_id, {"tier": "free", "api_access": False})

    return {"status": "ok"}


async def _find_clerk_user_by_customer(customer_id: str) -> str | None:
    """Look up Clerk user ID by Stripe customer ID stored in public_metadata."""
    if not customer_id:
        return None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.clerk.com/v1/users",
                headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
                params={"limit": 100},
            )
            if resp.status_code != 200:
                return None
            users = resp.json()
            if isinstance(users, dict):
                users = users.get("data", [])
            for user in users:
                meta = user.get("public_metadata", {})
                if meta.get("stripe_customer_id") == customer_id:
                    return user["id"]
    except Exception as e:
        logger.error("Error finding Clerk user: %s", e)
    return None

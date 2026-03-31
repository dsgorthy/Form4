from __future__ import annotations

import hashlib
import secrets
from datetime import datetime
import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth import UserContext, get_current_user
from api.config import CLERK_SECRET_KEY

router = APIRouter(prefix="/api/v1/api-keys", tags=["api-keys"])


class CreateKeyRequest(BaseModel):
    name: str = "Untitled Key"


async def _get_private_metadata(user_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to fetch user")
        return resp.json().get("private_metadata", {})


async def _save_private_metadata(user_id: str, metadata: dict) -> None:
    async with httpx.AsyncClient() as client:
        resp = await client.patch(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={
                "Authorization": f"Bearer {CLERK_SECRET_KEY}",
                "Content-Type": "application/json",
            },
            json={"private_metadata": metadata},
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to save metadata")


@router.get("")
async def list_api_keys(
    user: UserContext = Depends(get_current_user),
) -> dict:
    """List API keys for the authenticated user (hashes hidden)."""
    if not user.user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    private_meta = await _get_private_metadata(user.user_id)
    keys = private_meta.get("api_keys", [])

    # Migrate old format if needed
    old_hashes = private_meta.get("api_key_hashes", [])
    if old_hashes and not keys:
        keys = [
            {"id": f"legacy_{i}", "name": "Legacy Key", "hint": "****", "created_at": None}
            for i, _ in enumerate(old_hashes)
        ]

    return {
        "keys": [
            {"id": k["id"], "name": k["name"], "hint": k["hint"], "created_at": k.get("created_at")}
            for k in keys
        ],
        "max_keys": 3,
    }


@router.post("")
async def create_api_key(
    body: CreateKeyRequest,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Generate a new named API key for the authenticated user."""
    if not user.user_id:
        raise HTTPException(status_code=401, detail="Authentication required")
    if not user.api_access:
        raise HTTPException(status_code=403, detail="API add-on subscription required")

    private_meta = await _get_private_metadata(user.user_id)
    keys = private_meta.get("api_keys", [])

    # Migrate old format
    old_hashes = private_meta.get("api_key_hashes", [])
    if old_hashes and not keys:
        keys = [
            {"id": f"legacy_{i}", "name": "Legacy Key", "hash": h, "hint": "****", "created_at": None}
            for i, h in enumerate(old_hashes)
        ]

    if len(keys) >= 3:
        raise HTTPException(status_code=400, detail="Maximum 3 API keys allowed. Revoke an existing key first.")

    # Generate key
    random_part = secrets.token_hex(16)
    api_key = f"ie_{user.user_id}_{random_part}"
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    key_id = f"key_{secrets.token_hex(4)}"
    hint = f"...{random_part[-4:]}"

    keys.append({
        "id": key_id,
        "name": body.name[:50],
        "hash": key_hash,
        "hint": hint,
        "created_at": datetime.utcnow().strftime("%Y-%m-%d"),
    })

    await _save_private_metadata(user.user_id, {"api_keys": keys, "api_key_hashes": []})

    return {
        "api_key": api_key,
        "id": key_id,
        "name": body.name[:50],
        "message": "Save this key — it won't be shown again.",
    }


@router.delete("/{key_id}")
async def revoke_api_key(
    key_id: str,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Revoke a single API key by ID."""
    if not user.user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    private_meta = await _get_private_metadata(user.user_id)
    keys = private_meta.get("api_keys", [])

    new_keys = [k for k in keys if k["id"] != key_id]
    if len(new_keys) == len(keys):
        raise HTTPException(status_code=404, detail="Key not found")

    await _save_private_metadata(user.user_id, {"api_keys": new_keys, "api_key_hashes": []})

    return {"message": "API key revoked."}


@router.delete("")
async def revoke_all_keys(
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Revoke all API keys for the authenticated user."""
    if not user.user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    await _save_private_metadata(user.user_id, {"api_keys": [], "api_key_hashes": []})

    return {"message": "All API keys revoked."}

from __future__ import annotations

from sqids import Sqids

# Different alphabets per entity type prevent cross-type ID reuse.
# Lowercase-only (26 chars) so URLs stay case-insensitive.
_trade_sqids = Sqids(alphabet="k3mwf8jv2npx7hqr9styz6bgcd", min_length=6)
_insider_sqids = Sqids(alphabet="x7hqr9styz6bgcdwf8jv2npk3m", min_length=6)
_notification_sqids = Sqids(alphabet="bgcdwf8jv2npk3mx7hqr9styz6", min_length=6)


def encode_trade_id(id: int) -> str:
    return _trade_sqids.encode([id])


def decode_trade_id(encoded: str) -> int | None:
    result = _trade_sqids.decode(encoded)
    return result[0] if result else None


def encode_insider_id(id: int) -> str:
    return _insider_sqids.encode([id])


def decode_insider_id(encoded: str) -> int | None:
    result = _insider_sqids.decode(encoded)
    return result[0] if result else None


def encode_notification_id(id: int) -> str:
    return _notification_sqids.encode([id])


def decode_notification_id(encoded: str) -> int | None:
    result = _notification_sqids.decode(encoded)
    return result[0] if result else None


def encode_response_ids(items: list[dict], trade: bool = True, insider: bool = True) -> list[dict]:
    """Encode all ID fields in a list of response dicts at the API boundary."""
    for item in items:
        if trade and item.get("trade_id") is not None:
            item["trade_id"] = encode_trade_id(item["trade_id"])
        if insider and item.get("insider_id") is not None:
            item["insider_id"] = encode_insider_id(item["insider_id"])
        if insider and item.get("primary_insider_id") is not None:
            item["primary_insider_id"] = encode_insider_id(item["primary_insider_id"])
        controlled_by = item.get("controlled_by")
        if insider and controlled_by and controlled_by.get("insider_id") is not None:
            controlled_by["insider_id"] = encode_insider_id(controlled_by["insider_id"])
    return items

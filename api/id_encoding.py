from __future__ import annotations

from sqids import Sqids

# Different alphabets per entity type prevent cross-type ID reuse.
# Lowercase-only (26 chars) so URLs stay case-insensitive.
_trade_sqids = Sqids(alphabet="k3mwf8jv2npx7hqr9styz6bgcd", min_length=6)
_insider_sqids = Sqids(alphabet="x7hqr9styz6bgcdwf8jv2npk3m", min_length=6)
_notification_sqids = Sqids(alphabet="bgcdwf8jv2npk3mx7hqr9styz6", min_length=6)


def _decode_canonical(sqids: Sqids, encoded: str) -> int | None:
    """Decode, then require the input to be the ID's canonical encoding.

    Sqids decoding is not injective: many strings decode to the same number, and
    only one of them is what encode() produces. Without this check every string
    made of alphabet characters resolves to some real row —

        /filing/zzz  /filing/kkk  /filing/mmm  /filing/333

    all decoded to 624 and served the same IFF filing, and /insider/…-zzz served
    Sylebra Capital Ltd to anyone who mistyped a URL. Checking that the row
    exists does not help: 624 exists, which is exactly why it rendered.

    Two things this prevents. A mistyped or truncated URL lands on a stranger's
    page instead of a 404, and every filing acquires an unbounded number of
    alias URLs serving identical content — duplicate content at the scale of the
    alphabet, on the surface Google crawls most.

    Re-encoding is the check the sqids docs prescribe for exactly this.
    """
    result = sqids.decode(encoded)
    if not result:
        return None
    value = result[0]
    if sqids.encode([value]) != encoded:
        return None
    return value


def encode_trade_id(id: int) -> str:
    return _trade_sqids.encode([id])


def decode_trade_id(encoded: str) -> int | None:
    return _decode_canonical(_trade_sqids, encoded)


def encode_insider_id(id: int) -> str:
    return _insider_sqids.encode([id])


def decode_insider_id(encoded: str) -> int | None:
    return _decode_canonical(_insider_sqids, encoded)


def encode_notification_id(id: int) -> str:
    return _notification_sqids.encode([id])


def decode_notification_id(encoded: str) -> int | None:
    return _decode_canonical(_notification_sqids, encoded)


def identifier_from_slug(identifier: str) -> str:
    """Extract the stable ID from an SEO slug URL.

    Insider URLs are /insider/{name-slug}-{id} so the legal name is in the
    path for search, while the sqid (or CIK) stays authoritative for lookup.
    The ID is always the final hyphen-delimited segment, which is unambiguous
    because every sqid alphabet here is lowercase-alphanumeric with no hyphen,
    and CIKs are numeric. Names containing hyphens ("Smith-Jones") are
    therefore safe.

    Bare IDs pass through unchanged, so pre-slug URLs and external links keep
    working — no redirect table, no new column, no uniqueness problem.
    """
    if not identifier:
        return identifier
    return identifier.rsplit("-", 1)[-1] if "-" in identifier else identifier


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

"""Only an ID's canonical encoding may resolve to it.

Sqids decoding is not injective. Before this was enforced, every string made of
alphabet characters resolved to a real row:

    /filing/zzz  /filing/kkk  /filing/mmm  /filing/333   -> all trade 624 (IFF)
    /insider/nobody-at-all-zzz                           -> Sylebra Capital Ltd

Two failures in one. A mistyped URL serves a stranger's page instead of 404ing,
and every entity gains an unbounded set of alias URLs serving identical content
— duplicate content at the scale of the alphabet, on the surface Google crawls
most heavily (30,200 insider and 13,500 filing page views a week).

Checking the row exists does not fix it; 624 exists.
"""

from __future__ import annotations

import pytest

from api.id_encoding import (
    decode_insider_id,
    decode_notification_id,
    decode_trade_id,
    encode_insider_id,
    encode_notification_id,
    encode_trade_id,
)

CODECS = [
    ("trade", encode_trade_id, decode_trade_id),
    ("insider", encode_insider_id, decode_insider_id),
    ("notification", encode_notification_id, decode_notification_id),
]


@pytest.mark.parametrize("name,enc,dec", CODECS, ids=[c[0] for c in CODECS])
def test_canonical_encodings_round_trip(name, enc, dec):
    """The happy path must be untouched — every real URL still resolves."""
    for value in (1, 2, 624, 27044, 44733, 1757412, 1812229, 9_999_999):
        assert dec(enc(value)) == value


@pytest.mark.parametrize("name,enc,dec", CODECS, ids=[c[0] for c in CODECS])
def test_non_canonical_strings_are_refused(name, enc, dec):
    """The reported bug: junk that happens to decode must not resolve."""
    for junk in ("zzz", "kkk", "mmm", "333", "abc", "xxxxxx"):
        decoded = dec(junk)
        assert decoded is None or enc(decoded) == junk, (
            f"{name}: {junk!r} resolved to {decoded} whose canonical form is "
            f"{enc(decoded)!r}"
        )


def test_the_exact_urls_from_the_report():
    assert decode_trade_id("zzz") is None
    assert decode_trade_id("kkk") is None
    assert decode_trade_id("333") is None
    assert decode_insider_id("zzz") is None


def test_garbage_outside_the_alphabet_still_returns_none():
    for s in ("", "!!!", "nobody-at-all", "0002123683"):
        assert decode_insider_id(s) is None


def test_two_different_junk_strings_no_longer_collide_on_one_row():
    """The duplicate-content half of the bug."""
    resolved = [decode_trade_id(s) for s in ("zzz", "kkk", "mmm", "333")]
    assert set(resolved) == {None}

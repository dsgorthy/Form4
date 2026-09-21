"""Is this filer name the same person as that one?

Form 4 filers appear under several `insider_id`s because the same name was
filed in different orders or spellings -- "De Lima Filho Pedro Batista" and
"Pedro Batista de Lima Filho", "JPMORGAN CHASE & CO" and "J P MORGAN CHASE &
CO". The filed reporting-owner CIK is the identity; the name is how a row
was keyed when it was created. Two rows with one CIK and compatible names
are one filer. Two rows with one CIK and unrelated names are a JOINT FILING
whose lines were all stamped with the first owner's CIK (5,624 accessions,
measured 2026-09-21) -- never merge those.

`insiders.cik` is not the identity either: it disagrees with the filed owner
CIK on 110,004 rows and is NULL on 105,367 (many carry the ISSUER's CIK).
Key on `trades.rptowner_cik` / Silver's `rptowner_cik`.

Pure functions; no I/O. Shared by the identity-merge script and the
StockTwits generator's co-filer guard.
"""
from __future__ import annotations

import re

_STOP = frozenset({"inc", "llc", "lp", "ltd", "co", "corp", "the", "of", "and", "de", "jr", "sr", "ii", "iii", "iv", "mr", "mrs", "ms", "dr"})


def name_tokens(name: str | None) -> frozenset[str]:
    """The name as a set of tokens, case- and order-insensitive, without
    punctuation or the corporate/suffix words that vary between filings."""
    if not name:
        return frozenset()
    raw = re.sub(r"[^a-z0-9 ]", " ", name.lower()).split()
    return frozenset(t for t in raw if len(t) > 1 and t not in _STOP)


def same_person(a: str | None, b: str | None) -> bool:
    """Identical token sets: "SMITH JOHN A" and "John A. Smith"."""
    ka, kb = name_tokens(a), name_tokens(b)
    return bool(ka) and ka == kb


def compatible(a: str | None, b: str | None) -> bool:
    """Same person, or one name is a sub-form of the other ("Smith John" and
    "Smith John A"), or they share at least two tokens -- enough for a
    corporate rename that keeps the core ("Forest Laboratories Inc" and
    "Forest Laboratories, LLC") but not for two different people."""
    ka, kb = name_tokens(a), name_tokens(b)
    if not ka or not kb:
        return False
    if ka == kb or ka <= kb or kb <= ka:
        return True
    return len(ka & kb) >= 2


def classify_group(names: list[str]) -> str:
    """For rows that share one filed CIK: 'identical', 'variants' (all
    compatible with the most-common form), or 'different' (review by hand --
    a rename, or a joint filing mis-stamped with one owner's CIK)."""
    keys = [name_tokens(n) for n in names]
    if not keys or not keys[0]:
        return "different"
    if all(k == keys[0] for k in keys):
        return "identical"
    if all(compatible(names[0], n) for n in names[1:]):
        return "variants"
    return "different"

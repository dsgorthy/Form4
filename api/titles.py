"""The one place a filer's job title is turned into something readable.

WHY THIS EXISTS

SEC filers type the title field by hand and nothing validates it, so the corpus
carries "GroupPresident IntlVehiclePmts", "Director,TenPercentOwner; Director",
"Dir", "Other" and "EVP, GenCnsl & Secy" as literal stored values. All of them
shipped to public surfaces exactly as stored — the CPAY filing page rendered
"Alan King, GroupPresident IntlVehiclePmts at CORPAY, INC." on 2026-08-21.

5% of filings since 2026-01-01 carry a run-together CamelCase boundary.
"TenPercentOwner" alone is 11,703 of the 13,672.

WHY NOT `normalized_title`

That column is fully populated and has no run-together values, which makes it
look like the answer. It is not: it is a CLASSIFICATION, bucketing every title
into Director / CEO / CFO / 10% Owner / Other. "GroupPresident IntlVehiclePmts"
normalises to "Other", which throws away the actual role and then renders as
"Insider". Use it to group and filter; never to display.

This module is the single definition, mirroring the api/ratings.py pattern.
pipelines/insider_study/annotate_trade.py imports from here rather than keeping
a second copy, so the social posts and the website cannot drift.
"""
from __future__ import annotations

import re
from typing import Optional

__all__ = ["clean_title", "TITLE_FIXUPS", "TITLE_UNKNOWN"]


#: Whole-title abbreviations. Matched case-insensitively against a full
#: component, so "dir" becomes Director but "Dir of Ops" is handled word-wise.
TITLE_FIXUPS = {
    "dir": "Director", "dir.": "Director",
    "pres": "President", "ceo": "CEO", "cfo": "CFO", "coo": "COO",
    "evp": "EVP", "svp": "SVP", "vp": "VP",
    "off": "Officer", "10%": "10% Owner",
}

#: Values that mean "we do not know", which must read as the generic noun
#: rather than as a job title. "Unknown Goldman Sachs Group Inc. bought $10.3M"
#: shipped as a public post; 892 rows carried this in 2026 alone.
TITLE_UNKNOWN = {"unknown", "n/a", "na", "none", "null", "-", "--", "other"}

#: Word-level expansions applied after a run-together title is split apart.
#: Only abbreviations that actually appear in the corpus — this is not a
#: general dictionary, and a wrong expansion is worse than none.
_WORD_FIXUPS = {
    "intl": "International", "internatl": "International",
    "pmts": "Payments", "pmt": "Payment",
    "svcs": "Services", "svc": "Service",
    "mgmt": "Management", "mktg": "Marketing",
    "ops": "Operations", "admin": "Administration",
    "acctg": "Accounting", "cnsl": "Counsel",
    "treas": "Treasurer", "asst": "Assistant",
    "corp": "Corporate", "grp": "Group", "div": "Division",
    "gen": "General", "exec": "Executive", "sr": "Senior",
    "prin": "Principal", "off": "Officer", "offcr": "Officer",
    "dir": "Director", "pres": "President", "chrmn": "Chairman",
    "secy": "Secretary", "sec": "Secretary", "tech": "Technology",
    "comml": "Commercial", "bus": "Business", "dev": "Development",
}

_RUN_TOGETHER = re.compile(r"(?<=[a-z])(?=[A-Z])")


def _clean_component(part: str) -> Optional[str]:
    """One comma- or semicolon-separated piece of a title."""
    part = part.strip().strip(".").strip()
    if not part:
        return None
    key = part.lower()
    if key in TITLE_UNKNOWN:
        return None
    if key in TITLE_FIXUPS:
        return TITLE_FIXUPS[key]

    # "GroupPresident IntlVehiclePmts" -> "Group President Intl Vehicle Pmts".
    part = _RUN_TOGETHER.sub(" ", part)

    # By far the most common run-together value, and one we already have a
    # canonical rendering for.
    if part.lower() == "ten percent owner":
        return "10% Owner"

    words = []
    for w in part.split():
        lw = w.lower().strip(".")
        if lw in _WORD_FIXUPS:
            words.append(_WORD_FIXUPS[lw])
        elif lw in TITLE_FIXUPS:
            words.append(TITLE_FIXUPS[lw])
        else:
            words.append(w)
    return " ".join(words) or None


def clean_title(title: Optional[str]) -> str:
    """Expand the abbreviations SEC filers use and unjam run-together titles.

    Composite titles are the norm, not the exception — "Director,TenPercentOwner
    ; Director" is a real stored value — so this splits on both separators,
    cleans each piece, drops the ones that mean nothing, and de-duplicates.

    Always returns something renderable; "Insider" when there is nothing usable.
    """
    raw = (title or "").strip()
    if not raw:
        return "Insider"
    if raw.lower().rstrip(".") in TITLE_UNKNOWN:
        return "Insider"

    seen, parts = set(), []
    for chunk in re.split(r"[;,]", raw):
        cleaned = _clean_component(chunk)
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            parts.append(cleaned)
    return ", ".join(parts) if parts else "Insider"

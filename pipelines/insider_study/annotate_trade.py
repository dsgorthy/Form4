#!/usr/bin/env python3
"""Turn a filing into the two or three facts a person actually wants.

WHY THIS IS ITS OWN MODULE

The daily email and the per-ticker social posts need the same sentences, and
the alternative is writing them twice and watching them drift. It is also the
piece that decides what the product sounds like, so it is worth one careful
place rather than two careless ones.

WHAT WE LEARNED FROM CEO WATCHER

Their annotations are excellent product and mediocre prediction, and both
halves matter.

Tested against our own returns, their two most-repeated tags are close to
worthless: "largest purchase ever" is worth +0.19% at 30d against +0.05% for a
routine-sized buy, and "their last purchase was N days ago" is flat across
every dormancy bucket (-0.20% to +0.52%, non-monotone). By comparison our
career grade separates A+/A from the rest by ~2.9pp at 7d.

But their emails are far more readable than ours, and that is not a small
thing. Every trade arrives with two to four plain facts, so a reader can form
their own view instead of being handed a letter and asked to trust it.

So the split is deliberate: RANK on what predicts, ANNOTATE for what explains.
Selection uses the grade. These lines are there to make the selection legible,
and none of them should be read as the reason a trade was chosen.

ORDERING

Annotations are emitted strongest-first by how much they actually moved
returns in our data, not by how dramatic they sound. Size and role lead
because they measured highest on no-history filers (role spread ~1.7pp, size
~1.8pp, and sector ~2.4pp); cluster comes last because it measured flat.
"""
from __future__ import annotations

from typing import Optional

__all__ = ["annotate", "headline", "clean_title"]


def _money(v: Optional[float]) -> str:
    if not v:
        return "$0"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}K"
    # Share prices live down here. Rounding $4.35 to "$4" made the fill-vs-now
    # line read as though we could not be bothered to look it up.
    if v >= 100:
        return f"${v:,.0f}"
    return f"${v:,.2f}"


def _pct(v: Optional[float]) -> str:
    return f"{v * 100:+.0f}%" if v is not None else ""


#: SEC filers abbreviate freely and it reads as sloppiness in a public post.
_TITLE_FIXUPS = {
    "dir": "Director", "dir.": "Director",
    "pres": "President", "ceo": "CEO", "cfo": "CFO", "coo": "COO",
    "evp": "EVP", "svp": "SVP", "vp": "VP",
    "off": "Officer", "10%": "10% Owner",
}


def clean_title(title: Optional[str]) -> str:
    """Expand the abbreviations SEC filers use, leave everything else alone."""
    raw = (title or "").strip()
    if not raw:
        return "Insider"
    key = raw.lower().rstrip(".")
    if key in _TITLE_FIXUPS:
        return _TITLE_FIXUPS[key]
    return raw


def headline(t: dict) -> str:
    """One line: who, what, how much."""
    side = "bought" if t.get("signal_class") == "discretionary_buy" else "sold"
    title = clean_title(t.get("insider_title") or t.get("title"))
    name = (t.get("insider_name") or "").strip()
    # Entity filers often carry their own name inside the title — "10% Owner
    # (Magnetar Financial LLC)" — and appending it again reads as a typo.
    if name and name.lower() not in title.lower():
        who = f"{title} ({name})"
    else:
        who = title
    return f"{who} at {t.get('ticker')} {side} {_money(t.get('value'))}"


def annotate(t: dict, max_lines: int = 4) -> list[str]:
    """Plain-English context for one filing, strongest signal first.

    `t` is a trades row joined with whatever context is available. Every field
    is optional — a missing one drops its line rather than guessing, because a
    fabricated annotation is worse than a short one.
    """
    out: list[str] = []
    is_buy = t.get("signal_class") == "discretionary_buy"

    # --- Role. Highest-measured single spread on filers with no history:
    # officers and directors are positive at every trade size, 10% owners are
    # negative above $1M. Worth saying who this actually is.
    title = (t.get("insider_title") or t.get("title") or "").upper()
    if "10%" in title and t.get("value") and t["value"] > 1_000_000:
        # Only meaningful as a caution on the buy side, where we measured 10%
        # owners at -0.73% above $1M. A large institutional SALE is usually a
        # fund rebalancing, which is worth saying plainly instead of dressing
        # up as a signal.
        out.append("Large 10% owner buy — historically the weakest setup we track."
                   if is_buy else
                   "10% holder trimming — often a fund rebalancing, not a view.")
    elif any(k in title for k in ("CEO", "CHIEF EXECUTIVE")):
        out.append("Chief executive — buying their own company.")
    elif any(k in title for k in ("CFO", "CHIEF FINANCIAL")):
        out.append("CFO — the person who sees the numbers first.")

    # --- Conviction relative to what they already hold. A purchase that moves
    # someone's own position materially says more than a large dollar figure.
    qty, owned_after = t.get("qty"), t.get("shares_owned_after")
    if qty and owned_after and owned_after > qty:
        before = owned_after - qty
        if before > 0:
            change = qty / before
            if change >= 0.25:
                verb = "increased" if is_buy else "cut"
                out.append(f"This {verb} their stake by {change * 100:.0f}%.")

    # --- First-ever beats largest-ever, which is the opposite of the
    # convention. A first purchase averaged +0.52% at 30d; a largest-ever
    # purchase only +0.19%, barely above the +0.05% routine baseline.
    # is_largest_ever is computed over the trade's own side, so the wording has
    # to follow it. Saying "largest purchase" on a $235M SALE is the kind of
    # error that is invisible in code review and obvious in a public post.
    if t.get("is_first_ever"):
        out.append("First time they've ever bought this stock." if is_buy
                   else "First time they've ever sold any.")
    elif t.get("is_largest_ever"):
        out.append("Largest purchase they've ever made in it." if is_buy
                   else "Largest sale they've ever made in it.")

    # --- Where the stock sits. Buying weakness carried a positive coefficient
    # in the trade model; requiring strength (above both moving averages)
    # measured worse than ignoring it.
    dip1, dip3 = t.get("dip_1mo"), t.get("dip_3mo")
    if is_buy and dip1 is not None and dip1 <= -0.15:
        out.append(f"Bought into a {_pct(dip1)} month.")
    elif is_buy and dip3 is not None and dip3 <= -0.25:
        out.append(f"Bought after a {_pct(dip3)} quarter.")
    elif not is_buy and dip1 is not None and dip1 >= 0.20:
        out.append(f"Sold into a {_pct(dip1)} month.")

    # --- Reversal. Someone breaking their own pattern is rare and is one of
    # the few behavioural flags that survived our PIT audit clean.
    if t.get("is_rare_reversal"):
        prior = t.get("consecutive_sells_before") or 0
        if prior and is_buy:
            out.append(f"Bought after {prior} straight sales — a genuine reversal.")
        else:
            out.append("Breaks their own established pattern.")

    # --- Has the move already happened? Not predictive, purely practical, and
    # the single most useful line in a CEO Watcher email: it tells a reader
    # whether they missed it.
    tx, cur = t.get("price"), t.get("current_price")
    if tx and cur and tx > 0:
        gap = cur / tx - 1
        if abs(gap) >= 0.05:
            direction = "above" if gap > 0 else "below"
            out.append(f"Already {abs(gap) * 100:.0f}% {direction} their fill "
                       f"({_money(tx)} → {_money(cur)}).")

    # --- Others doing the same thing. Last because it measured flat on
    # no-history filers (0.62 / 0.58 / 0.54 / 0.55 across cluster buckets).
    cluster = t.get("pit_cluster_size") or 0
    if cluster >= 2:
        verb = "buying" if is_buy else "selling"
        out.append(f"{cluster} other insiders {verb} it this month.")

    return out[:max_lines]

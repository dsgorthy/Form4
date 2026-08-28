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

import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.ownership import position_change  # noqa: E402
from api.titles import TITLE_FIXUPS, TITLE_UNKNOWN, clean_title  # noqa: E402

__all__ = ["annotate", "context_lines", "headline", "clean_title"]


def _shares(v: float) -> str:
    """Share counts, at the precision a reader can hold in their head."""
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return f"{v:,.0f}"


def _money(v: Optional[float]) -> str:
    if not v:
        return "$0"
    if v >= 999_500:   # below the million, but rounds to "$1000K" in the K form
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


# Title cleaning lives in api/titles.py — one definition, so the social posts
# and the website cannot disagree about what somebody's job is. Re-exported
# here because this module's callers have always imported it from here.
_TITLE_FIXUPS = TITLE_FIXUPS
_TITLE_UNKNOWN = TITLE_UNKNOWN


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


def _md(iso: Optional[str]) -> str:
    """8/24 — the form a reader scans, not 2026-08-24."""
    if not iso:
        return ""
    y, m, d = iso[:10].split("-")
    return f"{int(m)}/{int(d)}"


def _ordinal(n: int) -> str:
    return {2: "Second", 3: "Third", 4: "Fourth", 5: "Fifth"}.get(n, f"{n}th")


def context_lines(t: dict) -> list[str]:
    """The multi-day facts, strongest first.

    These are deliberately separate from the single-filing annotations below.
    Everything below describes ONE Form 4; everything here describes the run it
    belongs to, and on 2026-08-25 the run was the story on four of nine posts.

    Every line is guarded on a field the caller may simply not have. A caller
    that does not fetch context gets the old one-day behaviour rather than a
    fabricated program — see attach_context in generate_stocktwits_posts.py.
    """
    out: list[str] = []
    is_buy = t.get("signal_class") == "discretionary_buy"

    # --- FIRST, ALWAYS: the sale that is really an option cash-out.
    #
    # This has to outrank every flattering line beneath it. CPAY led the
    # 2026-08-25 feed as "CEO sold $49.3M / Largest sale they've ever made"
    # when 450,000 options had been exercised four days earlier and ~75% of
    # what he sold came straight out of that. The exercise_and_sell tag spans
    # three days and this tranche was four days out, so nothing caught it.
    # Disclosing it is better than suppressing it: the sale is real, and the
    # exercise is the context that makes it readable.
    if not is_buy and t.get("mech_date"):
        qty = t.get("mech_qty") or 0
        if qty:
            out.append(f"Follows a {_shares(qty)}-share option exercise "
                       f"on {_md(t['mech_date'])}.")
        else:
            out.append(f"Follows a stock grant on {_md(t['mech_date'])}.")

    # --- The run this filing belongs to, broken out leg by leg. The headline
    # carries the total; this is where a reader sees it was two decisions on
    # two days rather than one big one.
    legs = t.get("prog_legs") or []
    n = t.get("prog_n_filings") or 0
    if n >= 2 and legs:
        if len(legs) == 2:
            (d1, v1), (d2, v2) = legs[0], legs[1]
            out.append(f"{_money(v1)} on {_md(d1)}, then another "
                       f"{_money(v2)} on {_md(d2)}.")
        elif len(legs) <= 4:
            amounts = ", ".join(_money(v) for _, v in legs[:-1])
            out.append(f"{_ordinal(len(legs))} disclosure since "
                       f"{_md(legs[0][0])}: {amounts}, then {_money(legs[-1][1])}.")
        else:
            # Past four, the list stops being a fact and becomes a wall.
            # CRWD rendered twelve amounts in one bullet -- "$3.67M, $3.61M,
            # $3.71M, $7.39M, $4.23M, ..." -- which nobody reads and which
            # tells the reader nothing the range does not.
            vals = [v for _, v in legs]
            out.append(f"{_ordinal(len(legs))} disclosure since "
                       f"{_md(legs[0][0])}, {_money(min(vals))} to "
                       f"{_money(max(vals))} a time.")

    # --- Anyone else at the same company doing the same thing this week.
    # day_cluster_n only ever saw inside one filing date, so a second insider
    # buying the next session was invisible.
    if (t.get("win_cluster_n") or 0) >= 2:
        peer, peer_val = t.get("peer_name"), t.get("peer_value")
        side = "bought" if is_buy else "sold"
        if peer and peer_val:
            who = clean_title(t.get("peer_title"))
            named = f"{who} {peer}" if peer.lower() not in who.lower() else who
            out.append(f"{named} {side} {_money(peer_val)} alongside them.")
        # Skipped when the caller already led with this exact count, the same
        # way the same-day cluster line is skipped below. Saying it twice on
        # one post is how the day-cluster version read before it was guarded.
        if not t.get("_cluster_led"):
            n_ins = t["win_cluster_n"]
            out.append(f"{n_ins} insiders have {side} {_money(t.get('win_cluster_value'))} "
                       f"here in the last {t.get('win_cluster_span_days', 30)} days.")

    # --- Regime. A purchase where the record holds nothing but exercises and
    # sales is a different event from a purchase in a company that is always
    # being bought. Only stated when the ticker is demonstrably covered —
    # is_first_on_record already requires prior activity on the other side, so
    # this cannot fire on a company we simply have no history for.
    if is_buy and t.get("is_first_on_record"):
        out.append("First insider purchase on record at this company.")
    if is_buy and (t.get("opp_n") or 0) >= 3:
        out.append(f"Insiders sold {_money(t.get('opp_value'))} here "
                   f"over the prior 90 days.")

    return out


def annotate(t: dict, max_lines: int = 4) -> list[str]:
    """Plain-English context for one filing, strongest signal first.

    `t` is a trades row joined with whatever context is available. Every field
    is optional — a missing one drops its line rather than guessing, because a
    fabricated annotation is worse than a short one.
    """
    out: list[str] = context_lines(t)
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
        # Both halves have to follow the trade direction. "Chief executive —
        # buying their own company" shipped on a $21.7M SALE, which is the
        # same class of error as calling a sale a largest purchase.
        out.append("Chief executive — buying their own company." if is_buy
                   else "Chief executive selling their own company.")
    elif any(k in title for k in ("CFO", "CHIEF FINANCIAL")):
        out.append("CFO — the person who sees the numbers first." if is_buy
                   else "CFO selling — the person who sees the numbers first.")

    # --- Conviction relative to what they already hold. A purchase that moves
    # someone's own position materially says more than a large dollar figure.
    #
    # The position maths lives in api.ownership because it is genuinely hard:
    # shares_owned_after is reported per ownership line, and dividing a summed
    # quantity by one line's balance produced "cut their stake by 73%" on a 3%
    # trim. Worse, the old code clamped the result at 100%, so 34 filings —
    # including a $371M Dell sale that was 65% of the stake — were about to be
    # described as selling out entirely. position_change returns None when the
    # balances cannot be reconciled, and None means say nothing.
    pc = position_change(t.get("lots") or [t], is_buy)
    if pc is not None:
        if is_buy:
            if pc.fraction >= 4.0:
                # "increased their stake by 994,205%" is arithmetically true and
                # unreadable. A multiple is the same fact a person can picture.
                out.append(f"Grew their position {pc.fraction + 1:.0f}x, "
                           f"to {_shares(pc.after)} shares.")
            elif pc.fraction >= 0.25:
                out.append(f"This increased their stake by {pc.fraction * 100:.0f}%.")
        elif pc.is_full_exit:
            out.append("This sold their entire position.")
        elif pc.fraction >= 0.25:
            out.append(f"This cut their stake by {pc.fraction * 100:.0f}%.")
        else:
            # The case that reads most misleadingly without help: a headline
            # dollar figure that is a rounding error against the position. Say
            # what they kept, or the number above is the only thing a reader
            # takes away. Charles Schwab sold $15.2M of an 84.2M-share
            # position, so the honest rounding of 0.2% is "under 1%", not the
            # "0% of their stake" that reads as a no-op.
            share = ("under 1%" if pc.fraction < 0.005
                     else f"{pc.fraction * 100:.0f}%")
            out.append(f"Still holds {_shares(pc.after)} shares — "
                       f"this was {share} of their stake.")

    # --- First-ever beats largest-ever, which is the opposite of the
    # convention. A first purchase averaged +0.52% at 30d; a largest-ever
    # purchase only +0.19%, barely above the +0.05% routine baseline.
    # is_largest_ever is computed over the trade's own side, so the wording has
    # to follow it. Saying "largest purchase" on a $235M SALE is the kind of
    # error that is invisible in code review and obvious in a public post.
    if t.get("is_first_ever"):
        out.append("First time they've ever bought this stock." if is_buy
                   else "First time they've ever sold it.")
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
    # `current_price` is the US listing's close, so the fill has to be in the
    # same units. A foreign line ("Common Shares (2330.TW)") is priced in the
    # local currency and comparing the two is meaningless, as is any gap wide
    # enough that a share-class or units mismatch is likelier than the move.
    tx, cur = t.get("price"), t.get("current_price")
    security = t.get("security_title") or ""
    foreign_listing = bool(re.search(r"\(\d{4}\.[A-Z]{2}\)", security))
    if tx and cur and tx > 0 and not foreign_listing:
        gap = cur / tx - 1
        if 0.05 <= abs(gap) <= 3.0:
            direction = "above" if gap > 0 else "below"
            out.append(f"Already {abs(gap) * 100:.0f}% {direction} their fill "
                       f"({_money(tx)} → {_money(cur)}).")

    # --- Others doing the same thing. Last because it measured flat on
    # no-history filers (0.62 / 0.58 / 0.54 / 0.55 across cluster buckets).
    #
    # Skipped when the caller has already led with the same-day cluster,
    # otherwise the post says it twice. pit_cluster_size is the fallback and
    # is deliberately backward-looking, so it undercounts a day that is still
    # in progress — fine as colour, wrong as a headline.
    if (t.get("day_cluster_n") or 1) - 1 < 2 and (t.get("win_cluster_n") or 0) < 2:
        cluster = t.get("pit_cluster_size") or 0
        if cluster >= 2:
            verb = "buying" if is_buy else "selling"
            out.append(f"{cluster} other insiders {verb} it this month.")

    return out[:max_lines]

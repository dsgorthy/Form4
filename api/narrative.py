"""Trade narrative classifier + templated-blurb generator.

Every Form 4 trade gets a narrative in three tiers of depth:

  high_signal — passes the high-signal filter (C-suite OR rare reversal OR
                largest-ever OR cluster>=3 OR A+/A grade), not scheduled.
                Full 4-field LLM narrative from trade_narrative table.

  routine     — scheduled / tax / recurring / cohen_routine. Vast majority.
                Templated 1-sentence reason. No LLM, no API calls.

  low_signal  — neither of the above. Open-market trade with no special
                flags. Short 2-sentence templated summary.

Computed on read — the LLM table only stores high_signal narratives;
templates are generated from trade flags every request. Cheap (sub-ms).
"""
from __future__ import annotations

from typing import Optional


def classify_tier(trade: dict) -> str:
    """Return 'high_signal' | 'routine' | 'low_signal' based on trade flags."""
    if _is_routine(trade):
        return "routine"
    if _is_high_signal(trade):
        return "high_signal"
    return "low_signal"


def _is_routine(t: dict) -> bool:
    return bool(
        t.get("is_10b5_1")
        or t.get("is_tax_sale")
        or t.get("is_recurring")
        or t.get("cohen_routine")
    )


def _is_high_signal(t: dict) -> bool:
    """Mirror of the high-signal filter in scripts/demo_narratives.py.
    Buy-side only — sells with these flags are interesting too but require
    a different narrative frame (TODO Phase 2)."""
    if t.get("trans_code") != "P":
        return False
    if (t.get("value") or 0) < 10_000:
        return False
    if (t.get("ticker") or "NONE") == "NONE":
        return False
    return bool(
        t.get("is_csuite")
        or t.get("is_rare_reversal")
        or t.get("is_largest_ever")
        or (t.get("pit_cluster_size") or 0) >= 3
        or t.get("pit_grade") in ("A+", "A")
        or t.get("career_grade") in ("A+", "A")
    )


def _fmt_usd(value: Optional[float]) -> str:
    if not value:
        return "$0"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}k"
    return f"${value:,.0f}"


def _role_phrase(t: dict) -> str:
    """Render the insider's role cleanly. Falls back to 'an insider'."""
    title = (t.get("title") or t.get("normalized_title") or "").strip()
    if t.get("is_csuite"):
        if title:
            return title
        return "a C-suite executive"
    if title:
        return title
    if t.get("is_director"):
        return "a director"
    return "an insider"


def _routine_blurb(t: dict) -> str:
    """Templated blurb for trades flagged scheduled / tax / recurring."""
    trans = t.get("trans_code")
    direction = "sale" if trans == "S" else ("purchase" if trans == "P" else "trade")
    amount = _fmt_usd(t.get("value"))
    role = _role_phrase(t)

    if t.get("is_10b5_1"):
        return (
            f"Scheduled {direction} ({amount}) by {role} under a pre-arranged 10b5-1 "
            "plan. Pre-set trade — not a discretionary signal on the stock."
        )
    if t.get("is_tax_sale"):
        return (
            f"Tax-driven {direction} ({amount}) by {role} — typically RSU/option "
            "vesting with shares sold to cover withholding tax. Not a directional view."
        )
    if t.get("cohen_routine"):
        return (
            f"Routine quarterly {direction} ({amount}) by {role} matching this "
            "insider's regular compensation pattern. Limited signal."
        )
    if t.get("is_recurring"):
        return (
            f"Recurring {direction} ({amount}) by {role} following this insider's "
            "established cadence. Pattern-driven, low signal."
        )
    # Should not reach here — caller ensured _is_routine(t)
    return f"Scheduled {direction} ({amount}) by {role}."


def _low_signal_blurb(t: dict) -> str:
    """Templated 1-2 sentence blurb for open-market trades that don't pass
    the high-signal filter (small buys, sells with no flags, etc.)."""
    trans = t.get("trans_code")
    amount = _fmt_usd(t.get("value"))
    role = _role_phrase(t)
    ticker = t.get("ticker", "")

    if trans == "P":
        # Small buys — note pre-existing context if any
        bits = []
        if t.get("pit_cluster_size") and t["pit_cluster_size"] >= 2:
            bits.append(f"{int(t['pit_cluster_size'])} other insiders trading {ticker} in last 30d")
        if t.get("career_grade") in ("B", "C", "D"):
            bits.append(f"insider career grade {t['career_grade']}")
        context = ("Context: " + "; ".join(bits) + ".") if bits else ""
        return (
            f"Open-market purchase of {amount} {ticker} by {role}. Below the "
            f"thresholds we flag for high-conviction alerts (no C-suite role, "
            f"rare reversal, cluster, or A-grade history). {context}"
        ).strip()
    if trans == "S":
        return (
            f"Open-market sale of {amount} {ticker} by {role}. Not flagged as "
            "scheduled, tax-driven, or recurring — but no specific bearish signal "
            "from our filters. Reasons for insider sales vary widely."
        )
    return f"{(trans or 'Trade')} of {amount} {ticker} by {role}."


def build_narrative(
    trade: dict,
    llm_narrative: Optional[dict] = None,
) -> dict:
    """Compose the narrative dict for a single trade.

    Args:
      trade: row from `trades` (and any joined columns). Must contain at
             least trans_code, ticker, value, title, and the signal flags.
      llm_narrative: optional row from trade_narrative (if a Tier A/high
             signal narrative has been generated by the LLM).

    Returns:
      {
        "tier": "high_signal" | "routine" | "low_signal",
        "summary": str,
        "price_context": str | None,
        "catalysts": str | None,
        "risks": str | None,
        "generated_at": str | None,
        "model_name": str | None,
      }
    """
    tier = classify_tier(trade)

    if tier == "high_signal" and llm_narrative and llm_narrative.get("summary"):
        return {
            "tier": "high_signal",
            "summary": llm_narrative.get("summary"),
            "price_context": llm_narrative.get("price_context"),
            "catalysts": llm_narrative.get("catalysts"),
            "risks": llm_narrative.get("risks"),
            "generated_at": llm_narrative.get("generated_at"),
            "model_name": llm_narrative.get("model_name"),
        }

    # Fallback: even high_signal trades get a placeholder until the LLM
    # pipeline catches up. Phase 2 will close that gap.
    if tier == "high_signal":
        return {
            "tier": "high_signal_pending",
            "summary": (
                "High-signal trade — passes our conviction filters but the "
                "AI narrative is still being generated. Check back shortly."
            ),
            "price_context": None,
            "catalysts": None,
            "risks": None,
            "generated_at": None,
            "model_name": None,
        }

    if tier == "routine":
        return {
            "tier": "routine",
            "summary": _routine_blurb(trade),
            "price_context": None,
            "catalysts": None,
            "risks": None,
            "generated_at": None,
            "model_name": None,
        }

    return {
        "tier": "low_signal",
        "summary": _low_signal_blurb(trade),
        "price_context": None,
        "catalysts": None,
        "risks": None,
        "generated_at": None,
        "model_name": None,
    }

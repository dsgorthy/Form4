"""
Conviction scoring for insider trading signals.

Two scoring paths:
- Reversal: driven by consecutive sells, streak break, grade, holdings change
- Composite (dip_cluster, momentum_largest): driven by role, cluster size,
  dip depth, first-ever buy, cohen routine — NO insider history needed

Derived from empirical return analysis (2020-2026):
- VP/SVP/EVP: 62% WR, +5.0% avg 30d on composite signals
- 4+ insiders + CEO cluster: 57% WR, +6.1% avg
- 40%+ dip: +5.3% avg regardless of insider role
- First-ever buy: +2.6% vs +1.2% for repeat
- Trade value $2M+: NEGATIVE signal (-1.5%) — excluded
"""

from __future__ import annotations


def _categorize_insider(title: str | None, is_csuite: bool = False) -> str:
    """Categorize insider title into a role bucket."""
    if not title:
        return "other"
    t = title.upper()
    if "CEO" in t or "CHIEF EXECUTIVE" in t:
        return "ceo"
    if "CFO" in t or "CHIEF FINANCIAL" in t:
        return "cfo"
    # VP/SVP/EVP must come before PRESIDENT check
    if "VICE PRESIDENT" in t or "SVP" in t or "EVP" in t or (" VP" in t or t.startswith("VP")):
        return "vp"
    # DIRECTOR must come before COO/CTO check ("DIRECTOR" contains "CTO")
    if "DIRECTOR" in t:
        return "director"
    if "COO" in t or "CHIEF OPERATING" in t or "CTO" in t or "CHIEF TECH" in t:
        return "csuite"
    # President without another C-suite title — exclude
    if "PRESIDENT" in t and not any(x in t for x in ["CEO", "CFO", "COO", "CTO", "CHIEF"]):
        return "president"
    if "10%" in t or "TENPERCENT" in t:
        return "10pct_owner"
    if is_csuite:
        return "csuite"
    return "other"


def compute_conviction(
    thesis: str,
    signal_grade: str | None = None,
    consecutive_sells: int | None = None,
    dip_1mo: float | None = None,
    dip_3mo: float | None = None,
    is_largest_ever: bool = False,
    above_sma50: bool = False,
    above_sma200: bool = False,
    insider_title: str | None = None,
    is_csuite: bool = False,
    holdings_pct_change: float | None = None,
    streak_break_days: int | None = None,
    cluster_size: int | None = None,
    is_first_buy: bool = False,
    is_opportunistic: bool = False,
    trade_value: float | None = None,
) -> float:
    """Compute conviction score (0-10) for an insider trade signal.

    Reversal trades score on: consecutive sells, grade, holdings, streak break.
    Composite trades score on: role, cluster, dip depth, first buy, opportunistic.
    Both filter out 10% owners and solo presidents.
    """
    score = 0.0

    # --- Insider type filter ---
    role = _categorize_insider(insider_title, is_csuite)
    if role == "10pct_owner":
        return 0.0
    if role == "president":
        return 0.0

    # --- Trade value filter (>$2M is negative signal for composite) ---
    if thesis in ("dip_cluster", "momentum_largest") and trade_value and trade_value >= 2_000_000:
        return 0.0  # -1.5% avg, 42% WR — actively bad

    if thesis == "reversal":
        return _score_reversal(
            score, role, signal_grade, consecutive_sells,
            dip_1mo, dip_3mo, is_largest_ever, above_sma50, above_sma200,
            holdings_pct_change, streak_break_days, cluster_size,
        )
    else:
        return _score_composite(
            score, role, thesis, signal_grade,
            dip_1mo, dip_3mo, is_largest_ever, above_sma50, above_sma200,
            cluster_size, is_first_buy, is_opportunistic, trade_value,
        )


def _score_reversal(
    score, role, signal_grade, consecutive_sells,
    dip_1mo, dip_3mo, is_largest_ever, above_sma50, above_sma200,
    holdings_pct_change, streak_break_days, cluster_size,
) -> float:
    """Reversal scoring — driven by insider behavior (sells→buy flip)."""

    # Thesis base
    score += 1.0

    # Signal grade (PIT)
    if signal_grade:
        grade_scores = {"A+": 3.5, "A": 3.0, "B": 2.0, "C": 1.0, "D": 0.0}
        score += grade_scores.get(signal_grade, 0.0)

    # Consecutive sells (strongest predictor)
    if consecutive_sells is not None and consecutive_sells >= 5:
        if consecutive_sells >= 50:
            score += 3.0
        elif consecutive_sells >= 20:
            score += 2.0
        elif consecutive_sells >= 10:
            score += 1.5
        else:
            score += 0.5

    # Dip depth
    best_dip = min(d for d in [dip_1mo, dip_3mo] if d is not None) if any(
        d is not None for d in [dip_1mo, dip_3mo]
    ) else None
    if best_dip is not None and best_dip <= -0.15:
        if best_dip <= -0.60:
            score += 3.0
        elif best_dip <= -0.40:
            score += 2.0
        elif best_dip <= -0.25:
            score += 1.0
        else:
            score += 0.5

    # Momentum context
    if above_sma50 and above_sma200:
        score += 0.5
    if is_largest_ever:
        score += 0.5

    # Insider type bonus
    if role == "vp":
        score += 0.5

    # Holdings % change (reversal only)
    if holdings_pct_change is not None:
        if holdings_pct_change >= 1.0:
            score += 1.0
        elif holdings_pct_change >= 0.5:
            score += 0.5

    # Streak break gap (reversal only)
    if streak_break_days is not None:
        if streak_break_days >= 730:
            score += 0.5
        elif streak_break_days >= 365:
            score += 0.25

    # Cluster size
    if cluster_size is not None and cluster_size >= 4:
        score += 0.5

    return round(min(10.0, max(0.0, score)), 2)


def _score_composite(
    score, role, thesis, signal_grade,
    dip_1mo, dip_3mo, is_largest_ever, above_sma50, above_sma200,
    cluster_size, is_first_buy, is_opportunistic, trade_value,
) -> float:
    """Composite scoring — driven by trade context, NOT insider history.

    Key insight: PIT insider grades give 70% of composite insiders grade D,
    making grade nearly useless. Instead, score on role + cluster + dip + context.
    """

    # --- Role (strongest composite predictor) ---
    # VP/SVP/EVP: 62% WR, +5.0% avg
    # CFO: 55% WR, +4.0% avg
    # CEO: 49% WR, +0.8% avg (not predictive alone)
    # Director: 50% WR, +1.5% avg
    role_scores = {
        "vp": 2.0,
        "cfo": 1.5,
        "ceo": 0.5,
        "csuite": 1.0,
        "director": 0.5,
        "other": 0.0,
    }
    score += role_scores.get(role, 0.0)

    # --- Cluster size (strong for composite) ---
    # 4+ insiders: 57% WR with CEO, +6.1%
    # 3+ insiders: 55% WR, +5.0%
    # 2+ insiders: 51% WR, +2.8%
    if cluster_size is not None:
        if cluster_size >= 4:
            score += 2.0
        elif cluster_size >= 3:
            score += 1.5
        elif cluster_size >= 2:
            score += 0.5

    # --- Dip depth (works for composite without history) ---
    # 40%+ dip: +5.3% avg
    # 25-40%: +0.7% avg
    best_dip = min(d for d in [dip_1mo, dip_3mo] if d is not None) if any(
        d is not None for d in [dip_1mo, dip_3mo]
    ) else None
    if best_dip is not None:
        if best_dip <= -0.40:
            score += 2.0
        elif best_dip <= -0.25:
            score += 1.0
        elif best_dip <= -0.15:
            score += 0.5

    # --- First-ever buy at this company ---
    # +2.6% vs +1.2% for repeat buyers
    if is_first_buy:
        score += 0.5

    # --- Opportunistic (cohen_routine = 0) ---
    # Filters out routine/scheduled buys
    if is_opportunistic:
        score += 0.5

    # --- Momentum context ---
    if above_sma50 and above_sma200:
        score += 0.5
    if is_largest_ever:
        score += 0.5

    # --- Grade (reduced weight for composite — PIT grades are mostly D) ---
    # Only give bonus for A (the few insiders with proven PIT records)
    if signal_grade == "A":
        score += 1.0
    elif signal_grade == "B":
        score += 0.5

    return round(min(10.0, max(0.0, score)), 2)


def pit_score_to_grade(blended_score: float | None) -> str | None:
    """Convert PIT blended_score to a letter grade.

    v2 thresholds (Bayesian scorer):
      A+ (≥2.5), A (≥2.0), B (≥1.2), C (≥0.6), D (≥0.0), None (no data)
    """
    if blended_score is None:
        return None
    if blended_score >= 2.5:
        return "A+"
    if blended_score >= 2.0:
        return "A"
    if blended_score >= 1.2:
        return "B"
    if blended_score >= 0.6:
        return "C"
    return "D"


# Minimum conviction to enter a position
MIN_CONVICTION = 5.0

# Minimum conviction advantage to replace an open position
REPLACEMENT_ADVANTAGE = 1.5

"""
Conviction scoring for CW insider trading signals.

Combines predictive indicators into a single score (0-10) used for:
1. Minimum entry threshold (filter out low-conviction noise)
2. Position replacement when at capacity (swap weakest for stronger)
3. Position sizing tiers (future: size proportional to conviction)

Derived from empirical return analysis (2020-2026):
- Consecutive sells: strong predictor (50+ sells = 66% WR, +4.2% avg 30d)
- Dip depth: very strong predictor (-60%+ dip = +14.3% avg 30d)
- Signal grade: moderate predictor (A/B >> C >> D/F)
- Insider type: VP/SVP/EVP outperform (68% WR at conv 5-7); 10% owners underperform (36% WR)
- Purchase size ratio: not predictive (excluded)
"""

from __future__ import annotations


def _categorize_insider(title: str | None, is_csuite: bool = False) -> str:
    """Categorize insider title into a role bucket."""
    if not title:
        return "other"
    t = title.upper()
    # Order matters — check VP/EVP/SVP before PRESIDENT (since "VICE PRESIDENT" contains "PRESIDENT")
    # Check DIRECTOR before CTO (since "DIRECTOR" contains "CTO")
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
    # President without another C-suite title — exclude (unreliable signal)
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
) -> float:
    """Compute conviction score (0-10) for an insider trade signal.

    Higher = stronger conviction. Used for entry thresholds and
    position replacement decisions.
    """
    score = 0.0

    # --- Insider type filter (returns -999 for excluded types) ---
    role = _categorize_insider(insider_title, is_csuite)
    if role == "10pct_owner":
        return 0.0  # Always excluded — 36% WR, -0.4% avg at conv 5-7
    if role == "president":
        return 0.0  # Excluded — 50% WR at high conviction, unreliable

    # --- Thesis base (0-1) ---
    thesis_scores = {
        "reversal": 1.0,
        "dip_cluster": 0.5,
        "momentum_largest": 0.3,
    }
    score += thesis_scores.get(thesis, 0.0)

    # --- Signal grade (0-3) ---
    # NOTE: If using pre-computed signal_grade from trades table, caller must
    # ensure it's PIT (computed from insider_ticker_scores with as_of_date <= filing_date).
    # The static signal_grade on trades uses full-history track records and is NOT PIT.
    # For backtesting, pass pit_score instead and use pit_score_to_grade().
    if signal_grade:
        grade_scores = {"A": 3.0, "B": 2.0, "C": 1.0, "D": 0.0, "F": -1.0}
        score += grade_scores.get(signal_grade, 0.0)

    # --- Consecutive sells (0-3) ---
    if consecutive_sells is not None and consecutive_sells >= 5:
        if consecutive_sells >= 50:
            score += 3.0
        elif consecutive_sells >= 20:
            score += 2.0
        elif consecutive_sells >= 10:
            score += 1.5
        else:
            score += 0.5

    # --- Dip depth (0-3) ---
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

    # --- Momentum context (0-0.5) ---
    if above_sma50 and above_sma200:
        score += 0.5
    if is_largest_ever:
        score += 0.5

    # --- Insider type bonus (0-0.5) ---
    # VP/SVP/EVP: 68% WR at conv 5-7, +11.2% avg — strongest performers
    if role == "vp":
        score += 0.5

    # --- Holdings % change (0-1.0) ---
    # Only for reversals — insider CHOOSING to size up on their first buy is meaningful.
    # For dip_cluster/momentum, everyone buys big when it's cheap — not a signal.
    if holdings_pct_change is not None and thesis == "reversal":
        if holdings_pct_change >= 1.0:  # doubled or more
            score += 1.0
        elif holdings_pct_change >= 0.5:  # 50-100% increase
            score += 0.5

    # --- Streak break gap (0-0.5) ---
    # Only for reversals — longer gap since last buy = more meaningful flip.
    if streak_break_days is not None and thesis == "reversal":
        if streak_break_days >= 730:  # 2+ years
            score += 0.5
        elif streak_break_days >= 365:  # 1-2 years
            score += 0.25

    # --- Cluster size (0-0.5) ---
    # For all theses — more insiders buying = stronger collective signal
    if cluster_size is not None and cluster_size >= 4:
        score += 0.5

    return round(min(10.0, max(0.0, score)), 2)


def pit_score_to_grade(blended_score: float | None) -> str | None:
    """Convert a PIT insider_ticker_scores.blended_score to a letter grade.

    Use this instead of the pre-computed signal_grade column on trades,
    which uses full-history track records (not PIT-safe for backtesting).

    Thresholds match the scoring tiers in pit_scoring.py:
    - A: score >= 2.0 (top performers)
    - B: score >= 1.0
    - C: score >= 0.5
    - D: score >= 0.0
    - F: score < 0.0 or no data
    """
    if blended_score is None:
        return None
    if blended_score >= 2.0:
        return "A"
    if blended_score >= 1.0:
        return "B"
    if blended_score >= 0.5:
        return "C"
    if blended_score >= 0.0:
        return "D"
    return "F"


# Minimum conviction to enter a position
MIN_CONVICTION = 2.0

# Minimum conviction advantage to replace an open position
REPLACEMENT_ADVANTAGE = 1.5

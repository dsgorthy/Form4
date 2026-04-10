#!/usr/bin/env python3
"""
Point-in-Time (PIT) scoring engine for insider-ticker pairs.

v2: Bayesian shrinkage scorer with recency weighting, multi-window (7d+30d),
sell-side scoring, and pluggable scorer interface.

Key properties:
  - Point-in-time: only uses trades + returns observable as of as_of_date
  - Per-ticker: separates insider's performance at each company
  - Blended: combines ticker-specific and global track record
  - Role-adjusted: C-suite at their primary company gets a boost
  - Bayesian: shrinks estimates toward uninformative priors for small samples
  - Recency-weighted: recent trades matter more (half-life 1.5 years)

Grade mapping (0-3 scale):
  A+ (≥2.5), A (≥2.0), B (≥1.2), C (≥0.6), D (≥0.0), None (no data)
"""

from __future__ import annotations

import logging
import math
from config.database import get_connection
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scorer interface + data classes
# ---------------------------------------------------------------------------

@dataclass
class ScoringContext:
    """All inputs needed to score an insider at a ticker on a date."""
    insider_id: int
    ticker: str
    as_of_date: str
    trade_type: str = "buy"  # "buy" or "sell"
    # Observable returns: list of (trade_date, abnormal_return)
    ticker_returns_7d: list[tuple[str, float]] = field(default_factory=list)
    ticker_returns_30d: list[tuple[str, float]] = field(default_factory=list)
    ticker_returns_90d: list[tuple[str, float]] = field(default_factory=list)
    global_returns_7d: list[tuple[str, float]] = field(default_factory=list)
    global_returns_30d: list[tuple[str, float]] = field(default_factory=list)
    global_returns_90d: list[tuple[str, float]] = field(default_factory=list)
    role_at_ticker: str | None = None
    is_primary_company: bool = False


@dataclass
class ScoringResult:
    """Output of the scoring engine."""
    insider_id: int
    ticker: str
    as_of_date: str
    blended_score: float        # 0-3 scale
    ticker_score: float
    global_score: float
    ticker_weight: float
    global_weight: float
    ticker_win_rate_7d: float | None
    ticker_avg_abnormal_7d: float | None
    global_win_rate_7d: float | None
    global_avg_abnormal_7d: float | None
    ticker_trade_count: int
    global_trade_count: int
    n_observable: int
    score_7d: float
    score_30d: float
    score_90d: float
    grade: str | None
    role_weight: float
    role_at_ticker: str | None
    is_primary_company: int
    sufficient_data: int
    method: str = "bayesian_v2"


class InsiderScorer(ABC):
    """Abstract base for pluggable scoring implementations."""
    @abstractmethod
    def score(self, ctx: ScoringContext) -> ScoringResult:
        ...

    @property
    @abstractmethod
    def method_name(self) -> str:
        ...


# ---------------------------------------------------------------------------
# Grade mapping
# ---------------------------------------------------------------------------

def pit_score_to_grade(blended_score: float | None) -> str | None:
    """Convert PIT blended_score to a letter grade."""
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
    if blended_score >= 0.0:
        return "D"
    return "D"  # score is clamped to [0, 3], no F grade


# ---------------------------------------------------------------------------
# Recency weighting
# ---------------------------------------------------------------------------

RECENCY_HALF_LIFE_DAYS = 547  # ~1.5 years


def _recency_weight(trade_date: str, as_of_date: str) -> float:
    """Exponential decay weight. Half-life = 1.5 years."""
    try:
        days_ago = (datetime.strptime(as_of_date, "%Y-%m-%d") -
                    datetime.strptime(trade_date, "%Y-%m-%d")).days
        if days_ago <= 0:
            return 1.0
        return 2.0 ** (-days_ago / RECENCY_HALF_LIFE_DAYS)
    except (ValueError, TypeError):
        return 1.0


# ---------------------------------------------------------------------------
# Bayesian Scorer v2
# ---------------------------------------------------------------------------

# Beta(2,2) prior for win rate — uninformative, centered at 50%
PRIOR_ALPHA = 2.0
PRIOR_BETA = 2.0

# Shrinkage prior for avg abnormal return: N(0, sigma), strength = 3 pseudo-obs
PRIOR_RETURN_MEAN = 0.0
PRIOR_RETURN_N = 3.0

# Window weights (re-normalized when fewer windows available)
WINDOW_7D_WEIGHT = 0.40
WINDOW_30D_WEIGHT = 0.35
WINDOW_90D_WEIGHT = 0.25


def _bayesian_window_quality(
    returns: list[tuple[str, float]],
    as_of_date: str,
) -> tuple[float, float | None, float | None, float]:
    """
    Compute quality score for one return window using Bayesian shrinkage.

    Returns: (quality, win_rate_posterior, avg_return_shrunk, n_effective)
    """
    if not returns:
        return 0.0, None, None, 0.0

    # Compute recency weights
    weights = [_recency_weight(td, as_of_date) for td, _ in returns]
    values = [r for _, r in returns]
    total_weight = sum(weights)

    if total_weight < 0.1:
        return 0.0, None, None, 0.0

    # Weighted win rate with Beta(2,2) prior
    weighted_wins = sum(w for (_, r), w in zip(returns, weights) if r > 0)
    wr_posterior = (weighted_wins + PRIOR_ALPHA) / (total_weight + PRIOR_ALPHA + PRIOR_BETA)

    # Weighted average return with shrinkage
    weighted_sum = sum(r * w for (_, r), w in zip(returns, weights))
    avg_abn = weighted_sum / total_weight
    shrunk_abn = (avg_abn * total_weight + PRIOR_RETURN_MEAN * PRIOR_RETURN_N) / (total_weight + PRIOR_RETURN_N)

    # Quality components
    wr_component = max(0.0, wr_posterior - 0.50) * 4.0  # 50%→0, 75%→1.0
    ret_component = max(0.0, min(1.0, shrunk_abn * 10 + 0.3))  # -3%→0, 0%→0.3, +7%→1.0

    quality = wr_component * 0.45 + ret_component * 0.55

    return quality, wr_posterior, shrunk_abn, total_weight


def _compute_blend_weights(ticker_n_eff: float) -> tuple[float, float]:
    """Adaptive blending: sigmoid transition from global-heavy to ticker-heavy."""
    if ticker_n_eff < 1.0:
        return 0.0, 1.0  # ticker_weight, global_weight
    ticker_frac = 1.0 / (1.0 + math.exp(-(ticker_n_eff - 6.0) / 2.5))
    ticker_weight = 0.15 + 0.70 * ticker_frac  # [0.15, 0.85]
    global_weight = 1.0 - ticker_weight
    return ticker_weight, global_weight


def _role_weight(role: str | None, is_primary: bool) -> float:
    """Role-based adjustment for C-suite at primary company."""
    if not role or not is_primary:
        return 1.0
    r = role.lower()
    if any(kw in r for kw in ("ceo", "chief exec", "chairman")):
        return 1.10
    if any(kw in r for kw in ("cfo", "president")):
        return 1.08
    if any(kw in r for kw in ("coo", "evp", "svp")):
        return 1.05
    if any(kw in r for kw in ("vp",)):
        return 1.03
    if any(kw in r for kw in ("10%", "tenpercent")):
        return 0.95
    return 1.0


class BayesianScorerV2(InsiderScorer):
    """
    Bayesian shrinkage scorer with recency weighting and multi-window blending.

    Fixes the v1 bug where n < 3 → score 0.0 by using Bayesian priors that
    give meaningful (if uncertain) scores even with 1 observable trade.
    """

    @property
    def method_name(self) -> str:
        return "bayesian_v2"

    def _blend_windows(self, q7, neff7, q30, neff30, q90, neff90):
        """Blend available windows with re-normalized weights."""
        windows = []
        if neff7 > 0.1:
            windows.append((q7, WINDOW_7D_WEIGHT))
        if neff30 > 0.5:
            windows.append((q30, WINDOW_30D_WEIGHT))
        if neff90 > 0.5:
            windows.append((q90, WINDOW_90D_WEIGHT))
        if not windows:
            return 0.0
        total_w = sum(w for _, w in windows)
        return sum(q * w / total_w for q, w in windows)

    def score(self, ctx: ScoringContext) -> ScoringResult:
        # --- Ticker-specific quality per window ---
        t_q7, t_wr7, t_avg7, t_neff7 = _bayesian_window_quality(ctx.ticker_returns_7d, ctx.as_of_date)
        t_q30, _, _, t_neff30 = _bayesian_window_quality(ctx.ticker_returns_30d, ctx.as_of_date)
        t_q90, _, _, t_neff90 = _bayesian_window_quality(ctx.ticker_returns_90d, ctx.as_of_date)

        ticker_quality = self._blend_windows(t_q7, t_neff7, t_q30, t_neff30, t_q90, t_neff90)
        ticker_n_eff = t_neff7

        # --- Global quality per window ---
        g_q7, g_wr7, g_avg7, g_neff7 = _bayesian_window_quality(ctx.global_returns_7d, ctx.as_of_date)
        g_q30, _, _, g_neff30 = _bayesian_window_quality(ctx.global_returns_30d, ctx.as_of_date)
        g_q90, _, _, g_neff90 = _bayesian_window_quality(ctx.global_returns_90d, ctx.as_of_date)

        global_quality = self._blend_windows(g_q7, g_neff7, g_q30, g_neff30, g_q90, g_neff90)

        global_n_eff = g_neff7

        # --- Blend ---
        ticker_w, global_w = _compute_blend_weights(ticker_n_eff)
        base_quality = ticker_quality * ticker_w + global_quality * global_w

        # --- Role adjustment ---
        rw = _role_weight(ctx.role_at_ticker, ctx.is_primary_company)

        # --- Scale to 0-3 ---
        blended = base_quality * rw * 2.7
        blended = min(3.0, max(0.0, blended))

        # --- Sufficient data ---
        total_n = len(ctx.global_returns_7d)
        sufficient = 1 if total_n >= 1 else 0

        # --- Grade ---
        grade = pit_score_to_grade(blended) if sufficient else None

        return ScoringResult(
            insider_id=ctx.insider_id,
            ticker=ctx.ticker,
            as_of_date=ctx.as_of_date,
            blended_score=round(blended, 4),
            ticker_score=round(ticker_quality * 3.0, 4),
            global_score=round(global_quality * 3.0, 4),
            ticker_weight=round(ticker_w, 4),
            global_weight=round(global_w, 4),
            ticker_win_rate_7d=round(t_wr7, 4) if t_wr7 is not None else None,
            ticker_avg_abnormal_7d=round(t_avg7, 6) if t_avg7 is not None else None,
            global_win_rate_7d=round(g_wr7, 4) if g_wr7 is not None else None,
            global_avg_abnormal_7d=round(g_avg7, 6) if g_avg7 is not None else None,
            ticker_trade_count=len(ctx.ticker_returns_7d),
            global_trade_count=len(ctx.global_returns_7d),
            n_observable=total_n,
            score_7d=round(t_q7 * 3.0, 4),
            score_30d=round(t_q30 * 3.0, 4),
            score_90d=round(t_q90 * 3.0, 4),
            grade=grade,
            role_weight=round(rw, 4),
            role_at_ticker=ctx.role_at_ticker,
            is_primary_company=1 if ctx.is_primary_company else 0,
            sufficient_data=sufficient,
            method=self.method_name,
        )


# Default scorer instance
DEFAULT_SCORER = BayesianScorerV2()


# ---------------------------------------------------------------------------
# Standalone scoring from DB (used by backfill_live.py for new trades)
# ---------------------------------------------------------------------------

# Observable return lags — must match build_pit_scores.py
_RETURN_LAGS = {"7d": 10, "30d": 40, "90d": 100}


def compute_insider_ticker_score(
    conn: object,
    insider_id: int,
    ticker: str,
    as_of_date: str,
    **_kwargs,
) -> ScoringResult:
    """Compute PIT score for one insider+ticker using BayesianScorerV2.

    Queries DB for observable returns with proper lag, then scores.
    Used by backfill_live.py for scoring newly-inserted trades.
    """
    def _get_returns(insider_id_val, ticker_val, window, lag):
        cutoff = (datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=lag)).strftime("%Y-%m-%d")
        col = f"abnormal_{window}"
        query = f"""
            SELECT t.trade_date, tr.{col}
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ? AND t.trade_type = 'buy'
              AND t.trade_date <= ? AND tr.{col} IS NOT NULL
        """
        params = [insider_id_val, cutoff]
        if ticker_val:
            query += " AND t.ticker = ?"
            params.append(ticker_val)
        return [(r[0], r[1]) for r in conn.execute(query, params).fetchall()]

    # Role lookup
    role_row = conn.execute(
        "SELECT title FROM insider_companies WHERE insider_id=? AND ticker=?",
        (insider_id, ticker)).fetchone()
    role_at_ticker = role_row[0] if role_row else None
    is_primary = False
    if role_at_ticker:
        primary_row = conn.execute(
            "SELECT ticker FROM insider_companies WHERE insider_id=? ORDER BY trade_count DESC LIMIT 1",
            (insider_id,)).fetchone()
        is_primary = primary_row and primary_row[0] == ticker

    ctx = ScoringContext(
        insider_id=insider_id,
        ticker=ticker,
        as_of_date=as_of_date,
        ticker_returns_7d=_get_returns(insider_id, ticker, "7d", _RETURN_LAGS["7d"]),
        ticker_returns_30d=_get_returns(insider_id, ticker, "30d", _RETURN_LAGS["30d"]),
        ticker_returns_90d=_get_returns(insider_id, ticker, "90d", _RETURN_LAGS["90d"]),
        global_returns_7d=_get_returns(insider_id, None, "7d", _RETURN_LAGS["7d"]),
        global_returns_30d=_get_returns(insider_id, None, "30d", _RETURN_LAGS["30d"]),
        global_returns_90d=_get_returns(insider_id, None, "90d", _RETURN_LAGS["90d"]),
        role_at_ticker=role_at_ticker,
        is_primary_company=is_primary,
    )
    return DEFAULT_SCORER.score(ctx)


def upsert_score(conn: object, score_data: dict | ScoringResult,
                 trigger_trade_id: int | None = None):
    """Insert or update a score in insider_ticker_scores and score_history."""
    if isinstance(score_data, ScoringResult):
        s = score_data
        d = {
            "insider_id": s.insider_id, "ticker": s.ticker, "as_of_date": s.as_of_date,
            "ticker_trade_count": s.ticker_trade_count,
            "ticker_win_rate_7d": s.ticker_win_rate_7d,
            "ticker_avg_abnormal_7d": s.ticker_avg_abnormal_7d,
            "ticker_score": s.ticker_score,
            "global_trade_count": s.global_trade_count,
            "global_win_rate_7d": s.global_win_rate_7d,
            "global_avg_abnormal_7d": s.global_avg_abnormal_7d,
            "global_score": s.global_score,
            "blended_score": s.blended_score,
            "role_at_ticker": s.role_at_ticker, "role_weight": s.role_weight,
            "is_primary_company": s.is_primary_company,
            "sufficient_data": s.sufficient_data,
        }
    else:
        d = score_data

    conn.execute("""
        INSERT INTO insider_ticker_scores
            (insider_id, ticker, as_of_date,
             ticker_trade_count, ticker_win_rate_7d, ticker_avg_abnormal_7d, ticker_score,
             global_trade_count, global_win_rate_7d, global_avg_abnormal_7d, global_score,
             blended_score, role_at_ticker, role_weight,
             is_primary_company, sufficient_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (insider_id, ticker, as_of_date) DO UPDATE SET
            ticker_trade_count = excluded.ticker_trade_count,
            ticker_win_rate_7d = excluded.ticker_win_rate_7d,
            ticker_avg_abnormal_7d = excluded.ticker_avg_abnormal_7d,
            ticker_score = excluded.ticker_score,
            global_trade_count = excluded.global_trade_count,
            global_win_rate_7d = excluded.global_win_rate_7d,
            global_avg_abnormal_7d = excluded.global_avg_abnormal_7d,
            global_score = excluded.global_score,
            blended_score = excluded.blended_score,
            role_at_ticker = excluded.role_at_ticker,
            role_weight = excluded.role_weight,
            is_primary_company = excluded.is_primary_company,
            sufficient_data = excluded.sufficient_data
    """, (
        d["insider_id"], d["ticker"], d["as_of_date"],
        d["ticker_trade_count"], d["ticker_win_rate_7d"],
        d["ticker_avg_abnormal_7d"], d["ticker_score"],
        d["global_trade_count"], d["global_win_rate_7d"],
        d["global_avg_abnormal_7d"], d["global_score"],
        d["blended_score"], d["role_at_ticker"], d["role_weight"],
        d["is_primary_company"], d["sufficient_data"],
    ))

    conn.execute("""
        INSERT INTO score_history
            (insider_id, ticker, as_of_date, trigger_trade_id,
             blended_score, global_score, ticker_score, trade_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        d["insider_id"], d["ticker"], d["as_of_date"],
        trigger_trade_id,
        d["blended_score"], d["global_score"], d["ticker_score"],
        d["global_trade_count"],
    ))


def sync_to_track_records(conn: object):
    """Backward compat: sync latest PIT scores to insider_track_records."""
    logger.info("Syncing PIT scores to insider_track_records...")
    rows = conn.execute("""
        SELECT insider_id,
               SUM(blended_score * ticker_trade_count) / SUM(ticker_trade_count) as weighted_score,
               MAX(blended_score) as best_score,
               SUM(ticker_trade_count) as total_trades
        FROM insider_ticker_scores its
        WHERE as_of_date = (
            SELECT MAX(as_of_date) FROM insider_ticker_scores its2
            WHERE its2.insider_id = its.insider_id AND its2.ticker = its.ticker
        )
        AND sufficient_data = 1
        GROUP BY insider_id
    """).fetchall()
    if not rows:
        logger.info("No PIT scores to sync")
        return
    scores = [(r[0], r[1]) for r in rows]
    scores.sort(key=lambda x: x[1])
    n = len(scores)
    updated = 0
    for rank, (insider_id, weighted_score) in enumerate(scores):
        percentile = (rank + 1) / n * 100
        score = min(3.0, max(0.0, weighted_score))
        tier = 3 if percentile >= 93 else 2 if percentile >= 80 else 1 if percentile >= 67 else 0
        conn.execute(
            "UPDATE insider_track_records SET score=?, score_tier=?, percentile=? WHERE insider_id=?",
            (round(score, 4), tier, round(percentile, 2), insider_id))
        updated += 1
    conn.commit()
    logger.info("Synced %d insiders", updated)


def get_pit_score(conn: object, insider_id: int, ticker: str,
                  as_of_date: str) -> Optional[dict]:
    """Look up the most recent PIT score for an insider+ticker as of a date."""
    row = conn.execute("""
        SELECT blended_score, global_score, ticker_score,
               ticker_trade_count, global_trade_count,
               ticker_win_rate_7d, global_win_rate_7d,
               sufficient_data, role_weight, is_primary_company
        FROM insider_ticker_scores
        WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
        ORDER BY as_of_date DESC LIMIT 1
    """, (insider_id, ticker, as_of_date)).fetchone()
    if not row:
        return None
    return {
        "blended_score": row[0], "global_score": row[1], "ticker_score": row[2],
        "ticker_trade_count": row[3], "global_trade_count": row[4],
        "ticker_win_rate_7d": row[5], "global_win_rate_7d": row[6],
        "sufficient_data": row[7], "role_weight": row[8], "is_primary_company": row[9],
    }

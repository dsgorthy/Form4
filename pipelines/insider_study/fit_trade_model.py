#!/usr/bin/env python3
"""V4 trade model — grade a filing from what is observable the moment it lands.

WHY THIS EXISTS SEPARATELY FROM THE INSIDER GRADE

47% of insiders have made exactly one discretionary purchase, and 65% have made
two or fewer. There is no estimator that recovers individual skill from one
observation, so for most of the feed a per-insider grade is noise wearing a
letter. That is the honest source of the 55%-D pile in career_grade.

But a first-ever filing is not unjudgeable. It carries a role, a sector, a size,
a cluster, a position in the stock's own range — all of it known at filing_date,
none of it requiring the insider to have ever traded before. Measured over the
32,418 discretionary buys whose filer had no prior purchase:

    role      Officer/Director positive at every size (+0.49 .. +1.19)
              10% Owner mostly negative, -0.73 above $1M
    sector    Basic Materials +2.22 .. Real Estate -0.22   (2.44pp spread)
    size      non-monotone, best at $10-50k; ">$1M is bad" is almost
              entirely a 10%-owner artifact, not a size effect
    cluster   flat — 0.62 / 0.58 / 0.54 / 0.55. No signal for first filings.

So the trade grade carries the weight where the insider grade cannot, and the
two are independent estimators that happen to be displayed together.

WHY A SCORECARD AND NOT A BLACK BOX

Every coefficient has to survive two audiences: a PIT audit, and a user asking
why their filing scored what it did. A ridge fit on one-hot features gives
displayable per-level effects — "Director +0.4, Healthcare +0.6, $10-50k +0.5" —
which is simultaneously the model and the annotation the digest needs. Gradient
boosting would score marginally better and explain nothing.

Ridge rather than summing marginal means, because the features are correlated:
10% owners make the large trades, so a naive additive score double-counts the
same effect through `role` and `size`.

PIT DISCIPLINE

  - Every feature is a column on the filing itself, read at filing_date.
  - Labels use abnormal_30d, which needs 40 days to be observable; the split
    below is by time, so no test-period return can reach a training row.
  - Coefficients are fitted on the TRAIN window only and evaluated out of
    sample on a later, disjoint window.
  - Winsorization is a fixed +/-100pp clip, not a fitted percentile.

  Known soft spot, stated rather than buried: ticker_metadata.sector is the
  CURRENT sector, not the sector as of the filing. A company that changed
  classification leaks a little. Low risk, not zero.

Usage:
    python3 pipelines/insider_study/fit_trade_model.py
    python3 pipelines/insider_study/fit_trade_model.py --horizon 7d
    python3 pipelines/insider_study/fit_trade_model.py --train-end 2022-12-31
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WINSOR = 1.0          # +/-100pp. Raw per-trade SD is 47pp; clipped it is 14pp.
HORIZONS = ("7d", "14d", "30d", "60d", "90d")


# ---------------------------------------------------------------------------
# Feature construction — every level is a string so the design matrix is a
# plain one-hot and every coefficient has a name a person can read.
# ---------------------------------------------------------------------------

def role_bucket(title: str | None) -> str:
    t = (title or "").upper()
    # Order matters: seniority wins over the 10% Owner tag when both appear,
    # because an owner who is also CEO behaves like the CEO.
    if "CEO" in t or "CHIEF EXECUTIVE" in t:
        return "CEO"
    if "CFO" in t or "CHIEF FINANCIAL" in t:
        return "CFO"
    if "CHAIRMAN" in t or "PRESIDENT" in t:
        return "Chair/Pres"
    if "DIRECTOR" in t or t.startswith("DIR"):
        return "Director"
    if "10% OWNER" in t:
        return "10% Owner"
    return "Other"


def size_bucket(value: float | None) -> str:
    v = value or 0.0
    if v < 10_000:
        return "size<10k"
    if v < 50_000:
        return "size10-50k"
    if v < 250_000:
        return "size50-250k"
    if v < 1_000_000:
        return "size250k-1M"
    return "size>1M"


def cluster_bucket(n: int | None) -> str:
    c = n or 0
    if c == 0:
        return "clus0"
    if c == 1:
        return "clus1"
    if c <= 3:
        return "clus2-3"
    return "clus4+"


def dip_bucket(dip: float | None) -> str:
    # dip_1mo is stored as the drawdown over the prior month. Unknown is its
    # own level rather than an imputed zero — 32% of rows have no price
    # history, and pretending they were flat is a fabricated feature.
    if dip is None:
        return "dip_unknown"
    if dip <= -0.20:
        return "dip<-20%"
    if dip <= -0.05:
        return "dip-20..-5%"
    if dip < 0.05:
        return "dip flat"
    return "dip>+5%"


FEATURE_FUNCS = {
    "role": lambda r: role_bucket(r["normalized_title"]),
    "sector": lambda r: f"sec:{r['sector'] or 'unknown'}",
    "size": lambda r: size_bucket(r["value"]),
    "cluster": lambda r: cluster_bucket(r["pit_cluster_size"]),
    "dip": lambda r: dip_bucket(r["dip_1mo"]),
    "largest": lambda r: "largest_ever" if r["is_largest_ever"] else "not_largest",
    "csuite": lambda r: "csuite" if r["is_csuite"] else "not_csuite",
}


def build_design(rows, levels: dict[str, int] | None = None):
    """One-hot design matrix. Returns (X, y, levels, meta)."""
    feats = [[f(r) for f in FEATURE_FUNCS.values()] for r in rows]
    if levels is None:
        seen = sorted({lv for row in feats for lv in row})
        levels = {lv: i for i, lv in enumerate(seen)}
    n, p = len(rows), len(levels)
    X = np.zeros((n, p + 1), dtype=np.float64)
    X[:, 0] = 1.0                                  # intercept
    for i, row in enumerate(feats):
        for lv in row:
            j = levels.get(lv)
            if j is not None:
                X[i, j + 1] = 1.0
    return X, levels


def ridge(X: np.ndarray, y: np.ndarray, lam: float) -> np.ndarray:
    """Closed-form ridge. The intercept column is deliberately unpenalised."""
    p = X.shape[1]
    P = np.eye(p) * lam
    P[0, 0] = 0.0
    return np.linalg.solve(X.T @ X + P, X.T @ y)


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def decile_table(score: np.ndarray, y: np.ndarray, n_bins: int = 10):
    """Out-of-sample decile means. The actual definition of 'the grade works'."""
    order = np.argsort(score)
    bins = np.array_split(order, n_bins)
    return [(len(b), float(score[b].mean()), float(y[b].mean())) for b in bins]


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    """Rank correlation without scipy."""
    ra = np.argsort(np.argsort(a)).astype(float)
    rb = np.argsort(np.argsort(b)).astype(float)
    ra -= ra.mean(); rb -= rb.mean()
    denom = np.sqrt((ra @ ra) * (rb @ rb))
    return float(ra @ rb / denom) if denom else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", default="30d", choices=HORIZONS)
    ap.add_argument("--train-end", default="2023-12-31")
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--lam", type=float, default=50.0)
    ap.add_argument("--first-time-only", action="store_true",
                    help="Restrict to filers with no prior discretionary buy")
    args = ap.parse_args()

    col = f"abnormal_{args.horizon}"
    conn = get_connection()

    first_time_clause = """
      AND NOT EXISTS (SELECT 1 FROM trades p
                       WHERE p.insider_id = t.insider_id
                         AND p.signal_class = 'discretionary_buy'
                         AND p.filing_date < t.filing_date)
    """ if args.first_time_only else ""

    logger.info("Loading discretionary buys since %s (horizon=%s)...", args.since, args.horizon)
    rows = conn.execute(f"""
        SELECT t.trade_id, t.filing_date, t.normalized_title, t.value,
               t.pit_cluster_size, t.dip_1mo, t.is_largest_ever, t.is_csuite,
               tm.sector,
               GREATEST(LEAST(tr.{col}, {WINSOR}), -{WINSOR}) AS y,
               t.career_grade
          FROM trades t
          JOIN trade_returns tr ON tr.trade_id = t.trade_id
          LEFT JOIN ticker_metadata tm ON tm.ticker = t.ticker
         WHERE t.signal_class = 'discretionary_buy'
           AND NOT COALESCE(t.value_suspect, FALSE)
           AND tr.{col} IS NOT NULL
           AND t.trade_date >= ?
           {first_time_clause}
         ORDER BY t.filing_date
    """, (args.since,)).fetchall()
    logger.info("  %d labelled trades", len(rows))

    train = [r for r in rows if r["filing_date"] <= args.train_end]
    test = [r for r in rows if r["filing_date"] > args.train_end]
    logger.info("  train %d (<= %s) / test %d", len(train), args.train_end, len(test))
    if not train or not test:
        logger.error("Empty split — widen --since or move --train-end")
        return 1

    Xtr, levels = build_design(train)
    ytr = np.array([r["y"] for r in train], dtype=np.float64)
    Xte, _ = build_design(test, levels)
    yte = np.array([r["y"] for r in test], dtype=np.float64)

    w = ridge(Xtr, ytr, args.lam)
    score_te = Xte @ w
    score_tr = Xtr @ w

    print("\n" + "=" * 72)
    print(f"  V4 TRADE MODEL — horizon {args.horizon}"
          f"{'  (first-time filers only)' if args.first_time_only else ''}")
    print("=" * 72)
    print(f"  train {len(train)} (<= {args.train_end})   test {len(test)}   lambda={args.lam}")
    print(f"  base rate: train {ytr.mean()*100:+.2f}%   test {yte.mean()*100:+.2f}%")

    print("\n  COEFFICIENTS (pp of 30d abnormal return, vs the average filing)")
    named = sorted(((lv, w[i + 1] * 100) for lv, i in levels.items()),
                   key=lambda kv: -kv[1])
    for lv, c in named:
        if abs(c) < 0.01:
            continue
        bar = "#" * min(int(abs(c) * 6), 30)
        print(f"    {lv:<22} {c:+6.2f}  {bar}")

    print("\n  OUT-OF-SAMPLE DECILES (test window only)")
    print(f"    {'decile':>6} {'n':>7} {'predicted':>11} {'actual':>10}")
    tbl = decile_table(score_te, yte)
    for i, (n, pm, am) in enumerate(tbl, 1):
        print(f"    {i:>6} {n:>7} {pm*100:>10.2f}% {am*100:>9.2f}%")

    top, bot = tbl[-1][2], tbl[0][2]
    rho = spearman(score_te, yte)
    monotone_pairs = sum(1 for a, b in zip(tbl, tbl[1:]) if b[2] >= a[2])
    print(f"\n  top decile - bottom decile : {(top - bot)*100:+.2f} pp")
    print(f"  spearman(score, return)    : {rho:+.4f}")
    print(f"  monotone steps             : {monotone_pairs}/9")

    # Baseline: what career_grade achieves on the same test rows.
    cg = [(r["career_grade"], r["y"]) for r in test if r["career_grade"]]
    if cg:
        import collections
        agg = collections.defaultdict(list)
        for g, yv in cg:
            agg[g].append(yv)
        print(f"\n  BASELINE career_grade on the same test rows (n={len(cg)})")
        for g in ("A+", "A", "B", "C", "D"):
            if g in agg:
                v = np.array(agg[g])
                print(f"    {g:<3} n={len(v):>6}  {v.mean()*100:+6.2f}%")
        ap_a = np.array([v for g, v in cg if g in ("A+", "A")])
        rest = np.array([v for g, v in cg if g not in ("A+", "A")])
        if len(ap_a) and len(rest):
            print(f"    A+/A minus B/C/D        : {(ap_a.mean()-rest.mean())*100:+.2f} pp")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

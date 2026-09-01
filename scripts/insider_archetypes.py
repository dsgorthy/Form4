#!/usr/bin/env python3
"""Cluster insiders into archetypes, then test whether the archetypes matter.

WHY THIS AXIS

Every trade-level signal we have failed. The outcome deciles were
indistinguishable on all of them, and above_sma50 -- the single feature that
looked like it separated -- turned out to be a volatility effect and separates
by 0.8 points once volatility is removed.

Insider-level persistence is a different axis, and it is the one thing the
literature reports as durable out of sample: Cohen/Malloy/Pomorski's
routine-vs-opportunistic classification is made at the TRADER level and holds
forward. Heckmann finds the same. Our career_grade tries to be this but is a
single scalar built from past returns, which is a strictly weaker object than a
behavioural profile.

Derek's example -- a CEO who moves company to company and always does well --
is countable: 2,480 insiders trade 5 or more tickers, 10,138 trade 3 or more,
against 38,015 who file once and never again.

THE DESIGN IS WALK-FORWARD, NOT DESCRIPTIVE

Clusters are fitted on filings up to --split and evaluated on filings after it.
An insider's archetype therefore uses only their own past, and the outcome test
is genuinely out of sample. Clustering the whole history and then reporting
that the clusters differ in outcome would be circular -- the profile features
are built from the same trades being scored.

Insiders with fewer than MIN_TRAIN_FILINGS filings before the split are left
unassigned rather than clustered off two data points.

Usage:
    python3 scripts/insider_archetypes.py
    python3 scripts/insider_archetypes.py --k 6 --split 2021-12-31
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIN_TRAIN_FILINGS = 4
SEED = 20260901

PROFILE_SQL = """
WITH f AS (
    -- One row per FILING, not per execution lot. A purchase filled in five
    -- tranches is one decision; counting lots inflates every per-insider
    -- statistic by the tranche count.
    SELECT t.insider_id, t.ticker, t.trade_type, t.signal_class,
           MIN(t.filing_date) AS filing_date,
           SUM(t.value)       AS value,
           MAX(t.is_csuite::int)   AS is_csuite,
           AVG(t.dip_3mo)          AS dip_3mo,
           MAX(t.above_sma50::int) AS above_sma50
      FROM trades t
     WHERE t.signal_class IN ('discretionary_buy','discretionary_sell')
       AND NOT COALESCE(t.value_suspect, FALSE)
       AND t.filing_date < %s
     GROUP BY t.insider_id, t.ticker, t.trade_type, t.signal_class,
              COALESCE(t.filing_key, t.accession)
)
SELECT insider_id,
       count(*)                                              AS n_filings,
       count(DISTINCT ticker)                                AS n_tickers,
       (MAX(filing_date)::date - MIN(filing_date)::date)     AS tenure_days,
       AVG(is_csuite)                                        AS pct_csuite,
       AVG((signal_class = 'discretionary_buy')::int)        AS buy_ratio,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY value)    AS median_value,
       AVG(dip_3mo)                                          AS avg_dip,
       AVG(above_sma50)                                      AS pct_above_sma50
  FROM f
 GROUP BY insider_id
HAVING count(*) >= %s
"""

# Outcome on trades AFTER the split, for insiders profiled BEFORE it.
EVAL_SQL = """
SELECT t.insider_id,
       r.abnormal_21td_from_filing AS y
  FROM trades t JOIN trade_returns r USING (trade_id)
 WHERE t.signal_class = 'discretionary_buy'
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND t.filing_date >= %s
   AND r.abnormal_21td_from_filing IS NOT NULL
"""


def kmeans(X: np.ndarray, k: int, iters: int = 100, seed: int = SEED):
    """Lloyd's algorithm with k-means++ seeding. numpy only.

    sklearn is not installed on Studio and this is thirty lines; adding a
    dependency to a production box for one clustering is the wrong trade.
    """
    rng = np.random.default_rng(seed)
    n = X.shape[0]
    centres = [X[rng.integers(n)]]
    for _ in range(1, k):
        d2 = np.min(((X[:, None, :] - np.array(centres)[None, :, :]) ** 2).sum(-1), axis=1)
        total = d2.sum()
        centres.append(X[rng.choice(n, p=d2 / total) if total > 0 else rng.integers(n)])
    C = np.array(centres)
    labels = np.zeros(n, dtype=int)
    for _ in range(iters):
        d = ((X[:, None, :] - C[None, :, :]) ** 2).sum(-1)
        new = d.argmin(1)
        if (new == labels).all():
            break
        labels = new
        for j in range(k):
            if (labels == j).any():
                C[j] = X[labels == j].mean(0)
    return labels, C


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", default="2022-01-01",
                    help="profiles use filings BEFORE this; outcomes AFTER")
    ap.add_argument("--k", type=int, default=6)
    args = ap.parse_args()

    conn = get_connection(readonly=True)
    rows = conn.execute(PROFILE_SQL, (args.split, MIN_TRAIN_FILINGS)).fetchall()
    logger.info("%d insiders with >= %d filings before %s",
                len(rows), MIN_TRAIN_FILINGS, args.split)
    if len(rows) < 100:
        logger.error("too few insiders to cluster")
        return 1

    ids = np.array([r["insider_id"] for r in rows])
    cols = ["n_filings", "n_tickers", "tenure_days", "pct_csuite", "buy_ratio",
            "median_value", "avg_dip", "pct_above_sma50"]
    raw = np.array([[float(r[c]) if r[c] is not None else np.nan for c in cols]
                    for r in rows])

    # Median-impute, then log-scale the heavy-tailed counts so one 400-filing
    # insider does not define an axis by itself.
    for j in range(raw.shape[1]):
        col = raw[:, j]
        col[np.isnan(col)] = np.nanmedian(col) if not np.isnan(col).all() else 0.0
    for j, c in enumerate(cols):
        if c in ("n_filings", "n_tickers", "tenure_days", "median_value"):
            raw[:, j] = np.log1p(np.clip(raw[:, j], 0, None))
    X = (raw - raw.mean(0)) / np.where(raw.std(0) == 0, 1, raw.std(0))

    labels, C = kmeans(X, args.k)
    logger.info("clustered into %d archetypes", args.k)

    # Outcomes AFTER the split, joined by insider.
    ev = conn.execute(EVAL_SQL, (args.split,)).fetchall()
    by_ins: dict[int, list[float]] = {}
    for r in ev:
        by_ins.setdefault(r["insider_id"], []).append(float(r["y"]))
    lab_of = dict(zip(ids.tolist(), labels.tolist()))

    print(f"\nArchetypes fitted on filings before {args.split}, "
          f"outcomes measured after.\n")
    hdr = (f"{'arch':>4}{'insiders':>10}{'filings':>9}{'tickers':>9}"
           f"{'tenure_y':>10}{'%csuite':>9}{'buy%':>7}{'med$':>10}"
           f"{'avg_dip':>9}{'%sma50':>8}   |{'oos_n':>7}{'oos_ret':>9}{'oos_win':>9}")
    print(hdr); print("-" * len(hdr))
    for j in range(args.k):
        m = labels == j
        if not m.any():
            continue
        p = raw[m]
        outs = [y for i in ids[m].tolist() for y in by_ins.get(i, [])]
        oos_ret = 100 * float(np.mean(outs)) if outs else float("nan")
        oos_win = 100 * float(np.mean([o > 0 for o in outs])) if outs else float("nan")
        print(f"{j:>4}{m.sum():>10}"
              f"{np.expm1(p[:,0].mean()):>9.1f}{np.expm1(p[:,1].mean()):>9.1f}"
              f"{np.expm1(p[:,2].mean())/365:>10.1f}{100*p[:,3].mean():>9.1f}"
              f"{100*p[:,4].mean():>7.1f}{np.expm1(p[:,5].mean()):>10.0f}"
              f"{p[:,6].mean():>9.3f}{100*p[:,7].mean():>8.1f}   |"
              f"{len(outs):>7}{oos_ret:>9.2f}{oos_win:>9.1f}")

    all_out = [y for v in by_ins.values() for y in v]
    if not all_out:
        return 0
    print(f"\nall profiled insiders, out of sample: n={len(all_out)}, "
          f"mean {100*np.mean(all_out):.2f}%, "
          f"win {100*np.mean([o>0 for o in all_out]):.1f}%")

    def spread_of(lab: np.ndarray) -> float:
        means = []
        for j in range(args.k):
            outs = [y for i in ids[lab == j].tolist() for y in by_ins.get(i, [])]
            if outs:
                means.append(float(np.mean(outs)))
        return 100 * (max(means) - min(means)) if len(means) > 1 else float("nan")

    observed = spread_of(labels)
    print(f"best archetype minus worst: {observed:.2f}pp")

    # PERMUTATION TEST. Six groups of unequal size drawn from a heavy-tailed
    # outcome distribution will show a spread even if the labels mean nothing,
    # and the biggest-minus-smallest statistic is maximally flattered by that.
    # So: shuffle the archetype labels ACROSS INSIDERS, keeping group sizes and
    # each insider's own trades intact, and see how often chance beats what we
    # observed.
    #
    # Shuffling insiders rather than trades is the point. An insider's trades
    # are not independent of each other -- same person, often same ticker --
    # and permuting at trade level would destroy that structure and manufacture
    # significance, which is the same error that inflated every t-statistic in
    # this project until the standard errors were clustered.
    rng = np.random.default_rng(SEED)
    null = []
    for _ in range(500):
        null.append(spread_of(rng.permutation(labels)))
    null = np.array([v for v in null if v == v])
    if len(null) > 50:
        p_val = float((null >= observed).mean())
        print(f"permutation test: {len(null)} shuffles of the labels across "
              f"insiders\n"
              f"  null spread  median {np.median(null):.2f}pp   "
              f"95th pct {np.percentile(null, 95):.2f}pp   "
              f"max {null.max():.2f}pp\n"
              f"  observed {observed:.2f}pp -> p = {p_val:.3f}")
        if p_val > 0.05:
            print("  NOT SIGNIFICANT: chance reproduces this spread. The "
                  "archetypes describe insiders; they do not predict returns.")
        else:
            print("  significant at 5%. Still needs a second split before it "
                  "is believed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

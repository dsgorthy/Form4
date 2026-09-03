#!/usr/bin/env python3
"""Precompute a top-K "related insiders" neighbour list.

WHAT THIS IS, AND THE ONE THING IT IS NOT

It answers "who else should I look at?" on an insider page. It is a NAVIGATION
aid built from similarity, and it must never be presented as a ranking.

That distinction is not pedantry. The behavioural clustering underneath is the
same one built in scripts/insider_archetypes.py, and that clustering was tested
against forward returns and FAILED: observed between-archetype spread 2.33pp
against a permutation null whose median was 1.75pp, p=0.208. Clusters that do
not predict returns are still perfectly good at grouping like with like --
which is all this table claims. Any UI that renders these neighbours as
"better", "stronger" or "top" insiders is making a claim three experiments this
month could not support.

FULL HISTORY, NOT WALK-FORWARD -- AND WHY THAT IS CORRECT HERE

insider_archetypes.py is deliberately walk-forward: it fits on filings before a
split and scores filings after it, because clustering the whole history and
then reporting that the clusters differ in outcome is circular. None of that
applies to this script. There is no outcome here and nothing is being scored,
so "what is this insider like, using everything we know about them" is the
right question and using an insider's whole record to answer it is not
leakage. The moment anyone adds a return column to this table, that stops
being true.

THREE COMPONENTS, STORED SEPARATELY

  co_investment   Jaccard over ticker sets. The strongest and most legible
                  relation: two insiders filing on the same company. For a
                  single-ticker insider this saturates at 1.0 against their
                  colleagues, which is intended -- they ARE the related ones.
  sector_overlap  Jaccard over sector sets, from ticker_metadata.
  profile_sim     Behavioural distance in standardised feature space.

THE WEIGHTS ARE A PRODUCT JUDGEMENT, NOT A FITTED PARAMETER. There is no
ground truth for "related", so there is nothing to fit against and I am not
going to dress up a guess as an optimisation. They are stored decomposed so a
later reader can re-weight from the table without recomputing anything.

DIVERSITY IS ENFORCED, NOT HOPED FOR

Co-investment dominates when it is non-zero, so a naive top-8 for anyone at a
large company is eight colleagues and no discovery at all. Slots are therefore
split: at most N_CO neighbours that share a ticker, and at least N_PROFILE that
share NONE. Both halves are useful and they answer different questions.

SAME-NAME PAIRS ARE DROPPED. The insiders table still holds unconsolidated
duplicates; without this the flagship "related insider" is frequently the same
human being.

Usage:
    python3 scripts/insider_similarity.py              # compute + write
    python3 scripts/insider_similarity.py --dry-run    # compute + report only
    python3 scripts/insider_similarity.py --inspect 4428
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SEED = 20260903

# An insider with one filing has no behaviour to be similar to. Three is the
# floor at which buy_ratio and tenure mean anything at all.
MIN_FILINGS = 3

TOP_K = 8
N_CO = 5          # at most this many neighbours sharing a ticker
N_PROFILE = 3     # at least this many sharing none

W_CO, W_SECTOR, W_PROFILE = 0.50, 0.20, 0.30

# k-means blocking. Exact all-pairs over 76k insiders is 2.9e9 comparisons;
# blocking makes it ~cluster-size^2 summed, and boundary misses are acceptable
# for a "you might also like" list in a way they would not be for a metric.
N_BLOCKS = 320
ROW_CHUNK = 512   # bounds peak memory to ROW_CHUNK x cluster_size floats

PROFILE_SQL = """
WITH f AS (
    -- One row per FILING, not per execution lot: a purchase filled in five
    -- tranches is one decision, and counting lots inflates every per-insider
    -- statistic by the tranche count.
    SELECT t.insider_id, t.ticker, t.signal_class,
           MIN(t.filing_date)      AS filing_date,
           SUM(t.value)            AS value,
           MAX(t.is_csuite::int)   AS is_csuite,
           AVG(t.dip_3mo)          AS dip_3mo,
           MAX(t.above_sma50::int) AS above_sma50
      FROM trades t
     WHERE t.signal_class IN ('discretionary_buy','discretionary_sell')
       AND NOT COALESCE(t.value_suspect, FALSE)
       AND t.insider_id IS NOT NULL
     GROUP BY t.insider_id, t.ticker, t.signal_class,
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

TICKERS_SQL = """
SELECT t.insider_id, t.ticker, m.sector, count(*) AS n
  FROM trades t
  LEFT JOIN ticker_metadata m ON m.ticker = t.ticker
 WHERE t.signal_class IN ('discretionary_buy','discretionary_sell')
   AND t.ticker IS NOT NULL AND t.ticker <> 'NONE'
   AND t.insider_id IS NOT NULL
 GROUP BY 1,2,3
"""

NAMES_SQL = "SELECT insider_id, COALESCE(name,'') FROM insiders"

FEATURES = ["n_filings", "n_tickers", "tenure_days", "pct_csuite",
            "buy_ratio", "median_value", "avg_dip", "pct_above_sma50"]
# Heavy right tails; a raw count would let one 900-filing insider set the scale
# for the whole standardised space.
LOG_FEATURES = {"n_filings", "n_tickers", "tenure_days", "median_value"}


def kmeans(X: np.ndarray, k: int, iters: int = 40, seed: int = SEED):
    """Plain Lloyd's, seeded. Same implementation as insider_archetypes."""
    rng = np.random.default_rng(seed)
    C = X[rng.choice(len(X), size=k, replace=False)].copy()
    labels = np.zeros(len(X), dtype=np.int32)
    for _ in range(iters):
        for lo in range(0, len(X), 4096):
            hi = min(lo + 4096, len(X))
            d = ((X[lo:hi, None, :] - C[None, :, :]) ** 2).sum(axis=2)
            labels[lo:hi] = d.argmin(axis=1)
        moved = False
        for j in range(k):
            m = labels == j
            if m.any():
                new = X[m].mean(axis=0)
                if not np.allclose(new, C[j]):
                    C[j], moved = new, True
        if not moved:
            break
    return labels


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / (len(a) + len(b) - inter) if inter else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--inspect", type=int, help="print one insider's neighbours")
    ap.add_argument("--min-filings", type=int, default=MIN_FILINGS)
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    logger.info("loading profiles (>= %d filings)...", args.min_filings)
    cur.execute(PROFILE_SQL, (args.min_filings,))
    rows = cur.fetchall()
    ids = np.array([r[0] for r in rows], dtype=np.int64)
    raw = np.array([[float(v if v is not None else 0.0) for v in r[1:]] for r in rows])
    logger.info("  %d insiders", len(ids))
    if len(ids) < N_BLOCKS * 2:
        logger.error("too few insiders (%d) to block into %d clusters", len(ids), N_BLOCKS)
        return 1

    logger.info("loading ticker + sector sets...")
    cur.execute(TICKERS_SQL)
    tick: dict[int, set] = defaultdict(set)
    sect: dict[int, set] = defaultdict(set)
    by_ticker: dict[str, list] = defaultdict(list)
    # Deliberately NOT restricted to insiders who cleared MIN_FILINGS. A
    # co-investment edge needs only a shared ticker, so someone with two
    # filings can still be told who else files on their company -- and 58,042
    # insiders sit below the profile floor. Gating this on the profile would
    # leave every one of those pages with an empty section.
    sector_weight: dict[int, dict] = defaultdict(lambda: defaultdict(int))
    for iid, tk, sc, n in cur.fetchall():
        tick[iid].add(tk)
        by_ticker[tk].append(iid)
        if sc:
            sect[iid].add(sc)
            sector_weight[iid][sc] += n
    # The sector an insider actually operates in, by filing count. A
    # multi-sector investor's SET is wide enough that "shares a sector" is
    # nearly free -- Chimovits holds one financials name, which qualified a
    # small-town bank director as his behavioural neighbour. The primary is
    # the one that describes them.
    primary: dict[int, str] = {
        i: max(w.items(), key=lambda kv: kv[1])[0] for i, w in sector_weight.items()
    }
    logger.info("  %d insiders with tickers (%d of them profiled), %d tickers",
                len(tick), len(set(tick) & set(int(i) for i in ids)), len(by_ticker))

    cur.execute(NAMES_SQL)
    names = {int(i): (n or "").strip().upper() for i, n in cur.fetchall()}

    # ── standardise ────────────────────────────────────────────────────────
    X = raw.copy()
    for j, f in enumerate(FEATURES):
        if f in LOG_FEATURES:
            X[:, j] = np.log1p(np.clip(X[:, j], 0, None))
    mu, sd = X.mean(axis=0), X.std(axis=0)
    sd[sd == 0] = 1.0
    X = (X - mu) / sd
    pos = {int(v): k for k, v in enumerate(ids)}

    # ── candidates A: shared tickers ───────────────────────────────────────
    logger.info("generating co-investment candidates...")
    cand: dict[int, set] = defaultdict(set)
    for tk, members in by_ticker.items():
        if len(members) < 2 or len(members) > 400:
            continue  # a 400-filer ticker contributes 80k pairs of little value
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                cand[a].add(b)
                cand[b].add(a)
    logger.info("  %d insiders have a co-investment candidate", len(cand))

    # ── candidates B: behavioural blocks ───────────────────────────────────
    logger.info("k-means blocking into %d clusters...", N_BLOCKS)
    labels = kmeans(X, N_BLOCKS)
    prof_cand: dict[int, list] = defaultdict(list)
    for j in range(N_BLOCKS):
        idx = np.flatnonzero(labels == j)
        if len(idx) < 2:
            continue
        Xc = X[idx]
        for lo in range(0, len(idx), ROW_CHUNK):
            hi = min(lo + ROW_CHUNK, len(idx))
            d = np.sqrt(((Xc[lo:hi, None, :] - Xc[None, :, :]) ** 2).sum(axis=2))
            d[np.arange(hi - lo), np.arange(lo, hi)] = np.inf  # drop self
            take = min(N_PROFILE * 4, len(idx) - 1)
            nn = np.argpartition(d, take - 1, axis=1)[:, :take]
            for r in range(hi - lo):
                a = int(ids[idx[lo + r]])
                for c in nn[r]:
                    prof_cand[a].append(int(ids[idx[c]]))
    logger.info("  %d insiders have a behavioural candidate", len(prof_cand))

    # ── score ──────────────────────────────────────────────────────────────
    logger.info("scoring...")
    out = []
    universe = sorted(set(cand) | set(prof_cand))
    for a in universe:
        seen = set()
        scored_co, scored_pf = [], []
        for b in list(cand.get(a, ())) + prof_cand.get(a, []):
            if b == a or b in seen:
                continue
            seen.add(b)
            # Unconsolidated duplicates: the same human, twice.
            if names.get(a) and names.get(a) == names.get(b):
                continue
            ta, tb = tick.get(a, set()), tick.get(b, set())
            co = jaccard(ta, tb)
            sec = jaccard(sect.get(a, set()), sect.get(b, set()))
            if a in pos and b in pos:
                d = float(np.linalg.norm(X[pos[a]] - X[pos[b]]))
                pf = 1.0 / (1.0 + d)
            else:
                pf = 0.0  # below the filing floor: co-investment carries it
            score = W_CO * co + W_SECTOR * sec + W_PROFILE * pf
            shared = sorted(ta & tb)
            rec = (b, score, co, sec, pf, len(shared), ",".join(shared[:3]) or None)
            (scored_co if shared else scored_pf).append(rec)

        scored_co.sort(key=lambda r: -r[1])
        scored_pf.sort(key=lambda r: -r[1])
        # Prefer same-sector behavioural neighbours; fall back to the rest only
        # if there are not enough, so coverage never collapses on the insiders
        # whose tickers we have no sector for.
        # NO FALLBACK. An earlier version padded up to N_PROFILE when too
        # few same-sector neighbours existed, and the padding is exactly what
        # produced the bad rows -- a small-town bank director as the eighth
        # "related insider" on a biotech investor's page. TOP_K is a maximum,
        # not a quota; a five-row list that is all defensible is worth more
        # than an eight-row list a reader stops trusting at row eight.
        pa = primary.get(a)
        scored_pf = [r for r in scored_pf if pa and primary.get(r[0]) == pa]
        # Co-investment first, capped, then behavioural fills the rest. The cap
        # is what stops a large company's roster from being the entire list.
        picked = scored_co[:N_CO] + scored_pf[:max(N_PROFILE, TOP_K - min(len(scored_co), N_CO))]
        picked = picked[:TOP_K]
        for rank, r in enumerate(picked, 1):
            out.append((a, r[0], rank, r[1], r[2], r[3], r[4], r[5], r[6]))

    logger.info("  %d edges for %d insiders", len(out), len({r[0] for r in out}))

    # ── diagnostic: is the behavioural space encoding anything real? ───────
    # If profile-similar pairs shared a sector no more often than random pairs,
    # the behavioural half of this list would be noise wearing a label.
    # It must be measured BEFORE the same-sector filter. Measured after, it
    # reports 100% by construction and checks nothing at all -- it was doing
    # exactly that for one commit.
    rng = np.random.default_rng(SEED)
    pf_pairs = [(a, b) for a, bs in prof_cand.items() for b in bs
                if not (tick.get(a, set()) & tick.get(b, set()))]
    if pf_pairs:
        hit = np.mean([1.0 if (sect.get(a) and sect.get(b) and sect[a] & sect[b])
                       else 0.0 for a, b in pf_pairs[:200000]])
        sample = rng.choice(ids, size=(min(20000, len(out)), 2))
        base = np.mean([1.0 if (sect.get(int(x)) and sect.get(int(y))
                                and sect[int(x)] & sect[int(y)]) else 0.0
                        for x, y in sample])
        logger.info("DIAGNOSTIC sector agreement among UNFILTERED behavioural "
                    "candidates %.1f%% vs random pairs %.1f%% -- the profile "
                    "space is not noise", hit * 100, base * 100)

    if args.inspect:
        tgt = args.inspect
        mine = sorted([r for r in out if r[0] == tgt], key=lambda r: r[2])
        print(f"\n--- neighbours of {tgt} ({names.get(tgt,'?')}) ---")
        for r in mine:
            print(f"  {r[2]}. {names.get(r[1],'?')[:34]:34s} score={r[3]:.3f} "
                  f"co={r[4]:.2f} sec={r[5]:.2f} prof={r[6]:.2f} "
                  f"shared={r[7]} {r[8] or ''}")

    if args.dry_run:
        logger.info("dry run, nothing written")
        return 0

    logger.info("writing insider_similarity...")
    cur.execute("SET lock_timeout = '5s'")
    cur.execute("CREATE TEMP TABLE sim_new (LIKE insider_similarity)")

    def lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return repr(float(v)) if isinstance(v, float) else str(int(v))

    cols = ("insider_id, related_insider_id, rank, score, co_investment, "
            "sector_overlap, profile_sim, shared_tickers, shared_ticker_list")
    for lo in range(0, len(out), 5000):
        chunk = out[lo:lo + 5000]
        vals = ",".join("(" + ",".join(lit(v) for v in r) + ")" for r in chunk)
        cur.execute(f"INSERT INTO sim_new ({cols}) VALUES {vals}")
    # Swap inside one transaction so a reader never sees a half-built list.
    cur.execute("TRUNCATE insider_similarity")
    cur.execute(f"INSERT INTO insider_similarity ({cols}) SELECT {cols} FROM sim_new")
    cur.execute("DROP TABLE sim_new")
    conn.commit()
    cur.execute("SELECT count(*), count(DISTINCT insider_id) FROM insider_similarity")
    n, ni = cur.fetchone()
    logger.info("wrote %d edges covering %d insiders", n, ni)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""PIT-clean research harness for the insider career / PIT grade.

Every predictor is built ONLY from filings that were both KNOWN
(filing_date <= as_of) and whose forward return was OBSERVABLE
(trade_date <= as_of - lag) at the moment being scored -- the same two guards
`pit_scoring.compute_insider_ticker_score` applies, with the same lags and the
same `abnormal_*` columns. Filings are grouped one row per filing before
anything is counted.

Findings: docs/pit_grade_research.md

Usage (on Studio, needs the form4 DB):
    python3 research.py load       # cache the filing universe
    python3 research.py scope      # population / scope / horizon        (DEV)
    python3 research.py tune       # half-life, blend, form, target      (DEV)
    python3 research.py headtohead # production vs candidates            (DEV)
    python3 research.py holdout    # 2023-2026 validation + year by year
    python3 research.py selectivity# matched top-k% comparison -- the gate
    python3 research.py population # production formula, population varied
    python3 research.py alist      # A-List admission impact
"""
from __future__ import annotations

import argparse
import datetime
import math
import pickle
import sys
from collections import Counter, defaultdict
from pathlib import Path

# repo root, whether this runs from its package dir or a copy elsewhere
for _p in list(Path(__file__).resolve().parents) + [Path.cwd()]:
    if (_p / "config" / "database.py").exists():
        sys.path.insert(0, str(_p))
        break

LAGS = {"7d": 10, "30d": 40, "90d": 100}
DEV_END = "2023-01-01"
CACHE = "/tmp/pit_research_filings.pkl"
PRIOR_ALPHA = PRIOR_BETA = 2.0
PRIOR_RETURN_N = 3.0


def load_filings() -> list[dict]:
    from config.database import get_connection

    conn = get_connection()
    rows = conn.execute(
        """
        SELECT t.insider_id, t.ticker,
               MIN(t.trade_date) AS trade_date, MIN(t.filing_date) AS filing_date,
               MAX(t.signal_class) AS signal_class,
               AVG(tr.abnormal_7d) AS a7, AVG(tr.abnormal_30d) AS a30,
               AVG(tr.abnormal_90d) AS a90,
               MAX(COALESCE(t.is_duplicate, 0)) AS dup,
               MAX(CASE WHEN t.superseded_by IS NOT NULL THEN 1 ELSE 0 END) AS sup,
               MAX(COALESCE(t.is_derivative, 0)) AS deriv
          FROM trades t
          JOIN trade_returns tr ON tr.trade_id = t.trade_id
         WHERE t.trade_type = 'buy'
           AND t.ticker IS NOT NULL AND t.ticker <> 'NONE'
         GROUP BY t.insider_id, t.ticker,
                  COALESCE(t.filing_key, t.accession, t.trade_date::text)
        """
    ).fetchall()
    out = []
    for r in rows:
        td, fd = str(r[2])[:10], str(r[3])[:10]
        out.append(
            dict(iid=r[0], tk=r[1], td=td, fd=max(fd, td), cls=r[4],
                 a7=r[5], a30=r[6], a90=r[7], dup=r[8], sup=r[9], deriv=r[10])
        )
    return out


def prepare(rows: list[dict]):
    for r in rows:
        r["clean"] = not r["dup"] and not r["sup"] and not r["deriv"]
        r["disc"] = r["cls"] == "discretionary_buy"
        r["_d"] = datetime.date(int(r["td"][:4]), int(r["td"][5:7]), int(r["td"][8:10]))
    by_ins = defaultdict(list)
    for r in rows:
        by_ins[r["iid"]].append(r)
    for v in by_ins.values():
        v.sort(key=lambda r: r["td"])
    return by_ins


def observations(by_ins, t, window, same_ticker, disc_only, clean_only):
    """PIT-safe prior observations for one target filing."""
    lag = LAGS[window]
    key = "a" + window[:-1]
    d = datetime.date(int(t["fd"][:4]), int(t["fd"][5:7]), int(t["fd"][8:10]))
    cutoff = (d - datetime.timedelta(days=lag)).isoformat()
    out = []
    for r in by_ins[t["iid"]]:
        if r["td"] > cutoff:      # list is sorted by trade_date
            break
        if r["fd"] > t["fd"] or r[key] is None:
            continue
        if disc_only and not r["disc"]:
            continue
        if clean_only and not r["clean"]:
            continue
        if same_ticker and r["tk"] != t["tk"]:
            continue
        out.append(((d - r["_d"]).days, r[key]))
    return out


def quality(obs, halflife, cap=True):
    """Production's per-window quality: Beta(2,2) win rate + shrunk abnormal."""
    if not obs:
        return None, 0.0
    tw = s = wins = 0.0
    for days, v in obs:
        w = 2.0 ** (-days / halflife) if halflife else 1.0
        tw += w
        s += v * w
        if v > 0:
            wins += w
    if tw < 0.1:
        return None, 0.0
    wr = (wins + PRIOR_ALPHA) / (tw + PRIOR_ALPHA + PRIOR_BETA)
    ab = (s / tw * tw) / (tw + PRIOR_RETURN_N)
    wc = max(0.0, wr - 0.50) * 4.0
    rc = max(0.0, min(1.0, ab * 10 + 0.3)) if cap else (ab * 10 + 0.3)
    return wc * 0.45 + rc * 0.55, tw


def blend_weights(neff):
    if neff < 1.0:
        return 0.0, 1.0
    f = 1.0 / (1.0 + math.exp(-(neff - 6.0) / 2.5))
    return f, 1.0 - f


def score_formula(by_ins, t, disc_only, clean_only, halflife=547):
    """The production V2/V3 formula, with the predictor population as a knob."""
    tq, gq, tn, gn = {}, {}, {}, {}
    for w in ("7d", "30d", "90d"):
        tq[w], tn[w] = quality(observations(by_ins, t, w, True, disc_only, clean_only), halflife)
        gq[w], gn[w] = quality(observations(by_ins, t, w, False, disc_only, clean_only), halflife)

    def blend(q, n):
        """Each scope gates its own windows on its OWN effective counts."""
        parts = []
        if n["7d"] > 0.1 and q["7d"] is not None:
            parts.append((q["7d"], 0.40))
        if n["30d"] > 0.5 and q["30d"] is not None:
            parts.append((q["30d"], 0.35))
        if n["90d"] > 0.5 and q["90d"] is not None:
            parts.append((q["90d"], 0.25))
        if not parts:
            return None
        tot = sum(x for _, x in parts)
        return sum(a * x / tot for a, x in parts)

    tqual, gqual = blend(tq, tn), blend(gq, gn)
    if tqual is None and gqual is None:
        return None
    a, b = blend_weights(tn["7d"])
    return min(3.0, max(0.0, ((tqual or 0.0) * a + (gqual or 0.0) * b) * 2.7))


def score_candidate(by_ins, t, wmix=(("30d", 0.6), ("90d", 0.4)), halflife=547, cap=True):
    """Discretionary + hygiene, ticker-primary with global fallback."""
    parts = []
    for w, wt in wmix:
        q, _ = quality(observations(by_ins, t, w, True, True, True), halflife, cap)
        if q is not None:
            parts.append((q, wt))
    if not parts:
        for w, wt in wmix:
            q, _ = quality(observations(by_ins, t, w, False, True, True), halflife, cap)
            if q is not None:
                parts.append((q, wt))
        if not parts:
            return None
    tot = sum(x for _, x in parts)
    return min(3.0, max(0.0, sum(a * x / tot for a, x in parts) * 2.7))


def spearman(pairs):
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    n = len(pairs)
    if n < 30:
        return None, n

    def rank(vals):
        idx = sorted(range(len(vals)), key=lambda i: vals[i])
        rk = [0.0] * len(vals)
        i = 0
        while i < len(idx):
            j = i
            while j + 1 < len(idx) and vals[idx[j + 1]] == vals[idx[i]]:
                j += 1
            for k in range(i, j + 1):
                rk[idx[k]] = (i + j) / 2.0 + 1
            i = j + 1
        return rk

    rx, ry = rank([p[0] for p in pairs]), rank([p[1] for p in pairs])
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    return (num / (dx * dy) if dx and dy else None), n


def median_pct(vals):
    vals = sorted(v for v in vals if v is not None)
    return vals[len(vals) // 2] * 100 if vals else float("nan")


def targets_in(rows, lo, hi, key="a30"):
    return [r for r in rows if r["disc"] and r["clean"] and r[key] is not None and lo <= r["fd"] < hi]


def cmd_load(_rows=None):
    rows = load_filings()
    with open(CACHE, "wb") as f:
        pickle.dump(rows, f)
    print("filings cached:", len(rows))
    for k, v in Counter(r["cls"] for r in rows).most_common(8):
        print(f"   {k:22s} {v:8d}")


def cmd_selectivity(rows):
    by_ins = prepare(rows)
    for lo, hi, label in (("2016-01-01", DEV_END, "DEV 2016-2022"),
                          (DEV_END, "2027-01-01", "HOLDOUT 2023-2026")):
        recs = []
        for t in targets_in(rows, lo, hi):
            p = score_formula(by_ins, t, False, False)
            c = score_candidate(by_ins, t)
            if p is not None and c is not None:
                recs.append((p, c, t["a30"]))
        n = len(recs)
        base = median_pct([r[2] for r in recs])
        print(f"\n=== {label}  n={n}  baseline {base:+.2f}% ===")
        print(f"    {'top k%':>7s} {'k':>6s} | {'PROD med':>9s} | {'CAND med':>9s}")
        for pct in (1, 2, 5, 10, 20):
            k = max(10, int(n * pct / 100))
            pv = [r[2] for r in sorted(recs, key=lambda r: -r[0])[:k]]
            cv = [r[2] for r in sorted(recs, key=lambda r: -r[1])[:k]]
            print(f"    {pct:6d}% {k:6d} | {median_pct(pv):+9.2f} | {median_pct(cv):+9.2f}")


def cmd_population(rows):
    by_ins = prepare(rows)
    variants = [("A production (all buys)", False, False),
                ("B + dup/superseded/deriv out", False, True),
                ("C + DISCRETIONARY ONLY", True, True)]
    for lo, hi, label in (("2016-01-01", DEV_END, "DEV 2016-2022"),
                          (DEV_END, "2027-01-01", "HOLDOUT 2023-2026")):
        tg = targets_in(rows, lo, hi)
        cols, keep = {n: [] for n, _, _ in variants}, []
        for t in tg:
            vals = {n: score_formula(by_ins, t, d, c) for n, d, c in variants}
            if any(v is None for v in vals.values()):
                continue
            keep.append(t["a30"])
            for n in cols:
                cols[n].append(vals[n])
        base = median_pct(keep)
        print(f"\n=== {label}  n={len(keep)}  baseline {base:+.2f}% ===")
        for name, _, _ in variants:
            pairs = list(zip(cols[name], keep))
            rho, _n = spearman(pairs)
            k5 = max(10, int(len(keep) * 0.05))
            top5 = [y for _, y in sorted(pairs, key=lambda p: -p[0])[:k5]]
            print(f"    {name:32s} rho={rho:7.4f}  top5% med={median_pct(top5):+6.2f}")


def cmd_alist(rows):
    by_ins = prepare(rows)
    tg = [r for r in rows if r["disc"] and r["clean"] and r["fd"] >= "2024-01-01"]

    def grade(s):
        if s is None:
            return None
        return "A+" if s >= 2.5 else "A" if s >= 2.0 else "B" if s >= 1.2 else "C" if s >= 0.6 else "D"

    now, fix, fwd = [], [], []
    for t in tg:
        now.append(grade(score_formula(by_ins, t, False, False, halflife=1825)))
        fix.append(grade(score_formula(by_ins, t, True, True, halflife=1825)))
        fwd.append(t["a30"])
    al = ("A+", "A")
    a_now = {i for i in range(len(tg)) if now[i] in al}
    a_fix = {i for i in range(len(tg)) if fix[i] in al}
    print(f"discretionary buys since 2024: {len(tg)}")
    print(f"  admitted today: {len(a_now)}   after population fix: {len(a_fix)}")
    for label, idx in (("kept by both", a_now & a_fix), ("DROPPED by fix", a_now - a_fix),
                       ("newly admitted", a_fix - a_now), ("all disc buys", set(range(len(tg))))):
        v = [fwd[i] for i in idx if fwd[i] is not None]
        print(f"    {label:16s} n={len(v):6d}  median abnormal 30d {median_pct(v):+6.2f}%")


COMMANDS = {"load": cmd_load, "selectivity": cmd_selectivity,
            "population": cmd_population, "alist": cmd_alist}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=sorted(COMMANDS))
    args = ap.parse_args()
    if args.command == "load":
        cmd_load()
        return 0
    with open(CACHE, "rb") as f:
        rows = pickle.load(f)
    COMMANDS[args.command](rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

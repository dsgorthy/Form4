#!/usr/bin/env python3
"""Decile curves, interaction cells and candidate counts on the TRADEABLE basis.

`signal_screen.py` answers "does this signal order returns" with a median split.
That is the right first question and the wrong second one. A median split cannot
say WHERE a threshold should sit, whether two signals are one setup or two, or
whether a proposed filter admits enough candidates to fill a book's slots. Those
are the three things you need before writing a filter into a yaml, and guessing
them from a median split is how a threshold becomes a curve fit.

So this is the companion, not a replacement, and it deliberately reuses the
shipped pieces rather than re-implementing them:

  - episodes from `exit_horizon_study.to_episodes` (gap-based chaining; a fixed
    calendar bucket inflated episode counts 13.8% and reversed two findings)
  - the ticker-clustered bootstrap t from `signal_screen._clustered_t` (the iid
    SE understates dispersion 1.38-1.62x on this corpus)
  - filing-anchored labels only (`abnormal_*td_from_filing`)

MODES

  --decile COL      decile table for one numeric signal, with the clustered t
                    for the top decile against the bottom
  --cells A,B       mean return per cell of two signals, to see whether they
                    are independent or one setup
  --count SPEC      episodes per YEAR that survive a filter spec, because a
                    filter that admits four candidates a year cannot fill three
                    slots and the book becomes SPY with extra steps

Usage:
    python3 scripts/signal_cells.py --since 2016-01-01 --until 2021-12-31 \\
        --decile value_pct_of_adv --grade "A+,A,B"
    python3 scripts/signal_cells.py --since 2016-01-01 --until 2021-12-31 \\
        --cells above_sma50,pct_off_52w_high
    python3 scripts/signal_cells.py --since 2016-01-01 --until 2021-12-31 \\
        --count "grade=A+,A,B;value_pct_of_adv>=0.5"
"""
from __future__ import annotations

import argparse
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402
from scripts.exit_horizon_study import to_episodes  # noqa: E402
from scripts.signal_screen import _clustered_t  # noqa: E402

HORIZON = 21

COLS = [
    "value_pct_of_adv", "pct_off_52w_high", "above_sma50", "above_sma200",
    "is_largest_ever", "dip_1mo", "dip_3mo", "week52_proximity",
    "ret_20d_pre_filing", "pit_cluster_size", "value", "is_csuite",
    "career_grade", "pit_grade",
]


def fetch(conn, since, until, grade, horizon):
    where = ["t.signal_class = 'discretionary_buy'",
             "NOT COALESCE(t.value_suspect, FALSE)",
             "t.filing_date >= ?", "t.filing_date <= ?",
             f"r.abnormal_{horizon}td_from_filing IS NOT NULL"]
    params = [since, until]
    if grade:
        marks = ",".join("?" for _ in grade)
        where.append(f"t.career_grade IN ({marks})")
        params += grade
    cols = ", ".join(f"t.{c}" for c in COLS)
    return conn.execute(f"""
        SELECT t.insider_id, t.ticker, t.filing_date, {cols},
               r.abnormal_{horizon}td_from_filing AS y
          FROM trades t JOIN trade_returns r USING (trade_id)
         WHERE {' AND '.join(where)}
         ORDER BY t.insider_id, t.ticker, t.filing_date
    """, tuple(params)).fetchall()


def _stats(vals):
    if not vals:
        return (0, 0.0, 0.0, 0.0)
    return (len(vals), statistics.mean(vals) * 100,
            statistics.median(vals) * 100,
            100.0 * sum(1 for v in vals if v > 0) / len(vals))


def deciles(eps, col):
    have = [r for r in eps if r[col] is not None]
    if len(have) < 200:
        print(f"  {col}: only {len(have)} episodes, skipping")
        return
    vals = sorted(float(r[col]) for r in have)
    cuts = [vals[int(len(vals) * i / 10)] for i in range(1, 10)]
    print(f"\n{col} — {len(have):,} episodes, {HORIZON}td abnormal vs SPY")
    print(f"{'decile':>7}{'from':>12}{'to':>12}{'n':>7}{'mean %':>9}"
          f"{'median %':>10}{'win %':>7}")
    buckets = defaultdict(list)
    for r in have:
        v = float(r[col])
        k = 0
        while k < 9 and v > cuts[k]:
            k += 1
        buckets[k].append(r)
    for k in range(10):
        rs = buckets.get(k, [])
        n, mean, med, win = _stats([r["y"] for r in rs])
        lo = "-inf" if k == 0 else f"{cuts[k-1]:.4g}"
        hi = "+inf" if k == 9 else f"{cuts[k]:.4g}"
        print(f"{k+1:>7}{lo:>12}{hi:>12}{n:>7}{mean:>9.2f}{med:>10.2f}{win:>7.0f}")
    top = [(r["ticker"], r["y"]) for r in buckets.get(9, [])]
    bot = [(r["ticker"], r["y"]) for r in buckets.get(0, [])]
    t = _clustered_t(top, bot)
    print(f"  top decile minus bottom: "
          f"{_stats([y for _, y in top])[1] - _stats([y for _, y in bot])[1]:+.2f} pp, "
          f"clustered t = {t:+.2f}")


def cells(eps, a, b):
    """Mean return per cell. Numerics split at their tertiles, flags at 0/1."""
    def bands(col):
        have = [float(r[col]) for r in eps if r[col] is not None]
        uniq = sorted(set(have))
        if len(uniq) <= 2:
            return [("0", lambda v: v == uniq[0]), ("1", lambda v: v != uniq[0])]
        q1, q2 = (statistics.quantiles(have, n=3) if len(have) > 3
                  else (uniq[0], uniq[-1]))
        return [(f"<={q1:.3g}", lambda v, q=q1: v <= q),
                (f"{q1:.3g}..{q2:.3g}", lambda v, a_=q1, b_=q2: a_ < v <= b_),
                (f">{q2:.3g}", lambda v, q=q2: v > q)]

    ba, bb = bands(a), bands(b)
    print(f"\n{a} (rows) x {b} (cols) — mean {HORIZON}td abnormal %, n in ()")
    print(f"{'':>18}" + "".join(f"{lb:>20}" for lb, _ in bb))
    for la, fa in ba:
        line = f"{la:>18}"
        for lb, fb in bb:
            rs = [r for r in eps
                  if r[a] is not None and r[b] is not None
                  and fa(float(r[a])) and fb(float(r[b]))]
            n, mean, _, _ = _stats([r["y"] for r in rs])
            line += f"{f'{mean:+.2f} ({n})':>20}"
        print(line)


def parse_spec(spec):
    """'grade=A+,A,B;value_pct_of_adv>=0.5;above_sma50=1' -> predicate list."""
    preds = []
    for clause in [c for c in spec.split(";") if c.strip()]:
        if clause.startswith("grade="):
            wanted = clause.split("=", 1)[1].split(",")
            preds.append((clause, lambda r, w=wanted: r["career_grade"] in w))
        elif ">=" in clause:
            col, v = clause.split(">=")
            preds.append((clause, lambda r, c=col.strip(), t=float(v):
                          r[c] is not None and float(r[c]) >= t))
        elif "<=" in clause:
            col, v = clause.split("<=")
            preds.append((clause, lambda r, c=col.strip(), t=float(v):
                          r[c] is not None and float(r[c]) <= t))
        elif "=" in clause:
            col, v = clause.split("=")
            preds.append((clause, lambda r, c=col.strip(), t=float(v):
                          r[c] is not None and float(r[c]) == t))
        else:
            raise SystemExit(f"cannot parse clause: {clause!r}")
    return preds


def count(eps, spec):
    preds = parse_spec(spec)
    kept = [r for r in eps if all(f(r) for _, f in preds)]
    n, mean, med, win = _stats([r["y"] for r in kept])
    print(f"\nfilter: {spec}")
    print(f"  {n:,} of {len(eps):,} episodes survive "
          f"({100.0*n/max(len(eps),1):.1f}%) — mean {mean:+.2f}%, "
          f"median {med:+.2f}%, win {win:.0f}%")
    per_year = defaultdict(int)
    for r in kept:
        per_year[r["filing_date"][:4]] += 1
    print("  episodes per year: " +
          "  ".join(f"{y}:{per_year[y]}" for y in sorted(per_year)))
    if kept:
        base = [(r["ticker"], r["y"]) for r in eps if r not in kept] or None
        if base:
            t = _clustered_t([(r["ticker"], r["y"]) for r in kept], base)
            print(f"  vs everything it excludes: clustered t = {t:+.2f}")


def _set_horizon(h: int) -> None:
    global HORIZON
    HORIZON = h


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--until", required=True,
                    help="TRAIN window end — required, so a threshold cannot "
                         "be chosen on the holdout")
    ap.add_argument("--horizon", type=int, default=HORIZON)
    ap.add_argument("--grade", default=None)
    ap.add_argument("--decile", action="append", default=[])
    ap.add_argument("--cells", action="append", default=[])
    ap.add_argument("--count", action="append", default=[])
    args = ap.parse_args()
    _set_horizon(args.horizon)

    grade = args.grade.split(",") if args.grade else None
    conn = get_connection(readonly=True)
    rows = fetch(conn, args.since, args.until, grade, args.horizon)
    eps = to_episodes(rows)
    print(f"{len(rows):,} filings -> {len(eps):,} episodes, "
          f"{args.grade or 'all grades'}, filed {args.since}..{args.until}, "
          f"{args.horizon}td label")

    n, mean, med, win = _stats([r["y"] for r in eps])
    print(f"baseline: mean {mean:+.2f}%, median {med:+.2f}%, win {win:.0f}%")

    for col in args.decile:
        deciles(eps, col)
    for pair in args.cells:
        a, b = pair.split(",")
        cells(eps, a.strip(), b.strip())
    for spec in args.count:
        count(eps, spec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

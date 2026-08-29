#!/usr/bin/env python3
"""Which available signals actually order forward returns? Episode-level.

WHY NOW

Two signals reached usable coverage for the first time on 2026-08-27:
industry_buy_pct_90d went from 21.8% to 90-95% populated, and
net_buyer_flow_90d to its ceiling. Both are marked "Observational; no strategy
depends on it yet" in freshness_contracts -- not because they were rejected,
but because there was never enough of them to test. The same is true of the
filing-anchored labels this measures against, which were 28.9% populated in
2024 and are now 84-88%.

So this is the first time these questions can be asked at all.

METHOD

For each signal, split episodes at its median (or on the flag, for booleans)
and compare mean abnormal return at 21 trading days from filing. Reported as a
spread in percentage points with a t-statistic, so a signal that merely sorts
noise is visible as one.

THE UNIT IS THE EPISODE. One insider buying RCG fourteen times in three weeks
is one bet. Counting filings inflates n by ~3.4x here, shrinks every interval,
and manufactures significance -- it produced two false alarms on 2026-08-25.

WHAT A HIT HERE IS AND IS NOT

A spread with |t| > 2 says the signal orders returns on the full corpus. It
does NOT say it will help a book that already filters on grade, conviction and
concentration -- the candidates that survive those filters are not this
population. Anything promising here has to be re-tested inside the simulator.
"""
from __future__ import annotations

import argparse
import math
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.database import get_connection  # noqa: E402

HORIZON = 21
EPISODE_GAP_DAYS = 30

#: Bootstrap replications for the ticker-clustered standard error.
BOOTSTRAP_REPS = 400
BOOTSTRAP_SEED = 20260828


def _clustered_t(hi_c, lo_c, reps: int = BOOTSTRAP_REPS) -> float:
    """t for the hi-lo spread, with the standard error CLUSTERED BY TICKER.

    Episodes on the same ticker are not independent draws: they share the
    issuer's news, its sector, its liquidity, and often overlap in time. The
    iid standard error therefore understates dispersion. Measured on this
    corpus it understates it by 1.38x to 1.62x, which is the difference
    between a result and an artefact:

        value               +0.32pp   iid t=+2.43   clustered t=+1.76
        pit_cluster_size    -0.63pp   iid t=-4.50   clustered t=-2.79

    Both were reported as hits on the iid figure. Neither clears the
    multiple-comparison threshold once clustered.

    A cluster bootstrap rather than a closed form: resample TICKERS with
    replacement, keeping every episode of a drawn ticker, and take the spread's
    dispersion across replications. Seeded, so a screen is reproducible.
    """
    import random
    by_t: dict = {}
    for t_, y in hi_c:
        by_t.setdefault(t_, ([], []))[0].append(y)
    for t_, y in lo_c:
        by_t.setdefault(t_, ([], []))[1].append(y)
    tickers = list(by_t)
    if len(tickers) < 20:
        return 0.0

    def spread(sample) -> float | None:
        h = [y for t_ in sample for y in by_t[t_][0]]
        l = [y for t_ in sample for y in by_t[t_][1]]
        if not h or not l:
            return None
        return statistics.mean(h) - statistics.mean(l)

    point = spread(tickers)
    if point is None:
        return 0.0
    rng = random.Random(BOOTSTRAP_SEED)
    draws = []
    for _ in range(reps):
        sample = [tickers[rng.randrange(len(tickers))] for _ in tickers]
        v = spread(sample)
        if v is not None:
            draws.append(v)
    if len(draws) < 30:
        return 0.0
    se = statistics.pstdev(draws)
    return point / se if se else 0.0

# (column, kind) — 'num' splits at the median, 'flag' splits on 0/1.
SIGNALS = [
    # Built 2026-08-28, never screened before this run.
    ("pct_of_prior_holding",     "num"),   # insider grew their OWN stake
    ("pct_off_52w_high",         "num"),   # literature's top feature (36% importance)
    ("value_pct_of_adv",         "num"),   # size normalised by liquidity
    ("ret_20d_pre_filing",       "num"),   # momentum into disclosure
    ("ret_60d_pre_filing",       "num"),
    ("ret_trade_to_filing",      "num"),   # the move the insider saw, we ignored
    ("filing_lag_days",          "num"),   # disclosure delay
    ("industry_buy_pct_90d",     "num"),
    ("net_buyer_flow_90d",       "num"),
    ("pit_cluster_size",         "num"),
    ("week52_proximity",         "num"),
    ("dip_3mo",                  "num"),
    ("dip_1mo",                  "num"),
    ("consecutive_sells_before", "num"),
    ("value",                    "num"),
    ("is_largest_ever",          "flag"),
    ("above_sma50",              "flag"),
    ("above_sma200",             "flag"),
    ("is_rare_reversal",         "flag"),
]


def _bonferroni_t(n_tests: int) -> float:
    """Two-sided 5% critical value after Bonferroni, normal approximation."""
    if n_tests <= 1:
        return 1.96
    p = 0.05 / n_tests / 2.0
    # Acklam-style inverse normal, adequate at these tail probabilities.
    import math as _m
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    pl = 0.02425
    if p < pl:
        q = _m.sqrt(-2 * _m.log(p))
        z = (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
            ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    else:
        q = p - 0.5
        r = q * q
        z = (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
            (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    return abs(z)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--grade", default=None,
                    help="restrict to a grade band, e.g. 'A+,A,B'")
    args = ap.parse_args()

    cols = ", ".join(f"t.{c}" for c, _ in SIGNALS)
    where_grade = ""
    params = [args.since]
    if args.grade:
        marks = ",".join("?" for _ in args.grade.split(","))
        where_grade = f" AND t.career_grade IN ({marks})"
        params += args.grade.split(",")

    conn = get_connection(readonly=True)
    rows = conn.execute(f"""
        SELECT t.insider_id, t.ticker, t.filing_date, {cols},
               r.abnormal_{HORIZON}td_from_filing AS y
          FROM trades t JOIN trade_returns r USING (trade_id)
         WHERE t.signal_class = 'discretionary_buy'
           AND NOT COALESCE(t.value_suspect, FALSE)
           AND t.filing_date >= ?{where_grade}
           AND r.abnormal_{HORIZON}td_from_filing IS NOT NULL
         ORDER BY t.insider_id, t.ticker, t.filing_date
    """, tuple(params)).fetchall()

    # collapse to episodes
    from datetime import date
    def d(s): return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    eps, key, last = [], None, None
    for r in rows:
        k = (r["insider_id"], r["ticker"]); fd = d(r["filing_date"])
        if k != key or last is None or (fd - last).days > EPISODE_GAP_DAYS:
            eps.append(r); key = k
        last = fd

    band = args.grade or "all grades"
    print(f"Signal screen — {len(rows):,} filings -> {len(eps):,} episodes, "
          f"{band}, {HORIZON}td abnormal return\n")
    print(f"{'signal':<26}{'n_hi':>7}{'n_lo':>7}{'hi %':>8}{'lo %':>8}"
          f"{'spread':>9}{'t':>7}")
    print("-" * 72)

    out = []
    for col, kind in SIGNALS:
        vals3 = [(r[col], r["y"], r["ticker"]) for r in eps
                 if r[col] is not None]
        vals = vals3
        if len(vals) < 200:
            continue
        if kind == "flag":
            hi_c = [(t_, y) for v, y, t_ in vals3 if float(v) == 1]
            lo_c = [(t_, y) for v, y, t_ in vals3 if float(v) == 0]
        else:
            med = statistics.median([float(v) for v, _, _ in vals3])
            hi_c = [(t_, y) for v, y, t_ in vals3 if float(v) > med]
            lo_c = [(t_, y) for v, y, t_ in vals3 if float(v) <= med]
        hi = [y for _, y in hi_c]
        lo = [y for _, y in lo_c]
        if len(hi) < 100 or len(lo) < 100:
            continue
        mh, ml = statistics.mean(hi) * 100, statistics.mean(lo) * 100
        t = _clustered_t(hi_c, lo_c)
        out.append((abs(t), col, len(hi), len(lo), mh, ml, mh - ml, t))

    # Bonferroni over the signals actually screened. At 12 signals the
    # two-sided 5% threshold is |t| > 2.87, not 2.00 -- at 2.00 the expected
    # number of false positives across this table is about 0.55.
    thresh = _bonferroni_t(len(out))
    for _, col, nh, nl, mh, ml, sp, t in sorted(out, reverse=True):
        star = " <<<" if abs(t) >= thresh else ""
        print(f"{col:<26}{nh:>7}{nl:>7}{mh:>8.2f}{ml:>8.2f}{sp:>+9.2f}{t:>+7.2f}{star}")
    print(f"\n<<< = |t| >= {thresh:.2f}: ticker-CLUSTERED standard error, "
          f"Bonferroni-corrected for {len(out)} signals screened.\n"
          "Must still be re-tested inside the simulator -- the candidates "
          "surviving grade/conviction filters are not this population.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

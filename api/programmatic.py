"""Is this filing part of a PROGRAMME rather than a decision?

THE SINGLE DEFINITION. Four overlapping notions of "routine" existed before
this module and none of them agreed:

  cohen_routine           Cohen et al. (2012): traded the same ticker in the
                          same calendar month for 3+ consecutive prior years.
                          KEPT SEPARATE AND UNCHANGED — it is a cited academic
                          measure answering a different question, and folding
                          it in would destroy a defensible published number.
  is_recurring            3+ buys at regular intervals. BUY SIDE ONLY, and
                          cadence only. Subsumed here.
  is_distribution_program 4+ sell filings in a window. Lived only inside
                          generate_stocktwits_posts.py, sell side only, a raw
                          count with no notion of regularity. Subsumed here.
  is_routine              a dead column. NOTHING has ever written it, yet
                          compute_trade_grade still deducts 5 points for it.
                          Replaced by this.

TWO QUESTIONS, NOT ONE

  FREQUENCY   how often does this insider do this? A quarterly seller against
              a once-a-decade one. Answered by `median_interval_days` and
              `n_filings`, and it holds even when the amounts are erratic.
  REGULARITY  is it a metronome — same cadence AND same size? Answered by
              `is_programmatic`, which is a strict subset of the above.

These are different and both are worth having. Neither is the same as
`is_10b5_1`, which is a DISCLOSURE MECHANISM: plenty of people sell every
quarter with no plan on file, and plenty of plans fire irregularly.
`signal_class` is different again — it classifies the FILING (comp grant,
scheduled, discretionary), not the insider's habit.

WHAT MAKES A PROGRAMME

A programme is regular in BOTH cadence and size. Either alone is ordinary:
plenty of people buy every quarter in wildly different amounts, and plenty buy
the same amount at random times. It is the pair that identifies a metronome —
CRWD's CEO filing $3.67M, $3.61M, $3.71M, $7.39M in a month, which none of the
incumbent flags could see because none of them looks at size.

Measured per insider+ticker+DIRECTION on FILINGS, never lots: a purchase
filled in five tranches is one decision, and counting tranches would make every
large fill look like a programme.

BOTH SIDES, deliberately. is_recurring being buy-only is why the sell-side case
had to be reinvented inside the Stocktwits generator.

PREDICTIVE WEIGHT IS SMALL — measured on filing-anchored 21td abnormal returns,
median spread +0.42pp on buys and -0.80pp on sells against roughly +10pp for a
deep dip. So this earns its place as a TAG and as context, not as a heavy gate.
Worth knowing: the sell-side sign is the opposite of the folk assumption.
Programmatic sells are followed by BIGGER declines (-1.13% vs -0.33%), i.e.
more informative than one-off sells, not less.
"""
from __future__ import annotations

from statistics import mean, pstdev

#: Minimum filings before a sequence can be called a programme. Two points make
#: a line and tell you nothing about regularity.
MIN_FILINGS = 3

#: Coefficient of variation ceilings: stdev/mean of the gaps between filings,
#: and of the filing values. Chosen from the threshold sweep where both sides
#: separated most cleanly. RE-VALIDATE against the full corpus before treating
#: these as settled — the sweep ran while the filing-anchored backfill was
#: still in flight.
MAX_CV_INTERVAL = 0.5
MAX_CV_VALUE = 0.3


def coefficient_of_variation(values: list[float]) -> float | None:
    """stdev/mean. None when it is not defined rather than 0, so a caller
    cannot mistake "no dispersion measurable" for "perfectly regular"."""
    if len(values) < 2:
        return None
    m = mean(values)
    if not m:
        return None
    return pstdev(values) / abs(m)


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    n = len(ys)
    return ys[n // 2] if n % 2 else (ys[n // 2 - 1] + ys[n // 2]) / 2


def score_sequence(
    dates_and_values: list[tuple],
    min_filings: int = MIN_FILINGS,
) -> dict:
    """Score one insider+ticker+direction sequence.

    `dates_and_values` is [(date, filing_value), ...] for FILINGS, any order.
    Returns cv_interval, cv_value, n_filings and is_programmatic. A sequence
    too short to judge comes back is_programmatic=0 with None CVs — never a
    default that reads as "regular".
    """
    rows = sorted(dates_and_values, key=lambda x: x[0])
    n = len(rows)
    if n < 2:
        return {"n_filings": n, "cv_interval": None, "cv_value": None,
                "median_interval_days": None, "is_programmatic": 0}

    gaps = [(rows[i][0] - rows[i - 1][0]).days for i in range(1, n)]
    values = [float(v) for _, v in rows if v]

    # REGULARITY NEEDS ENOUGH FILINGS TO JUDGE; FREQUENCY DOES NOT.
    #
    # The n >= 2 gate above exists so a median gap can be reported from a
    # single interval -- "this insider files about quarterly" is meaningful
    # from two filings. A COEFFICIENT OF VARIATION IS NOT. One gap has zero
    # dispersion by construction, and two equal values give cv_value = 0.0,
    # which reads as "perfectly regular sizes" from two data points -- the
    # exact opposite of what two data points tell you.
    #
    # Loosening the gate without re-guarding these is precisely what happened:
    # score_sequence(2 filings) returned cv_value 0.0, and the test that would
    # have caught it was silenced into `assert ... or True` rather than fixed.
    # compute_programmatic.py persists prog_cv_value, so a future reader
    # thresholding the stored column would have read two-filing insiders as
    # metronomes.
    judgeable = n >= min_filings
    cv_i = coefficient_of_variation([float(g) for g in gaps]) if judgeable else None
    cv_v = coefficient_of_variation(values) if judgeable else None

    # FREQUENCY IS A SEPARATE QUESTION FROM REGULARITY, and both are useful.
    #
    # "Does this insider sell every quarter or once a decade" is answered by
    # the median gap, and it holds for someone whose amounts are lumpy — which
    # is_programmatic deliberately excludes. A quarterly seller with erratic
    # sizes is routine in the plain sense and not a metronome. Exposing the
    # cadence rather than only the boolean lets a caller ask either question
    # without re-deriving it, which is how five definitions of "routine"
    # happened the first time.
    #
    #   ~30 days  monthly      ~90 days  quarterly     ~365 days  annual
    med_gap = _median([float(g) for g in gaps])

    programmatic = int(
        n >= min_filings
        and cv_i is not None and cv_v is not None
        and cv_i <= MAX_CV_INTERVAL and cv_v <= MAX_CV_VALUE
    )
    return {"n_filings": n, "cv_interval": cv_i, "cv_value": cv_v,
            "median_interval_days": med_gap, "is_programmatic": programmatic}


def is_programmatic(cv_interval: float | None, cv_value: float | None,
                    n_filings: int | None) -> bool:
    """The predicate, for callers holding stored columns rather than a sequence.

    Every consumer must come through here or through the stored
    `is_programmatic` column. Re-typing the thresholds at a call site is how
    four definitions became four definitions.
    """
    return bool(
        (n_filings or 0) >= MIN_FILINGS
        and cv_interval is not None and cv_value is not None
        and cv_interval <= MAX_CV_INTERVAL and cv_value <= MAX_CV_VALUE
    )

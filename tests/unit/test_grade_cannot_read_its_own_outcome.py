"""A trade must not appear in the track record used to grade it.

WHAT WENT WRONG

`_get_returns` in pit_scoring gathered an insider's observable history with two
guards: `trade_date <= as_of_date - lag` (the forward return has matured) and
`filing_date <= as_of_date` (we knew about the trade). Neither excludes the
trade BEING GRADED.

For a prompt filing that is harmless: the score is stamped as_of filing_date,
the trade executed two days earlier, and the 7d cutoff is ten days back, so it
falls outside. But a Form 4 lodged 124 days after execution clears every
cutoff — 7d/30d/90d lags are 10/40/100 — and `filing_date <= as_of_date` is
satisfied with equality. The trade lands in its own history, carrying its own
realised return.

MEASURED, on 225,836 graded discretionary buys — a trade's own 90d abnormal
return, grouped by the grade that trade received:

    clean (lag <= 100d)   A+/A/B   0.69%   C/D   1.65%   gap  -0.96pp
    late  (lag >  100d)   A+/A/B  36.59%   C/D  -6.94%   gap +43.53pp

On clean rows the grade knows nothing about the outcome. On late rows it knows
43.5 points' worth. 5.4% of graded buys can self-grade (2.44% via the 7d window
alone, 1.22% via 7d+30d, 1.78% via all three).

WHAT IT COST: a `min_filing_lag_days=21` book backtested at 115.6/55.2/98.8
CAGR across three folds with an 81.4% win rate, turning $100k into $146.7M. It
survived every other check — entry timing, survivorship, delisting, splits,
liquidity, regime, the label generator — because none of those was the problem.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "strategies" / "insider_catalog" / "pit_scoring.py"


def _returns_query() -> str:
    s = SRC.read_text(encoding="utf-8")
    body = s[s.index("def _get_returns"):]
    return body[:body.index("return [")]


def test_filing_date_guard_is_strict():
    """`<=` admits the trade being graded; `<` does not."""
    q = _returns_query()
    assert re.search(r"t\.filing_date\s*<\s*\?", q), (
        "the filing_date guard is not strict. With `<=`, a trade filed long "
        "after execution enters its own track record and grades itself on its "
        "own realised return."
    )
    assert not re.search(r"t\.filing_date\s*<=\s*\?", q), (
        "the non-strict filing_date guard is back"
    )


def test_the_trade_date_lag_guard_survives():
    """The strict filing guard does not replace the maturity guard — both are
    needed. trade_date <= as_of - lag is what makes the forward return
    observable at all."""
    q = _returns_query()
    assert re.search(r"t\.trade_date\s*<=\s*\?", q), (
        "the observable-return lag guard is gone; forward returns that have "
        "not matured would enter the score"
    )


def test_the_lags_still_bracket_their_windows():
    """A 90d return needs at least 90 days to exist. If a lag ever drops below
    its window the maturity guard becomes decorative."""
    s = SRC.read_text(encoding="utf-8")
    m = re.search(r"_RETURN_LAGS\s*=\s*\{([^}]*)\}", s)
    assert m, "_RETURN_LAGS is gone"
    lags = dict(re.findall(r'"(\d+)d"\s*:\s*(\d+)', m.group(1)))
    assert lags, "could not parse _RETURN_LAGS"
    for window, lag in lags.items():
        assert int(lag) >= int(window), (
            f"the {window}d window has a {lag}-day lag; a return cannot be "
            "observable before the window it measures has elapsed"
        )


def test_the_measurement_is_recorded():
    """The 43.53pp gap is the evidence. Losing it invites someone to relax the
    guard back to <= for the extra observations."""
    q = _returns_query()
    assert "43.53" in q and "0.69" in q, (
        "the measured contamination gap is no longer documented beside the guard"
    )

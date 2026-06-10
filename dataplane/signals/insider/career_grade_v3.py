"""insider.career_grade.v3 — PIT-correct career-track-record grade.

Reference port from Form4. The underlying scoring lives in the existing
form4 PostgreSQL database (column ``trades.career_grade``, populated by
``pipelines/insider_study/compute_career_grades.py``). For Phase 1 of
the dataplane bootstrap, this signal reads form4 directly via a separate
connection — a "bridge" pattern that's acceptable until raw form4 data
is itself ingested as a ``insider.trades.raw`` dataplane signal.

PIT correctness here is borrowed from the upstream scorer:
``insider_ticker_scores.as_of_date`` already pins the point-in-time
constraint. We read scores ``WHERE as_of_date <= (current_as_of - 24h)``
to respect the SEC's 24-hour filing lag.

Phase 2 will:
  1. Ingest form4.trades into ``insider.trades.raw`` (a dataplane signal)
  2. Compute career_grade as a pure dataplane transformation over that raw
  3. Drop the form4 DB bridge from this file
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2

from dataplane import PIT, PITTestCase, Signal, SignalObservation, Upstream


class CareerGradeV3(Signal):
    """A+/A/B/C/D career grade for (insider, ticker) at as_of.

    Output value shape:
        {
          "grade":            "A+" | "A" | "B" | "C" | "D" | "New",
          "score":            float | null,    # underlying V3 score
          "insider_name":     str,             # most recently filed name
          "as_of":            ISO timestamp,
          "source":           "form4.bridge"   # phase 1 marker
        }
    """

    signal_id = "insider.career_grade"
    version = "v3.0.1"
    owner = "derek"
    sla_hours = 30.0  # same SLA as form4 freshness contract
    business_hours_only = True
    description = (
        "V3 career-track-record grade (5y half-life). PIT-pinned via "
        "insider_ticker_scores.as_of_date with 24h SEC filing lag."
    )
    upstream = [
        # In phase 1 this signal reads the form4 DB directly. We declare an
        # "external" upstream marker so the catalog page can show the source
        # honestly. The Signal.read() machinery doesn't enforce against this
        # row because the data isn't in signal_observations yet.
        Upstream("external.form4.insider_ticker_scores", pit_lag=timedelta(hours=24)),
    ]
    output_schema = {
        "grade": "text",
        "score": "real",
        "insider_name": "text",
        "as_of": "text",
        "source": "text",
    }

    # Phase 1 bridge: separate DB connection, read directly from form4.
    # Documented in module docstring; replaced in phase 2.
    _FORM4_DSN = "dbname=form4 host=localhost"

    @PIT.strict
    def compute(self, ticker: str, as_of: datetime) -> SignalObservation:
        # The upstream lag is enforced manually here because the bridge
        # bypasses Signal.read(). When we move to in-plane raw ingestion
        # this becomes self.read("insider.trades.raw", ticker, as_of).
        pit_lag = self.upstream[0].pit_lag
        usable_as_of = as_of - pit_lag

        # form4's insider_ticker_scores.as_of_date is TEXT (YYYY-MM-DD).
        # Cast our usable_as_of to the same format for the comparison.
        usable_as_of_str = usable_as_of.strftime("%Y-%m-%d")

        with psycopg2.connect(self._FORM4_DSN) as form4_conn:
            cur = form4_conn.cursor()
            # Best-grade lookup: the most recent insider score row for any
            # insider who traded this ticker before usable_as_of. We pick
            # the highest blended_score with sufficient_data=1.
            cur.execute(
                """
                SELECT i.name,
                       its.blended_score,
                       its.as_of_date
                  FROM insider_ticker_scores its
                  JOIN insiders i ON i.insider_id = its.insider_id
                 WHERE its.ticker = %s
                   AND its.as_of_date <= %s
                   AND its.sufficient_data = 1
                 ORDER BY its.blended_score DESC NULLS LAST,
                          its.as_of_date    DESC
                 LIMIT 1
                """,
                (ticker, usable_as_of_str),
            )
            row = cur.fetchone()
            cur.close()

        if row is None:
            return self.observation(
                ticker, as_of,
                value={
                    "grade": "New",
                    "score": None,
                    "insider_name": None,
                    "as_of": usable_as_of.isoformat(),
                    "source": "form4.bridge",
                },
            )

        insider_name, score, score_as_of = row
        grade = _score_to_grade(score)
        return self.observation(
            ticker, as_of,
            value={
                "grade": grade,
                "score": float(score) if score is not None else None,
                "insider_name": insider_name,
                "as_of": score_as_of.isoformat() if hasattr(score_as_of, "isoformat") else str(score_as_of),
                "source": "form4.bridge",
            },
            confidence=_score_to_confidence(score),
        )

    @classmethod
    def test_cases(cls):
        # Known-stable historical points. Each (ticker, as_of) must produce
        # the same grade under normal + frozen modes. expected_value left
        # None on most because the underlying scorer's exact output drifts
        # with each refresh of insider_ticker_scores; we assert PIT
        # correctness only here, not specific value regression.
        return [
            PITTestCase(
                ticker="AAPL",
                as_of=datetime(2026, 6, 1, tzinfo=timezone.utc),
                description="High-coverage ticker; well-defined output",
            ),
            PITTestCase(
                ticker="NVDA",
                as_of=datetime(2026, 5, 15, tzinfo=timezone.utc),
                description="Active insider buy ticker",
            ),
            PITTestCase(
                ticker="ZZZZZ_NOT_A_TICKER",
                as_of=datetime(2026, 6, 1, tzinfo=timezone.utc),
                description="Unknown ticker should return New",
            ),
        ]


# ── grade thresholds (mirror Form4's compute_career_grades.py) ───────────

def _score_to_grade(score) -> str:
    """Map V3 blended_score → A+/A/B/C/D. Aligned with Form4's bands."""
    if score is None:
        return "New"
    s = float(score)
    if s >= 0.85:
        return "A+"
    if s >= 0.70:
        return "A"
    if s >= 0.50:
        return "B"
    if s >= 0.30:
        return "C"
    return "D"


def _score_to_confidence(score) -> float | None:
    """Confidence is just the raw score clipped to [0, 1]. Phase 2 will
    derive a Bayesian-shrinkage confidence; for now this is good enough
    to populate the column."""
    if score is None:
        return None
    return max(0.0, min(1.0, float(score)))


if __name__ == "__main__":
    # CLI: python3 -m signals.insider.career_grade_v3 --ticker AAPL --as-of 2026-06-01
    import argparse
    import json as _json

    p = argparse.ArgumentParser()
    p.add_argument("--ticker", required=True)
    p.add_argument("--as-of", required=True,
                   help="ISO date (UTC, e.g. 2026-06-01 or 2026-06-01T12:00:00Z)")
    args = p.parse_args()

    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

    signal = CareerGradeV3()
    obs = signal.compute(args.ticker, as_of)
    print(_json.dumps({
        "signal_id": obs.signal_id,
        "ticker": obs.ticker,
        "as_of_date": obs.as_of_date.isoformat(),
        "value": obs.value,
        "confidence": obs.confidence,
    }, indent=2))

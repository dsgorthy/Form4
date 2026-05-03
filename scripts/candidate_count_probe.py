#!/usr/bin/env python3
"""Daily candidate-count probe.

Re-implements the SQL filters from `cw_runner.py:_build_thesis_query` and
counts how many candidates each strategy *would have* generated today.
This is independent of whether the runner actually ran — it's a pure
data-side audit of the strategies' filterable universe.

Why this matters: the April 2026 silent outage manifested as `0 candidates`
in the runner's logs every morning for 21 days. A daily zero-candidate
alert at 18:00 ET would have caught it on Day 1. This probe is that alert.

Alert thresholds:
  - count = 0 on a market day → P0 immediately
  - count = 0 for 2 consecutive market days → P0 escalation
  - count > 0 after a stale period → recovery notification

State: `logs/candidate_count_state.json`

Usage (Studio):
    python3 scripts/candidate_count_probe.py
    python3 scripts/candidate_count_probe.py --json
    python3 scripts/candidate_count_probe.py --no-alert    # dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection
from framework.alerts.log import alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STATE_FILE = REPO / "logs" / "candidate_count_state.json"

# Strategy filter SQL — keep in sync with cw_runner.py:_build_thesis_query.
# We use a 7-day window here (vs the runner's 2-day) so the probe is robust
# against weekends and 1-day holidays without false-alerting.
STRATEGY_FILTERS = {
    "quality_momentum": """
        SELECT COUNT(*) FROM trades
         WHERE trans_code = 'P'
           AND filing_date >= ?
           AND COALESCE(is_duplicate, 0) = 0
           AND pit_grade IN ('A+', 'A')
           AND above_sma50 = 1
           AND above_sma200 = 1
           AND COALESCE(is_recurring, 0) = 0
           AND COALESCE(is_tax_sale, 0) = 0
    """,
    "reversal_dip": """
        SELECT COUNT(*) FROM trades
         WHERE trans_code = 'P'
           AND filing_date >= ?
           AND COALESCE(is_duplicate, 0) = 0
           AND is_rare_reversal = 1
           AND consecutive_sells_before >= 10
           AND dip_3mo <= -0.25
           AND COALESCE(is_recurring, 0) = 0
           AND COALESCE(is_tax_sale, 0) = 0
           AND COALESCE(cohen_routine, 0) = 0
           AND COALESCE(is_10b5_1, 0) = 0
    """,
    # tenb51_surprise needs a correlated subquery; build separately below.
    "tenb51_surprise": None,
}


def count_tenb51(conn, since: str) -> int:
    """tenb51_surprise: insider has ≥5 prior 10b5-1 sells on this ticker."""
    row = conn.execute("""
        SELECT COUNT(*)
          FROM trades b
         WHERE b.trans_code = 'P'
           AND b.filing_date >= ?
           AND COALESCE(b.is_duplicate, 0) = 0
           AND COALESCE(b.is_recurring, 0) = 0
           AND COALESCE(b.is_tax_sale, 0) = 0
           AND (SELECT COUNT(*) FROM trades x
                 WHERE x.insider_id = b.insider_id
                   AND x.ticker = b.ticker
                   AND x.trans_code = 'S'
                   AND x.is_10b5_1 = 1
                   AND x.filing_date < b.filing_date) >= 5
    """, (since,)).fetchone()
    return int(row[0]) if row else 0


def is_market_day(d: date) -> bool:
    """Mon-Fri, no major holidays (cheap heuristic — finer logic if needed)."""
    return d.weekday() < 5


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(STATE_FILE)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lookback-days", type=int, default=7,
                   help="Trailing window for candidate count (default 7)")
    p.add_argument("--json", action="store_true")
    p.add_argument("--no-alert", action="store_true")
    args = p.parse_args()

    today = date.today()
    since = (today - timedelta(days=args.lookback_days)).isoformat()
    now = datetime.now(timezone.utc).isoformat()

    conn = get_connection(readonly=True)
    state = _load_state()
    today_key = today.isoformat()
    history = state.setdefault("history", {})

    results: dict[str, int] = {}
    for strategy, sql in STRATEGY_FILTERS.items():
        if sql is None:
            results[strategy] = count_tenb51(conn, since)
        else:
            row = conn.execute(sql, (since,)).fetchone()
            results[strategy] = int(row[0]) if row else 0

    conn.close()

    history[today_key] = {
        "checked_at": now,
        "lookback_days": args.lookback_days,
        "counts": results,
    }
    state["history"] = {k: v for k, v in history.items()
                        if k >= (today - timedelta(days=30)).isoformat()}
    _save_state(state)

    # Alert logic
    market_day = is_market_day(today)
    yesterday_key = (today - timedelta(days=1)).isoformat()
    prev_counts = history.get(yesterday_key, {}).get("counts", {})

    new_zeros: list[str] = []
    consecutive_zeros: list[str] = []
    recovered: list[str] = []
    for strategy, count in results.items():
        prev = prev_counts.get(strategy)
        if count == 0 and market_day:
            # First-day zero alert (transition from >0 or first observation)
            if prev is None or prev > 0:
                new_zeros.append(strategy)
            else:
                # 2+ consecutive trading days at zero → P0 escalation
                consecutive_zeros.append(strategy)
        elif count > 0 and prev == 0:
            recovered.append(strategy)

    if new_zeros and not args.no_alert:
        body = "\n".join(f"  • {s} produced 0 qualifying candidates today"
                         for s in new_zeros)
        alert.critical(
            "candidate_count_probe",
            f"{len(new_zeros)} strategy(ies) silent today (1d):\n{body}\n\nRunbook: R-002",
            silent_strategies=new_zeros,
        )

    if consecutive_zeros and not args.no_alert:
        body = "\n".join(f"  • {s} has been silent for ≥2 consecutive market days"
                         for s in consecutive_zeros)
        alert.critical(
            "candidate_count_probe",
            f"P0 ESCALATION — {len(consecutive_zeros)} strategy(ies) sustained silence:\n{body}\n\nRunbook: R-002",
            silent_strategies=consecutive_zeros,
            consecutive_days=2,
        )

    if recovered and not args.no_alert:
        body = "\n".join(f"  • {s} produced candidates again ({results[s]} qualifying)"
                         for s in recovered)
        alert.info("candidate_count_probe",
                   f"{len(recovered)} strategy(ies) recovered:\n{body}")

    if args.json:
        print(json.dumps({
            "checked_at": now,
            "today": today_key,
            "is_market_day": market_day,
            "lookback_days": args.lookback_days,
            "counts": results,
            "new_zeros": new_zeros,
            "consecutive_zeros": consecutive_zeros,
            "recovered": recovered,
        }, indent=2))
    else:
        print(f"\nCandidate counts (last {args.lookback_days} days, as of {today}):")
        print(f"  market_day = {market_day}")
        for strategy, count in results.items():
            mark = "🚨" if count == 0 and market_day else "✅"
            prev = prev_counts.get(strategy, "—")
            print(f"  {mark} {strategy:22s} {count:>4}  (yesterday: {prev})")
        if new_zeros:
            print(f"\n🚨 {len(new_zeros)} new zero-candidate alert(s) → alert log")
        if consecutive_zeros:
            print(f"\n🆘 {len(consecutive_zeros)} ESCALATION (≥2 days) → alert log")
        if recovered:
            print(f"\n✅ {len(recovered)} recovery notification(s) → alert log")


if __name__ == "__main__":
    main()

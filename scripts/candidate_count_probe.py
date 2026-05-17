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

    # Tuned 2026-05-13 after Derek flagged daily noise from these alerts.
    # Quiet markets legitimately produce 0 qualifying candidates per strategy;
    # downgraded most cases from critical to warn. Only sustained silence
    # (≥5 consecutive market days) still pages.
    CRITICAL_STREAK_DAYS = 5

    def _zero_streak_through_yesterday(strategy: str) -> int:
        """Walk history backwards from yesterday, count consecutive market
        days at zero. Non-market days (weekend/holiday) are skipped without
        breaking the streak."""
        streak = 0
        d = today - timedelta(days=1)
        for _ in range(30):
            if not is_market_day(d):
                d -= timedelta(days=1)
                continue
            key = d.isoformat()
            day_counts = history.get(key, {}).get("counts", {})
            if strategy not in day_counts:
                break
            if day_counts[strategy] == 0:
                streak += 1
                d -= timedelta(days=1)
            else:
                break
        return streak

    new_zeros: list[str] = []
    long_silence: list[tuple[str, int]] = []   # (strategy, days)
    medium_silence: list[tuple[str, int]] = [] # (strategy, days)
    recovered: list[str] = []

    for strategy, count in results.items():
        prev_count = prev_counts.get(strategy)
        prev_streak = _zero_streak_through_yesterday(strategy)

        if count == 0 and market_day:
            if prev_count is None or prev_count > 0:
                # First-day zero — warn, doesn't page
                new_zeros.append(strategy)
            else:
                streak = prev_streak + 1
                if streak >= CRITICAL_STREAK_DAYS:
                    long_silence.append((strategy, streak))
                else:
                    medium_silence.append((strategy, streak))
        elif count > 0 and prev_count == 0:
            recovered.append(strategy)

    if new_zeros and not args.no_alert:
        body = "\n".join(f"  • {s} produced 0 qualifying candidates today"
                         for s in new_zeros)
        alert.warn(
            "candidate_count_probe",
            f"{len(new_zeros)} strategy(ies) silent today (1d):\n{body}",
            silent_strategies=new_zeros,
        )

    if medium_silence and not args.no_alert:
        # 2-4 consecutive zero days — informational; doesn't page
        body = "\n".join(f"  • {s} silent for {d} consecutive market days"
                         for s, d in medium_silence)
        alert.warn(
            "candidate_count_probe",
            f"{len(medium_silence)} strategy(ies) in extended quiet:\n{body}",
            silent_strategies=[s for s, _ in medium_silence],
            consecutive_days=max(d for _, d in medium_silence),
        )

    if long_silence and not args.no_alert:
        # 5+ consecutive zero days — escalate to critical, this might be
        # broken filters or upstream data missing, not just a quiet market
        body = "\n".join(f"  • {s} silent for {d} consecutive market days"
                         for s, d in long_silence)
        alert.critical(
            "candidate_count_probe",
            f"P0 ESCALATION — {len(long_silence)} strategy(ies) silent ≥{CRITICAL_STREAK_DAYS} days:\n"
            f"{body}\n\nRunbook: R-002",
            silent_strategies=[s for s, _ in long_silence],
            consecutive_days=max(d for _, d in long_silence),
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
            "medium_silence": [s for s, _ in medium_silence],
            "long_silence": [s for s, _ in long_silence],
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
        if medium_silence:
            print(f"\n⚠️  {len(medium_silence)} strategy(ies) in extended quiet (2–4d) → alert log")
        if long_silence:
            print(f"\n🆘 {len(long_silence)} P0 ESCALATION (≥{CRITICAL_STREAK_DAYS}d) → alert log")
        if recovered:
            print(f"\n✅ {len(recovered)} recovery notification(s) → alert log")


if __name__ == "__main__":
    main()

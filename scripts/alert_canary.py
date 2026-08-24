#!/usr/bin/env python3
"""End-to-end proof that a signal reaches a subscriber.

WHY THIS EXISTS

Every monitor we run watches one component. On 2026-08-24 an audit found the
alert pipeline severed in four independent places at once, and all of them were
green:

  * the strategy runner failed its daily cycle 367 times while heartbeating fine
  * preflight halted three books on 23 of ~80 trading days
  * the notifier read execution_source IN ('simulated','paper','live') while
    the runner writes 'alert'
  * a rebuild would have erased the alert history anyway

No single-component check could have caught any of it, because each component
was healthy on its own terms. This walks the whole chain and asserts a
subscriber was told.

WHAT IT DOES

Injects a synthetic alert-sourced position for a canary user, runs the real
notification scanner, and asserts a notification row appears — then cleans up.
It exercises the production code path, not a copy of it.

The canary user has email_enabled = 0, so nothing is ever sent to a human.

    python3 scripts/alert_canary.py            # run and report
    python3 scripts/alert_canary.py --json     # machine-readable
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

#: Resolve the repo from this file's location, then sanity-check it. Running a
#: copy from /tmp silently resolved REPO to "/" and the scanner subprocess
#: failed with a bare exit 2.
REPO = Path(__file__).resolve().parents[1]
if not (REPO / "pipelines/notification_scanner.py").exists():
    raise SystemExit(
        f"alert_canary must run from inside the repo; resolved REPO={REPO} "
        "which has no pipelines/notification_scanner.py"
    )
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from config.database import get_connection

CANARY_USER = "canary__alert_pipeline"
CANARY_TICKER = "SPY"
#: A strategy the notifier is configured to watch. Reads the registry rather
#: than naming one, so retiring a strategy cannot silently disarm the canary.
from api.public_fields import ACTIVE_STRATEGIES  # noqa: E402

STRATEGY = ACTIVE_STRATEGIES[0]


def _ensure_canary_user(conn) -> None:
    conn.execute(
        """INSERT INTO notifications.notification_preferences
               (user_id, email_enabled, in_app_enabled, email_frequency,
                portfolio_alert, watchlist_activity)
           VALUES (?, 0, 1, 'daily', 1, 1)
           ON CONFLICT (user_id) DO UPDATE
             SET email_enabled = 0, portfolio_alert = 1, watchlist_activity = 1""",
        (CANARY_USER,),
    )
    conn.commit()


def _cleanup(conn, marker: str) -> None:
    conn.execute("DELETE FROM notifications.notifications WHERE user_id = ?", (CANARY_USER,))
    conn.execute("DELETE FROM strategy_portfolio WHERE ticker = ? AND entry_reasoning = ?",
                 (CANARY_TICKER, marker))
    conn.execute("DELETE FROM notifications.watchlist WHERE user_id = ?", (CANARY_USER,))
    conn.commit()


def run(verbose: bool = True) -> dict:
    conn = get_connection()
    marker = f"canary:{int(time.time())}"
    steps: list[dict] = []
    t0 = time.monotonic()

    def step(name, ok, detail=""):
        steps.append({"step": name, "ok": bool(ok), "detail": detail,
                      "at_ms": round((time.monotonic() - t0) * 1000)})
        if verbose:
            print(f"  {'PASS' if ok else 'FAIL'}  {name}"
                  + (f"  — {detail}" if detail else ""))

    try:
        _cleanup(conn, marker)
        _ensure_canary_user(conn)
        step("canary user exists", True, f"{CANARY_USER}, email disabled")

        # Advance the watermark to now so the scanner only considers our row.
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn.execute(
            """INSERT INTO notifications.scan_watermarks
                   (event_type, last_processed_date, last_processed_at)
               VALUES ('portfolio_alert', ?, NOW())
               ON CONFLICT (event_type) DO UPDATE
                 SET last_processed_date = EXCLUDED.last_processed_date""",
            ((datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),),
        )
        conn.commit()

        # THE LINK THAT WAS SEVERED: the runner writes execution_source='alert'.
        conn.execute(
            """INSERT INTO strategy_portfolio
                   (strategy, ticker, trade_type, direction, entry_date,
                    entry_price, target_hold, stop_pct, position_size,
                    status, execution_source, is_live,
                    insider_name, entry_reasoning, company)
               VALUES (?, ?, 'buy_stock', 'long', ?, 1.0, 42, -0.5, 0.0,
                       'open', 'alert', false, 'Canary', ?, 'Canary Co')""",
            (STRATEGY, CANARY_TICKER, today, marker),
        )
        conn.commit()
        step("alert-sourced position written", True,
             f"{STRATEGY} / {CANARY_TICKER}, execution_source='alert'")

        # Run the REAL scanner, in process.
        #
        # CLERK IS THE ONLY THING STUBBED. The canary user is not a real Clerk
        # account, and _account_exists correctly drops accounts Clerk reports
        # as gone — so without this the canary tests nothing and reports a
        # false red. Every other line, including the SQL that was broken, is
        # production code executing for real against the production database.
        import pipelines.notification_scanner as ns

        ns._TIER_CACHE[CANARY_USER] = "pro"
        ns._email_cache[CANARY_USER] = None      # no address -> no send, ever

        # THE AUDIENCE IS RESTRICTED TO THE CANARY USER.
        #
        # portfolio_alert is not per-ticker — it fans out to every subscriber.
        # The first version of this canary therefore notified three real users
        # about a fabricated $1.00 SPY position and sent one of them a realtime
        # email. A canary that is meant to run every morning must be incapable
        # of that, so the audience is pinned here rather than trusted to be
        # empty. The SQL under test — the watermark bound and the
        # execution_source match, which is what was actually broken — runs
        # untouched.
        _real_subscribers = ns._get_subscribed_users

        def _canary_audience(nconn_, event_type):
            return [u for u in _real_subscribers(nconn_, event_type)
                    if u["user_id"] == CANARY_USER]

        ns._get_subscribed_users = _canary_audience

        latest = conn.execute("SELECT NOW() AS d FROM trades LIMIT 1").fetchone()["d"]
        iconn = ns._open_insiders_db()
        nconn = ns._open_notifications_db()
        try:
            ns._reset_cycle_counts()
            n = ns.scan_portfolio_alerts(iconn, nconn, latest)
            step("scanner matched the alert row", n > 0,
                 f"{n} notification(s) created"
                 if n else "0 — the notifier did not select execution_source='alert'")
        finally:
            ns._get_subscribed_users = _real_subscribers
            iconn.close()
            nconn.close()

        row = conn.execute(
            """SELECT event_type, title, created_at FROM notifications.notifications
                WHERE user_id = ? AND event_type = 'portfolio_alert'
                ORDER BY created_at DESC LIMIT 1""",
            (CANARY_USER,)).fetchone()
        step("subscriber was notified", row is not None,
             (row["title"][:70] if row else
              "NO NOTIFICATION — the chain is broken between the runner and the user"))

        # ── leg 2: a per-INSIDER subscription ────────────────────────────
        #
        # watchlist has carried insider_id since it was built and the scanner
        # only ever read w.ticker, so following an insider delivered nothing.
        # Nothing detected it because no check walked that path.
        real = conn.execute(
            """SELECT t.insider_id, t.ticker FROM trades t
                WHERE t.ingested_at > NOW() - INTERVAL '2 days'
                  AND t.signal_class IN ('discretionary_buy','discretionary_sell')
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                ORDER BY t.ingested_at DESC LIMIT 1""").fetchone()
        if real is None:
            step("insider subscription matched", True,
                 "skipped — no discretionary filing in the last 2 days")
        else:
            conn.execute(
                "INSERT INTO notifications.watchlist (user_id, insider_id) "
                "VALUES (?, ?) ON CONFLICT DO NOTHING",
                (CANARY_USER, real["insider_id"]))
            conn.execute(
                """INSERT INTO notifications.scan_watermarks
                       (event_type, last_processed_date, last_processed_at)
                   VALUES ('watchlist_activity', ?, NOW() - INTERVAL '2 days')
                   ON CONFLICT (event_type) DO UPDATE
                     SET last_processed_at = NOW() - INTERVAL '2 days'""",
                ((datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),))
            conn.commit()

            iconn2 = ns._open_insiders_db()
            nconn2 = ns._open_notifications_db()
            try:
                ns._reset_cycle_counts()
                ns.scan_watchlist_activity(iconn2, nconn2, latest)
            finally:
                iconn2.close()
                nconn2.close()

            hit = conn.execute(
                """SELECT title FROM notifications.notifications
                    WHERE user_id = ? AND event_type = 'watchlist_activity'
                    ORDER BY created_at DESC LIMIT 1""",
                (CANARY_USER,)).fetchone()
            step("insider subscription matched", hit is not None,
                 hit["title"][:70] if hit else
                 f"followed insider {real['insider_id']} and was NOT notified")

        ok = all(s["ok"] for s in steps)
        elapsed = round((time.monotonic() - t0) * 1000)
        if verbose:
            print(f"\n  {'CANARY PASS' if ok else 'CANARY FAIL'}  ({elapsed} ms)")
        return {"ok": ok, "elapsed_ms": elapsed, "steps": steps}
    finally:
        _cleanup(conn, marker)
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    result = run(verbose=not args.json)
    if args.json:
        print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

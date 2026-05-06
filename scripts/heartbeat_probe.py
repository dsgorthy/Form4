#!/usr/bin/env python3
"""Heartbeat staleness probe.

Each cw_runner writes `strategies/cw_strategies/data/{strategy}_heartbeat.json`
on every loop tick. This probe runs every 15 min (launchd) and fires a
critical alert if any heartbeat is stale beyond its threshold:

  - Market hours: stale > 30 min  → critical (now SMS-fanout via alert.log.critical)
  - Off hours:    stale > 90 min  → warn

Daily-summary log is also probed: if no successful entry in the last 36h
during market days, alert. That catches "summary script silently broken"
which would otherwise leave the operator without their daily reconciliation
point.

Idempotent: only alerts on transition (ok→stale or stale→ok). State stored
in `logs/heartbeat_probe_state.json` so we don't pager-storm.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from framework.alerts.log import alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
STRATEGIES = ["quality_momentum", "reversal_dip", "tenb51_surprise"]
# Modes: paper heartbeat at {strategy}_heartbeat.json (legacy filename for
# backwards-compat), live at {strategy}_live_heartbeat.json. Probe scopes
# itself to whichever files exist on disk — a missing live file is fine
# (the live runner isn't loaded), a stale paper file is not.
HEARTBEAT_MODES = [
    ("paper", ""),
    ("live", "_live"),
]
HEARTBEAT_DIR = REPO / "strategies/cw_strategies/data"
STATE_PATH = REPO / "logs" / "heartbeat_probe_state.json"
DAILY_SUMMARY_LOG = REPO / "logs" / "daily-summary.log"

MARKET_HOURS_THRESHOLD_MIN = 30
OFF_HOURS_THRESHOLD_MIN = 90
DAILY_SUMMARY_MAX_AGE_HOURS = 36


def _is_market_hours_now() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    return dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def _read_heartbeat(strategy: str, mode_suffix: str = "") -> dict | None:
    p = HEARTBEAT_DIR / f"{strategy}{mode_suffix}_heartbeat.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _heartbeat_age_minutes(hb: dict) -> float | None:
    ts_str = hb.get("timestamp")
    if not ts_str:
        return None
    try:
        # cw_runner writes timestamps in ET local naive; treat as ET.
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ET)
    except Exception:
        return None
    return (datetime.now(ET) - ts).total_seconds() / 60.0


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, default=str))


def check_strategies() -> dict:
    """Returns {(strategy, mode): {ok, age_min, threshold, status, ...}}.

    Paper and live modes are checked independently. A missing paper
    heartbeat is a problem (paper runner is always-loaded); a missing
    live heartbeat is fine (live plist may not be loaded yet)."""
    market_hours = _is_market_hours_now()
    threshold = MARKET_HOURS_THRESHOLD_MIN if market_hours else OFF_HOURS_THRESHOLD_MIN
    out = {}
    for s in STRATEGIES:
        for mode, suffix in HEARTBEAT_MODES:
            hb = _read_heartbeat(s, suffix)
            key = f"{s}_{mode}"
            if hb is None:
                # Live missing is expected when the plist isn't loaded.
                # Paper missing IS a problem — runner always-loaded.
                if mode == "live":
                    out[key] = {"ok": True, "age_min": None,
                                "threshold": threshold, "status": "not_loaded",
                                "hb_status": None, "mode": mode, "strategy": s}
                else:
                    out[key] = {"ok": False, "age_min": None,
                                "threshold": threshold, "status": "missing",
                                "hb_status": None, "mode": mode, "strategy": s}
                continue
            age = _heartbeat_age_minutes(hb)
            ok = age is not None and age <= threshold
            out[key] = {"ok": ok, "age_min": age, "threshold": threshold,
                        "status": "fresh" if ok else "stale",
                        "hb_status": hb.get("status"),
                        "mode": mode, "strategy": s}
    return out


def check_daily_summary() -> dict:
    """Did the daily summary run recently? Returns {ok, last_run, age_hours}."""
    if not DAILY_SUMMARY_LOG.exists():
        return {"ok": False, "last_run": None, "age_hours": None,
                "reason": "no daily-summary.log"}
    last_line = None
    try:
        with open(DAILY_SUMMARY_LOG) as f:
            for line in f:
                line = line.strip()
                if line and "sent=True" in line:
                    last_line = line
    except Exception:
        return {"ok": False, "last_run": None, "age_hours": None,
                "reason": "log unreadable"}
    if last_line is None:
        return {"ok": False, "last_run": None, "age_hours": None,
                "reason": "no successful run logged"}
    try:
        ts_str = last_line.split()[0]
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return {"ok": False, "last_run": last_line, "age_hours": None,
                "reason": "timestamp parse failed"}
    age = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
    # On weekends this is OK (no Friday afternoon run yet); only alert if
    # the last successful run is older than the threshold AND we're past
    # 17:30 ET on a weekday.
    return {"ok": age < DAILY_SUMMARY_MAX_AGE_HOURS, "last_run": ts.isoformat(),
            "age_hours": age, "reason": ""}


def main():
    state = _load_state()
    new_state = dict(state)

    # 1. Heartbeat checks (per strategy × mode)
    results = check_strategies()
    for key, r in results.items():
        prev = state.get(f"hb_{key}", "ok")
        cur = "ok" if r["ok"] else r["status"]
        if cur != prev:
            label = f"{r['strategy']}({r['mode']})"
            if not r["ok"]:
                age_str = f"{r['age_min']:.0f}m" if r["age_min"] is not None else "missing"
                alert.critical(
                    f"heartbeat_probe.{key}",
                    f"{label} heartbeat stale: age={age_str} threshold={r['threshold']}m",
                    strategy=r["strategy"], mode=r["mode"],
                    age_minutes=r["age_min"], threshold=r["threshold"],
                    hb_status=r["hb_status"],
                )
            else:
                alert.critical(
                    f"heartbeat_probe.{key}",
                    f"{label} heartbeat recovered (was {prev})",
                    strategy=r["strategy"], mode=r["mode"],
                )
            new_state[f"hb_{key}"] = cur
        logger.info("[%s/%s] heartbeat: %s (age=%s, status=%s)",
                    r["strategy"], r["mode"], cur,
                    f"{r['age_min']:.0f}m" if r["age_min"] is not None else "missing",
                    r["hb_status"])

    # 2. Daily-summary freshness — only check during market hours / weekday
    #    afternoons; outside that window the digest hasn't been due yet.
    if datetime.now(ET).weekday() < 5 and datetime.now(ET).time() >= dt_time(18, 0):
        ds = check_daily_summary()
        prev = state.get("daily_summary", "ok")
        cur = "ok" if ds["ok"] else "stale"
        if cur != prev:
            if not ds["ok"]:
                alert.critical(
                    "heartbeat_probe.daily_summary",
                    f"Daily summary stale: {ds.get('reason', '')} "
                    f"(age={ds.get('age_hours', 'n/a')})",
                    last_run=str(ds.get("last_run")),
                    age_hours=ds.get("age_hours"),
                )
            else:
                alert.critical(
                    "heartbeat_probe.daily_summary",
                    "Daily summary recovered",
                )
            new_state["daily_summary"] = cur
        logger.info("Daily summary: %s (last=%s)", cur, ds.get("last_run"))

    _save_state(new_state)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Tail the API container's log and push on INCIDENTS, not on errors.

Run by launchd (com.openclaw.form4-error-tail, KeepAlive) through
scripts/api_error_tail.sh, which only execs this.

## Why it pages per incident

Every earlier version pushed per ERROR and then tried to cap the flood:
  - 2026-08-24: it logged and never pushed; the Stripe webhook returned 500 for
    weeks and a paying customer had to email support. So: push.
  - 2026-09-30: it deduped by concrete path, so one broken route pushed once per
    insider id -- 94 pushes in six hours. So: dedupe by route template.
  - 2026-10-02: a dead database made every route its own key -- 150 pushes in
    48 hours. So: a global cap of 6 an hour.
  - 2026-11-07..09: a chronic low-rate fault (PoolError 500s on ~0.3% of
    requests) for 60 hours. The cap was a ROLLING window, and its "cap reached"
    notice re-fired every time the count dipped below 6, so it pushed about 7
    times an hour all weekend: 413 pushes, every one saying the same thing.

Thirty pushes say nothing that one does not. So now:
  - an incident OPENS at the first 500 after QUIET_S (2h) without one: one push, sent
    START_DELAY_S later so it can name the exception behind the 500;
  - an exception CLASS not yet seen in the open incident: one push (at most
    MAX_NEW_CLASS_PUSHES per incident), so a second, different failure is not
    hidden behind a chronic one;
  - every SUMMARY_EVERY_S while it stays open: one summary with counts;
  - QUIET_S without a 500 CLOSES it: one push, unless it was a blip.

Nothing is lost for diagnosis: every 500 and every exception line still goes to
logs/api-errors.log, and route-deduped alerts to logs/alerts.ndjson, as before.
"""
from __future__ import annotations

import json
import os
import re
import select
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CONTAINER = "trading-framework-api-1"
LOG_FILE = REPO / "logs" / "api-errors.log"
ALERT_LOG = REPO / "logs" / "alerts.ndjson"
STATE_FILE = REPO / "logs" / "api_error_pager_state.json"

QUIET_S = 2 * 3600             # two hours without a 500 end an incident
START_DELAY_S = 5              # wait this long to learn the cause before pushing
SUMMARY_EVERY_S = 12 * 3600    # at most two "still failing" pushes a day
CLOSE_MIN_ERRORS = 10          # a blip opens with a push but closes silently
MAX_NEW_CLASS_PUSHES = 3       # per incident
EXC_LINK_S = 60                # an exception counts only next to a recent 500
ALERT_DEDUPE_S = 300           # alerts.ndjson: one line per route per 5 minutes
TICK_S = 30

# uvicorn access line: ... "GET /api/v1/filings/xyz HTTP/1.1" 500 Internal Server Error
ACCESS_500 = re.compile(r'"([A-Z]+ (\S+) HTTP/[\d.]+)" 500\b')
# Last line of a traceback: "psycopg2.pool.PoolError: connection pool exhausted".
# Qualified names of any shape, or bare ones that look like exceptions. Never
# indented (frames are), and "ERROR:    Exception in ASGI application" or
# "INFO:     ..." do not qualify.
EXC_FINAL = re.compile(
    r"^((?:[a-z_]\w*\.)+[A-Z]\w*|[A-Z]\w*(?:Error|Exception|Timeout\w*))(?:: (.*))?$"
)


def route_template(path: str) -> str:
    """Collapse ids so one broken route is one route, however many ids it serves."""
    p = path.split("?", 1)[0]
    p = re.sub(r"/(filings|trades)/[A-Za-z0-9]{5,}", r"/\1/{id}", p)
    p = re.sub(r"/insiders/[^/]+", "/insiders/{id}", p)
    p = re.sub(r"/companies/[^/]+", "/companies/{T}", p)
    p = re.sub(r"/[0-9]{3,}", "/{n}", p)
    return p


def parse_line(line: str):
    """-> ("500", request, path) | ("exc", class, message) | None"""
    m = ACCESS_500.search(line)
    if m:
        return ("500", m.group(1), m.group(2))
    m = EXC_FINAL.match(line)
    if m:
        return ("exc", m.group(1), (m.group(2) or "").strip())
    return None


@dataclass
class Push:
    title: str
    body: str
    priority: int = 4


def _hm(seconds: float) -> str:
    seconds = int(seconds)
    h, m = divmod(seconds // 60, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m"


def _clock(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%a %H:%M")


def _top(counts: dict, n: int) -> str:
    return ", ".join(f"{k} x{v}" for k, v in Counter(counts).most_common(n))


class IncidentPager:
    """Decides what to push. Pure: no I/O, time passed in, state is plain JSON."""

    def __init__(self, *, quiet_s=QUIET_S, start_delay_s=START_DELAY_S,
                 summary_every_s=SUMMARY_EVERY_S, close_min_errors=CLOSE_MIN_ERRORS,
                 max_new_class_pushes=MAX_NEW_CLASS_PUSHES, exc_link_s=EXC_LINK_S):
        self.quiet_s = quiet_s
        self.start_delay_s = start_delay_s
        self.summary_every_s = summary_every_s
        self.close_min_errors = close_min_errors
        self.max_new_class_pushes = max_new_class_pushes
        self.exc_link_s = exc_link_s
        self.inc: dict | None = None

    # -- state ---------------------------------------------------------------
    def to_state(self) -> dict:
        return {"inc": self.inc}

    def load_state(self, state: dict | None) -> "IncidentPager":
        inc = (state or {}).get("inc")
        self.inc = inc if isinstance(inc, dict) and "opened" in inc else None
        return self

    # -- events --------------------------------------------------------------
    def on_500(self, now: float, path: str) -> list[Push]:
        out = self._close_if_quiet(now)
        if self.inc is None:
            self.inc = {
                "opened": now, "last": now, "n": 0, "n_since_push": 0,
                "first": route_template(path), "cause": None,
                "routes": {}, "classes": {}, "notified": [],
                "new_class_pushes": 0, "started": False, "last_push": None,
            }
        inc = self.inc
        inc["last"] = now
        inc["n"] += 1
        inc["n_since_push"] += 1
        r = route_template(path)
        inc["routes"][r] = inc["routes"].get(r, 0) + 1
        return out + self.on_tick(now)

    def on_exception(self, now: float, cls: str, msg: str) -> list[Push]:
        inc = self.inc
        if inc is None or now - inc["last"] > self.exc_link_s:
            return []   # not next to a failing request: logged, never paged
        short = cls.rsplit(".", 1)[-1]
        inc["classes"][short] = inc["classes"].get(short, 0) + 1
        if inc["cause"] is None:
            inc["cause"] = f"{cls}: {msg}"[:200]
        out: list[Push] = []
        if (inc["started"] and short not in inc["notified"]
                and inc["new_class_pushes"] < self.max_new_class_pushes):
            inc["notified"].append(short)
            inc["new_class_pushes"] += 1
            out.append(Push(
                "form4 API: a new kind of error",
                f"{cls}: {msg}"[:300]
                + f"\nAlongside an incident open since {_clock(inc['opened'])}"
                  f" ({inc['n']} 500s). Log: logs/api-errors.log",
            ))
        return out + self.on_tick(now)

    def on_tick(self, now: float) -> list[Push]:
        out = self._close_if_quiet(now)
        inc = self.inc
        if inc is None:
            return out
        if not inc["started"]:
            if now - inc["opened"] >= self.start_delay_s:
                inc["started"] = True
                inc["last_push"] = now
                inc["n_since_push"] = 0
                inc["notified"] = sorted(inc["classes"])
                cause = f"\nCause: {inc['cause']}" if inc["cause"] else ""
                out.append(Push(
                    "form4 API errors started",
                    f"500 on {inc['first']}{cause}\n"
                    f"Further errors are counted, not pushed: a summary every "
                    f"{_hm(self.summary_every_s)} while it lasts, and one when it "
                    f"stops. Log: logs/api-errors.log",
                ))
        elif now - inc["last_push"] >= self.summary_every_s and inc["n_since_push"] > 0:
            out.append(Push(
                "form4 API errors continuing",
                f"{inc['n_since_push']} 500s in the last {_hm(now - inc['last_push'])}"
                f" ({inc['n']} since {_clock(inc['opened'])}).\n"
                f"Routes: {_top(inc['routes'], 3)}\n"
                f"Causes: {_top(inc['classes'], 3) or 'none logged'}",
            ))
            inc["last_push"] = now
            inc["n_since_push"] = 0
        return out

    def _close_if_quiet(self, now: float) -> list[Push]:
        inc = self.inc
        if inc is None or now - inc["last"] < self.quiet_s:
            return []
        self.inc = None
        if not inc["started"] or inc["n"] < self.close_min_errors:
            return []
        return [Push(
            "form4 API errors stopped",
            f"{inc['n']} 500s over {_hm(inc['last'] - inc['opened'])}, none since "
            f"{_clock(inc['last'])}.\nRoutes: {_top(inc['routes'], 3)}\n"
            f"Causes: {_top(inc['classes'], 3) or 'none logged'}",
            priority=3,
        )]


# -- I/O -----------------------------------------------------------------------

def _log(msg: str) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")


def _alert_line(message: str) -> None:
    rec = {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "severity": "error", "component": "api_error_tail", "message": message}
    with ALERT_LOG.open("a") as f:
        f.write(json.dumps(rec) + "\n")


def _load_topic() -> None:
    if os.environ.get("NTFY_ALERT_TOPIC"):
        return
    env = REPO / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("NTFY_ALERT_TOPIC="):
                os.environ["NTFY_ALERT_TOPIC"] = line.split("=", 1)[1].strip().strip("\"'")
                return


def _send(push: Push) -> None:
    def run():
        try:
            sys.path.insert(0, str(REPO))
            from framework.alerts.ntfy import send_ntfy
            send_ntfy(push.body, title=push.title, priority=push.priority,
                      tags=["rotating_light"])
        except Exception as exc:  # a failed push must never kill the tail
            _log(f"PUSH FAILED ({exc}): {push.title}")
    threading.Thread(target=run, daemon=True).start()


def _save(pager: IncidentPager) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(pager.to_state()))
    tmp.replace(STATE_FILE)


def _load() -> dict | None:
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return None


def main() -> int:
    _load_topic()
    pager = IncidentPager().load_state(_load())
    last_alert: dict[str, float] = {}
    proc = subprocess.Popen(["docker", "logs", "--follow", "--tail", "0", CONTAINER],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    fd = proc.stdout.fileno()
    buf = b""
    last_save = 0.0
    _log(f"Started error tail for container {CONTAINER} (incident pager)")
    while True:
        ready, _, _ = select.select([fd], [], [], TICK_S)
        now = time.time()
        pushes: list[Push] = []
        if ready:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            *lines, buf = (buf + chunk).split(b"\n")
            for raw in lines:
                ev = parse_line(raw.decode("utf-8", "replace"))
                if ev is None:
                    continue
                if ev[0] == "500":
                    _, request, path = ev
                    _log(f"500: {request}")
                    key = route_template(path)
                    if now - last_alert.get(key, 0) >= ALERT_DEDUPE_S:
                        last_alert[key] = now
                        _alert_line(f"form4 API 500 — {request}")
                    pushes += pager.on_500(now, path)
                else:
                    _, cls, msg = ev
                    _log(f"EXC: {cls}: {msg}"[:400])
                    pushes += pager.on_exception(now, cls, msg)
        pushes += pager.on_tick(now)
        for p in pushes:
            _log(f"PUSHED: {p.title} | {p.body.splitlines()[0]}")
            _send(p)
        if pushes or now - last_save >= TICK_S:
            _save(pager)
            last_save = now
    _save(pager)
    _log("Tail loop exited (container restarted or docker error). KeepAlive restarts it.")
    time.sleep(1)   # let an in-flight push thread finish
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""File-based alert writer — replaces Telegram entirely.

Derek's decision (2026-05-02, confirming the 2026-04-19 handoff):
no active push channel. Alerts go to a structured NDJSON log file that the
operator reviews via dashboard / morning routine. Accepted SLO floor: 24h
mean-time-to-detect.

Why NDJSON not plain text: every entry is a single line of valid JSON, so
it's grep-able by humans AND machine-readable. A future Grafana panel or
shell script can `tail -f logs/alerts.ndjson | jq` and filter by severity.

Usage:
    from framework.alerts.log import alert
    alert.critical("freshness_probe", "trades stale 26h, contract 24h")
    alert.info("compute_signals", "5 detectors completed")

Severity levels (in order):
    info     — informational only; bulk noise, never wakes anyone
    warn     — anomaly worth a glance during morning review
    error    — failure that didn't halt critical path
    critical — P0; trading-decision path is degraded
"""
from __future__ import annotations

import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Resolve log path lazily so tests can override the env var.
_LOG_LOCK = threading.Lock()


def _log_path() -> Path:
    """Where alerts go. Override with FORM4_ALERT_LOG env var."""
    custom = os.environ.get("FORM4_ALERT_LOG")
    if custom:
        return Path(custom)
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "logs" / "alerts.ndjson"


def _write(severity: str, component: str, message: str,
           extra: Optional[dict] = None) -> None:
    """Append one JSON line to the alert log. Thread-safe; never raises."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "severity": severity,
        "component": component,
        "message": message,
    }
    if extra:
        entry["extra"] = extra
    line = json.dumps(entry, separators=(",", ":")) + "\n"

    path = _log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with _LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as e:
        # Last-resort: stderr. Never propagate — alert paths must be best-effort.
        print(f"[alert.log] write failed: {e}; original entry: {line.strip()}",
              file=sys.stderr)


# ── Public API: severity verbs ──────────────────────────────────────────────

def info(component: str, message: str, **extra) -> None:
    """Bulk informational. Most pipeline successes write this."""
    _write("info", component, message, extra or None)


def warn(component: str, message: str, **extra) -> None:
    """Anomaly noticed; worth a morning-review glance."""
    _write("warn", component, message, extra or None)


def error(component: str, message: str, **extra) -> None:
    """Failure that didn't halt the trading-decision path."""
    _write("error", component, message, extra or None)


def critical(component: str, message: str, **extra) -> None:
    """P0 — trading-decision path degraded. Top of morning review."""
    _write("critical", component, message, extra or None)


# Module-level dotted access (alert.critical / alert.info) — convenience.
class _AlertProxy:
    info = staticmethod(info)
    warn = staticmethod(warn)
    error = staticmethod(error)
    critical = staticmethod(critical)


alert = _AlertProxy()


# ── CLI for tail / inspection ───────────────────────────────────────────────

if __name__ == "__main__":
    """Quick inspection of the alert log.

    Usage:
        python3 -m framework.alerts.log              # last 20 entries
        python3 -m framework.alerts.log --critical   # only criticals
        python3 -m framework.alerts.log --hours 24   # last 24 hours
    """
    import argparse
    from datetime import timedelta

    p = argparse.ArgumentParser()
    p.add_argument("-n", "--lines", type=int, default=20)
    p.add_argument("--severity", choices=["info", "warn", "error", "critical"],
                   help="Filter to one severity")
    p.add_argument("--hours", type=int, help="Only show entries within last N hours")
    p.add_argument("--component", help="Filter by component name (substring match)")
    p.add_argument("--json", action="store_true", help="Raw NDJSON output")
    args = p.parse_args()

    path = _log_path()
    if not path.exists():
        print(f"No alerts yet ({path} doesn't exist)")
        sys.exit(0)

    cutoff = None
    if args.hours:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=args.hours))

    with open(path) as f:
        entries = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if args.severity and e.get("severity") != args.severity:
                continue
            if args.component and args.component not in e.get("component", ""):
                continue
            if cutoff:
                ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
                if ts < cutoff:
                    continue
            entries.append(e)

    entries = entries[-args.lines:]
    if args.json:
        for e in entries:
            print(json.dumps(e, separators=(",", ":")))
    else:
        sev_marks = {"info": "ℹ️ ", "warn": "⚠️ ", "error": "🔴 ", "critical": "🆘 "}
        for e in entries:
            mark = sev_marks.get(e["severity"], "  ")
            print(f"{e['ts']}  {mark}{e['severity']:<8s}  {e['component']:<30s}  {e['message']}")
        print(f"\n{len(entries)} entr{'y' if len(entries)==1 else 'ies'} (file: {path})")

"""Pyrrho Dataplane Desk — tailnet-only browser view.

Renders the same StatusSnapshot the CLI prints, as a glanceable HTML page.
Auto-refreshes every 30s via <meta refresh> (no JS).

  GET /                  - HTML dashboard
  GET /api/status.json   - same data as JSON (for tooling / future MCP)

Binds 100.78.9.66:3031 by default (Tailscale IP) so this isn't exposed to
the LAN — same security model as the Dagster UI on :3030.

Run:  python3 -m dataplane.desk            # default host/port
      python3 -m dataplane.desk --host 0.0.0.0 --port 3031

launchd-installed as com.openclaw.pyrrho-desk on Studio.
"""
from __future__ import annotations

import argparse
import json
import socketserver
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from typing import Optional

from dataplane.status import (
    DagsterRun,
    SignalStatus,
    StatusSnapshot,
    StrategyOutcomes,
    gather_status,
)


_BADGE_COLORS = {
    "GREEN":   "#2dd36f",
    "YELLOW":  "#ffc409",
    "RED":     "#eb445a",
    "UNKNOWN": "#92949c",
}

_RUN_COLORS = {
    "SUCCESS":  "#2dd36f",
    "FAILURE":  "#eb445a",
    "STARTED":  "#5260ff",
    "CANCELED": "#92949c",
    "QUEUED":   "#92949c",
}


def _age(td_hours: Optional[float]) -> str:
    if td_hours is None:
        return "—"
    if td_hours < 1:
        return f"{int(td_hours*60)}m"
    if td_hours < 48:
        return f"{td_hours:.1f}h"
    return f"{td_hours/24:.1f}d"


def _ts(d: Optional[datetime]) -> str:
    if d is None:
        return "—"
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone().strftime("%Y-%m-%d %H:%M")


def _snapshot_to_json(snap: StatusSnapshot) -> dict:
    """Serialise StatusSnapshot to JSON-safe dict (for /api/status.json)."""
    def sig(s: SignalStatus) -> dict:
        return {
            "signal_id":          s.signal_id,
            "version":            s.version,
            "signal_class":       s.signal_class,
            "owner":              s.owner,
            "sla_hours":          s.sla_hours,
            "row_count":          s.row_count,
            "rows_24h":           s.rows_24h,
            "rows_7d":            s.rows_7d,
            "latest_ingested_at": s.latest_ingested_at.isoformat() if s.latest_ingested_at else None,
            "latest_as_of":       s.latest_as_of.isoformat() if s.latest_as_of else None,
            "earliest_as_of":     s.earliest_as_of.isoformat() if s.earliest_as_of else None,
            "freshness_status":   s.freshness_status,
            "age_hours":          s.age_hours,
            "is_strategy":        s.is_strategy,
        }

    def strat(o: StrategyOutcomes) -> dict:
        return {
            "evals_24h":       o.evals_24h,
            "evals_7d":        o.evals_7d,
            "triggered_24h":   o.triggered_24h,
            "triggered_7d":    o.triggered_7d,
            "top_fail_reason": o.top_fail_reason,
            "top_fail_count":  o.top_fail_count,
        }

    def run(r: DagsterRun) -> dict:
        return {
            "job_name":         r.job_name,
            "partition":        r.partition,
            "status":           r.status,
            "started_at":       r.started_at.isoformat() if r.started_at else None,
            "duration_seconds": r.duration_seconds,
        }

    return {
        "as_of":            snap.as_of.isoformat(),
        "healthy":          snap.healthy_pipelines,
        "total":            snap.non_strategy_count,
        "evals_24h":        snap.total_evals_24h,
        "triggered_24h":    snap.total_triggered_24h,
        "signals":          [sig(s) for s in snap.signals],
        "strategies":       {sid: strat(o) for sid, o in snap.strategies.items()},
        "recent_runs":      [run(r) for r in snap.recent_runs],
    }


def render_html(snap: StatusSnapshot) -> str:
    raw = [s for s in snap.signals if not s.is_strategy]
    strats = [s for s in snap.signals if s.is_strategy]

    def sig_row(s: SignalStatus, with_strategy: bool = False) -> str:
        color = _BADGE_COLORS.get(s.freshness_status, "#92949c")
        badge = f'<span class="dot" style="background:{color}"></span>'
        if with_strategy:
            o = snap.strategies.get(s.signal_id) or StrategyOutcomes()
            top = (o.top_fail_reason or "—")[:40]
            return (
                f"<tr>"
                f"<td>{badge}</td>"
                f"<td class='name'>{s.signal_id}</td>"
                f"<td class='num'>{o.evals_24h:,}</td>"
                f"<td class='num'>{o.triggered_24h:,}</td>"
                f"<td class='num'>{_age(s.age_hours)}</td>"
                f"<td class='miss'>{top}</td>"
                f"</tr>"
            )
        return (
            f"<tr>"
            f"<td>{badge}</td>"
            f"<td class='name'>{s.signal_id}</td>"
            f"<td class='num'>{s.row_count:,}</td>"
            f"<td class='num'>{s.rows_24h:,}</td>"
            f"<td class='num'>{s.rows_7d:,}</td>"
            f"<td class='num'>{_age(s.age_hours)}</td>"
            f"<td class='num'>{int(s.sla_hours)}h</td>"
            f"</tr>"
        )

    def run_row(r: DagsterRun) -> str:
        color = _RUN_COLORS.get(r.status, "#92949c")
        dot = f'<span class="dot" style="background:{color}"></span>'
        dur = f"{int(r.duration_seconds)}s" if r.duration_seconds else "—"
        return (
            f"<tr>"
            f"<td>{dot}</td>"
            f"<td class='name'>{r.job_name}</td>"
            f"<td class='ts'>{_ts(r.started_at)}</td>"
            f"<td class='name'>{r.partition or '—'}</td>"
            f"<td class='num'>{dur}</td>"
            f"</tr>"
        )

    raw_rows = "".join(sig_row(s) for s in raw)
    strat_rows = "".join(sig_row(s, with_strategy=True) for s in strats)
    run_rows = "".join(run_row(r) for r in snap.recent_runs)

    ts = snap.as_of.astimezone().strftime("%a %Y-%m-%d %H:%M %Z")

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="30">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pyrrho · Dataplane Desk</title>
<style>
  :root {{
    --bg: #0b0d10;
    --panel: #14171c;
    --border: #1f242c;
    --fg: #d7dade;
    --dim: #8b9098;
    --accent: #5260ff;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    background: var(--bg); color: var(--fg);
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    margin: 0; padding: 18px;
  }}
  h1 {{
    font-size: 14px; font-weight: 500; margin: 0 0 4px;
    letter-spacing: 0.4px; color: var(--fg);
  }}
  .sub {{ font-size: 12px; color: var(--dim); margin-bottom: 18px; }}
  .summary {{
    background: var(--panel); border: 1px solid var(--border);
    padding: 12px 14px; margin-bottom: 14px; font-size: 13px;
    display: flex; gap: 32px; flex-wrap: wrap;
  }}
  .summary span b {{ color: var(--fg); font-weight: 500; }}
  .summary span {{ color: var(--dim); }}
  section {{
    background: var(--panel); border: 1px solid var(--border);
    padding: 12px 14px; margin-bottom: 14px;
  }}
  section h2 {{
    font-size: 11px; text-transform: uppercase; color: var(--dim);
    margin: 0 0 10px; letter-spacing: 1.2px; font-weight: 500;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  td, th {{ padding: 4px 8px; text-align: left; vertical-align: middle; }}
  th {{ color: var(--dim); font-weight: 500; font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; border-bottom: 1px solid var(--border); }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; color: var(--fg); }}
  td.name {{ color: var(--fg); }}
  td.ts {{ color: var(--dim); font-variant-numeric: tabular-nums; }}
  td.miss {{ color: var(--dim); font-size: 12px; }}
  .dot {{
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    margin-right: 4px;
  }}
  .foot {{ color: var(--dim); font-size: 11px; margin-top: 18px; }}
</style>
</head>
<body>
  <h1>Pyrrho · Dataplane Desk</h1>
  <div class="sub">{ts} · auto-refresh 30s</div>

  <div class="summary">
    <span>pipelines healthy: <b>{snap.healthy_pipelines}/{snap.non_strategy_count}</b></span>
    <span>evals 24h: <b>{snap.total_evals_24h:,}</b></span>
    <span>triggered 24h: <b>{snap.total_triggered_24h:,}</b></span>
  </div>

  <section>
    <h2>Signals</h2>
    <table>
      <tr><th></th><th>signal</th><th class='num'>rows</th><th class='num'>24h</th><th class='num'>7d</th><th class='num'>age</th><th class='num'>sla</th></tr>
      {raw_rows}
    </table>
  </section>

  {('<section><h2>Strategies</h2><table>' +
    "<tr><th></th><th>strategy</th><th class='num'>evals 24h</th><th class='num'>trig 24h</th><th class='num'>age</th><th>top miss</th></tr>" +
    strat_rows + '</table></section>') if strats else ''}

  <section>
    <h2>Recent Dagster Runs</h2>
    <table>
      <tr><th></th><th>job</th><th>started</th><th>partition</th><th class='num'>dur</th></tr>
      {run_rows}
    </table>
  </section>

  <div class="foot">tailnet only · binds 100.78.9.66:3031 · same SQL as <code>python3 -m dataplane status</code></div>
</body>
</html>"""


# ── HTTP handler ────────────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # quiet stderr; rely on launchd logs
        return

    def do_GET(self):  # noqa: N802
        if self.path == "/" or self.path.startswith("/?"):
            try:
                snap = gather_status()
                body = render_html(snap).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                self._err(f"render error: {exc}")
            return

        if self.path == "/api/status.json":
            try:
                snap = gather_status()
                body = json.dumps(_snapshot_to_json(snap), default=str).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                self._err(f"json error: {exc}")
            return

        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return

        self.send_response(404)
        self.end_headers()

    def _err(self, msg: str) -> None:
        body = msg.encode("utf-8")
        self.send_response(500)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main(argv=None):
    p = argparse.ArgumentParser(prog="dataplane.desk")
    p.add_argument("--host", default="100.78.9.66", help="bind address (default: Studio tailnet IP)")
    p.add_argument("--port", type=int, default=3031)
    args = p.parse_args(argv)
    print(f"Pyrrho Dataplane Desk listening on http://{args.host}:{args.port}", flush=True)
    with _ThreadedHTTPServer((args.host, args.port), _Handler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    main()

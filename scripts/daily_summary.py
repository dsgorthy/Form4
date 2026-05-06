#!/usr/bin/env python3
"""Daily 5:30 PM ET performance digest.

Aggregates today's strategy activity into a single HTML email Derek reads
with dinner. The digest is the *active* daily reconciliation point — even
when nothing's broken, this email landing in the inbox confirms:
  1. The runners ran today.
  2. The reconciler ran today.
  3. The summary script itself ran today.

If three weekdays go by with no digest, the heartbeat probe escalates to
a critical alert.

Sections:
  - Today by strategy: entries, exits, current open positions
  - 30d performance: cumulative P&L per strategy + win rate vs SPY
  - Active divergences from alpaca_reconciliation
  - Unresolved order_audit (pending/timeout/exception/rejected)
  - Last 24h alerts grouped by severity

Send target: DAILY_DIGEST_TO env var (defaults to derek.gorthy@gmail.com).
Sender: shared Resend transactional pipeline.

Usage:
    python3 scripts/daily_summary.py            # send live
    python3 scripts/daily_summary.py --dry-run  # render to stdout, no send
    python3 scripts/daily_summary.py --to other@example.com
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

import httpx

from config.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STRATEGIES = ["quality_momentum", "reversal_dip", "tenb51_surprise"]
RESEND_URL = "https://api.resend.com/emails"
FROM_ADDRESS = "Form4 <alerts@form4.app>"


# ── Data gathering ──────────────────────────────────────────────────────────

def _today_et() -> str:
    # Naive ET — we accept the off-by-an-hour-during-DST risk; the digest
    # runs at 17:30 ET which is well clear of midnight.
    return datetime.now(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")


def fetch_today_activity(conn, today: str) -> list[dict]:
    """Per-strategy: today's entries, exits, currently open."""
    out = []
    for s in STRATEGIES:
        rows_today_in = conn.execute(
            """SELECT id, ticker, insider_name, entry_price, shares, dollar_amount
                 FROM strategy_portfolio
                WHERE strategy = ? AND entry_date = ?""",
            (s, today),
        ).fetchall()
        rows_today_out = conn.execute(
            """SELECT id, ticker, insider_name, exit_price, exit_reason,
                      pnl_pct, pnl_dollar, hold_days
                 FROM strategy_portfolio
                WHERE strategy = ? AND exit_date = ? AND status = 'closed'""",
            (s, today),
        ).fetchall()
        rows_open = conn.execute(
            """SELECT ticker, insider_name, entry_date, entry_price, target_hold,
                      planned_exit_date, shares, dollar_amount
                 FROM strategy_portfolio
                WHERE strategy = ? AND status = 'open'
                ORDER BY entry_date""",
            (s,),
        ).fetchall()
        out.append({
            "strategy": s,
            "entries_today": [dict(r) for r in rows_today_in],
            "exits_today": [dict(r) for r in rows_today_out],
            "open_positions": [dict(r) for r in rows_open],
        })
    return out


def fetch_30d_performance(conn) -> list[dict]:
    """Rolling 30d: count, win rate, mean P&L, total $."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    out = []
    for s in STRATEGIES:
        row = conn.execute(
            """SELECT
                  COUNT(*) AS n,
                  AVG(pnl_pct) AS mean_pnl,
                  SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
                  SUM(pnl_dollar) AS total_dollars
                FROM strategy_portfolio
               WHERE strategy = ? AND status = 'closed' AND exit_date >= ?""",
            (s, cutoff),
        ).fetchone()
        d = dict(row) if row else {}
        n = int(d.get("n") or 0)
        wins = int(d.get("wins") or 0)
        out.append({
            "strategy": s,
            "trades_30d": n,
            "win_rate": (wins / n) if n else 0.0,
            "mean_pnl_pct": float(d.get("mean_pnl") or 0.0),
            "total_pnl_dollar": float(d.get("total_dollars") or 0.0),
        })
    return out


def fetch_spy_30d(conn) -> dict:
    """SPY benchmark: 30d return."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT date, close FROM prices.daily_prices
            WHERE ticker = 'SPY' AND date >= ? ORDER BY date""",
        (cutoff,),
    ).fetchall()
    if len(rows) < 2:
        return {"first": None, "last": None, "pct": 0.0}
    rows = [dict(r) for r in rows]
    first = float(rows[0]["close"])
    last = float(rows[-1]["close"])
    pct = (last - first) / first if first > 0 else 0.0
    return {"first": rows[0]["date"], "last": rows[-1]["date"],
            "first_price": first, "last_price": last, "pct": pct}


def fetch_active_divergences(conn) -> list[dict]:
    rows = conn.execute(
        """SELECT strategy, ticker, issue_type, severity, db_qty, alpaca_qty,
                  db_entry_price, alpaca_avg_cost, detail, detected_at
             FROM alpaca_reconciliation
            WHERE resolved_at IS NULL
            ORDER BY severity DESC, detected_at DESC"""
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_unresolved_orders(conn) -> list[dict]:
    """order_audit rows that haven't reached a clean terminal state.
    `filled` and `skipped` are clean; everything else needs operator eyes."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    rows = conn.execute(
        """SELECT order_id, strategy, ticker, side, qty, fill_status,
                  rejection_reason, decided_at
             FROM order_audit
            WHERE fill_status NOT IN ('filled', 'skipped')
              AND decided_at >= ?
            ORDER BY decided_at DESC""",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_recent_alerts(hours: int = 24) -> list[dict]:
    """NDJSON alerts in the last N hours, grouped by severity."""
    log_path = REPO / "logs" / "alerts.ndjson"
    if not log_path.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            except Exception:
                continue
            if ts < cutoff:
                continue
            out.append(e)
    return out


# ── Rendering ───────────────────────────────────────────────────────────────

def _pct(x: float) -> str:
    return f"{x*100:+.2f}%"


def _dollars(x: float) -> str:
    return f"${x:+,.0f}"


def render_html(activity: list[dict], perf: list[dict], spy: dict,
                divergences: list[dict], unresolved_orders: list[dict],
                alerts: list[dict], today: str) -> str:
    sev_count = {"info": 0, "warn": 0, "error": 0, "critical": 0}
    for a in alerts:
        s = a.get("severity", "info")
        sev_count[s] = sev_count.get(s, 0) + 1

    # Activity table
    activity_rows = ""
    for s in activity:
        ent = len(s["entries_today"])
        exi = len(s["exits_today"])
        opn = len(s["open_positions"])
        # Today's exit P&L
        exit_pnl = sum(float(e.get("pnl_dollar") or 0) for e in s["exits_today"])
        activity_rows += (
            f"<tr><td><b>{s['strategy']}</b></td>"
            f"<td style='text-align:right'>{ent}</td>"
            f"<td style='text-align:right'>{exi}</td>"
            f"<td style='text-align:right; color:{'#22C55E' if exit_pnl>=0 else '#EF4444'}'>{_dollars(exit_pnl) if exi else '—'}</td>"
            f"<td style='text-align:right'>{opn}</td></tr>"
        )

    # 30d performance
    perf_rows = ""
    for p in perf:
        n = p["trades_30d"]
        wr = p["win_rate"]
        mean = p["mean_pnl_pct"]
        tot = p["total_pnl_dollar"]
        if n == 0:
            perf_rows += f"<tr><td><b>{p['strategy']}</b></td><td colspan='4' style='color:#55556A'>no closed trades</td></tr>"
            continue
        perf_rows += (
            f"<tr><td><b>{p['strategy']}</b></td>"
            f"<td style='text-align:right'>{n}</td>"
            f"<td style='text-align:right'>{wr*100:.0f}%</td>"
            f"<td style='text-align:right; color:{'#22C55E' if mean>=0 else '#EF4444'}'>{_pct(mean)}</td>"
            f"<td style='text-align:right; color:{'#22C55E' if tot>=0 else '#EF4444'}'>{_dollars(tot)}</td></tr>"
        )

    spy_color = "#22C55E" if spy.get("pct", 0) >= 0 else "#EF4444"
    spy_html = (f"<tr><td><b>SPY benchmark</b></td><td colspan='3' "
                f"style='text-align:right; color:{spy_color}'>{_pct(spy.get('pct', 0))} "
                f"({spy.get('first', '?')} → {spy.get('last', '?')})</td></tr>")

    # Open positions
    open_rows = ""
    for s in activity:
        for o in s["open_positions"]:
            open_rows += (
                f"<tr><td><b>{s['strategy']}</b></td>"
                f"<td>{o['ticker']}</td>"
                f"<td style='color:#8888A0'>{o.get('insider_name', '')[:30]}</td>"
                f"<td>{o.get('entry_date', '')}</td>"
                f"<td style='text-align:right'>${float(o.get('entry_price') or 0):.2f}</td>"
                f"<td style='text-align:right'>{o.get('shares') or 0}</td>"
                f"<td>{o.get('planned_exit_date', '')}</td></tr>"
            )
    if not open_rows:
        open_rows = "<tr><td colspan='7' style='color:#55556A; text-align:center'>no open positions</td></tr>"

    # Divergences
    div_html = ""
    if divergences:
        div_rows = ""
        for d in divergences:
            sev_color = {"critical": "#EF4444", "warn": "#F59E0B"}.get(d.get("severity", ""), "#55556A")
            div_rows += (
                f"<tr><td>{d['strategy']}</td>"
                f"<td><b>{d['ticker']}</b></td>"
                f"<td><code>{d['issue_type']}</code></td>"
                f"<td style='color:{sev_color}'>{d['severity']}</td>"
                f"<td style='font-size:11px; color:#8888A0'>{d.get('detail', '')}</td></tr>"
            )
        div_html = (f"<h3 style='color:#F59E0B; margin-top:20px'>"
                    f"⚠️ Active divergences ({len(divergences)})</h3>"
                    f"<table style='width:100%; font-size:13px'>"
                    f"<thead><tr><th align='left'>Strategy</th><th align='left'>Ticker</th>"
                    f"<th align='left'>Issue</th><th align='left'>Sev</th>"
                    f"<th align='left'>Detail</th></tr></thead>"
                    f"<tbody>{div_rows}</tbody></table>")

    # Unresolved orders
    unres_html = ""
    if unresolved_orders:
        u_rows = ""
        for u in unresolved_orders:
            u_rows += (
                f"<tr><td>{u['strategy']}</td><td><b>{u['ticker']}</b></td>"
                f"<td>{u['side']} {u['qty']}</td>"
                f"<td style='color:#EF4444'>{u['fill_status']}</td>"
                f"<td style='font-size:11px; color:#8888A0'>{u.get('rejection_reason', '')[:60]}</td></tr>"
            )
        unres_html = (f"<h3 style='color:#EF4444; margin-top:20px'>"
                      f"🔴 Unresolved orders ({len(unresolved_orders)})</h3>"
                      f"<table style='width:100%; font-size:13px'>"
                      f"<thead><tr><th align='left'>Strategy</th><th align='left'>Ticker</th>"
                      f"<th align='left'>Side/Qty</th><th align='left'>Status</th>"
                      f"<th align='left'>Reason</th></tr></thead>"
                      f"<tbody>{u_rows}</tbody></table>")

    # Alerts summary
    alerts_html = ""
    if any(sev_count.values()):
        alerts_html = (f"<p style='color:#8888A0; font-size:13px'>"
                       f"<b>Alerts (24h):</b> "
                       f"<span style='color:#EF4444'>{sev_count['critical']} critical</span> · "
                       f"<span style='color:#F59E0B'>{sev_count['error']} error</span> · "
                       f"<span style='color:#FBBF24'>{sev_count['warn']} warn</span> · "
                       f"<span style='color:#55556A'>{sev_count['info']} info</span></p>")

    return f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
                max-width:720px;margin:0 auto;padding:24px;background:#0A0A0F;color:#E8E8ED;">
      <div style="margin-bottom:20px;">
        <span style="font-size:18px;font-weight:bold;">Form<span style="color:#3B82F6;">4</span></span>
        <span style="margin-left:8px;font-size:13px;color:#8888A0;">Daily Summary · {today}</span>
      </div>

      <div style="background:#12121A;border:1px solid #2A2A3A;border-radius:8px;padding:20px;">
        <h3 style="margin:0 0 12px;font-size:15px;">Today's activity</h3>
        <table style="width:100%; font-size:13px; border-collapse:collapse">
          <thead style="color:#55556A; font-size:11px; text-transform:uppercase">
            <tr><th align="left">Strategy</th><th align="right">Entries</th>
                <th align="right">Exits</th><th align="right">Today $</th>
                <th align="right">Open</th></tr>
          </thead>
          <tbody>{activity_rows}</tbody>
        </table>

        <h3 style="margin:20px 0 12px;font-size:15px;">30-day performance</h3>
        <table style="width:100%; font-size:13px; border-collapse:collapse">
          <thead style="color:#55556A; font-size:11px; text-transform:uppercase">
            <tr><th align="left">Strategy</th><th align="right">Trades</th>
                <th align="right">WR</th><th align="right">Mean PnL</th>
                <th align="right">Total $</th></tr>
          </thead>
          <tbody>{perf_rows}{spy_html}</tbody>
        </table>

        <h3 style="margin:20px 0 12px;font-size:15px;">Open positions</h3>
        <table style="width:100%; font-size:13px; border-collapse:collapse">
          <thead style="color:#55556A; font-size:11px; text-transform:uppercase">
            <tr><th align="left">Strategy</th><th align="left">Ticker</th>
                <th align="left">Insider</th><th align="left">Entered</th>
                <th align="right">Entry $</th><th align="right">Shares</th>
                <th align="left">Planned exit</th></tr>
          </thead>
          <tbody>{open_rows}</tbody>
        </table>

        {div_html}
        {unres_html}
        {alerts_html}
      </div>

      <p style="margin-top:16px;font-size:11px;color:#55556A;">
        admin diagnostics: <a href="https://form4.app/admin/strategies/quality_momentum"
        style="color:#3B82F6;">/admin/strategies</a>
      </p>
    </div>
    """


# ── Sending ─────────────────────────────────────────────────────────────────

def send_via_resend(to: str, subject: str, html: str) -> bool:
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        logger.warning("RESEND_API_KEY not set — dry-run-only mode")
        return False
    try:
        resp = httpx.post(
            RESEND_URL,
            json={"from": FROM_ADDRESS, "to": [to], "subject": subject, "html": html},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        if resp.status_code in (200, 201):
            return True
        logger.error("Resend %d: %s", resp.status_code, resp.text[:300])
        return False
    except httpx.HTTPError as exc:
        logger.error("Resend request failed: %s", exc)
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Render to stdout, don't send")
    p.add_argument("--to", default=os.getenv("DAILY_DIGEST_TO", "derek.gorthy@gmail.com"))
    args = p.parse_args()

    today = _today_et()
    conn = get_connection(readonly=True)
    activity = fetch_today_activity(conn, today)
    perf = fetch_30d_performance(conn)
    spy = fetch_spy_30d(conn)
    divergences = fetch_active_divergences(conn)
    unresolved = fetch_unresolved_orders(conn)
    alerts = fetch_recent_alerts(hours=24)
    conn.close()

    html = render_html(activity, perf, spy, divergences, unresolved, alerts, today)
    subject = f"Form4 Daily · {today}"

    n_open = sum(len(s["open_positions"]) for s in activity)
    n_today = sum(len(s["entries_today"]) + len(s["exits_today"]) for s in activity)
    if divergences or unresolved:
        subject += f" · {len(divergences)+len(unresolved)} drift"
    if n_today:
        subject += f" · {n_today} trades"

    if args.dry_run:
        print(f"=== Dry run: {subject} → {args.to} ===")
        print(f"Activity: {n_today} trades today; {n_open} open; "
              f"{len(divergences)} divergences; {len(unresolved)} unresolved; "
              f"{len(alerts)} alerts (24h)")
        out_path = REPO / "logs" / "daily_summary_preview.html"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html)
        print(f"HTML preview written to {out_path}")
        return 0

    ok = send_via_resend(args.to, subject, html)
    log_path = REPO / "logs" / "daily-summary.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()} "
                f"sent={ok} to={args.to} subject={subject!r}\n")
    if ok:
        logger.info("Daily summary sent to %s", args.to)
        return 0
    logger.error("Daily summary send failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())

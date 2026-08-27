#!/usr/bin/env python3
"""Before/after for the SEC bulk reload. What moved, and by how much.

Nothing user-facing changes until this is read. `trades` roughly doubles, and
every grade, signal and published figure downstream of it was computed over
the old half — so the question is not whether the numbers move but whether
they move in ways that make sense.

Reads the baselines captured before the load:
    logs/snapshot_before.json          row counts, grade distributions, books
    logs/api_before_{strategy}.json    the published CAGR / drawdown figures

Usage:
    python3 scripts/rebuild_diff_report.py
    python3 scripts/rebuild_diff_report.py --json > logs/rebuild_diff.json
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from config.database import get_connection  # noqa: E402

LOGS = REPO / "logs"
BOOKS = ("quality_notrend", "quality_momentum", "reversal_dip")
LABEL = {"quality_notrend": "A-List Buys",
         "quality_momentum": "Insider Breakout",
         "reversal_dip": "Insider Dip Buys"}
GRADES = ("A+", "A", "B", "C", "D", "(null)")


def _n(v):
    return int(v) if v is not None else 0


def pct(new, old):
    if not old:
        return "  n/a"
    return f"{(new - old) / old * 100:+6.1f}%"


def after_snapshot(conn) -> dict:
    out = {}
    r = conn.execute("SELECT COUNT(*) AS a, COUNT(DISTINCT accession) AS b FROM trades").fetchone()
    out["trades_rows"], out["trades_filings"] = _n(r[0]), _n(r[1])
    out["career_grade_dist"] = {
        r["g"]: _n(r["n"]) for r in conn.execute(
            """SELECT COALESCE(career_grade,'(null)') g, COUNT(*) AS n FROM trades
                WHERE filing_date >= '2016' GROUP BY 1""").fetchall()}
    out["pit_grade_dist"] = {
        r["g"]: _n(r["n"]) for r in conn.execute(
            """SELECT COALESCE(pit_grade,'(null)') g, COUNT(*) AS n FROM trades
                WHERE filing_date >= '2016' GROUP BY 1""").fetchall()}
    out["books"] = [
        {"strategy": r["s"], "positions": _n(r["n"]), "closed": _n(r["closed"]),
         "avg_pnl_pct": float(r["avg_pnl"]) if r["avg_pnl"] is not None else None}
        for r in conn.execute(
            """SELECT strategy AS s,
                      COUNT(*) AS n,
                      COUNT(*) FILTER (WHERE exit_date IS NOT NULL) AS closed,
                      ROUND(AVG(pnl_pct)::numeric, 3) AS avg_pnl
                 FROM strategy_portfolio
                WHERE execution_source IN ('simulated','alert')
                GROUP BY 1 ORDER BY 1""").fetchall()]
    for k, sql in (("insider_ticker_scores", "insider_ticker_scores"),
                   ("trade_returns", "trade_returns"),
                   ("trade_signals", "trade_signals")):
        out[k] = _n(conn.execute(f"SELECT COUNT(*) AS n FROM {sql}").fetchone()[0])
    out["graded_insiders"] = _n(conn.execute(
        """SELECT COUNT(DISTINCT effective_insider_id) AS n FROM trades
            WHERE career_grade IS NOT NULL""").fetchone()[0])
    return out


def live_api(strategy: str) -> dict:
    """The published figures, read the way a subscriber gets them.

    Deliberately over HTTP rather than recomputed here: the API is what the
    site serves, and a scratch reimplementation disagreeing with it has
    already cost this project a day once.
    """
    url = f"https://form4.app/api/v1/portfolio?strategy={strategy}"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.loads(r.read().decode())
    except Exception as exc:
        return {"error": str(exc)[:80]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    before = json.loads((LOGS / "snapshot_before.json").read_text())
    conn = get_connection()
    after = after_snapshot(conn)

    report = {"before": before, "after": after, "books": {}}

    if args.json:
        for s in BOOKS:
            b = json.loads((LOGS / f"api_before_{s}.json").read_text())
            report["books"][s] = {"before": b, "after": live_api(s)}
        print(json.dumps(report, indent=2, default=str))
        return 0

    print("=" * 74)
    print("SEC BULK RELOAD — WHAT MOVED")
    print("=" * 74)
    print(f"\n{'':28s}{'before':>14s}{'after':>14s}{'change':>12s}")
    for k in ("trades_rows", "trades_filings", "trade_returns", "trade_signals",
              "insider_ticker_scores", "graded_insiders"):
        bo, af = _n(before.get(k)), _n(after.get(k))
        print(f"  {k:26s}{bo:>14,}{af:>14,}{pct(af, bo):>12s}")

    print("\n-- career grade, rows on filings since 2016 " + "-" * 30)
    print(f"{'':10s}{'before':>12s}{'after':>12s}{'change':>12s}")
    bd, ad = before.get("career_grade_dist", {}), after.get("career_grade_dist", {})
    for g in GRADES:
        bo, af = _n(bd.get(g)), _n(ad.get(g))
        if bo or af:
            print(f"  {g:8s}{bo:>12,}{af:>12,}{pct(af, bo):>12s}")

    print("\n-- the three published books " + "-" * 45)
    bb = {b["strategy"]: b for b in before.get("books", [])}
    ab = {b["strategy"]: b for b in after.get("books", [])}
    print(f"{'book':20s}{'positions':>22s}{'closed':>18s}")
    for s in BOOKS:
        b, a = bb.get(s, {}), ab.get(s, {})
        print(f"  {LABEL[s]:18s}"
              f"{_n(b.get('positions')):>9,} -> {_n(a.get('positions')):>9,}"
              f"{_n(b.get('closed')):>8,} -> {_n(a.get('closed')):>7,}")

    print("\n-- published figures (live API) " + "-" * 42)
    print(f"{'book':20s}{'CAGR':>18s}{'max DD':>16s}{'win rate':>16s}")
    for s in BOOKS:
        try:
            b = json.loads((LOGS / f"api_before_{s}.json").read_text())
        except Exception:
            b = {}
        a = live_api(s)
        def g(d, k):
            v = d.get(k)
            return f"{v:.1f}" if isinstance(v, (int, float)) else "?"
        print(f"  {LABEL[s]:18s}"
              f"{g(b,'cagr'):>8s} -> {g(a,'cagr'):>7s}"
              f"{g(b,'max_drawdown'):>8s} -> {g(a,'max_drawdown'):>5s}"
              f"{g(b,'win_rate'):>9s} -> {g(a,'win_rate'):>5s}")

    print("\n-- rebuild steps " + "-" * 56)
    try:
        for r in conn.execute(
                "SELECT step, status, seconds FROM rebuild_progress ORDER BY finished_at").fetchall():
            print(f"  {r[0]:20s}{r[1]:>10s}{float(r[2] or 0):>10.0f}s")
    except Exception:
        print("  (rebuild_progress not populated yet)")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

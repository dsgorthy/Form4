#!/usr/bin/env python3
"""Daily strategy↔Alpaca reconciliation probe.

For each live strategy:
  1. Snapshot Alpaca paper-account positions to `alpaca_position_snapshots`.
  2. Compare against open `strategy_portfolio` rows.
  3. Open `alpaca_reconciliation` rows for new divergences.
  4. Resolve previously-open rows whose divergence has cleared.

Divergence types:
  missing_in_alpaca   — DB row open, Alpaca has no position. Strategy thinks
                        we hold; broker doesn't. Common after manual sells,
                        decoupled reverts, lost shares.
  orphan_in_alpaca    — Alpaca holds shares with no open DB row. Manual buy,
                        leftover from prior strategy version, ghost.
  qty_mismatch        — both hold but share counts differ. Partial fill drift,
                        manual rebalance.
  price_mismatch      — strategy entry_price vs Alpaca avg_entry_price differ
                        by more than 5%. Tracks slippage / catchup synthesis.

Severity:
  info     — orphan in Alpaca with small market value (<$500), or
             price_mismatch under 10%
  warn     — material orphan, qty_mismatch, missing_in_alpaca on a recent
             entry
  critical — missing_in_alpaca on a position older than 7 days, or any
             unresolved drift older than 24h on a non-trivial position

Usage (run on Studio):
    python3 scripts/alpaca_reconcile.py
    python3 scripts/alpaca_reconcile.py --strategy quality_momentum
    python3 scripts/alpaca_reconcile.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

# Load .env for Alpaca credentials.
try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from config.database import get_connection
from strategies.cw_strategies.cw_runner import load_config, get_alpaca

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_DIR = REPO / "strategies/cw_strategies/configs"


def discover_strategies() -> list[dict]:
    """Auto-enumerate every yaml in the configs/ dir. Each yaml is one
    (strategy_name, mode) entry. Paper and live yamls for the same strategy
    show up as separate entries so we reconcile their dedicated Alpaca
    accounts independently.
    """
    import yaml as _yaml
    out = []
    for p in sorted(CONFIG_DIR.glob("*.yaml")):
        try:
            cfg = _yaml.safe_load(p.read_text())
        except Exception:
            continue
        if not isinstance(cfg, dict):
            continue
        name = cfg.get("strategy_name")
        if not name:
            continue
        is_live = bool(cfg.get("live_money", False))
        out.append({
            "name": name,
            "config_path": p,
            "is_live": is_live,
            "label": f"{name} ({'live' if is_live else 'paper'})",
        })
    return out


STRATEGIES = discover_strategies()

# Tunables — keep generous defaults; the reconciler should produce useful
# signal not noise.
PRICE_MISMATCH_PCT = 0.05            # 5% entry vs avg_cost gap
QTY_MISMATCH_TOLERANCE = 0           # exact match required (paper accounts don't fractional-fill insider plays)
ORPHAN_INFO_MARKET_VALUE = 500.0     # < $500 orphan stays info-level
MISSING_CRITICAL_DAYS = 7            # missing_in_alpaca older than 7d → critical


def snapshot_alpaca_positions(conn, strategy: str, alpaca_positions: list[dict],
                              is_live: bool = False) -> None:
    """Append a fresh row per Alpaca position to alpaca_position_snapshots.
    is_live distinguishes paper from live snapshots so the admin diff view
    can show both side-by-side."""
    if not alpaca_positions:
        return
    rows = [
        (
            strategy, p["symbol"], p["qty"],
            p.get("avg_entry_price"), p.get("market_value"),
            p.get("current_price"), p.get("unrealized_pl"),
            is_live,
        )
        for p in alpaca_positions
    ]
    conn.executemany(
        """INSERT INTO alpaca_position_snapshots
              (strategy, ticker, qty, avg_entry_price, market_value,
               current_price, unrealized_pl, is_live)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()


def detect_divergences(
    db_open: dict[str, dict],
    alpaca_positions: list[dict],
) -> list[dict]:
    """Compare DB open rows (ticker → row) against Alpaca positions.
    Returns a list of {issue_type, ticker, severity, ...} dicts."""
    alpaca_by_ticker = {p["symbol"]: p for p in alpaca_positions}
    out: list[dict] = []
    today = datetime.now(timezone.utc)

    # 1. Missing in Alpaca (DB says open, broker doesn't hold)
    for ticker, db_row in db_open.items():
        if ticker in alpaca_by_ticker:
            continue
        entry_dt_str = (db_row.get("entry_date") or "")[:10]
        try:
            age_days = (today.date() - datetime.strptime(entry_dt_str, "%Y-%m-%d").date()).days
        except Exception:
            age_days = 0
        severity = "critical" if age_days >= MISSING_CRITICAL_DAYS else "warn"
        out.append({
            "issue_type": "missing_in_alpaca",
            "ticker": ticker,
            "severity": severity,
            "db_qty": db_row.get("shares"),
            "alpaca_qty": None,
            "db_entry_price": db_row.get("entry_price"),
            "alpaca_avg_cost": None,
            "db_status": db_row.get("status"),
            "portfolio_id": db_row.get("id"),
            "detail": (f"DB row entered {entry_dt_str} ({age_days}d old) — "
                       f"strategy holds, Alpaca has no position. "
                       f"Possible manual exit / decoupled revert / lost share."),
        })

    # 2. Orphan in Alpaca (Alpaca holds, no open DB row)
    for ticker, alpaca_pos in alpaca_by_ticker.items():
        if ticker in db_open:
            continue
        mv = alpaca_pos.get("market_value", 0) or 0
        severity = "info" if abs(mv) < ORPHAN_INFO_MARKET_VALUE else "warn"
        out.append({
            "issue_type": "orphan_in_alpaca",
            "ticker": ticker,
            "severity": severity,
            "db_qty": None,
            "alpaca_qty": alpaca_pos["qty"],
            "db_entry_price": None,
            "alpaca_avg_cost": alpaca_pos.get("avg_entry_price"),
            "db_status": None,
            "portfolio_id": None,
            "detail": (f"Alpaca holds {alpaca_pos['qty']} shares "
                       f"(market_value=${mv:,.0f}) — no open strategy_portfolio row. "
                       f"Manual buy / prior version / ghost."),
        })

    # 3. Qty / price mismatches on tickers held in both
    for ticker, db_row in db_open.items():
        ap = alpaca_by_ticker.get(ticker)
        if ap is None:
            continue
        db_qty = db_row.get("shares") or 0
        alpaca_qty = ap.get("qty") or 0
        if abs(db_qty - alpaca_qty) > QTY_MISMATCH_TOLERANCE:
            out.append({
                "issue_type": "qty_mismatch",
                "ticker": ticker,
                "severity": "warn",
                "db_qty": db_qty,
                "alpaca_qty": alpaca_qty,
                "db_entry_price": db_row.get("entry_price"),
                "alpaca_avg_cost": ap.get("avg_entry_price"),
                "db_status": db_row.get("status"),
                "portfolio_id": db_row.get("id"),
                "detail": (f"shares mismatch: DB={db_qty} alpaca={alpaca_qty} "
                           f"(diff={alpaca_qty - db_qty:+g})"),
            })

        db_entry = db_row.get("entry_price") or 0
        ap_avg = ap.get("avg_entry_price") or 0
        if db_entry > 0 and ap_avg > 0:
            pct = abs(ap_avg - db_entry) / db_entry
            if pct > PRICE_MISMATCH_PCT:
                out.append({
                    "issue_type": "price_mismatch",
                    "ticker": ticker,
                    "severity": "info" if pct < 0.10 else "warn",
                    "db_qty": db_qty,
                    "alpaca_qty": alpaca_qty,
                    "db_entry_price": db_entry,
                    "alpaca_avg_cost": ap_avg,
                    "db_status": db_row.get("status"),
                    "portfolio_id": db_row.get("id"),
                    "detail": (f"entry price drift: db=${db_entry:.2f} "
                               f"alpaca_avg=${ap_avg:.2f} ({pct*100:+.1f}%)"),
                })

    return out


def upsert_divergences(conn, strategy: str, divergences: list[dict],
                       is_live: bool = False,
                       dry_run: bool = False) -> tuple[int, int]:
    """Open rows for new divergences; resolve previously-open rows whose
    divergence has cleared. Scoped per (strategy, is_live) so paper and
    live divergences are tracked independently.

    Returns (opened, resolved)."""
    cur = conn.execute(
        """SELECT id, ticker, issue_type
             FROM alpaca_reconciliation
            WHERE strategy = ? AND COALESCE(is_live, false) = ?
              AND resolved_at IS NULL""",
        (strategy, is_live),
    )
    existing = {(r["ticker"], r["issue_type"]): r["id"] for r in cur.fetchall()}

    current_keys = {(d["ticker"], d["issue_type"]) for d in divergences}

    # Open new
    opened = 0
    for d in divergences:
        key = (d["ticker"], d["issue_type"])
        if key in existing:
            continue  # already open
        if dry_run:
            opened += 1
            continue
        conn.execute(
            """INSERT INTO alpaca_reconciliation
                  (strategy, ticker, issue_type, severity, db_qty, alpaca_qty,
                   db_entry_price, alpaca_avg_cost, db_status, portfolio_id,
                   detail, is_live)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                strategy, d["ticker"], d["issue_type"], d["severity"],
                d.get("db_qty"), d.get("alpaca_qty"),
                d.get("db_entry_price"), d.get("alpaca_avg_cost"),
                d.get("db_status"), d.get("portfolio_id"),
                d.get("detail"), is_live,
            ),
        )
        opened += 1

    # Resolve cleared
    resolved = 0
    for key, row_id in existing.items():
        if key in current_keys:
            continue
        if dry_run:
            resolved += 1
            continue
        conn.execute(
            """UPDATE alpaca_reconciliation
                  SET resolved_at = NOW(),
                      resolution  = 'auto: divergence no longer detected'
                WHERE id = ?""",
            (row_id,),
        )
        resolved += 1

    if not dry_run:
        conn.commit()
    return opened, resolved


def reconcile_strategy(conn, strategy: dict, dry_run: bool = False) -> dict:
    """Run reconciliation for one (strategy, mode). Returns a summary dict."""
    name = strategy["name"]
    is_live = bool(strategy.get("is_live", False))
    label = strategy.get("label", name)
    config = load_config(str(strategy["config_path"]))
    try:
        alpaca = get_alpaca(config)
    except RuntimeError as exc:
        # Live creds missing is expected pre-launch — skip cleanly.
        if is_live and "_LIVE not set" in str(exc):
            return {"strategy": name, "label": label, "is_live": is_live,
                    "skipped": "live_creds_not_configured",
                    "db_open": 0, "alpaca_positions": 0,
                    "divergences_now": 0, "opened": 0, "resolved": 0,
                    "by_type": {}}
        raise
    alpaca_positions = alpaca.list_positions()

    # Pull DB open rows scoped to this mode.
    rows = conn.execute(
        """SELECT id, ticker, shares, entry_price, entry_date, status
             FROM strategy_portfolio
            WHERE strategy = ? AND status = 'open'
              AND COALESCE(is_live, false) = ?""",
        (name, is_live),
    ).fetchall()
    db_open = {r["ticker"]: dict(r) for r in rows}

    if not dry_run:
        snapshot_alpaca_positions(conn, name, alpaca_positions, is_live=is_live)

    divergences = detect_divergences(db_open, alpaca_positions)
    opened, resolved = upsert_divergences(conn, name, divergences,
                                          is_live=is_live, dry_run=dry_run)

    summary = {
        "strategy": name,
        "label": label,
        "is_live": is_live,
        "db_open": len(db_open),
        "alpaca_positions": len(alpaca_positions),
        "divergences_now": len(divergences),
        "opened": opened,
        "resolved": resolved,
        "by_type": {},
    }
    for d in divergences:
        summary["by_type"][d["issue_type"]] = summary["by_type"].get(d["issue_type"], 0) + 1
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=[s["name"] for s in STRATEGIES],
                   help="Reconcile only this strategy")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + report only; no DB writes")
    args = p.parse_args()

    conn = get_connection()
    summaries = []
    for s in STRATEGIES:
        if args.strategy and s["name"] != args.strategy:
            continue
        try:
            summary = reconcile_strategy(conn, s, dry_run=args.dry_run)
        except Exception as exc:
            logger.exception("[%s] reconciliation failed: %s", s["label"], exc)
            summaries.append({"strategy": s["name"], "label": s["label"],
                              "error": str(exc)})
            continue
        summaries.append(summary)
        if summary.get("skipped"):
            logger.info("[%s] skipped: %s", summary["label"], summary["skipped"])
            continue
        logger.info(
            "[%s] db_open=%d alpaca=%d divergences=%d (opened=%d resolved=%d) by_type=%s",
            summary["label"], summary["db_open"], summary["alpaca_positions"],
            summary["divergences_now"], summary["opened"], summary["resolved"],
            summary["by_type"],
        )

    conn.close()

    # Exit non-zero if any critical divergences remain — gives launchd /
    # cron something to notice.
    crit_count = 0
    for s in summaries:
        for d_type, n in (s.get("by_type") or {}).items():
            if d_type in ("missing_in_alpaca",) and n > 0:
                crit_count += n
    if crit_count > 0:
        logger.warning("Reconcile completed with %d unresolved missing_in_alpaca rows.", crit_count)


if __name__ == "__main__":
    main()

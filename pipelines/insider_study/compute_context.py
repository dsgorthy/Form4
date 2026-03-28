#!/usr/bin/env python3
"""
Compute trade context facts for the insider catalog.

Each generator produces descriptive context strings (e.g., "Increased holdings by 17%",
"3rd largest purchase, out of 8") stored as structured metadata in trade_context.
Static facts are pre-rendered; stale-risk facts (value_rank, cluster_count) store
metadata only — the API renders final strings with live denominators.

Usage:
    python3 pipelines/insider_study/compute_context.py
    python3 pipelines/insider_study/compute_context.py --since 2026-03-01
    python3 pipelines/insider_study/compute_context.py --context-type holdings_pct_change
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

try:
    from pipelines.insider_study.price_utils import (
        load_prices,
        compute_period_change,
        available_tickers,
    )
except ModuleNotFoundError:
    from price_utils import (
        load_prices,
        compute_period_change,
        available_tickers,
    )

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Context registry ───────────────────────────────────────────────────────

CONTEXT_REGISTRY: dict[str, callable] = {}


def register_context(fn):
    CONTEXT_REGISTRY[fn.__name__] = fn
    return fn


# ─── Helpers ────────────────────────────────────────────────────────────────

def _ordinal(n: int) -> str:
    """Return ordinal string for integer (1st, 2nd, 3rd, etc.)."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _format_pct(pct: float) -> str:
    """Format percentage with sign, e.g. '-40%' or '+17%'."""
    if pct >= 0:
        return f"+{pct:.0f}%"
    return f"{pct:.0f}%"


def _period_label(days: int) -> str:
    """Convert days to human-readable label."""
    if days <= 31:
        return "month"
    elif days <= 93:
        return "3 months"
    else:
        return "6 months"


# ─── Generator 1: holdings_pct_change ───────────────────────────────────────

@register_context
def holdings_pct_change(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Compute percentage change in holdings from this trade."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.trade_type, t.qty, t.shares_owned_after
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          AND t.shares_owned_after IS NOT NULL
          AND t.shares_owned_after > 0
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        shares_after = r["shares_owned_after"]
        qty = r["qty"]
        if r["trade_type"] == "buy":
            shares_before = shares_after - qty
            if shares_before <= 0:
                continue
            pct = (qty / shares_before) * 100
            direction = "Increased"
        else:
            shares_before = shares_after + qty
            if shares_before <= 0:
                continue
            pct = (qty / shares_before) * 100
            direction = "Decreased"

        if pct < 1:
            continue

        text = f"{direction} holdings by {pct:.0f}%"
        metadata = json.dumps({
            "pct": round(pct, 1),
            "direction": direction.lower(),
            "shares_before": round(shares_before),
            "shares_after": round(shares_after),
        })
        results.append((r["trade_id"], "holdings_pct_change", text, 1, metadata))

    logger.info("holdings_pct_change: %d contexts", len(results))
    return results


# ─── Generator 2: value_rank ───────────────────────────────────────────────

@register_context
def value_rank(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Rank this filing by aggregated value among all filings by the same insider+ticker+type.
    Groups by accession (or trade_date fallback) to match feed-level aggregation.
    Stored as metadata only (live rendering needed for 'out of N').
    """
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    # filing_key groups lots into a single filing (same logic as API)
    rows = conn.execute(f"""
        WITH filing_values AS (
            SELECT
                MIN(trade_id) AS trade_id,
                insider_id, ticker, trade_type,
                SUM(value) AS filing_value,
                CASE WHEN accession IS NOT NULL THEN accession ELSE trade_date END AS filing_key
            FROM trades
            WHERE trans_code IN ('P', 'S')
            GROUP BY insider_id, ticker, trade_type, filing_key
        ),
        ranked AS (
            SELECT
                trade_id, insider_id, ticker, trade_type, filing_value,
                RANK() OVER (
                    PARTITION BY insider_id, ticker, trade_type
                    ORDER BY filing_value DESC
                ) AS rnk,
                COUNT(*) OVER (
                    PARTITION BY insider_id, ticker, trade_type
                ) AS total_filings
            FROM filing_values
        )
        SELECT r.trade_id, r.insider_id, r.ticker, r.trade_type, r.filing_value, r.rnk
        FROM ranked r
        JOIN trades t ON r.trade_id = t.trade_id
        WHERE r.rnk <= 3
          AND r.total_filings >= 2
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        metadata = json.dumps({
            "rank": r["rnk"],
            "value": r["filing_value"],
            "insider_id": r["insider_id"],
            "ticker": r["ticker"],
            "trade_type": r["trade_type"],
        })
        # context_text is NULL — live rendering at API time
        results.append((r["trade_id"], "value_rank", None, 2, metadata))

    logger.info("value_rank: %d contexts", len(results))
    return results


# ─── Generator 3: days_since_last ──────────────────────────────────────────

@register_context
def days_since_last(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Days since the insider's previous trade of the same type in the same ticker."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        WITH prev AS (
            SELECT
                trade_id, insider_id, ticker, trade_type, trade_date,
                LAG(trade_date) OVER (
                    PARTITION BY insider_id, ticker, trade_type
                    ORDER BY trade_date
                ) AS prev_date
            FROM trades
            WHERE trans_code IN ('P', 'S')
        )
        SELECT p.trade_id, p.trade_date, p.prev_date,
               CAST(julianday(p.trade_date) - julianday(p.prev_date) AS INTEGER) AS days
        FROM prev p
        JOIN trades t ON p.trade_id = t.trade_id
        WHERE p.prev_date IS NOT NULL
          AND CAST(julianday(p.trade_date) - julianday(p.prev_date) AS INTEGER) >= 90
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        days = r["days"]
        text = f"Last {('purchase' if True else 'sale')} was {days:,} days ago"
        # Better: determine buy/sell
        trade = conn.execute(
            "SELECT trade_type FROM trades WHERE trade_id = ?", (r["trade_id"],)
        ).fetchone()
        action = "purchase" if trade and trade["trade_type"] == "buy" else "sale"
        text = f"Last {action} was {days:,} days ago"
        metadata = json.dumps({
            "days": days,
            "prev_date": r["prev_date"],
        })
        results.append((r["trade_id"], "days_since_last", text, 3, metadata))

    logger.info("days_since_last: %d contexts", len(results))
    return results


# ─── Generator 4: cluster_count ────────────────────────────────────────────

@register_context
def cluster_count(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Count of other insiders who traded the same ticker+direction in the last 30 days.
    Stored as metadata only (live rendering needed for fresh count).
    """
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_type, t.trade_date
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    # Batch by ticker+type for efficiency
    from collections import defaultdict
    by_ticker_type: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_ticker_type[(r["ticker"], r["trade_type"])].append(dict(r))

    results = []
    for (ticker, trade_type), trades in by_ticker_type.items():
        # For each trade, count distinct other insiders in 30d window
        # Sort by date for efficient windowing
        trades.sort(key=lambda x: x["trade_date"])
        for trade in trades:
            count = conn.execute("""
                SELECT COUNT(DISTINCT insider_id) AS cnt
                FROM trades
                WHERE ticker = ? AND trade_type = ?
                  AND trade_date BETWEEN date(?, '-30 days') AND ?
                  AND insider_id != ?
                  AND trans_code IN ('P', 'S')
            """, (ticker, trade_type, trade["trade_date"], trade["trade_date"],
                  trade["insider_id"])).fetchone()

            cnt = count["cnt"] if count else 0
            if cnt >= 2:
                metadata = json.dumps({
                    "count": cnt,
                    "as_of": trade["trade_date"],
                    "ticker": ticker,
                    "trade_type": trade_type,
                })
                results.append((trade["trade_id"], "cluster_count", None, 4, metadata))

    logger.info("cluster_count: %d contexts", len(results))
    return results


# ─── Generator 5: price_context ────────────────────────────────────────────

@register_context
def price_context(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Stock price change in the period before the trade (best of 1m/3m/6m)."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    avail = available_tickers()
    if not avail:
        logger.info("price_context: 0 contexts (no price files)")
        return []

    placeholders = ",".join("?" * len(avail))
    tickers = list(avail)
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          AND t.ticker IN ({placeholders})
          {where_since}
    """, tickers).fetchall()

    # Windows to check: 30d (1m), 90d (3m), 180d (6m)
    windows = [(30, "month"), (90, "3 months"), (180, "6 months")]
    results = []
    for r in rows:
        prices = load_prices(r["ticker"])
        if not prices:
            continue

        # Find the most notable change
        best_change = None
        best_window = None
        best_days = None
        for days, label in windows:
            change = compute_period_change(prices, r["trade_date"], days)
            if change is not None and abs(change) >= 0.10:
                if best_change is None or abs(change) > abs(best_change):
                    best_change = change
                    best_window = label
                    best_days = days

        if best_change is not None:
            pct = round(best_change * 100, 0)
            direction = "up" if best_change > 0 else "down"
            text = f"Stock {direction} {_format_pct(pct)} in previous {best_window}"
            metadata = json.dumps({
                "best_window": best_window,
                "best_days": best_days,
                "change_pct": round(best_change * 100, 1),
            })
            results.append((r["trade_id"], "price_context", text, 5, metadata))

    logger.info("price_context: %d contexts", len(results))
    return results


# ─── Generator 6: routine_flag ─────────────────────────────────────────────

@register_context
def routine_flag(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Flag trades that are part of a 10b5-1 trading plan."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id
        FROM trades t
        WHERE t.is_10b5_1 = 1
          AND t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        text = "Part of a 10b5-1 trading plan"
        metadata = json.dumps({"reason": "10b5_1"})
        results.append((r["trade_id"], "routine_flag", text, 6, metadata))

    logger.info("routine_flag: %d contexts", len(results))
    return results


# ─── Generator 7: reversal_detail ──────────────────────────────────────────

@register_context
def reversal_detail(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Count of consecutive opposite-direction trades before this one."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_type, t.trade_date
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        opposite = "sell" if r["trade_type"] == "buy" else "buy"
        # Get previous trades by this insider for this ticker, ordered by date desc
        prev_trades = conn.execute("""
            SELECT trade_type FROM trades
            WHERE insider_id = ? AND ticker = ?
              AND trade_date < ?
              AND trans_code IN ('P', 'S')
            ORDER BY trade_date DESC
            LIMIT 10
        """, (r["insider_id"], r["ticker"], r["trade_date"])).fetchall()

        if not prev_trades:
            continue

        consecutive = 0
        for pt in prev_trades:
            if pt["trade_type"] == opposite:
                consecutive += 1
            else:
                break

        if consecutive >= 2:
            opposite_label = "sales" if opposite == "sell" else "purchases"
            text = f"Made {consecutive} straight {opposite_label} before this {'purchase' if r['trade_type'] == 'buy' else 'sale'}"
            metadata = json.dumps({
                "consecutive": consecutive,
                "opposite_type": opposite,
            })
            results.append((r["trade_id"], "reversal_detail", text, 7, metadata))

    logger.info("reversal_detail: %d contexts", len(results))
    return results


# ─── Generator 8: conflicting_activity ─────────────────────────────────────

@register_context
def conflicting_activity(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Flag trades where the opposite direction has significant discretionary activity
    in the same ticker within 30 days. E.g., a buy when proven sellers are dumping."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_type, t.trade_date, t.value
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        opposite = "sell" if r["trade_type"] == "buy" else "buy"
        opposite_code = "S" if opposite == "sell" else "P"

        # Find opposing discretionary activity in 30d window
        opposing = conn.execute("""
            SELECT COUNT(DISTINCT t2.insider_id) AS n_opposing,
                   SUM(t2.value) AS opposing_value,
                   AVG(itr.sell_win_rate_7d) AS avg_sell_acc
            FROM trades t2
            LEFT JOIN insider_track_records itr ON t2.insider_id = itr.insider_id
            WHERE t2.ticker = ? AND t2.trans_code = ?
              AND t2.trade_date BETWEEN date(?, '-30 days') AND date(?, '+7 days')
              AND t2.insider_id != ?
              AND (t2.is_routine != 1 OR t2.is_routine IS NULL)
              AND (t2.is_10b5_1 != 1 OR t2.is_10b5_1 IS NULL)
        """, (r["ticker"], opposite_code, r["trade_date"], r["trade_date"],
              r["insider_id"])).fetchone()

        n_opp = opposing["n_opposing"] if opposing else 0
        opp_val = opposing["opposing_value"] if opposing else 0
        avg_acc = opposing["avg_sell_acc"] if opposing else None

        if n_opp < 2 or not opp_val or opp_val < 100000:
            continue

        # Build the context text
        if r["trade_type"] == "buy":
            if avg_acc and avg_acc >= 0.55:
                text = f"{n_opp} proven sellers ({avg_acc:.0%} accuracy) also selling this stock"
                sort = 0  # high priority — show first
            else:
                text = f"{n_opp} insiders selling while this insider is buying"
                sort = 4
        else:
            text = f"{n_opp} insiders buying while this insider is selling"
            sort = 4

        metadata = json.dumps({
            "n_opposing": n_opp,
            "opposing_value": round(opp_val),
            "opposing_direction": opposite,
            "avg_sell_accuracy": round(avg_acc, 3) if avg_acc else None,
        })
        results.append((r["trade_id"], "conflicting_activity", text, sort, metadata))

    logger.info("conflicting_activity: %d contexts", len(results))
    return results


# ─── Generator 9: week52_context ───────────────────────────────────────────

@register_context
def week52_context(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Flag trades where the stock is near its 52-week high or low.
    Insiders buying at 52-week highs = 12.8% annual alpha (Lasfer 2024).
    Insiders buying at 52-week lows = classic dip buy."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""

    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date, t.trade_type
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        prices = load_prices(r["ticker"])
        if not prices:
            continue

        trade_date = r["trade_date"]
        current_price = prices.get(trade_date)
        if not current_price:
            # Try nearby dates
            from datetime import datetime, timedelta
            try:
                td = datetime.strptime(trade_date, "%Y-%m-%d")
            except ValueError:
                continue
            for off in range(1, 5):
                nearby = (td - timedelta(days=off)).strftime("%Y-%m-%d")
                if nearby in prices:
                    current_price = prices[nearby]
                    break
            if not current_price:
                continue

        # Compute 52-week high and low
        from datetime import datetime, timedelta
        try:
            td = datetime.strptime(trade_date, "%Y-%m-%d")
        except ValueError:
            continue

        start_52w = (td - timedelta(days=365)).strftime("%Y-%m-%d")
        window_prices = [v for k, v in prices.items() if start_52w <= k <= trade_date]
        if len(window_prices) < 50:  # need reasonable history
            continue

        high_52w = max(window_prices)
        low_52w = min(window_prices)

        if high_52w <= 0:
            continue

        pct_from_high = ((current_price - high_52w) / high_52w) * 100
        pct_from_low = ((current_price - low_52w) / low_52w) * 100 if low_52w > 0 else 0

        # Near 52-week high (within 5%)
        if pct_from_high >= -5:
            if r["trade_type"] == "buy":
                text = f"Buying near 52-week high ({pct_from_high:+.0f}% from peak)"
            else:
                text = f"Selling near 52-week high ({pct_from_high:+.0f}% from peak)"
            metadata = json.dumps({
                "pct_from_high": round(pct_from_high, 1),
                "high_52w": round(high_52w, 2),
                "current": round(current_price, 2),
            })
            results.append((r["trade_id"], "week52_context", text, 3, metadata))

        # Near 52-week low (within 10%)
        elif low_52w > 0 and pct_from_low <= 10:
            if r["trade_type"] == "buy":
                text = f"Buying near 52-week low ({pct_from_low:+.0f}% from bottom)"
            else:
                text = f"Selling near 52-week low ({pct_from_low:+.0f}% from bottom)"
            metadata = json.dumps({
                "pct_from_low": round(pct_from_low, 1),
                "low_52w": round(low_52w, 2),
                "current": round(current_price, 2),
            })
            results.append((r["trade_id"], "week52_context", text, 3, metadata))

    logger.info("week52_context: %d contexts", len(results))
    return results


# ─── Generator 10: market_cap_tier ─────────────────────────────────────────

@register_context
def market_cap_tier(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Add market cap context. Small-cap insider buys have significantly more alpha
    than large-cap buys (academic consensus + our validation)."""
    import json as _json
    from pathlib import Path

    mcap_path = Path(__file__).resolve().parent.parent.parent / "data" / "market_caps.json"
    if not mcap_path.exists():
        logger.info("market_cap_tier: 0 contexts (no market_caps.json)")
        return []

    mcaps = _json.loads(mcap_path.read_text())
    if not mcaps:
        logger.info("market_cap_tier: 0 contexts (empty market_caps.json)")
        return []

    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    tiers = [
        (3e8, "Micro-Cap", "Under $300M market cap — highest insider signal alpha"),
        (2e9, "Small-Cap", "Under $2B market cap — strong insider signal"),
        (1e10, "Mid-Cap", "$2B-$10B market cap"),
        (1e11, "Large-Cap", "$10B-$100B market cap — limited insider alpha"),
        (float("inf"), "Mega-Cap", "Over $100B — insider trades are typically low-signal"),
    ]

    results = []
    for r in rows:
        mc = mcaps.get(r["ticker"])
        if not mc:
            continue
        for threshold, label, desc in tiers:
            if mc < threshold:
                metadata = _json.dumps({
                    "market_cap": mc,
                    "tier": label,
                })
                results.append((r["trade_id"], "market_cap_tier", f"{label} ({desc})", 8, metadata))
                break

    logger.info("market_cap_tier: %d contexts", len(results))
    return results


# ─── Orchestrator ────────────────────────────────────────────────────────────

def ensure_table(conn: sqlite3.Connection):
    """Create trade_context table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_context (
            context_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id     INTEGER NOT NULL REFERENCES trades(trade_id),
            context_type TEXT    NOT NULL,
            context_text TEXT,
            sort_order   INTEGER NOT NULL DEFAULT 0,
            metadata     TEXT    NOT NULL,
            computed_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_id, context_type)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tc_trade ON trade_context(trade_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tc_type  ON trade_context(context_type)")
    conn.commit()


def run_generator(conn: sqlite3.Connection, name: str, fn, since: str | None) -> int:
    """Run one generator: clear old results, compute new, insert."""
    conn.execute("DELETE FROM trade_context WHERE context_type = ?", (name,))

    rows = fn(conn, since)
    if rows:
        conn.executemany(
            """INSERT OR IGNORE INTO trade_context
               (trade_id, context_type, context_text, sort_order, metadata)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
    conn.commit()
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Compute trade context facts")
    parser.add_argument("--since", help="Only process trades since this date (YYYY-MM-DD)")
    parser.add_argument("--context-type", help="Run a single context generator by name")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to insiders.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    ensure_table(conn)

    if args.context_type:
        if args.context_type not in CONTEXT_REGISTRY:
            logger.error("Unknown context type: %s. Available: %s",
                        args.context_type, ", ".join(CONTEXT_REGISTRY))
            sys.exit(1)
        generators = {args.context_type: CONTEXT_REGISTRY[args.context_type]}
    else:
        generators = CONTEXT_REGISTRY

    total = 0
    for name, fn in generators.items():
        logger.info("Running generator: %s", name)
        try:
            n = run_generator(conn, name, fn, args.since)
            total += n
        except Exception:
            logger.exception("Generator %s failed", name)

    # Summary
    summary = conn.execute("""
        SELECT context_type, COUNT(*) AS cnt
        FROM trade_context
        GROUP BY context_type
        ORDER BY cnt DESC
    """).fetchall()

    logger.info("=== Context Summary ===")
    for row in summary:
        logger.info("  %-25s %d", row["context_type"], row["cnt"])
    logger.info("Total contexts computed: %d", total)

    conn.close()


if __name__ == "__main__":
    main()

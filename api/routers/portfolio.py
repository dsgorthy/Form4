from __future__ import annotations

import json
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import FREE_TIER_DAYS
from api.id_encoding import encode_trade_id

router = APIRouter(prefix="/api/v1/portfolio", tags=["portfolio"])


def _build_trade_row(r: dict, scale: float, gated: bool = False) -> dict:
    """Format a single trade row, redacting if gated."""
    row = {
        "id": r["id"],
        "trade_id": encode_trade_id(r["trade_id"]) if r["trade_id"] else None,
        "ticker": r["ticker"],
        "trade_type": r["trade_type"],
        "direction": r["direction"],
        "entry_date": r["entry_date"],
        "entry_price": r["entry_price"],
        "exit_date": r["exit_date"],
        "exit_price": round(r["exit_price"], 2) if r["exit_price"] else None,
        "hold_days": r["hold_days"],
        "target_hold": r["target_hold"],
        "stop_hit": bool(r["stop_hit"]),
        "pnl_pct": round(r["pnl_pct"] * 100, 2) if r["pnl_pct"] else None,
        "pnl_dollar": round(r["pnl_dollar"] * scale, 2) if r["pnl_dollar"] else None,
        "position_size": r["position_size"],
        "insider_name": r["insider_name"],
        "insider_pit_wr": round(r["insider_pit_wr"] * 100, 0) if r["insider_pit_wr"] else None,
        "signal_quality": r["signal_quality"],
        "exit_reason": r["exit_reason"],
        "status": r["status"],
        "execution_source": r.get("execution_source", "backtest"),
        "is_estimated": bool(r.get("is_estimated", True)),
        "company": r.get("company"),
        "gated": gated,
    }
    if gated:
        row["ticker"] = r["ticker"][:1] + "•••"
        row["trade_id"] = None
        row["insider_name"] = "Insider ••••"
        row["entry_price"] = None
        row["exit_price"] = None
        row["pnl_dollar"] = None
        row["insider_pit_wr"] = None
        row["signal_quality"] = None
        row["company"] = None
    return row


ALLOWED_STRATEGIES = {"form4_insider", "cw_reversal"}


@router.get("")
def get_portfolio(
    strategy: str = Query(default="form4_insider"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=25, ge=10, le=100),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Portfolio summary + equity curve + paginated trades."""
    if strategy not in ALLOWED_STRATEGIES:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy}' not found")

    with get_db() as conn:
        # Read starting capital from portfolios config
        portfolio_row = conn.execute(
            "SELECT starting_capital FROM portfolios WHERE name = ?", (strategy,)
        ).fetchone()
        starting = portfolio_row["starting_capital"] if portfolio_row else 100_000

        # Scale factor: if the simulation was run at a different starting capital
        # than what we display, scale P&L accordingly. Since we now simulate at
        # $100K directly, scale = 1.0.
        scale = 1.0

        # Summary stats
        summary = conn.execute("""
            SELECT
                COUNT(*) AS total_trades,
                SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN stop_hit = 1 THEN 1 ELSE 0 END) AS stops,
                AVG(pnl_pct) AS avg_return,
                SUM(pnl_dollar) AS total_pnl,
                MIN(entry_date) AS first_trade,
                MAX(exit_date) AS last_trade,
                MAX(equity_after) AS peak_equity
            FROM strategy_portfolio
            WHERE strategy = ? AND status = 'closed'
        """, (strategy,)).fetchone()

        # Equity curve — rebuild from P&L to avoid snapshot artifacts
        curve_raw = conn.execute("""
            SELECT exit_date, pnl_dollar, ticker, exit_reason
            FROM strategy_portfolio
            WHERE strategy = ? AND status = 'closed' AND exit_date IS NOT NULL
            ORDER BY exit_date
        """, (strategy,)).fetchall()

        # Rebuild running equity from starting capital + cumulative P&L
        running_equity = starting
        curve = []
        for r in curve_raw:
            scaled_pnl = (r["pnl_dollar"] or 0) * scale
            running_equity += scaled_pnl
            curve.append({
                "exit_date": r["exit_date"],
                "equity_after": round(running_equity, 2),
                "pnl_dollar": round(scaled_pnl, 2),
                "ticker": r["ticker"],
                "exit_reason": r["exit_reason"],
            })

        # SPY benchmark: normalized to same starting capital
        spy_benchmark = []
        if curve:
            first_date = curve[0]["exit_date"]
            spy_rows = conn.execute("""
                SELECT date, close FROM daily_prices
                WHERE ticker = 'SPY' AND date >= ?
                ORDER BY date
            """, (first_date,)).fetchall()
            if spy_rows:
                spy_start = spy_rows[0]["close"]
                for r in spy_rows:
                    spy_equity = starting * (r["close"] / spy_start)
                    spy_benchmark.append({
                        "date": r["date"],
                        "equity": round(spy_equity, 2),
                    })

        # Total trade count for pagination
        total_count = conn.execute("""
            SELECT COUNT(*) AS cnt FROM strategy_portfolio WHERE strategy = ?
        """, (strategy,)).fetchone()["cnt"]

        # Per-trade data for client-side filtering (lightweight: date, return, exit type)
        trade_points = [dict(r) for r in conn.execute("""
            SELECT exit_date, ROUND(pnl_pct * 100, 2) AS pnl_pct, exit_reason,
                   ROUND(hold_days, 0) AS hold_days, signal_quality
            FROM strategy_portfolio
            WHERE strategy = ? AND status = 'closed' AND pnl_pct IS NOT NULL
            ORDER BY exit_date
        """, (strategy,)).fetchall()]

        # Still provide flat array for backward compat
        return_distribution = [r["pnl_pct"] for r in trade_points]

        # Exit breakdown (ALL closed trades)
        exit_breakdown = [dict(r) for r in conn.execute("""
            SELECT exit_reason,
                   COUNT(*) AS count,
                   ROUND(AVG(pnl_pct) * 100, 2) AS avg_return,
                   ROUND(SUM(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) AS win_rate,
                   ROUND(AVG(hold_days), 1) AS avg_hold
            FROM strategy_portfolio
            WHERE strategy = ? AND status = 'closed'
            GROUP BY exit_reason
            ORDER BY count DESC
        """, (strategy,)).fetchall()]

        # Annual returns (ALL closed trades)
        annual_returns = [dict(r) for r in conn.execute("""
            SELECT SUBSTR(exit_date, 1, 4) AS year,
                   COUNT(*) AS trades,
                   ROUND(SUM(pnl_dollar), 2) AS pnl,
                   ROUND(SUM(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) AS win_rate
            FROM strategy_portfolio
            WHERE strategy = ? AND status = 'closed'
            GROUP BY SUBSTR(exit_date, 1, 4)
            ORDER BY year
        """, (strategy,)).fetchall()]

        # Paginated trades
        offset = (page - 1) * per_page
        trades = conn.execute("""
            SELECT id, trade_id, ticker, trade_type, direction,
                   entry_date, entry_price, exit_date, exit_price,
                   hold_days, target_hold, stop_hit,
                   pnl_pct, pnl_dollar, position_size,
                   portfolio_value, equity_after,
                   insider_name, insider_pit_n, insider_pit_wr, signal_quality,
                   exit_reason, status,
                   execution_source, is_estimated, company
            FROM strategy_portfolio
            WHERE strategy = ?
            ORDER BY entry_date DESC
            LIMIT ? OFFSET ?
        """, (strategy, per_page, offset)).fetchall()

    # Compute max drawdown from equity curve
    # Two versions: all-time and post-2020 (excludes COVID crash)
    peak = starting
    max_dd = 0
    peak_post2020 = None
    max_dd_post2020 = 0
    for r in curve:
        eq = r["equity_after"] or 0
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
        # Post-2020 drawdown (excludes COVID black swan)
        if r["exit_date"] and r["exit_date"] >= "2021-01-01":
            if peak_post2020 is None:
                peak_post2020 = eq
            if eq > peak_post2020:
                peak_post2020 = eq
            dd2 = (peak_post2020 - eq) / peak_post2020 if peak_post2020 > 0 else 0
            if dd2 > max_dd_post2020:
                max_dd_post2020 = dd2

    final = curve[-1]["equity_after"] if curve else starting
    years = 1
    if summary["first_trade"] and summary["last_trade"]:
        try:
            d1 = datetime.strptime(summary["first_trade"][:10], "%Y-%m-%d")
            d2 = datetime.strptime(summary["last_trade"][:10], "%Y-%m-%d")
            years = max(0.5, (d2 - d1).days / 365)
        except Exception:
            pass
    cagr = ((final / starting) ** (1 / years) - 1) * 100 if final > 0 else 0

    # Gating: free users see last FREE_VISIBLE trades ungated, rest blurred
    FREE_VISIBLE = 10
    is_pro = user.has_full_feed
    free_cutoff = (
        datetime.utcnow() - timedelta(days=FREE_TIER_DAYS)
    ).strftime("%Y-%m-%d")

    trade_rows = []
    for i, r in enumerate(trades):
        if is_pro:
            gated = False
        else:
            global_idx = offset + i
            gated = global_idx >= FREE_VISIBLE
        trade_rows.append(_build_trade_row(dict(r), scale, gated=gated))

    total_pages = max(1, (total_count + per_page - 1) // per_page)

    return {
        "summary": {
            "strategy": strategy,
            "starting_capital": starting,
            "current_equity": round(final, 2),
            "total_pnl": round((summary["total_pnl"] or 0) * scale, 2),
            "cagr": round(cagr, 1),
            "total_trades": summary["total_trades"],
            "wins": summary["wins"],
            "win_rate": round((summary["wins"] / summary["total_trades"]) * 100, 1) if summary["total_trades"] else 0,
            "stops_hit": summary["stops"],
            "max_drawdown": round((max_dd_post2020 if max_dd_post2020 > 0 else max_dd) * 100, 1),
            "max_drawdown_all_time": round(max_dd * 100, 1),
            "max_drawdown_note": "Excl. COVID crash (Mar 2020)" if max_dd_post2020 > 0 and max_dd_post2020 < max_dd else None,
            "avg_return": round((summary["avg_return"] or 0) * 100, 2),
            "first_trade": summary["first_trade"],
            "last_trade": summary["last_trade"],
        },
        "equity_curve": [
            {
                "date": r["exit_date"],
                "equity": r["equity_after"],
                "pnl": round(r["pnl_dollar"], 2) if r["pnl_dollar"] else 0,
                "ticker": r["ticker"],
                "exit_reason": r["exit_reason"],
            }
            for r in curve
        ],
        "spy_benchmark": spy_benchmark,
        "return_distribution": return_distribution,
        "trade_points": trade_points,
        "exit_breakdown": exit_breakdown,
        "annual_returns": annual_returns,
        "trades": trade_rows,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total_count,
            "total_pages": total_pages,
        },
    }


# ---------------------------------------------------------------------------
# Overlay curves — blended equity with idle cash in a base ETF
# ---------------------------------------------------------------------------

BASE_ASSETS = ["SPY", "QQQ", "IWM", "TLT", "GLD", "CASH"]


@router.get("/overlay")
def get_portfolio_overlay(
    strategy: str = Query(default="form4_insider"),
) -> dict:
    """Daily equity curves for insider-only vs blended (idle cash in base ETF).

    Returns daily data points with insider allocation, base ETF allocation,
    blended equity for each base asset, and pure base asset equity (benchmark).
    Downsampled to weekly for performance (every 5th trading day).
    Only allowed strategies are served; others return 404.
    """
    if strategy not in ALLOWED_STRATEGIES:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy}' not found")
    with get_db() as conn:
        portfolio_row = conn.execute(
            "SELECT starting_capital FROM portfolios WHERE name = ?", (strategy,)
        ).fetchone()
        starting = portfolio_row["starting_capital"] if portfolio_row else 100_000

        # All trades
        trades = [dict(r) for r in conn.execute("""
            SELECT entry_date, exit_date, position_size, pnl_dollar,
                   portfolio_value, dollar_amount, status
            FROM strategy_portfolio
            WHERE strategy = ?
            ORDER BY entry_date
        """, (strategy,)).fetchall()]

        # All trading dates — start from first trade, not hardcoded 2016
        first_trade_date = None
        if trades:
            first_trade_date = min(t["entry_date"] for t in trades if t["entry_date"])
        start_from = first_trade_date or "2020-01-01"

        dates = [r["date"] for r in conn.execute("""
            SELECT DISTINCT date FROM daily_prices
            WHERE ticker = 'SPY' AND date >= ?
            ORDER BY date
        """, (start_from,)).fetchall()]

        # Load daily closes for all base assets
        base_prices: dict[str, dict[str, float]] = {a: {} for a in BASE_ASSETS if a != "CASH"}
        for asset in base_prices:
            for r in conn.execute(
                "SELECT date, close FROM daily_prices WHERE ticker = ? ORDER BY date",
                (asset,),
            ):
                base_prices[asset][r["date"]] = r["close"]

    # Build insider excess P&L per trade: trade_pnl minus what the base ETF
    # would have returned on that same capital over the same hold period.
    # excess = trade_pnl - (position_size * portfolio_value * base_return_during_hold)
    closed_trades = [t for t in trades if t["status"] == "closed" and t["exit_date"]]

    # Precompute excess P&L by exit date for each base asset
    excess_by_exit: dict[str, dict[str, float]] = {a: {} for a in BASE_ASSETS}
    pnl_by_exit: dict[str, float] = {}

    for t in closed_trades:
        entry_d = t["entry_date"]
        exit_d = t["exit_date"]
        raw_pnl = t["pnl_dollar"] or 0
        cap = (t["position_size"] or 0.05) * (t["portfolio_value"] or starting)

        pnl_by_exit.setdefault(exit_d, 0.0)
        pnl_by_exit[exit_d] += raw_pnl

        for asset in BASE_ASSETS:
            excess_by_exit[asset].setdefault(exit_d, 0.0)
            if asset == "CASH":
                # vs cash, all P&L is excess
                excess_by_exit[asset][exit_d] += raw_pnl
            else:
                entry_price = base_prices[asset].get(entry_d)
                exit_price = base_prices[asset].get(exit_d)
                if entry_price and exit_price and entry_price > 0:
                    base_ret = (exit_price - entry_price) / entry_price
                    base_pnl = cap * base_ret
                    excess_by_exit[asset][exit_d] += raw_pnl - base_pnl
                else:
                    # No base price data for this period — treat all P&L as excess
                    excess_by_exit[asset][exit_d] += raw_pnl

    # Simulate day by day
    # insider_equity: cash-drag version (insider P&L only, idle cash earns 0)
    # blended: full portfolio in base ETF + insider excess return on top
    # pure_base: 100% in base ETF
    insider_equity = starting
    blended: dict[str, float] = {a: starting for a in BASE_ASSETS}
    pure_base: dict[str, float] = {a: starting for a in BASE_ASSETS}
    prev_closes: dict[str, float | None] = {a: None for a in BASE_ASSETS if a != "CASH"}

    result_points: list[dict] = []

    for i, date in enumerate(dates):
        open_pos = [t for t in trades
                    if t["entry_date"] <= date
                    and (t["exit_date"] is None or t["exit_date"] > date)
                    and t["status"] in ("open", "closed")]  # include all

        # For allocation display, only count closed trades (known hold periods)
        # plus truly open positions. Cap at 100%.
        closed_open = [t for t in open_pos if t["status"] == "closed"]
        truly_open = [t for t in open_pos if t["status"] == "open" and t["entry_date"] <= date]
        display_pos = closed_open + truly_open
        alloc_pct = min(sum(t["position_size"] or 0.05 for t in display_pos), 1.0)

        # Insider-only equity
        if date in pnl_by_exit:
            insider_equity += pnl_by_exit[date]

        # Blended and pure base
        for asset in BASE_ASSETS:
            if asset == "CASH":
                blended["CASH"] = insider_equity
                pure_base["CASH"] = starting
            else:
                close = base_prices[asset].get(date)
                prev = prev_closes.get(asset)
                if close and prev and prev > 0:
                    daily_ret = (close - prev) / prev
                    # Full portfolio gets base ETF return
                    pure_base[asset] *= (1 + daily_ret)
                    blended[asset] *= (1 + daily_ret)

                # Add insider EXCESS return (above base) on exit dates
                ex = excess_by_exit[asset].get(date, 0)
                if ex != 0:
                    blended[asset] += ex

        for asset in base_prices:
            c = base_prices[asset].get(date)
            if c:
                prev_closes[asset] = c

        # Downsample to weekly (every 5th trading day) + always include last date
        if i % 5 == 0 or i == len(dates) - 1:
            point: dict = {
                "date": date,
                "insider_equity": round(insider_equity, 0),
                "insider_alloc_pct": round(alloc_pct * 100, 1),
                "n_positions": len(open_pos),
            }
            for asset in BASE_ASSETS:
                point[f"blended_{asset}"] = round(blended[asset], 0)
                point[f"pure_{asset}"] = round(pure_base[asset], 0)
            result_points.append(point)

    return {
        "starting_capital": starting,
        "base_assets": BASE_ASSETS,
        "data": result_points,
    }


# ---------------------------------------------------------------------------
# Trade detail endpoint
# ---------------------------------------------------------------------------

@router.get("/trades/{trade_row_id}")
def get_trade_detail(
    trade_row_id: int = Path(..., description="strategy_portfolio.id"),
) -> dict:
    """Full trade detail with reasoning JSON for the trade detail page.

    No auth gating here — the trade list already controls which trades
    are visible vs blurred. If a user has the ID, they can view the detail.
    """
    with get_db() as conn:
        row = conn.execute("""
            SELECT *
            FROM strategy_portfolio
            WHERE id = ?
        """, (trade_row_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Trade not found")

    row = dict(row)

    # Look up insider_id from trades table for cross-linking
    insider_id = None
    if row.get("trade_id"):
        with get_db() as conn2:
            insider_row = conn2.execute(
                "SELECT insider_id FROM trades WHERE trade_id = ?", (row["trade_id"],)
            ).fetchone()
            if insider_row:
                insider_id = insider_row["insider_id"]

    # Scale factor — simulation now runs at display capital directly
    scale = 1.0

    # Parse reasoning JSONs
    entry_reasoning = None
    if row.get("entry_reasoning"):
        try:
            entry_reasoning = json.loads(row["entry_reasoning"])
        except (json.JSONDecodeError, TypeError):
            entry_reasoning = None

    exit_reasoning = None
    if row.get("exit_reasoning"):
        try:
            exit_reasoning = json.loads(row["exit_reasoning"])
        except (json.JSONDecodeError, TypeError):
            exit_reasoning = None

    return {
        "id": row["id"],
        "trade_id": encode_trade_id(row["trade_id"]) if row["trade_id"] else None,
        "ticker": row["ticker"],
        "company": row.get("company"),
        "trade_type": row["trade_type"],
        "direction": row["direction"],
        "status": row["status"],

        # Dates
        "entry_date": row["entry_date"],
        "exit_date": row.get("exit_date"),
        "filing_date": row.get("filing_date"),
        "trade_date": row.get("trade_date"),

        # Prices
        "entry_price": row["entry_price"],
        "exit_price": round(row["exit_price"], 4) if row.get("exit_price") else None,
        "actual_fill_price": row.get("actual_fill_price"),

        # P&L
        "pnl_pct": round(row["pnl_pct"] * 100, 2) if row.get("pnl_pct") else None,
        "pnl_dollar": round(row["pnl_dollar"] * scale, 2) if row.get("pnl_dollar") else None,
        "peak_return": round(row["peak_return"] * 100, 2) if row.get("peak_return") else None,
        "hold_days": row.get("hold_days"),
        "target_hold": row["target_hold"],
        "exit_reason": row.get("exit_reason"),
        "stop_hit": bool(row.get("stop_hit")),

        # Position
        "position_size": row["position_size"],
        "shares": row.get("shares"),
        "dollar_amount": round(row["dollar_amount"] * scale, 2) if row.get("dollar_amount") else None,
        "portfolio_value": round(row["portfolio_value"] * scale, 2) if row.get("portfolio_value") else None,
        "stop_pct": row["stop_pct"],

        # Insider
        "insider_name": row["insider_name"],
        "insider_id": insider_id,
        "insider_title": row.get("insider_title"),
        "insider_pit_n": row.get("insider_pit_n"),
        "insider_pit_wr": round(row["insider_pit_wr"] * 100, 1) if row.get("insider_pit_wr") else None,
        "trade_value": row.get("trade_value"),

        # Signal
        "signal_quality": row.get("signal_quality"),
        "signal_grade": row.get("signal_grade"),
        "is_csuite": bool(row.get("is_csuite")),
        "holdings_pct_change": round(row["holdings_pct_change"] * 100, 1) if row.get("holdings_pct_change") else None,
        "is_rare_reversal": bool(row.get("is_rare_reversal")),
        "is_cluster": bool(row.get("is_cluster")),
        "cluster_size": row.get("cluster_size"),

        # Execution
        "execution_source": row.get("execution_source", "backtest"),
        "is_estimated": bool(row.get("is_estimated", True)),
        "slippage_applied": row.get("slippage_applied"),

        # Reasoning blobs (full detail for UI)
        "entry_reasoning": entry_reasoning,
        "exit_reasoning": exit_reasoning,
    }

from __future__ import annotations

import json
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import FREE_TIER_DAYS
from api.id_encoding import encode_trade_id

router = APIRouter(prefix="/api/v1/portfolio", tags=["portfolio"])


def _build_trade_row(r: dict, scale: float, gated: bool = False, current_price=None) -> dict:
    """Format a single trade row, redacting if gated.

    For OPEN positions, `current_price` (the latest available close) drives the
    unrealized P&L fields so the frontend shows live gains instead of a dash.
    """
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
        "pit_grade": r.get("pit_grade"),
        "career_grade": r.get("career_grade"),
        "current_price": round(current_price, 2) if current_price else None,
        "unrealized_pnl_pct": None,
        "unrealized_pnl_dollar": None,
        "gated": gated,
    }
    # Open positions: mark to the latest close so the UI shows live unrealized
    # gains instead of a dash. dollar_amount is the capital basis; fall back to
    # shares * entry_price for legacy rows written before dollar_amount existed.
    if r.get("status") == "open" and current_price and r.get("entry_price"):
        ep = r["entry_price"]
        if ep > 0:
            upct = (current_price - ep) / ep
            basis = r.get("dollar_amount") or ((r.get("shares") or 0) * ep)
            row["unrealized_pnl_pct"] = round(upct * 100, 2)
            row["unrealized_pnl_dollar"] = round((basis or 0) * upct * scale, 2)
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
        row["pit_grade"] = None
        row["career_grade"] = None
        # Redact dollar figures (matches pnl_dollar/entry_price); the % stays
        # visible like pnl_pct so gated rows still show direction of move.
        row["current_price"] = None
        row["unrealized_pnl_dollar"] = None
    return row


# Productized strategy set (max 3). Legacy rows for form4_insider, cw_reversal,
# cw_composite, and reversal_quality still exist in strategy_portfolio as
# historical archive, but are intentionally unreachable from the API and UI.
# Retirement dates: reversal_quality 2026-04-09 (split into reversal_dip +
# quality_momentum); form4_insider + cw_reversal + cw_composite 2026-04-11
# (replaced by current 3 or merged into reversal_dip/quality_momentum runners).
ALLOWED_STRATEGIES = {"quality_momentum", "reversal_dip", "tenb51_surprise"}


@router.get("")
def get_portfolio(
    strategy: str = Query(default="quality_momentum"),
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

        # Every strategy_portfolio read in this function filters
        # execution_source = 'simulated' — the canonical day-by-day simulation
        # that is this view's single source of truth (matches /overlay). For the
        # same strategies, cw_runner also writes operational 'alert'/'paper'/
        # 'live' rows (dedup + capacity tracking, see cw_runner alert_only mode).
        # Those rows overlap the simulated positions one-for-one, so without this
        # filter they double up the open-positions list (duplicate open
        # positions bug, 2026-06-22). Keep the filter on any new query added here.

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
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated' AND status = 'closed'
        """, (strategy,)).fetchone()

        # Equity curve — rebuild from P&L to avoid snapshot artifacts.
        # We anchor the curve at the strategy's earliest activity (first
        # entry_date, open or closed) so the chart spans the full simulation
        # period instead of starting at the first exit. Open positions are
        # marked-to-market with the latest available close so the curve
        # extends through "today" rather than ending at the last closed exit.
        curve_raw = conn.execute("""
            SELECT exit_date, pnl_dollar, ticker, exit_reason
            FROM strategy_portfolio
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated' AND status = 'closed' AND exit_date IS NOT NULL
            ORDER BY exit_date
        """, (strategy,)).fetchall()

        # Earliest activity date (open or closed) — anchor point for the curve
        anchor_row = conn.execute("""
            SELECT MIN(entry_date) AS earliest
            FROM strategy_portfolio
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated'
        """, (strategy,)).fetchone()
        anchor_date = anchor_row["earliest"] if anchor_row else None

        # Open positions for mark-to-market
        open_positions = conn.execute("""
            SELECT ticker, entry_date, entry_price, dollar_amount, position_size
            FROM strategy_portfolio
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated'
              AND status = 'open' AND entry_price > 0
        """, (strategy,)).fetchall()

        # Bulk-fetch the latest close per ticker we need
        open_tickers = sorted({p["ticker"] for p in open_positions})
        latest_closes: dict[str, tuple[str, float]] = {}
        if open_tickers:
            placeholders = ",".join(["?"] * len(open_tickers))
            for r in conn.execute(f"""
                SELECT dp.ticker, dp.date::text AS date, dp.close
                FROM prices.daily_prices dp
                JOIN (
                    SELECT ticker, MAX(date) AS max_date
                    FROM prices.daily_prices
                    WHERE ticker IN ({placeholders})
                    GROUP BY ticker
                ) latest USING (ticker)
                WHERE dp.date = latest.max_date
            """, tuple(open_tickers)).fetchall():
                latest_closes[r["ticker"]] = (r["date"], float(r["close"]))

        # Build curve: anchor → each closed exit → mark-to-market today
        running_equity = starting
        curve = []
        if anchor_date:
            curve.append({
                "exit_date": anchor_date,
                "equity_after": round(running_equity, 2),
                "pnl_dollar": 0.0,
                "ticker": None,
                "exit_reason": "anchor",
            })
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

        # Append today's mark-to-market for open positions
        open_unrealized = 0.0
        for p in open_positions:
            ticker = p["ticker"]
            entry_price = p["entry_price"] or 0
            capital = p["dollar_amount"] or 0
            if entry_price <= 0 or capital <= 0:
                continue
            lc = latest_closes.get(ticker)
            if not lc:
                continue
            _last_date, last_close = lc
            open_unrealized += capital * ((last_close - entry_price) / entry_price)
        if open_positions:
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            curve.append({
                "exit_date": today_str,
                "equity_after": round(running_equity + open_unrealized, 2),
                "pnl_dollar": round(open_unrealized, 2),
                "ticker": None,
                "exit_reason": "mark_to_market",
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

        # Total trade count for pagination — must match the trade-log row set:
        # one open row per ticker (operational ∪ simulated) + simulated closed.
        total_count = conn.execute("""
            SELECT
              (SELECT COUNT(DISTINCT ticker) FROM strategy_portfolio
                WHERE strategy = ? AND COALESCE(is_live, false) = false
                  AND status = 'open'
                  AND execution_source IN ('alert','paper','live','simulated'))
              + (SELECT COUNT(*) FROM strategy_portfolio
                  WHERE strategy = ? AND COALESCE(is_live, false) = false
                    AND status = 'closed' AND execution_source = 'simulated') AS cnt
        """, (strategy, strategy)).fetchone()["cnt"]

        # Per-trade data for client-side filtering (lightweight: date, return, exit type)
        trade_points = [dict(r) for r in conn.execute("""
            SELECT exit_date, ROUND(pnl_pct * 100, 2) AS pnl_pct, exit_reason,
                   ROUND(hold_days, 0) AS hold_days, signal_quality
            FROM strategy_portfolio
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated' AND status = 'closed' AND pnl_pct IS NOT NULL
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
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated' AND status = 'closed'
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
            WHERE strategy = ? AND COALESCE(is_live, false) = false AND execution_source = 'simulated' AND status = 'closed'
            GROUP BY SUBSTR(exit_date, 1, 4)
            ORDER BY year
        """, (strategy,)).fetchall()]

        # Paginated trades — JOIN trades for the entry trade's pit_grade /
        # career_grade so the frontend can display Career Grade per row.
        offset = (page - 1) * per_page
        # Trade log: include open positions (the strategy's currently-held
        # set). Open rows have no exit_date/exit_price/pnl_pct yet — frontend
        # renders these with an OPEN badge. Closed-only filter removed
        # 2026-05-12 after the strategy_portfolio rebuild produced legitimate
        # in-flight positions that should be visible to subscribers.
        trades = conn.execute("""
            WITH open_pick AS (
                -- Open positions = ONE row per ticker, preferring the live
                -- operational entry (alert/paper/live = what we actually
                -- pinged/traded, the entry price the user saw) over the
                -- nightly simulated counterpart. This is what de-dups the
                -- table: the same held ticker no longer shows once per source.
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY ticker
                               ORDER BY CASE execution_source
                                            WHEN 'live'  THEN 0
                                            WHEN 'paper' THEN 1
                                            WHEN 'alert' THEN 2
                                            ELSE 3 END,
                                        entry_date DESC, id DESC
                           ) AS rk
                    FROM strategy_portfolio
                    WHERE strategy = ? AND COALESCE(is_live, false) = false
                      AND status = 'open'
                      AND execution_source IN ('alert','paper','live','simulated')
                ) x WHERE rk = 1
            ),
            closed_pick AS (
                -- Closed track record = the canonical nightly simulation.
                SELECT id FROM strategy_portfolio
                WHERE strategy = ? AND COALESCE(is_live, false) = false
                  AND status = 'closed' AND execution_source = 'simulated'
            )
            SELECT sp.id, sp.trade_id, sp.ticker, sp.trade_type, sp.direction,
                   sp.entry_date, sp.entry_price, sp.exit_date, sp.exit_price,
                   sp.hold_days, sp.target_hold, sp.stop_hit,
                   sp.pnl_pct, sp.pnl_dollar, sp.position_size,
                   sp.dollar_amount, sp.shares,
                   sp.portfolio_value, sp.equity_after,
                   sp.insider_name, sp.insider_pit_n, sp.insider_pit_wr, sp.signal_quality,
                   sp.exit_reason, sp.status,
                   sp.execution_source, sp.is_estimated, sp.company,
                   t.pit_grade, t.career_grade
            FROM strategy_portfolio sp
            LEFT JOIN trades t ON t.trade_id = sp.trade_id
            WHERE sp.id IN (SELECT id FROM open_pick)
               OR sp.id IN (SELECT id FROM closed_pick)
            ORDER BY (sp.status = 'open') DESC, sp.entry_date DESC
            LIMIT ? OFFSET ?
        """, (strategy, strategy, per_page, offset)).fetchall()

        # Latest close per displayed OPEN ticker → unrealized P&L. The
        # displayed open set is the operational/alert track (one row per
        # ticker) which can differ from the simulated open set used by the
        # equity curve above, so fetch closes for exactly what we show.
        disp_open_tickers = sorted({r["ticker"] for r in trades if r["status"] == "open"})
        disp_closes: dict[str, float] = {}
        if disp_open_tickers:
            ph = ",".join(["?"] * len(disp_open_tickers))
            for r in conn.execute(f"""
                SELECT dp.ticker, dp.close
                FROM prices.daily_prices dp
                JOIN (
                    SELECT ticker, MAX(date) AS md
                    FROM prices.daily_prices
                    WHERE ticker IN ({ph})
                    GROUP BY ticker
                ) l ON l.ticker = dp.ticker AND l.md = dp.date
            """, tuple(disp_open_tickers)).fetchall():
                disp_closes[r["ticker"]] = float(r["close"])

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
        cp = disp_closes.get(r["ticker"]) if r["status"] == "open" else None
        trade_rows.append(_build_trade_row(dict(r), scale, gated=gated, current_price=cp))

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
    strategy: str = Query(default="quality_momentum"),
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

        # Load simulated rows (closed + open). Open positions have NULL
        # exit_date / pnl_pct; the simulator loop opens them on entry_date
        # and never closes them, so they correctly stay in
        # `blended_insider_caps` at the latest data point — that's how the
        # dashboard's "Current Allocation" panel knows what's currently held.
        trades = [dict(r) for r in conn.execute("""
            SELECT id, entry_date, exit_date, position_size, pnl_pct, pnl_dollar,
                   portfolio_value, dollar_amount, status
            FROM strategy_portfolio
            WHERE strategy = ? AND execution_source = 'simulated'
              AND (
                (status = 'closed' AND exit_date IS NOT NULL AND pnl_pct IS NOT NULL)
                OR status = 'open'
              )
            ORDER BY entry_date, id
        """, (strategy,)).fetchall()]

        # All trading dates — start from first trade
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

    # Pre-bucket trades by entry/exit date for the simulation loop.
    opens_by_date: dict[str, list[dict]] = {}
    closes_by_date: dict[str, list[dict]] = {}
    for t in trades:
        opens_by_date.setdefault(t["entry_date"], []).append(t)
        closes_by_date.setdefault(t["exit_date"], []).append(t)

    # ── Corrected overlay simulation ───────────────────────────────────
    # At every moment, total equity = idle_capital + sum(insider_capital).
    # Idle capital earns the base ETF's daily return.
    # Insider capital earns the trade's pnl_pct on exit (no MTM during hold).
    # If allocation tries to exceed 100% (legacy backtest overlap edge case),
    # we admit positions in order and cap at available idle_cap — same as
    # what cw_runner would do in live.
    #
    # Each BASE_ASSET runs its own independent simulation so the dashboard
    # can show "what if idle was in SPY vs QQQ vs CASH" side-by-side.

    insider_only_equity = starting           # legacy: insider P&L only, idle = 0
    pure_base: dict[str, float] = {a: starting for a in BASE_ASSETS}
    blended_equity: dict[str, float] = {a: starting for a in BASE_ASSETS}
    blended_idle: dict[str, float] = {a: starting for a in BASE_ASSETS}
    blended_insider_caps: dict[str, dict] = {a: {} for a in BASE_ASSETS}
    prev_closes: dict[str, float | None] = {a: None for a in BASE_ASSETS if a != "CASH"}

    result_points: list[dict] = []

    for i, date in enumerate(dates):
        # Daily base-asset return
        daily_ret: dict[str, float] = {}
        for asset in BASE_ASSETS:
            if asset == "CASH":
                daily_ret["CASH"] = 0.0
            else:
                close = base_prices[asset].get(date)
                prev = prev_closes.get(asset)
                daily_ret[asset] = (close - prev) / prev if close and prev and prev > 0 else 0.0
                if close:
                    prev_closes[asset] = close
                # Pure base: 100% always in base asset
                pure_base[asset] *= (1 + daily_ret[asset])

        # Insider-only (cash-drag) — recognize lump P&L on exits
        for t in closes_by_date.get(date, []):
            insider_only_equity += t["pnl_dollar"] or 0

        # Blended sims (one per base asset)
        for asset in BASE_ASSETS:
            # 1) Realize exits — capital + P&L returns to idle
            for t in closes_by_date.get(date, []):
                tid = t["id"]
                if tid in blended_insider_caps[asset]:
                    entry_cap = blended_insider_caps[asset][tid]
                    exit_value = entry_cap * (1 + (t["pnl_pct"] or 0))
                    blended_idle[asset] += exit_value
                    blended_equity[asset] += exit_value - entry_cap  # equity changes by P&L only
                    del blended_insider_caps[asset][tid]

            # 2) New entries — move capital from idle into insider
            for t in opens_by_date.get(date, []):
                target = (t["position_size"] or 0.10) * blended_equity[asset]
                allocated = min(target, max(0.0, blended_idle[asset]))
                if allocated > 0:
                    blended_insider_caps[asset][t["id"]] = allocated
                    blended_idle[asset] -= allocated
                # else: capacity-skip (mirrors what would happen live)

            # 3) Idle capital earns daily return
            growth = blended_idle[asset] * daily_ret[asset]
            blended_idle[asset] += growth
            blended_equity[asset] += growth

        # Downsample to weekly + always include last date
        if i % 5 == 0 or i == len(dates) - 1:
            # Allocation snapshot (using SPY blend as reference; identical for others)
            ref_caps = blended_insider_caps.get("SPY", {})
            insider_cap_total = sum(ref_caps.values())
            ref_equity = blended_equity.get("SPY", starting)
            alloc_pct = (insider_cap_total / ref_equity * 100) if ref_equity > 0 else 0

            point: dict = {
                "date": date,
                "insider_equity": round(insider_only_equity, 0),
                "insider_alloc_pct": round(alloc_pct, 1),
                "n_positions": len(ref_caps),
                # NEW — explicit insider/idle dollars per base asset (frontend
                # uses these to show "Insider $X + Idle $Y = Total $Z").
                "insider_capital_dollars": {
                    a: round(sum(blended_insider_caps[a].values()), 0)
                    for a in BASE_ASSETS
                },
                "idle_capital_dollars": {
                    a: round(blended_idle[a], 0) for a in BASE_ASSETS
                },
            }
            for asset in BASE_ASSETS:
                point[f"blended_{asset}"] = round(blended_equity[asset], 0)
                point[f"pure_{asset}"] = round(pure_base[asset], 0)
            result_points.append(point)

    return {
        "starting_capital": starting,
        "base_assets": BASE_ASSETS,
        "data": result_points,
        # NEW — surface the fix metadata for transparency
        "overlay_math": "corrected_2026_05_12",
        "note": ("Equity = insider_capital + idle_capital, both shown in "
                 "insider_capital_dollars / idle_capital_dollars. Total "
                 "allocation always ≤ 100% of equity."),
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
            WHERE id = ? AND COALESCE(is_live, false) = false
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

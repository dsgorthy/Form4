#!/usr/bin/env python3
"""Form4 Paper Trading Daemon — generic YAML-configured insider strategy runner.

Reads a YAML config file specifying a productized strategy, queries insiders.db
for qualifying trades, submits orders to Alpaca paper (via a dedicated per-
strategy paper account declared in the yaml as `alpaca_env_prefix`), tracks
positions in the strategy_portfolio table, and applies thesis-based exits.

Usage:
    python3 strategies/cw_strategies/cw_runner.py --config configs/quality_momentum.yaml
    python3 strategies/cw_strategies/cw_runner.py --config configs/reversal_dip.yaml --dry-run
    python3 strategies/cw_strategies/cw_runner.py --config configs/tenb51_surprise.yaml --once
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import yaml

# Ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env for all credential env vars (trading + data).
# This replaces the legacy pattern where credentials were injected via the
# launchd plist's EnvironmentVariables dict. Plists no longer hardcode secrets.
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # python-dotenv is in requirements.txt; fallback is raw os.environ

from config.database import get_connection
from framework.execution.paper import PaperBackend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Default data dir for state/heartbeat files
DATA_DIR = Path(__file__).resolve().parent / "data"

# ---------------------------------------------------------------------------
# Telegram (sync, minimal)
# ---------------------------------------------------------------------------

def send_telegram(msg: str, prefix: str = "") -> None:
    """Send a Telegram notification. Fails silently."""
    import requests as _req
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        return
    text = f"{prefix} {msg}".strip() if prefix else msg
    try:
        _req.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as exc:
        logger.warning("Telegram send failed: %s", exc)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _now_et() -> datetime:
    return datetime.now(ZoneInfo("America/New_York"))


def _is_market_day() -> bool:
    return _now_et().weekday() < 5


def _is_market_hours() -> bool:
    now = _now_et()
    if now.hour < 9 or now.hour >= 16:
        return False
    if now.hour == 9 and now.minute < 30:
        return False
    return True


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = {"strategy_name", "starting_capital", "position_size_pct", "max_concurrent"}


def load_config(path: str) -> dict:
    """Parse YAML config, validate required keys."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with open(p) as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config must be a YAML mapping, got {type(cfg).__name__}")
    missing = _REQUIRED_KEYS - set(cfg.keys())
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")

    # Normalise: if no 'theses' list and there is a top-level 'filters'+'exit', wrap it
    if "theses" not in cfg and "filters" in cfg and "exit" in cfg:
        cfg["theses"] = [
            {
                "name": cfg["strategy_name"],
                "filters": cfg["filters"],
                "exit": cfg["exit"],
            }
        ]

    # Defaults
    cfg.setdefault("display_name", cfg["strategy_name"])
    cfg.setdefault("telegram_prefix", f"[{cfg['strategy_name'][:6].upper()}]")
    cfg.setdefault("filing_lookback_days", 2)
    cfg.setdefault("circuit_breaker_dd_pct", 0.10)

    for thesis in cfg.get("theses", []):
        thesis.setdefault("name", "default")
        thesis.setdefault("filters", {})
        thesis.setdefault("exit", {"strategy": "fixed_hold", "hold_days": 7, "stop_loss_pct": -0.15})

    return cfg


# ---------------------------------------------------------------------------
# Alpaca + DB
# ---------------------------------------------------------------------------

def get_alpaca(config: dict) -> PaperBackend:
    """Load the dedicated per-strategy trading credentials.

    Strategy yaml must declare `alpaca_env_prefix: QUALITY_MOMENTUM` (or similar).
    The runner reads ALPACA_API_KEY_{prefix} / ALPACA_API_SECRET_{prefix} from .env.
    Never uses ALPACA_DATA_API_KEY — those are read-only data credentials.
    """
    prefix = config.get("alpaca_env_prefix")
    if not prefix:
        raise RuntimeError(
            f"Strategy '{config.get('strategy_name')}' config is missing required "
            f"'alpaca_env_prefix' field (e.g., 'QUALITY_MOMENTUM'). "
            f"Each strategy must have its own dedicated Alpaca paper account — "
            f"never commingle two strategies on the same credentials."
        )
    key_var = f"ALPACA_API_KEY_{prefix}"
    secret_var = f"ALPACA_API_SECRET_{prefix}"
    api_key = os.getenv(key_var, "")
    api_secret = os.getenv(secret_var, "")
    if not api_key or not api_secret:
        raise RuntimeError(f"{key_var} / {secret_var} not set in .env")
    return PaperBackend(api_key, api_secret)


def _data_api_headers() -> dict:
    """Shared read-only data API headers for bars/trades/snapshots.

    Uses ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET from .env — never the
    per-strategy trading credentials. Never use this for order execution.
    """
    return {
        "APCA-API-KEY-ID": os.getenv("ALPACA_DATA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_DATA_API_SECRET", ""),
    }


def get_db(readonly: bool = False):
    return get_connection(readonly=readonly)


# ---------------------------------------------------------------------------
# Portfolio row bootstrap
# ---------------------------------------------------------------------------

def ensure_portfolio_row(conn, config: dict) -> int:
    """Insert into portfolios table on first run, return portfolio_id."""
    conn.execute(
        """INSERT OR IGNORE INTO portfolios (name, display_name, description, config, starting_capital)
           VALUES (?, ?, ?, ?, ?)""",
        (
            config["strategy_name"],
            config["display_name"],
            f"CW strategy: {config['display_name']}",
            json.dumps(config, default=str),
            config["starting_capital"],
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM portfolios WHERE name = ?", (config["strategy_name"],)
    ).fetchone()
    return row["id"]


# ---------------------------------------------------------------------------
# Theoretical equity
# ---------------------------------------------------------------------------

def get_theoretical_equity(conn, config: dict) -> float:
    """Starting capital + cumulative closed P&L for this strategy."""
    row = conn.execute(
        """SELECT COALESCE(SUM(pnl_dollar), 0) AS total_pnl
           FROM strategy_portfolio
           WHERE strategy = ? AND status = 'closed'""",
        (config["strategy_name"],),
    ).fetchone()
    return config["starting_capital"] + (row["total_pnl"] or 0)


# ---------------------------------------------------------------------------
# Signal scanner
# ---------------------------------------------------------------------------

def _build_thesis_query(thesis: dict, lookback_days: int) -> tuple[str, list]:
    """Build WHERE clauses and params for a single thesis filter set."""
    filters = thesis.get("filters", {})
    clauses: list[str] = []
    params: list[Any] = []

    # Base: recent P-code buys, not duplicated
    clauses.append("t.trans_code = 'P'")
    clauses.append(f"t.filing_date >= date('now', '-{int(lookback_days)} days')")
    clauses.append("(t.is_duplicate = 0 OR t.is_duplicate IS NULL)")

    # Optional filters
    if filters.get("is_rare_reversal"):
        clauses.append("t.is_rare_reversal = 1")

    if "min_consecutive_sells" in filters:
        clauses.append("t.consecutive_sells_before >= ?")
        params.append(int(filters["min_consecutive_sells"]))

    if "max_dip_1mo" in filters:
        clauses.append("t.dip_1mo <= ?")
        params.append(float(filters["max_dip_1mo"]))

    if filters.get("above_sma50"):
        clauses.append("t.above_sma50 = 1")

    if filters.get("above_sma200"):
        clauses.append("t.above_sma200 = 1")

    if filters.get("is_largest_ever"):
        clauses.append("t.is_largest_ever = 1")

    if "min_signal_grade" in filters:
        grade = filters["min_signal_grade"].upper()
        allowed = []
        for g in ("A", "B", "C", "D", "F"):
            allowed.append(g)
            if g == grade:
                break
        placeholders = ",".join("?" for _ in allowed)
        clauses.append(f"t.signal_grade IN ({placeholders})")
        params.extend(allowed)

    # PIT grade filter (e.g., ["A+", "A"])
    if "pit_grade" in filters:
        grades = filters["pit_grade"]
        if isinstance(grades, list):
            placeholders = ",".join("?" for _ in grades)
            clauses.append(f"t.pit_grade IN ({placeholders})")
            params.extend(grades)
        else:
            clauses.append("t.pit_grade = ?")
            params.append(str(grades))

    # 3-month dip threshold (e.g., -0.25)
    if "min_dip_3mo" in filters:
        clauses.append("t.dip_3mo <= ?")
        params.append(float(filters["min_dip_3mo"]))

    # Exclude 10b5-1 trades
    if filters.get("exclude_10b5_1"):
        clauses.append("COALESCE(t.is_10b5_1, 0) = 0")

    if filters.get("exclude_recurring"):
        clauses.append("COALESCE(t.is_recurring, 0) = 0")

    if filters.get("exclude_tax_sales"):
        clauses.append("COALESCE(t.is_tax_sale, 0) = 0")

    if filters.get("exclude_routine"):
        clauses.append("COALESCE(t.cohen_routine, 0) = 0")

    return " AND ".join(clauses), params


def scan_signals(conn, config: dict) -> list[dict]:
    """Query trades for qualifying signals across all theses. Return de-duped candidates."""
    strategy_name = config["strategy_name"]
    lookback = config.get("filing_lookback_days", 2)
    theses = config.get("theses", [])
    all_candidates: list[dict] = []
    seen_trade_ids: set[int] = set()

    # Tickers already open for this strategy
    held_tickers = {
        r["ticker"]
        for r in conn.execute(
            "SELECT ticker FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
            (strategy_name,),
        ).fetchall()
    }
    # Trade IDs already in portfolio (open or closed)
    used_trade_ids = {
        r["trade_id"]
        for r in conn.execute(
            "SELECT trade_id FROM strategy_portfolio WHERE strategy = ?",
            (strategy_name,),
        ).fetchall()
        if r["trade_id"] is not None
    }

    for thesis in theses:
        thesis_name = thesis["name"]
        where_clause, where_params = _build_thesis_query(thesis, lookback)

        require_cluster = thesis.get("filters", {}).get("require_cluster", False)
        if require_cluster:
            join_clause = "JOIN trade_signals ts ON ts.trade_id = t.trade_id AND ts.signal_type = 'top_trade'"
        else:
            join_clause = ""

        sql = f"""
            SELECT
                t.trade_id,
                t.ticker,
                t.filing_date,
                t.filed_at,
                t.price,
                COALESCE(i.display_name, i.name) AS insider_name,
                t.company,
                t.title,
                t.signal_quality,
                t.signal_grade,
                t.is_rare_reversal,
                t.consecutive_sells_before,
                t.dip_1mo,
                t.dip_3mo,
                t.above_sma50,
                t.above_sma200,
                t.is_csuite,
                t.is_largest_ever,
                t.pit_n_trades,
                t.pit_win_rate_7d
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            {join_clause}
            WHERE {where_clause}
            ORDER BY t.filing_date DESC
        """

        rows = conn.execute(sql, where_params).fetchall()

        for r in rows:
            tid = r["trade_id"]
            ticker = r["ticker"]

            # Dedup: skip already-used trades
            if tid in used_trade_ids or tid in seen_trade_ids:
                continue
            # Dedup: skip tickers with open positions
            if ticker in held_tickers:
                continue

            # Compute PIT grade from insider_ticker_scores
            pit_row = conn.execute('''
                SELECT blended_score FROM insider_ticker_scores
                WHERE insider_id = (SELECT insider_id FROM trades WHERE trade_id = ?)
                  AND ticker = ? AND as_of_date <= ?
                ORDER BY as_of_date DESC LIMIT 1
            ''', (tid, ticker, r["filing_date"])).fetchone()

            from pipelines.insider_study.conviction_score import (
                compute_conviction, pit_score_to_grade, MIN_CONVICTION,
            )
            pit_grade = pit_score_to_grade(pit_row[0] if pit_row else None) or "C"

            # PIT filter: min_prior_10b5_1_sells (count 10b5-1 sells filed before this buy)
            min_10b5_1 = thesis.get("filters", {}).get("min_prior_10b5_1_sells")
            if min_10b5_1:
                insider_id_row = conn.execute(
                    "SELECT insider_id FROM trades WHERE trade_id = ?", (tid,)
                ).fetchone()
                if insider_id_row:
                    cnt_row = conn.execute("""
                        SELECT COUNT(*) FROM trades
                        WHERE insider_id = ? AND ticker = ?
                          AND trans_code = 'S' AND is_10b5_1 = 1
                          AND filing_date < ?
                    """, (insider_id_row[0], ticker, r["filing_date"])).fetchone()
                    if (cnt_row[0] or 0) < int(min_10b5_1):
                        continue
                else:
                    continue

            # Compute conviction with PIT grade + new features
            conv = compute_conviction(
                thesis=thesis_name,
                signal_grade=pit_grade,
                consecutive_sells=r["consecutive_sells_before"],
                dip_1mo=r["dip_1mo"],
                is_largest_ever=bool(r["is_largest_ever"]),
                above_sma50=bool(r["above_sma50"]),
                above_sma200=bool(r["above_sma200"]),
                insider_title=r["title"],
                is_csuite=bool(r["is_csuite"]),
            )

            # Skip below minimum conviction
            if conv < MIN_CONVICTION:
                continue

            seen_trade_ids.add(tid)
            all_candidates.append({
                "trade_id": tid,
                "ticker": ticker,
                "filing_date": r["filing_date"],
                "filed_at": r["filed_at"],
                "price": r["price"],
                "insider_name": r["insider_name"],
                "company": r["company"],
                "title": r["title"],
                "signal_quality": r["signal_quality"],
                "signal_grade": pit_grade,
                "conviction": conv,
                "is_rare_reversal": bool(r["is_rare_reversal"]),
                "consecutive_sells_before": r["consecutive_sells_before"],
                "dip_1mo": r["dip_1mo"],
                "pit_n": r["pit_n_trades"],
                "pit_wr": r["pit_win_rate_7d"],
                "thesis_name": thesis_name,
                "exit_config": thesis["exit"],
            })

    # Sort by conviction (highest first)
    all_candidates.sort(key=lambda c: c.get("conviction", 0), reverse=True)

    logger.info("scan_signals: %d candidates across %d theses", len(all_candidates), len(theses))
    return all_candidates


# ---------------------------------------------------------------------------
# Entry execution
# ---------------------------------------------------------------------------

def _get_latest_price(alpaca: PaperBackend, ticker: str) -> Optional[float]:
    """Fetch latest trade price from Alpaca data API."""
    try:
        data = alpaca._request("GET", "/../../v2/stocks/{}/trades/latest".format(ticker))
        return float(data.get("trade", {}).get("p", 0)) or None
    except Exception:
        pass
    # Fallback: use Alpaca data endpoint directly (shared read-only credentials)
    import requests as _req
    try:
        resp = _req.get(
            f"https://data.alpaca.markets/v2/stocks/{ticker}/trades/latest",
            headers=_data_api_headers(),
            timeout=10,
        )
        if resp.status_code == 200:
            return float(resp.json().get("trade", {}).get("p", 0)) or None
    except Exception as exc:
        logger.warning("Price fetch failed for %s: %s", ticker, exc)
    return None


def execute_entries(
    conn,
    alpaca: PaperBackend,
    candidates: list[dict],
    config: dict,
    dry_run: bool = False,
) -> list[dict]:
    """Submit market buy orders for qualifying candidates."""
    strategy_name = config["strategy_name"]
    max_concurrent = config["max_concurrent"]
    size_pct = config["position_size_pct"]
    prefix = config.get("telegram_prefix", "")

    n_open = conn.execute(
        "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
        (strategy_name,),
    ).fetchone()[0]
    slots = max_concurrent - n_open

    if slots <= 0:
        logger.info("Max concurrent (%d) reached, skipping entries", max_concurrent)
        return []

    # Circuit breaker: check drawdown
    equity = get_theoretical_equity(conn, config)
    dd_pct = 1.0 - (equity / config["starting_capital"])
    if dd_pct >= config.get("circuit_breaker_dd_pct", 0.10):
        logger.warning("Circuit breaker: drawdown %.1f%% exceeds limit, halting entries", dd_pct * 100)
        send_telegram(
            f"Circuit breaker tripped. DD={dd_pct*100:.1f}%, equity=${equity:,.0f}. Entries halted.",
            prefix,
        )
        return []

    portfolio_id = ensure_portfolio_row(conn, config)
    entered: list[dict] = []
    held_tickers = {
        r["ticker"]
        for r in conn.execute(
            "SELECT ticker FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
            (strategy_name,),
        ).fetchall()
    }

    logger.info("Equity: $%.2f | Open: %d | Slots: %d", equity, n_open, slots)

    soft_cap = config.get("soft_cap", max_concurrent)
    min_conv_above_soft = config.get("min_conviction_above_soft", 7.0)

    for c in candidates:
        if slots <= 0:
            break
        ticker = c["ticker"]
        if ticker in held_tickers:
            continue
        if not ticker or ticker in ("NONE", ""):
            continue

        # Soft/hard cap logic
        conv = c.get("conviction", 0)
        current_open = n_open + len(entered)
        min_conv_at_hard = config.get("min_conviction_at_hard", 7.5)
        replacement_adv = config.get("replacement_advantage", 1.5)

        if current_open >= max_concurrent:
            at_capacity_rule = config.get("at_capacity", "skip")
            if at_capacity_rule == "skip":
                continue

            # For replace_weakest: require higher conviction than existing
            if at_capacity_rule == "replace_weakest" and conv < min_conv_at_hard:
                continue

            if at_capacity_rule == "replace_oldest" and not dry_run:
                # Find oldest open position by entry_date
                open_rows = conn.execute('''
                    SELECT id, ticker, entry_date, entry_reasoning
                    FROM strategy_portfolio
                    WHERE strategy = ? AND status = 'open'
                    ORDER BY entry_date ASC
                    LIMIT 1
                ''', (strategy_name,)).fetchall()

                if not open_rows:
                    continue
                oldest_id = open_rows[0]["id"]
                oldest_ticker = open_rows[0]["ticker"]

            elif at_capacity_rule == "replace_weakest" and not dry_run:
                # Find weakest open position by conviction
                open_rows = conn.execute('''
                    SELECT id, ticker, entry_reasoning
                    FROM strategy_portfolio
                    WHERE strategy = ? AND status = 'open'
                    ORDER BY id
                ''', (strategy_name,)).fetchall()

                weakest_id = None
                weakest_conv = 999
                weakest_ticker = None
                for row in open_rows:
                    try:
                        import json as _json
                        r = _json.loads(row["entry_reasoning"] or "{}")
                        rc = r.get("conviction", 999)
                    except Exception:
                        rc = 999
                    if rc < weakest_conv:
                        weakest_conv = rc
                        weakest_id = row["id"]
                        weakest_ticker = row["ticker"]

                if at_capacity_rule == "replace_oldest":
                    # Close the oldest position at current price
                    try:
                        snap = alpaca._request("GET", f"/v2/stocks/{oldest_ticker}/snapshot")
                        close_price = snap.get("latestTrade", {}).get("p", 0)
                    except Exception:
                        close_price = 0

                    if close_price > 0:
                        old_row = conn.execute(
                            "SELECT entry_price, dollar_amount FROM strategy_portfolio WHERE id = ?",
                            (oldest_id,)).fetchone()
                        if old_row:
                            op = old_row["entry_price"]
                            pnl_pct = (close_price - op) / op if op > 0 else 0
                            pnl_dollar = (old_row["dollar_amount"] or 0) * pnl_pct
                            conn.execute('''
                                UPDATE strategy_portfolio
                                SET status = 'closed', exit_date = date('now'),
                                    exit_price = ?, pnl_pct = ?, pnl_dollar = ?,
                                    exit_reason = 'replaced_oldest'
                                WHERE id = ?
                            ''', (close_price, pnl_pct, pnl_dollar, oldest_id))
                            conn.commit()
                            alpaca.submit_order(oldest_ticker,
                                abs(int(old_row["dollar_amount"] / op)), "sell")
                            held_tickers.discard(oldest_ticker)
                            logger.info("REPLACED oldest %s → %s (conv=%.1f)",
                                        oldest_ticker, ticker, conv)
                            send_telegram(
                                f"Replaced oldest {oldest_ticker} → {ticker} (conv={conv:.1f})", prefix)
                        else:
                            continue
                    else:
                        continue

                elif weakest_id and conv >= weakest_conv + replacement_adv:
                    # Close the weakest position at current price
                    try:
                        snap = alpaca._request("GET", f"/v2/stocks/{weakest_ticker}/snapshot")
                        close_price = snap.get("latestTrade", {}).get("p", 0)
                    except Exception:
                        close_price = 0

                    if close_price > 0:
                        weak_row = conn.execute(
                            "SELECT entry_price, dollar_amount FROM strategy_portfolio WHERE id = ?",
                            (weakest_id,)).fetchone()
                        if weak_row:
                            wp = weak_row["entry_price"]
                            pnl_pct = (close_price - wp) / wp if wp > 0 else 0
                            pnl_dollar = (weak_row["dollar_amount"] or 0) * pnl_pct
                            conn.execute('''
                                UPDATE strategy_portfolio
                                SET status = 'closed', exit_date = date('now'),
                                    exit_price = ?, pnl_pct = ?, pnl_dollar = ?,
                                    exit_reason = 'replaced_by_higher_conviction'
                                WHERE id = ?
                            ''', (close_price, pnl_pct, pnl_dollar, weakest_id))
                            conn.commit()
                            alpaca.submit_order(weakest_ticker,
                                abs(int(weak_row["dollar_amount"] / wp)), "sell")
                            held_tickers.discard(weakest_ticker)
                            logger.info("REPLACED %s (conv=%.1f) with %s (conv=%.1f)",
                                        weakest_ticker, weakest_conv, ticker, conv)
                            send_telegram(
                                f"Replaced {weakest_ticker} (conv={weakest_conv:.1f}) → "
                                f"{ticker} (conv={conv:.1f})", prefix)
                        else:
                            continue
                    else:
                        continue
                else:
                    continue
            else:
                continue  # skip or dry run
        elif current_open >= soft_cap and conv < min_conv_above_soft:
            # Between soft and hard cap — only take high conviction
            continue

        dollar_amount = equity * size_pct
        exit_cfg = c["exit_config"]

        filed_at = c.get("filed_at", "")
        entry_type = "INTRADAY" if (_is_market_hours() and filed_at and filed_at[:10] == _now_et().strftime("%Y-%m-%d")) else "OPEN"
        logger.info(
            "ENTRY [%s] %s: thesis=%s conv=%.1f insider=%s filed=%s",
            entry_type, ticker, c["thesis_name"], c.get("conviction", 0), c["insider_name"], filed_at or c["filing_date"],
        )

        if dry_run:
            entered.append(c)
            held_tickers.add(ticker)
            slots -= 1
            continue

        # Get current price
        current_price = _get_latest_price(alpaca, ticker)
        if not current_price or current_price <= 0:
            logger.warning("No valid price for %s, skipping", ticker)
            continue

        qty = math.floor(dollar_amount / current_price)
        if qty <= 0:
            logger.warning("Qty 0 for %s ($%.0f / $%.2f), skip", ticker, dollar_amount, current_price)
            continue

        # Check Alpaca doesn't already hold this
        if alpaca.get_position(ticker) is not None:
            logger.info("Alpaca already holds %s, skip", ticker)
            held_tickers.add(ticker)
            continue

        # Submit market order
        try:
            result = alpaca.submit_order(
                symbol=ticker,
                qty=qty,
                side="buy",
                order_type="market",
                time_in_force="day",
            )
            if result.status == "pending":
                result = alpaca.wait_for_fill(result.order_id, timeout=10)

            if result.status != "filled":
                logger.warning("Order for %s not filled (status=%s), skipping", ticker, result.status)
                continue

            entry_price = result.filled_price or current_price
            logger.info("Filled %s: %d shares @ $%.2f", ticker, qty, entry_price)

        except Exception as exc:
            logger.error("Order failed for %s: %s", ticker, exc)
            continue

        # Build entry reasoning JSON
        reasoning = json.dumps({
            "thesis": c["thesis_name"],
            "exit_config": exit_cfg,
            "insider_name": c["insider_name"],
            "filing_date": c["filing_date"],
            "filed_at": c.get("filed_at"),
            "entry_type": entry_type,
            "signal_quality": c.get("signal_quality"),
            "signal_grade": c.get("signal_grade"),
            "conviction": c.get("conviction"),
            "is_rare_reversal": c.get("is_rare_reversal"),
            "consecutive_sells_before": c.get("consecutive_sells_before"),
            "dip_1mo": c.get("dip_1mo"),
            "pit_n": c.get("pit_n"),
            "pit_wr": c.get("pit_wr"),
        }, default=str)

        # Determine target_hold and stop_pct from exit config
        target_hold = exit_cfg.get("hold_days") or exit_cfg.get("max_hold_days") or 30
        stop_pct = exit_cfg.get("stop_loss_pct") or exit_cfg.get("stop_pct")
        if stop_pct is not None and stop_pct > 0:
            stop_pct = -stop_pct  # Normalise to negative

        today = _now_et().strftime("%Y-%m-%d")

        # Pre-compute deterministic exit date (trading days)
        from framework.data.calendar import MarketCalendar
        _cal = MarketCalendar()
        planned_exit = _cal.add_trading_days(today, target_hold).isoformat()

        conn.execute(
            """INSERT INTO strategy_portfolio (
                strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                entry_date, entry_price, target_hold, stop_pct,
                position_size, portfolio_value,
                insider_name, insider_pit_n, insider_pit_wr,
                signal_quality, status,
                execution_source, is_estimated, actual_fill_price,
                entry_reasoning,
                company, insider_title, filing_date, trade_date,
                signal_grade, is_rare_reversal,
                shares, dollar_amount, planned_exit_date
            ) VALUES (?, ?, ?, ?, 'buy_stock', 'long',
                      ?, ?, ?, ?,
                      ?, ?,
                      ?, ?, ?,
                      ?, 'open',
                      'paper', 0, ?,
                      ?,
                      ?, ?, ?, ?,
                      ?, ?,
                      ?, ?, ?)""",
            (
                strategy_name, portfolio_id,
                c["trade_id"], ticker,
                today, round(entry_price, 4), target_hold, stop_pct or -0.15,
                size_pct, equity,
                c["insider_name"], c.get("pit_n"), c.get("pit_wr"),
                c.get("signal_quality"),
                round(entry_price, 4),
                reasoning,
                c.get("company"), c.get("title"), c.get("filing_date"), c.get("filing_date"),
                c.get("signal_grade"), 1 if c.get("is_rare_reversal") else 0,
                qty, round(qty * entry_price, 2),
                planned_exit,
            ),
        )
        conn.commit()

        send_telegram(
            f"BUY *{ticker}* {qty} shares @ ${entry_price:.2f}\n"
            f"Thesis: {c['thesis_name']} | Insider: {c['insider_name']}\n"
            f"Size: ${qty * entry_price:,.0f} ({size_pct*100:.0f}% of ${equity:,.0f})\n"
            f"Exit: {planned_exit} ({target_hold} trading days)",
            prefix,
        )

        entered.append({**c, "entry_price": entry_price, "qty": qty})
        held_tickers.add(ticker)
        slots -= 1

    return entered


# ---------------------------------------------------------------------------
# Exit checking
# ---------------------------------------------------------------------------

# In-memory peak return tracking (persisted to state file)
_peak_returns: dict[int, float] = {}


def _load_peak_returns(state_path: Path) -> None:
    global _peak_returns
    if state_path.exists():
        try:
            data = json.loads(state_path.read_text())
            _peak_returns = {int(k): v for k, v in data.get("peak_returns", {}).items()}
        except Exception:
            _peak_returns = {}


def _save_peak_returns(state_path: Path) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict = {}
    if state_path.exists():
        try:
            existing = json.loads(state_path.read_text())
        except Exception:
            pass
    existing["peak_returns"] = {str(k): v for k, v in _peak_returns.items()}
    existing["updated_at"] = _now_et().isoformat()
    state_path.write_text(json.dumps(existing, indent=2))


def _compute_sma50(alpaca: PaperBackend, ticker: str) -> Optional[float]:
    """Fetch 55 daily bars from Alpaca and compute 50-day SMA of close."""
    try:
        data = alpaca._request(
            "GET",
            f"/../../v2/stocks/{ticker}/bars",
            params={"timeframe": "1Day", "limit": 55},
        )
    except Exception:
        # Fallback: direct request to data API (shared read-only credentials)
        import requests as _req
        try:
            resp = _req.get(
                f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                headers=_data_api_headers(),
                params={"timeframe": "1Day", "limit": 55},
                timeout=15,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
        except Exception:
            return None

    bars = data.get("bars", [])
    if len(bars) < 50:
        logger.warning("SMA50 for %s: only %d bars available", ticker, len(bars))
        return None

    closes = [b["c"] for b in bars[-50:]]
    return sum(closes) / len(closes)


def check_scheduled_exits(
    conn,
    alpaca: PaperBackend,
    config: dict,
    state_path: Path,
) -> list[dict]:
    """Close positions whose planned_exit_date is today or overdue.

    This replaces the time-exit path in check_exits. Runs once daily
    at the configured exit time (default 15:45 ET).
    """
    strategy_name = config["strategy_name"]
    prefix = config.get("telegram_prefix", "")
    today = _now_et().strftime("%Y-%m-%d")

    due_rows = conn.execute(
        """SELECT * FROM strategy_portfolio
           WHERE strategy = ? AND status = 'open'
             AND execution_source != 'backtest'
             AND planned_exit_date IS NOT NULL
             AND planned_exit_date <= ?
           ORDER BY planned_exit_date""",
        (strategy_name, today),
    ).fetchall()

    if not due_rows:
        return []

    closed: list[dict] = []
    for pos in due_rows:
        pos = dict(pos)
        ticker = pos["ticker"]
        entry_price = pos["entry_price"]
        pos_id = pos["id"]
        shares = pos.get("shares") or 0

        overdue_days = 0
        if pos["planned_exit_date"] < today:
            from datetime import date as _date
            overdue_days = (_date.fromisoformat(today) - _date.fromisoformat(pos["planned_exit_date"])).days
            logger.warning("OVERDUE %s by %dd (planned %s)", ticker, overdue_days, pos["planned_exit_date"])

        # Get current price and submit sell
        alpaca_pos = alpaca.get_position(ticker)
        if alpaca_pos is None:
            current_price = entry_price
            logger.warning("No Alpaca position for %s, using entry price", ticker)
        else:
            current_price = alpaca_pos["current_price"]

        fill_price = current_price
        if alpaca_pos is not None and shares > 0:
            try:
                order = alpaca.submit_order(
                    ticker, shares, "sell", order_type="market", time_in_force="day"
                )
                if order:
                    for _ in range(30):
                        import time as _time
                        _time.sleep(1)
                        status = alpaca.get_order(order["id"])
                        if status and status.get("filled_avg_price"):
                            fill_price = float(status["filled_avg_price"])
                            break
            except Exception as e:
                logger.error("Sell order failed for %s: %s", ticker, e)

        pnl_pct = (fill_price - entry_price) / entry_price if entry_price > 0 else 0.0
        pnl_dollar = (fill_price - entry_price) * shares
        hold_days = 0
        try:
            from datetime import datetime as _dt
            hold_days = (_dt.strptime(today, "%Y-%m-%d") - _dt.strptime(pos["entry_date"][:10], "%Y-%m-%d")).days
        except Exception:
            pass

        conn.execute(
            """UPDATE strategy_portfolio SET
                status = 'closed', exit_date = ?, exit_price = ?, exit_reason = ?,
                hold_days = ?, pnl_pct = ?, pnl_dollar = ?, peak_return = ?,
                actual_fill_price = ?, equity_after = ?
               WHERE id = ?""",
            (
                today, round(fill_price, 4), "time_exit",
                hold_days, round(pnl_pct, 6), round(pnl_dollar, 2),
                _peak_returns.get(pos_id, 0.0),
                round(fill_price, 4),
                get_theoretical_equity(conn, config),
                pos_id,
            ),
        )
        conn.commit()

        _peak_returns.pop(pos_id, None)
        _save_peak_returns(state_path)

        logger.info("EXIT %s: reason=time_exit pnl=%.2f%% hold=%dd price=$%.2f (planned=%s)",
                     ticker, pnl_pct * 100, hold_days, fill_price, pos["planned_exit_date"])
        send_telegram(
            f"SELL *{ticker}* @ ${fill_price:.2f} ({pnl_pct*100:+.1f}%)\n"
            f"Reason: scheduled exit (hold {hold_days}d)\n"
            f"P&L: ${pnl_dollar:+,.0f}",
            prefix,
        )
        closed.append(pos)

    return closed


def check_exits(
    conn,
    alpaca: PaperBackend,
    config: dict,
    state_path: Path,
) -> list[dict]:
    """Check all open positions for exit conditions."""
    strategy_name = config["strategy_name"]
    prefix = config.get("telegram_prefix", "")

    open_rows = conn.execute(
        "SELECT * FROM strategy_portfolio WHERE strategy = ? AND status = 'open' AND execution_source != 'backtest' ORDER BY entry_date",
        (strategy_name,),
    ).fetchall()

    today = _now_et().strftime("%Y-%m-%d")
    closed: list[dict] = []

    for pos in open_rows:
        pos = dict(pos)
        ticker = pos["ticker"]
        entry_price = pos["entry_price"]
        entry_date = pos["entry_date"]
        pos_id = pos["id"]
        shares = pos.get("shares") or 0

        # Parse exit config from entry_reasoning
        exit_cfg = _parse_exit_config(pos)

        # Get current price
        alpaca_pos = alpaca.get_position(ticker)
        if alpaca_pos is None:
            logger.warning("No Alpaca position for %s (id=%d), marking closed", ticker, pos_id)
            current_price = entry_price
            exit_reason = "missing_position"
            should_exit = True
        else:
            current_price = alpaca_pos["current_price"]
            exit_reason = None
            should_exit = False

        pnl_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

        # Hold days
        try:
            d_entry = datetime.strptime(entry_date[:10], "%Y-%m-%d")
            d_today = datetime.strptime(today[:10], "%Y-%m-%d")
            hold_days = (d_today - d_entry).days
        except Exception:
            hold_days = 0

        # Track peak return
        peak_return = max(_peak_returns.get(pos_id, 0.0), pnl_pct)

        # Apply exit strategy
        exit_strategy = exit_cfg.get("strategy", "fixed_hold")

        if not should_exit:
            if exit_strategy == "fixed_hold":
                target_hold = exit_cfg.get("hold_days", pos.get("target_hold", 7))
                stop_loss = exit_cfg.get("stop_loss_pct", pos.get("stop_pct", -0.15))
                if stop_loss is not None and pnl_pct <= stop_loss:
                    exit_reason = "stop_loss"
                    should_exit = True
                elif hold_days >= target_hold:
                    exit_reason = "time_exit"
                    should_exit = True

            elif exit_strategy == "trailing_stop":
                stop_pct = exit_cfg.get("stop_pct", 0.15)
                max_hold = exit_cfg.get("max_hold_days", 90)
                if peak_return > 0 and (peak_return - pnl_pct) >= stop_pct:
                    exit_reason = "trailing_stop"
                    should_exit = True
                elif hold_days >= max_hold:
                    exit_reason = "time_exit"
                    should_exit = True

            elif exit_strategy == "sma50_break":
                max_hold = exit_cfg.get("max_hold_days", 90)
                sma50 = _compute_sma50(alpaca, ticker)
                if sma50 is not None and current_price < sma50:
                    exit_reason = "sma50_break"
                    should_exit = True
                elif hold_days >= max_hold:
                    exit_reason = "time_exit"
                    should_exit = True

        if not should_exit:
            # Update peak return and continue
            _peak_returns[pos_id] = peak_return
            _save_peak_returns(state_path)
            continue

        # Execute sell
        logger.info(
            "EXIT %s: reason=%s pnl=%.2f%% hold=%dd price=$%.2f",
            ticker, exit_reason, pnl_pct * 100, hold_days, current_price,
        )

        if alpaca_pos is not None and shares > 0:
            try:
                result = alpaca.submit_order(
                    symbol=ticker,
                    qty=shares,
                    side="sell",
                    order_type="market",
                    time_in_force="day",
                )
                if result.status == "pending":
                    fill = alpaca.wait_for_fill(result.order_id, timeout=30)
                    if fill.status == "filled":
                        current_price = fill.filled_price or current_price
                elif result.status == "filled":
                    current_price = result.filled_price or current_price
                else:
                    logger.warning("Sell order for %s: status=%s", ticker, result.status)
            except Exception as exc:
                logger.error("Sell order failed for %s: %s", ticker, exc)

        # Compute final P&L
        pnl_pct_final = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0
        pnl_dollar = (pos.get("dollar_amount") or (shares * entry_price)) * pnl_pct_final

        # Update DB
        conn.execute(
            """UPDATE strategy_portfolio SET
                exit_date = ?,
                exit_price = ?,
                hold_days = ?,
                pnl_pct = ?,
                pnl_dollar = ?,
                stop_hit = ?,
                exit_reason = ?,
                status = 'closed',
                peak_return = ?,
                actual_fill_price = ?
            WHERE id = ?""",
            (
                today,
                round(current_price, 4),
                hold_days,
                round(pnl_pct_final, 6),
                round(pnl_dollar, 2),
                1 if exit_reason == "stop_loss" else 0,
                exit_reason,
                round(peak_return, 6),
                round(current_price, 4),
                pos_id,
            ),
        )
        conn.commit()

        # Clean up peak return tracking
        _peak_returns.pop(pos_id, None)
        _save_peak_returns(state_path)

        win_loss = "WIN" if pnl_pct_final >= 0 else "LOSS"
        send_telegram(
            f"SELL *{ticker}* [{win_loss}]\n"
            f"PnL: {pnl_pct_final*100:+.1f}% (${pnl_dollar:+,.0f})\n"
            f"Reason: {exit_reason} | Held: {hold_days}d",
            prefix,
        )

        closed.append({
            "ticker": ticker,
            "exit_reason": exit_reason,
            "pnl_pct": pnl_pct_final,
            "pnl_dollar": pnl_dollar,
            "hold_days": hold_days,
        })

    return closed


def _parse_exit_config(pos: dict) -> dict:
    """Extract exit config from entry_reasoning JSON, with fallbacks."""
    try:
        reasoning = json.loads(pos.get("entry_reasoning") or "{}")
        exit_cfg = reasoning.get("exit_config")
        if isinstance(exit_cfg, dict):
            return exit_cfg
    except (json.JSONDecodeError, TypeError):
        pass
    # Fallback: reconstruct from DB columns
    return {
        "strategy": "fixed_hold",
        "hold_days": pos.get("target_hold", 7),
        "stop_loss_pct": pos.get("stop_pct", -0.15),
    }


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _write_heartbeat(config: dict, status: str = "ok", detail: str = "", **extra: Any) -> None:
    hb_path = DATA_DIR / f"{config['strategy_name']}_heartbeat.json"
    beat = {
        "strategy": config["strategy_name"],
        "status": status,
        "timestamp": _now_et().isoformat(),
        "pid": os.getpid(),
        "detail": detail,
    }
    beat.update(extra)
    try:
        hb_path.parent.mkdir(parents=True, exist_ok=True)
        hb_path.write_text(json.dumps(beat, indent=2))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Daily cycle
# ---------------------------------------------------------------------------

def run_daily(config: dict, dry_run: bool = False) -> dict:
    """One daily cycle: scan -> check exits -> enter."""
    state_path = DATA_DIR / f"{config['strategy_name']}_state.json"
    _load_peak_returns(state_path)

    alpaca = None if dry_run else get_alpaca(config)
    conn = get_db(readonly=dry_run)

    try:
        if not dry_run:
            ensure_portfolio_row(conn, config)

        # 1. Scan for new signals
        candidates = scan_signals(conn, config)
        logger.info("Found %d candidates", len(candidates))
        for c in candidates:
            logger.info(
                "  %s: %s thesis=%s quality=%s insider=%s",
                c["ticker"], c["filing_date"], c["thesis_name"],
                c.get("signal_quality", "?"), c["insider_name"],
            )

        # 2. Check exits (frees up slots)
        closed: list[dict] = []
        if alpaca:
            closed = check_exits(conn, alpaca, config, state_path)
            if closed:
                logger.info("Closed %d positions", len(closed))
                for cl in closed:
                    logger.info(
                        "  %s: %s (%.1f%%, $%.0f, %dd)",
                        cl["ticker"], cl["exit_reason"],
                        cl["pnl_pct"] * 100, cl["pnl_dollar"], cl["hold_days"],
                    )

        # 3. Execute entries
        entered: list[dict] = []
        if candidates:
            if dry_run:
                entered = candidates
                logger.info("[DRY RUN] Would enter %d positions", len(entered))
            elif alpaca:
                entered = execute_entries(conn, alpaca, candidates, config)
                if entered:
                    logger.info("Opened %d new positions", len(entered))

        n_open = conn.execute(
            "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
            (config["strategy_name"],),
        ).fetchone()[0]

        return {
            "scanned": len(candidates),
            "entered": len(entered),
            "closed": len(closed),
            "open": n_open,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Daemon loop
# ---------------------------------------------------------------------------

def run_daemon(config: dict) -> None:
    """Main loop: pre-market scan, market-open entries, intraday exit checks."""
    strategy_name = config["strategy_name"]
    prefix = config.get("telegram_prefix", "")
    state_path = DATA_DIR / f"{strategy_name}_state.json"

    logger.info("CW Runner starting: %s", strategy_name)
    send_telegram(f"CW Runner starting: {strategy_name}", prefix)
    _load_peak_returns(state_path)

    ran_daily = False  # Track whether we already ran today's daily cycle
    ran_scheduled_exits = False  # Track whether scheduled exits ran today

    while True:
        now = _now_et()
        _write_heartbeat(config, "alive", f"loop at {now.strftime('%H:%M')}")

        # Weekend: sleep 1 hour
        if not _is_market_day():
            _write_heartbeat(config, "sleeping", "Weekend")
            logger.info("Weekend — sleeping 1h")
            time.sleep(3600)
            ran_daily = False
            ran_scheduled_exits = False
            continue

        # Pre-market (<9:25): sleep until 9:25
        if now.hour < 9 or (now.hour == 9 and now.minute < 25):
            target_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
            sleep_sec = max(60, (target_time - now).total_seconds())
            _write_heartbeat(config, "sleeping", f"Pre-market, {sleep_sec/60:.0f}m to scan")
            logger.info("Pre-market — sleeping %.0fm", sleep_sec / 60)
            time.sleep(min(sleep_sec, 1800))
            ran_daily = False
            ran_scheduled_exits = False
            continue

        # 9:25-9:30: scan (read-only preview)
        if now.hour == 9 and 25 <= now.minute < 31:
            logger.info("Pre-market scan window")
            try:
                conn = get_db(readonly=True)
                try:
                    candidates = scan_signals(conn, config)
                    logger.info("Pre-market: %d candidates queued", len(candidates))
                finally:
                    conn.close()
            except Exception as exc:
                logger.error("Pre-market scan failed: %s", exc)
            # Wait until 9:31
            target_open = now.replace(hour=9, minute=31, second=0, microsecond=0)
            wait = max(1, (target_open - _now_et()).total_seconds())
            time.sleep(wait)
            continue

        # 9:31-9:35: run daily cycle (entries) + close overdue positions
        if now.hour == 9 and 31 <= now.minute <= 35 and not ran_daily:
            try:
                result = run_daily(config)
                logger.info("Daily cycle complete: %s", result)
                ran_daily = True
            except Exception as exc:
                logger.error("Daily cycle failed: %s", exc)
                send_telegram(f"Daily cycle error: {exc}", prefix)
            # Close any overdue positions immediately at open
            try:
                alpaca = get_alpaca(config)
                conn = get_db(readonly=False)
                try:
                    closed = check_scheduled_exits(conn, alpaca, config, state_path)
                    if closed:
                        logger.info("Overdue exits at open: closed %d", len(closed))
                finally:
                    conn.close()
            except Exception as exc:
                logger.error("Overdue exit check failed: %s", exc)

        # Intraday: every 5 min during market hours
        if _is_market_hours():
            # 15:45: scheduled exits (deterministic time-based exits)
            if now.hour == 15 and 45 <= now.minute <= 50 and not ran_scheduled_exits:
                try:
                    alpaca = get_alpaca(config)
                    conn = get_db(readonly=False)
                    try:
                        closed = check_scheduled_exits(conn, alpaca, config, state_path)
                        if closed:
                            logger.info("Scheduled exits: closed %d positions", len(closed))
                        ran_scheduled_exits = True
                    finally:
                        conn.close()
                except Exception as exc:
                    logger.error("Scheduled exit failed: %s", exc)

            # Re-scan at :00 and :30
            if now.minute in (0, 30):
                try:
                    result = run_daily(config)
                    if result["entered"] > 0:
                        logger.info("Intraday entries: %s", result)
                except Exception as exc:
                    logger.error("Intraday re-scan failed: %s", exc)

            # Check stops only (not time exits) every 5 min
            elif now.minute not in (0, 30):
                try:
                    alpaca = get_alpaca(config)
                    conn = get_db(readonly=False)
                    try:
                        closed = check_exits(conn, alpaca, config, state_path)
                        if closed:
                            logger.info("Intraday: closed %d positions", len(closed))
                    finally:
                        conn.close()
                except Exception as exc:
                    logger.error("Intraday exit check failed: %s", exc)

            n_open = 0
            try:
                conn = get_db(readonly=True)
                n_open = conn.execute(
                    "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
                    (config["strategy_name"],),
                ).fetchone()[0]
                conn.close()
            except Exception:
                pass

            _write_heartbeat(config, "active", "Market hours", open_positions=n_open)
            time.sleep(300)  # 5 min
            continue

        # After 16:00: daily summary, then sleep
        if now.hour >= 16:
            try:
                conn = get_db(readonly=True)
                try:
                    n_open = conn.execute(
                        "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
                        (strategy_name,),
                    ).fetchone()[0]
                    today_str = now.strftime("%Y-%m-%d")
                    today_entries = conn.execute(
                        "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND entry_date = ?",
                        (strategy_name, today_str),
                    ).fetchone()[0]
                    today_exits = conn.execute(
                        "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND exit_date = ?",
                        (strategy_name, today_str),
                    ).fetchone()[0]
                    equity = get_theoretical_equity(conn, config)
                    logger.info(
                        "EOD: %d open, %d entries, %d exits, equity=$%.0f",
                        n_open, today_entries, today_exits, equity,
                    )
                    if today_entries > 0 or today_exits > 0:
                        send_telegram(
                            f"EOD: {n_open} open, +{today_entries} entries, -{today_exits} exits\n"
                            f"Equity: ${equity:,.0f}",
                            prefix,
                        )
                finally:
                    conn.close()
            except Exception as exc:
                logger.error("EOD summary failed: %s", exc)

            _write_heartbeat(config, "sleeping", "After hours")
            logger.info("After hours — sleeping until tomorrow 9:25")
            time.sleep(3600)
            ran_daily = False
            ran_scheduled_exits = False
            continue

        # Fallback
        time.sleep(60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CW Paper Trading Daemon")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--dry-run", action="store_true", help="Scan and log, no orders")
    parser.add_argument("--once", action="store_true", help="Run one daily cycle and exit")
    parser.add_argument(
        "--alpaca-env-prefix",
        default=None,
        help="Override the alpaca_env_prefix from the yaml config (for testing only)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    if args.alpaca_env_prefix:
        config["alpaca_env_prefix"] = args.alpaca_env_prefix
        logger.info("Overriding alpaca_env_prefix from CLI: %s", args.alpaca_env_prefix)
    logger.info("Loaded config: %s (%d theses)", config["strategy_name"], len(config.get("theses", [])))

    if args.once or args.dry_run:
        result = run_daily(config, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
    else:
        run_daemon(config)


if __name__ == "__main__":
    main()

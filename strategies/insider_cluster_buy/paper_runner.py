#!/usr/bin/env python3
"""
Insider Cluster Buy — Paper Trading Daemon

Single-process daemon that monitors EDGAR for Form 4 cluster buy signals,
manages paper trades via Alpaca, and sends Telegram alerts.

Schedule:
  Every 10 min: poll EDGAR RSS → parse Form 4 → check cluster logic → queue signals
  At 9:31 ET:   submit queued market-on-open orders via Alpaca
  Every 5 min (9:30-16:00 ET): check positions for -15% stop or T+7 exit
  At 16:05 ET:  close T+7 positions at market, send daily summary

Usage:
  python paper_runner.py start    # Run daemon (foreground)
  python paper_runner.py status   # Check if running
  python paper_runner.py stop     # Stop daemon
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from dotenv import load_dotenv

# Add framework root to sys.path for imports
STRATEGY_DIR = Path(__file__).resolve().parent
FRAMEWORK_ROOT = STRATEGY_DIR.parent.parent
sys.path.insert(0, str(FRAMEWORK_ROOT))
sys.path.insert(0, str(STRATEGY_DIR))

from framework.execution.paper import PaperBackend
from edgar_monitor import (
    poll_edgar_rss,
    fetch_form4_xml,
    parse_form4_xml,
    update_rolling_window,
    check_cluster_trigger,
)
from order_manager import (
    can_open_position,
    submit_entry,
    check_stop_loss,
    check_time_exit,
    close_trade,
    get_vix,
)
from options_leg import (
    submit_options_entry,
    check_options_exit,
    close_options_leg,
)
from state import load_state, save_state, compute_rolling_dd, add_trading_days

# Solo insider catalog (Phase 1)
CATALOG_DIR = STRATEGY_DIR.parent / "insider_catalog"
sys.path.insert(0, str(CATALOG_DIR))
try:
    from lookup import (
        check_solo_trigger, format_solo_signal, enrich_signal,
        pit_confidence_multiplier, MIN_PIT_SCORE_OPTIONS,
    )
    SOLO_AVAILABLE = True
except ImportError:
    SOLO_AVAILABLE = False

# ── Constants ────────────────────────────────────────────────────────────

STATE_FILE = STRATEGY_DIR / "state.json"
PID_FILE = STRATEGY_DIR / "runner.pid"
HEALTH_FILE = STRATEGY_DIR / "health.json"
LOG_DIR = STRATEGY_DIR / "logs"

# US Eastern timezone offset (simplified: no pytz dependency)
# ET = UTC-5 (EST) or UTC-4 (EDT). We detect via local time.
ET_OFFSET_HOURS = -5  # Adjusted at runtime if DST

logger = logging.getLogger("insider_paper")


# ── Telegram (sync, via requests) ────────────────────────────────────────

class TelegramSync:
    """Synchronous Telegram message sender using raw HTTP API."""

    def __init__(self, bot_token: str, chat_id: str):
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self.chat_id = chat_id

    def send(self, text: str) -> bool:
        try:
            resp = requests.post(self.url, json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "Markdown",
            }, timeout=10)
            if resp.status_code != 200:
                logger.warning("Telegram send failed: %d %s", resp.status_code, resp.text[:200])
                return False
            return True
        except Exception as e:
            logger.error("Telegram send error: %s", e)
            return False


# ── Time Helpers ─────────────────────────────────────────────────────────

def now_et() -> datetime:
    """Current time in US Eastern (approximate DST handling)."""
    utc_now = datetime.utcnow()
    # Simple DST check: second Sunday of March to first Sunday of November
    year = utc_now.year
    # March: second Sunday
    mar1 = datetime(year, 3, 1)
    dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)
    # November: first Sunday
    nov1 = datetime(year, 11, 1)
    dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)

    if dst_start <= utc_now.replace(tzinfo=None) < dst_end:
        return utc_now - timedelta(hours=4)  # EDT
    else:
        return utc_now - timedelta(hours=5)  # EST


def is_market_hours(now: datetime) -> bool:
    """True if now is between 9:30 and 16:00 ET on a weekday."""
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dt_time
    return dt_time(9, 30) <= t <= dt_time(16, 0)


def is_market_open_window(now: datetime) -> bool:
    """True if now is in the 9:31-9:35 ET window (for submitting MOO orders)."""
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dt_time
    return dt_time(9, 31) <= t <= dt_time(9, 35)


def is_market_close_window(now: datetime) -> bool:
    """True if now is in the 16:05-16:10 ET window (for EOD cleanup)."""
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dt_time
    return dt_time(16, 5) <= t <= dt_time(16, 10)


def is_weekday(now: datetime) -> bool:
    return now.weekday() < 5


# ── PID Management ───────────────────────────────────────────────────────

def write_pid():
    PID_FILE.write_text(str(os.getpid()))


def read_pid() -> int | None:
    if PID_FILE.exists():
        try:
            return int(PID_FILE.read_text().strip())
        except ValueError:
            pass
    return None


def cleanup_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()


def is_running() -> bool:
    pid = read_pid()
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


# ── Main Runner ──────────────────────────────────────────────────────────

class InsiderPaperRunner:
    def __init__(self, config_path: str):
        load_dotenv(config_path)

        # Config
        self.api_key = os.environ["ALPACA_API_KEY"]
        self.api_secret = os.environ["ALPACA_API_SECRET"]
        self.base_url = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
        self.portfolio_value = float(os.environ.get("PORTFOLIO_VALUE", "30000"))
        self.size_pct = float(os.environ.get("POSITION_SIZE_PCT", "0.05"))
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "3"))
        self.stop_loss_pct = float(os.environ.get("STOP_LOSS_PCT", "-0.15"))
        self.hold_days = int(os.environ.get("HOLD_DAYS", "7"))
        self.vix_threshold = float(os.environ.get("VIX_THRESHOLD", "30"))
        self.vix_reduced_pct = float(os.environ.get("VIX_REDUCED_SIZE_PCT", "0.03"))
        self.circuit_breaker_dd = float(os.environ.get("CIRCUIT_BREAKER_DD_PCT", "0.10"))
        self.edgar_interval = int(os.environ.get("EDGAR_POLL_INTERVAL_SEC", "600"))
        self.position_interval = int(os.environ.get("POSITION_CHECK_INTERVAL_SEC", "300"))
        self.user_agent = os.environ.get("EDGAR_USER_AGENT", "InsiderClusterBot contact@example.com")

        # Solo insider follow config
        self.solo_enabled = (
            SOLO_AVAILABLE
            and os.environ.get("SOLO_INSIDER_ENABLED", "false").lower() == "true"
        )
        self.solo_size_pct = float(os.environ.get("SOLO_POSITION_SIZE_PCT", "0.03"))
        self.solo_max_concurrent = int(os.environ.get("SOLO_MAX_CONCURRENT", "5"))
        self.solo_hold_days = int(os.environ.get("SOLO_HOLD_DAYS", "7"))
        self.solo_vix_reduced_pct = float(os.environ.get("SOLO_VIX_REDUCED_SIZE_PCT", "0.02"))

        # Options overlay config
        self.options_enabled = os.environ.get("OPTIONS_OVERLAY_ENABLED", "false").lower() == "true"
        self.options_strike_mult = float(os.environ.get("OPTIONS_STRIKE_MULT", "1.05"))
        self.options_target_dte = int(os.environ.get("OPTIONS_TARGET_DTE", "90"))
        self.options_hold_days = int(os.environ.get("OPTIONS_HOLD_DAYS", "14"))
        self.options_size_pct = float(os.environ.get("OPTIONS_SIZE_PCT", "0.01"))
        self.options_max_contracts = int(os.environ.get("OPTIONS_MAX_CONTRACTS", "2"))
        self.options_profit_target = float(os.environ.get("OPTIONS_PROFIT_TARGET", "0.50"))

        # Backends
        self.backend = PaperBackend(
            api_key=self.api_key,
            api_secret=self.api_secret,
            base_url=self.base_url,
        )
        # Reuse Alpaca session for data API calls
        self.data_session = requests.Session()
        self.data_session.headers.update({
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        })

        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        self.telegram = TelegramSync(tg_token, tg_chat) if tg_token and tg_chat else None

        # State
        self.state = load_state(str(STATE_FILE))
        self.running = True

        # Timing trackers
        self._last_edgar_poll = 0.0
        self._last_position_check = 0.0
        self._submitted_today = False
        self._closed_today = False
        self._start_time = time.monotonic()

    def run(self):
        """Main daemon loop."""
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        write_pid()

        # Validate Alpaca connection
        try:
            account = self.backend.get_account()
            equity = account["equity"]
            logger.info("Connected to Alpaca Paper — equity=$%.2f, cash=$%.2f",
                        equity, account["cash"])
            solo_status = "ON" if self.solo_enabled else "OFF"
            self._send_telegram(
                f"*Insider Cluster Buy — Started*\n"
                f"Equity: ${equity:,.2f}\n"
                f"Open positions: {len(self.state.get('open_positions', []))}\n"
                f"Queued signals: {len(self.state.get('queued_signals', []))}\n"
                f"Solo follow: {solo_status}"
            )
        except Exception as e:
            logger.error("Failed to connect to Alpaca: %s", e)
            self._send_telegram(f"STARTUP FAILED: {e}")
            cleanup_pid()
            sys.exit(1)

        logger.info("Daemon started — PID %d", os.getpid())

        # Main loop: 60-second tick
        while self.running:
            try:
                self._tick()
            except Exception:
                logger.exception("Unhandled error in main loop")
            time.sleep(60)

        # Graceful shutdown
        save_state(self.state, str(STATE_FILE))
        self._write_health()
        cleanup_pid()
        logger.info("Daemon stopped gracefully")

    def _tick(self):
        """One iteration of the main loop."""
        now = now_et()
        mono = time.monotonic()

        # Reset daily flags at midnight ET
        if now.hour == 0 and now.minute < 2:
            self._submitted_today = False
            self._closed_today = False

        # 1. EDGAR poll (every edgar_interval seconds, anytime)
        if mono - self._last_edgar_poll >= self.edgar_interval:
            self._poll_edgar()
            self._last_edgar_poll = mono

        # 2. Submit queued orders at market open (9:31-9:35 ET, once per day)
        if is_market_open_window(now) and not self._submitted_today:
            self._submit_queued_orders()
            self._submitted_today = True

        # 3. Position monitoring (every position_interval, during market hours)
        if is_market_hours(now) and mono - self._last_position_check >= self.position_interval:
            self._check_positions()
            self._last_position_check = mono

        # 4. EOD: close expired positions + daily summary (16:05-16:10 ET, once per day)
        if is_market_close_window(now) and not self._closed_today:
            self._close_expired_positions()
            self._send_daily_summary()
            self._closed_today = True

        # 5. Health heartbeat
        self._write_health()

    def _poll_edgar(self):
        """Poll EDGAR for new Form 4 filings and check for cluster signals."""
        logger.info("Polling EDGAR for Form 4 filings...")

        new_filings, latest_accession = poll_edgar_rss(
            last_seen_accession=self.state.get("last_seen_accession"),
            user_agent=self.user_agent,
        )

        if latest_accession:
            self.state["last_seen_accession"] = latest_accession
        self.state["last_edgar_check"] = datetime.utcnow().isoformat()

        if not new_filings:
            logger.debug("No new Form 4 filings")
            save_state(self.state, str(STATE_FILE))
            return

        # For each filing, fetch and parse the actual Form 4 XML
        all_trades = []
        for filing in new_filings:
            cik = filing.get("cik", "")
            accession = filing.get("accession", "")
            if not cik or not accession:
                continue

            xml_text = fetch_form4_xml(cik, accession, self.user_agent)
            if not xml_text:
                continue

            trades = parse_form4_xml(
                xml_text,
                cik=cik,
                filing_date=filing.get("filing_date", date.today().isoformat()),
                company=filing.get("company", ""),
            )
            all_trades.extend(trades)

        if all_trades:
            logger.info("Parsed %d purchase transactions from %d filings",
                        len(all_trades), len(new_filings))

            # Update rolling window
            update_rolling_window(all_trades, self.state["rolling_window"])

            # Check each ticker that had new filings for cluster triggers
            tickers_checked = set()
            for trade in all_trades:
                ticker = trade["ticker"]
                if ticker in tickers_checked:
                    continue
                tickers_checked.add(ticker)

                signal = check_cluster_trigger(ticker, self.state["rolling_window"])
                if signal:
                    # Enrich cluster signal with insider track records
                    if self.solo_enabled:
                        signal = enrich_signal(signal)
                    self._handle_signal(signal)

            # Solo insider check: every individual buy trade
            if self.solo_enabled:
                cluster_tickers = (
                    {s["ticker"] for s in self.state.get("queued_signals", [])}
                    | {p["ticker"] for p in self.state.get("open_positions", [])}
                )
                for trade in all_trades:
                    solo_signal = check_solo_trigger(trade)
                    if solo_signal:
                        self._handle_solo_signal(solo_signal, cluster_tickers)

        save_state(self.state, str(STATE_FILE))

    def _handle_signal(self, signal: dict):
        """Handle a new cluster buy signal."""
        ticker = signal["ticker"]

        # Check if we already have this ticker queued or open
        queued_tickers = {s["ticker"] for s in self.state.get("queued_signals", [])}
        open_tickers = {p["ticker"] for p in self.state.get("open_positions", [])}

        if ticker in queued_tickers:
            logger.info("Signal for %s already queued — skipping", ticker)
            return
        if ticker in open_tickers:
            logger.info("Signal for %s already has open position — skipping", ticker)
            return

        allowed, reason = can_open_position(
            self.state,
            max_concurrent=self.max_concurrent,
            circuit_breaker_dd_pct=self.circuit_breaker_dd,
            ticker=ticker,
        )

        if allowed:
            self.state["queued_signals"].append(signal)
            msg = (
                f"*SIGNAL DETECTED*: {ticker}\n"
                f"Company: {signal.get('company', 'N/A')}\n"
                f"Insiders: {signal.get('n_insiders', 0)} ({', '.join(signal.get('insiders', [])[:3])})\n"
                f"Total Value: ${signal.get('total_value', 0):,.0f}\n"
                f"Confidence: {signal.get('confidence', 0):.1f}\n"
                f"Quality: {signal.get('quality_score', 0):.2f}\n"
                f"_Queued for next market open_"
            )
            logger.info("Signal queued: %s", ticker)
            self._send_telegram(msg)
        else:
            self.state.setdefault("skipped_signals", []).append({
                **signal,
                "skip_reason": reason,
                "skip_date": date.today().isoformat(),
            })
            msg = (
                f"*SIGNAL SKIPPED*: {ticker}\n"
                f"Reason: {reason}\n"
                f"Insiders: {signal.get('n_insiders', 0)}, "
                f"Value: ${signal.get('total_value', 0):,.0f}"
            )
            logger.info("Signal skipped: %s — %s", ticker, reason)
            self._send_telegram(msg)

    def _handle_solo_signal(self, signal: dict, cluster_tickers: set):
        """Handle a solo insider follow signal."""
        ticker = signal["ticker"]

        # Overlap check: cluster buy takes priority
        if ticker in cluster_tickers:
            logger.info("Solo signal for %s skipped — cluster buy takes priority", ticker)
            return

        # Check if already queued or open as solo
        queued_solo = {s["ticker"] for s in self.state.get("queued_solo_signals", [])}
        open_solo = {p["ticker"] for p in self.state.get("open_solo_positions", [])}

        if ticker in queued_solo:
            logger.info("Solo signal for %s already queued — skipping", ticker)
            return
        if ticker in open_solo:
            logger.info("Solo signal for %s already has open position — skipping", ticker)
            return

        # Check capacity
        open_count = len(self.state.get("open_solo_positions", []))
        queued_count = len(self.state.get("queued_solo_signals", []))
        if open_count + queued_count >= self.solo_max_concurrent:
            logger.info("Solo signal for %s skipped — max concurrent (%d)", ticker, self.solo_max_concurrent)
            return

        # Circuit breaker (shared)
        if self.state.get("circuit_breaker_active", False):
            logger.info("Solo signal for %s skipped — circuit breaker active", ticker)
            return

        # Queue it
        signal["entry_date"] = date.today().isoformat()
        self.state["queued_solo_signals"].append(signal)

        msg = format_solo_signal(signal)
        logger.info("Solo signal queued: %s (insider: %s)", ticker, signal.get("insider_name"))
        self._send_telegram(msg)

    def _submit_queued_orders(self):
        """Submit queued signals as market orders at open."""
        queued = self.state.get("queued_signals", [])
        if not queued:
            return

        logger.info("Submitting %d queued orders at market open", len(queued))

        # Get current equity
        try:
            account = self.backend.get_account()
            equity = account["equity"]
        except Exception as e:
            logger.error("Failed to get account for order submission: %s", e)
            return

        submitted = []
        for signal in queued:
            today_str = date.today().isoformat()
            # Only submit signals queued before today (T+1 entry)
            entry_date = signal.get("entry_date", today_str)
            if entry_date > today_str:
                continue

            # PIT-based position sizing: scale by confidence multiplier
            pit_mult = signal.get("pit_confidence_mult", 1.0)
            if pit_mult <= 0:
                pit_mult = 0.6  # default for cluster signals (cluster is its own signal)
            adjusted_size_pct = self.size_pct * pit_mult
            adjusted_vix_pct = self.vix_reduced_pct * pit_mult

            trade = submit_entry(
                signal=signal,
                backend=self.backend,
                session=self.data_session,
                equity=equity,
                size_pct=adjusted_size_pct,
                vix_threshold=self.vix_threshold,
                reduced_size_pct=adjusted_vix_pct,
            )

            if trade:
                trade["pit_confidence_mult"] = pit_mult

                # Options overlay: only if best PIT score >= 1.5
                best_pit = signal.get("best_pit_score") or 0
                options_eligible = self.options_enabled and best_pit >= MIN_PIT_SCORE_OPTIONS
                if options_eligible:
                    opts_leg = submit_options_entry(
                        signal=signal,
                        backend=self.backend,
                        current_price=trade["entry_price"],
                        portfolio_value=equity,
                        strike_mult=self.options_strike_mult,
                        target_dte=self.options_target_dte,
                        hold_days=self.options_hold_days,
                        size_pct=self.options_size_pct,
                        max_contracts=self.options_max_contracts,
                        session=self.data_session,
                    )
                    if opts_leg:
                        trade["options_leg"] = opts_leg
                        logger.info("Options leg opened: %s", opts_leg["occ_symbol"])

                self.state["open_positions"].append(trade)
                submitted.append(signal["ticker"])

                msg = (
                    f"*ORDER FILLED*: {trade['ticker']}\n"
                    f"Shares: {trade['qty']} @ ${trade['entry_price']:.2f}\n"
                    f"Stop: ${trade['stop_price']:.2f} (-15%)\n"
                    f"Exit target: {trade['exit_date_target']}\n"
                    f"VIX at entry: {trade.get('vix_at_entry', 'N/A'):.1f}"
                )
                if trade.get("options_leg"):
                    ol = trade["options_leg"]
                    msg += (
                        f"\n*OPTIONS*: {ol['qty']}x {ol['occ_symbol']}"
                        f" @ ${ol['entry_price']:.2f}"
                    )
                self._send_telegram(msg)

        # Remove submitted signals from queue
        self.state["queued_signals"] = [
            s for s in queued
            if s["ticker"] not in submitted and s.get("entry_date", "") > date.today().isoformat()
        ]

        # Submit solo signals
        if self.solo_enabled:
            self._submit_solo_orders(equity)

        save_state(self.state, str(STATE_FILE))

    def _submit_solo_orders(self, equity: float):
        """Submit queued solo insider signals as market orders at open."""
        queued = self.state.get("queued_solo_signals", [])
        if not queued:
            return

        logger.info("Submitting %d queued solo orders at market open", len(queued))

        submitted = []
        today_str = date.today().isoformat()

        for signal in queued:
            entry_date = signal.get("entry_date", today_str)
            if entry_date > today_str:
                continue

            # Overlap: skip if cluster took this ticker since queuing
            cluster_tickers = {p["ticker"] for p in self.state.get("open_positions", [])}
            if signal["ticker"] in cluster_tickers:
                logger.info("Solo %s skipped at submit — cluster position exists", signal["ticker"])
                submitted.append(signal["ticker"])
                continue

            # PIT-based position sizing for solo signals
            pit_mult = signal.get("pit_confidence_mult", 0.6)
            adjusted_solo_pct = self.solo_size_pct * pit_mult
            adjusted_solo_vix = self.solo_vix_reduced_pct * pit_mult

            trade = submit_entry(
                signal=signal,
                backend=self.backend,
                session=self.data_session,
                equity=equity,
                size_pct=adjusted_solo_pct,
                vix_threshold=self.vix_threshold,
                reduced_size_pct=adjusted_solo_vix,
            )

            if trade:
                trade["strategy"] = "solo_insider"
                trade["insider_name"] = signal.get("insider_name", "")
                trade["insider_tier"] = signal.get("insider_tier", 0)
                trade["insider_score"] = signal.get("insider_score", 0)
                trade["pit_blended_score"] = signal.get("pit_blended_score")
                trade["pit_confidence_mult"] = pit_mult
                trade["exit_date_target"] = add_trading_days(
                    date.today(), self.solo_hold_days
                ).isoformat()
                self.state["open_solo_positions"].append(trade)
                submitted.append(signal["ticker"])

                msg = (
                    f"*SOLO ORDER FILLED*: {trade['ticker']}\n"
                    f"Insider: {signal.get('insider_name', '?')} "
                    f"({'*' * signal.get('insider_tier', 0)} Tier {signal.get('insider_tier', 0)})\n"
                    f"Shares: {trade['qty']} @ ${trade['entry_price']:.2f}\n"
                    f"Stop: ${trade['stop_price']:.2f} (-15%)\n"
                    f"Exit target: {trade['exit_date_target']}"
                )
                self._send_telegram(msg)

        self.state["queued_solo_signals"] = [
            s for s in queued
            if s["ticker"] not in submitted and s.get("entry_date", "") > today_str
        ]

    def _check_positions(self):
        """Check all open positions for stop-loss or time exit."""
        open_positions = self.state.get("open_positions", [])
        if not open_positions:
            return

        to_close = []
        for trade in open_positions:
            ticker = trade["ticker"]

            if check_stop_loss(trade, self.backend):
                closed = close_trade(trade, "stop_loss", self.backend, self.portfolio_value)
                to_close.append((trade, closed))
                msg = (
                    f"*STOP LOSS*: {ticker}\n"
                    f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.2%})\n"
                    f"Entry: ${trade['entry_price']:.2f} → Exit: ${closed['exit_price']:.2f}"
                )
                self._send_telegram(msg)

            elif check_time_exit(trade, self.backend):
                closed = close_trade(trade, "time_exit", self.backend, self.portfolio_value)
                to_close.append((trade, closed))
                msg = (
                    f"*TIME EXIT (T+7)*: {ticker}\n"
                    f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.2%})\n"
                    f"Entry: ${trade['entry_price']:.2f} → Exit: ${closed['exit_price']:.2f}"
                )
                self._send_telegram(msg)

        # Check options legs independently (they have longer hold period)
        if self.options_enabled:
            for trade in open_positions:
                ol = trade.get("options_leg")
                if ol and ol.get("status") == "open":
                    should_exit, reason = check_options_exit(
                        ol, self.backend, self.options_profit_target,
                    )
                    if should_exit:
                        ol["exit_reason"] = reason
                        trade["options_leg"] = close_options_leg(
                            ol, self.backend, self.portfolio_value,
                        )
                        perf = self.state["performance"]
                        perf["options_trades"] = perf.get("options_trades", 0) + 1
                        perf["options_pnl"] = perf.get("options_pnl", 0) + ol["pnl"]
                        if ol["pnl"] > 0:
                            perf["options_wins"] = perf.get("options_wins", 0) + 1
                        perf.setdefault("options_returns", []).append(ol["pnl_pct"])
                        msg = (
                            f"*OPTIONS EXIT*: {ol['occ_symbol']}\n"
                            f"Reason: {reason}\n"
                            f"P&L: ${ol['pnl']:+.2f} ({ol['pnl_pct']:+.1%})"
                        )
                        self._send_telegram(msg)

        # Update state
        for original, closed in to_close:
            self.state["open_positions"].remove(original)
            self.state["closed_trades"].append(closed)
            # Update performance
            perf = self.state["performance"]
            perf["trades"] += 1
            perf["total_pnl"] += closed["pnl"]
            if closed["pnl"] > 0:
                perf["wins"] += 1
            perf["returns"].append(closed.get("portfolio_return", closed["pnl_pct"]))
            perf["max_dd"] = max(perf["max_dd"], compute_rolling_dd(perf["returns"]))

        # Check solo positions
        if self.solo_enabled:
            solo_to_close = []
            for trade in self.state.get("open_solo_positions", []):
                ticker = trade["ticker"]
                if check_stop_loss(trade, self.backend):
                    closed = close_trade(trade, "stop_loss", self.backend, self.portfolio_value)
                    solo_to_close.append((trade, closed))
                    self._send_telegram(
                        f"*SOLO STOP LOSS*: {ticker}\n"
                        f"Insider: {trade.get('insider_name', '?')}\n"
                        f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.2%})"
                    )
                elif check_time_exit(trade, self.backend):
                    closed = close_trade(trade, "time_exit", self.backend, self.portfolio_value)
                    solo_to_close.append((trade, closed))
                    self._send_telegram(
                        f"*SOLO TIME EXIT (T+{self.solo_hold_days})*: {ticker}\n"
                        f"Insider: {trade.get('insider_name', '?')}\n"
                        f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.2%})"
                    )

            for original, closed in solo_to_close:
                self.state["open_solo_positions"].remove(original)
                self.state["closed_solo_trades"].append(closed)
                perf = self.state["solo_performance"]
                perf["trades"] += 1
                perf["total_pnl"] += closed["pnl"]
                if closed["pnl"] > 0:
                    perf["wins"] += 1
                perf["returns"].append(closed.get("portfolio_return", closed["pnl_pct"]))
                perf["max_dd"] = max(perf["max_dd"], compute_rolling_dd(perf["returns"]))

            if solo_to_close:
                to_close.extend(solo_to_close)  # ensure save triggers

        if to_close or self.options_enabled:
            save_state(self.state, str(STATE_FILE))

    def _close_expired_positions(self):
        """At EOD, force-close any positions past their T+7 target."""
        open_positions = self.state.get("open_positions", [])
        if not open_positions:
            return

        today_str = date.today().isoformat()
        to_close = []

        for trade in open_positions:
            exit_target = trade.get("exit_date_target", "")
            if exit_target and today_str >= exit_target:
                logger.info("EOD force-close: %s (target was %s)", trade["ticker"], exit_target)
                result = self.backend.close_position(trade["ticker"])
                if not result.is_error:
                    closed = close_trade(trade, "eod_time_exit", self.backend, self.portfolio_value)
                    to_close.append((trade, closed))

        for original, closed in to_close:
            # Close orphaned options leg if equity is closing
            if self.options_enabled:
                ol = closed.get("options_leg")
                if ol and ol.get("status") == "open":
                    ol["exit_reason"] = "equity_closed"
                    closed["options_leg"] = close_options_leg(
                        ol, self.backend, self.portfolio_value,
                    )
                    perf = self.state["performance"]
                    perf["options_trades"] = perf.get("options_trades", 0) + 1
                    perf["options_pnl"] = perf.get("options_pnl", 0) + ol["pnl"]
                    if ol["pnl"] > 0:
                        perf["options_wins"] = perf.get("options_wins", 0) + 1
                    perf.setdefault("options_returns", []).append(ol["pnl_pct"])

            self.state["open_positions"].remove(original)
            self.state["closed_trades"].append(closed)
            perf = self.state["performance"]
            perf["trades"] += 1
            perf["total_pnl"] += closed["pnl"]
            if closed["pnl"] > 0:
                perf["wins"] += 1
            perf["returns"].append(closed.get("portfolio_return", closed["pnl_pct"]))

        # EOD close expired solo positions
        if self.solo_enabled:
            solo_to_close = []
            for trade in self.state.get("open_solo_positions", []):
                exit_target = trade.get("exit_date_target", "")
                if exit_target and today_str >= exit_target:
                    logger.info("EOD solo force-close: %s (target was %s)", trade["ticker"], exit_target)
                    result = self.backend.close_position(trade["ticker"])
                    if not result.is_error:
                        closed = close_trade(trade, "eod_time_exit", self.backend, self.portfolio_value)
                        solo_to_close.append((trade, closed))

            for original, closed in solo_to_close:
                self.state["open_solo_positions"].remove(original)
                self.state["closed_solo_trades"].append(closed)
                perf = self.state["solo_performance"]
                perf["trades"] += 1
                perf["total_pnl"] += closed["pnl"]
                if closed["pnl"] > 0:
                    perf["wins"] += 1
                perf["returns"].append(closed.get("portfolio_return", closed["pnl_pct"]))

            if solo_to_close:
                to_close.extend(solo_to_close)

        if to_close:
            save_state(self.state, str(STATE_FILE))

    def _send_daily_summary(self):
        """Send end-of-day summary via Telegram."""
        perf = self.state.get("performance", {})
        open_count = len(self.state.get("open_positions", []))
        queued_count = len(self.state.get("queued_signals", []))
        total_trades = perf.get("trades", 0)
        wins = perf.get("wins", 0)
        wr = (wins / total_trades * 100) if total_trades > 0 else 0
        total_pnl = perf.get("total_pnl", 0)

        msg = (
            f"*Daily Summary — {date.today().isoformat()}*\n"
            f"Open positions: {open_count}\n"
            f"Queued signals: {queued_count}\n"
            f"Total trades: {total_trades}\n"
            f"Win rate: {wr:.0f}%\n"
            f"Total P&L: ${total_pnl:+,.2f}\n"
            f"Max DD: {perf.get('max_dd', 0):.1%}\n"
            f"Circuit breaker: {'ACTIVE' if self.state.get('circuit_breaker_active') else 'OFF'}"
        )
        if self.options_enabled:
            opt_trades = perf.get("options_trades", 0)
            opt_wins = perf.get("options_wins", 0)
            opt_wr = (opt_wins / opt_trades * 100) if opt_trades > 0 else 0
            opt_pnl = perf.get("options_pnl", 0)
            msg += (
                f"\n--- Options Overlay ---\n"
                f"Options trades: {opt_trades}\n"
                f"Options WR: {opt_wr:.0f}%\n"
                f"Options P&L: ${opt_pnl:+,.2f}"
            )
        if self.solo_enabled:
            sp = self.state.get("solo_performance", {})
            s_trades = sp.get("trades", 0)
            s_wins = sp.get("wins", 0)
            s_wr = (s_wins / s_trades * 100) if s_trades > 0 else 0
            s_pnl = sp.get("total_pnl", 0)
            s_open = len(self.state.get("open_solo_positions", []))
            s_queued = len(self.state.get("queued_solo_signals", []))
            msg += (
                f"\n--- Solo Insider Follow ---\n"
                f"Open: {s_open} | Queued: {s_queued}\n"
                f"Trades: {s_trades} | WR: {s_wr:.0f}%\n"
                f"P&L: ${s_pnl:+,.2f} | Max DD: {sp.get('max_dd', 0):.1%}"
            )
        self._send_telegram(msg)

    def _send_telegram(self, text: str):
        """Send Telegram message (best-effort, never crashes daemon)."""
        if self.telegram:
            try:
                self.telegram.send(text)
            except Exception as e:
                logger.error("Telegram send failed: %s", e)

    def _write_health(self):
        """Write health heartbeat file."""
        try:
            health = {
                "pid": os.getpid(),
                "uptime_sec": int(time.monotonic() - self._start_time),
                "last_heartbeat": datetime.utcnow().isoformat(),
                "open_positions": len(self.state.get("open_positions", [])),
                "queued_signals": len(self.state.get("queued_signals", [])),
                "total_trades": self.state.get("performance", {}).get("trades", 0),
                "total_pnl": self.state.get("performance", {}).get("total_pnl", 0),
                "circuit_breaker": self.state.get("circuit_breaker_active", False),
                "last_edgar_check": self.state.get("last_edgar_check"),
                "solo_enabled": self.solo_enabled,
                "solo_open": len(self.state.get("open_solo_positions", [])),
                "solo_queued": len(self.state.get("queued_solo_signals", [])),
                "solo_trades": self.state.get("solo_performance", {}).get("trades", 0),
                "solo_pnl": self.state.get("solo_performance", {}).get("total_pnl", 0),
            }
            HEALTH_FILE.write_text(json.dumps(health, indent=2))
        except Exception:
            pass

    def _shutdown(self, signum, frame):
        """Graceful shutdown handler."""
        logger.info("Received signal %d — shutting down", signum)
        self.running = False


# ── CLI ──────────────────────────────────────────────────────────────────

def setup_logging():
    """Configure rotating file + console logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "runner.log"

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler: 10MB x 5 backups
    fh = RotatingFileHandler(str(log_file), maxBytes=10_000_000, backupCount=5)
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(fh)
    root.addHandler(ch)


def cmd_start():
    if is_running():
        pid = read_pid()
        print(f"Already running (PID {pid})")
        sys.exit(1)

    setup_logging()
    config_path = str(STRATEGY_DIR / "config.env")
    if not os.path.exists(config_path):
        print(f"Config not found: {config_path}")
        sys.exit(1)

    runner = InsiderPaperRunner(config_path)
    runner.run()


def cmd_status():
    if is_running():
        pid = read_pid()
        # Read health file
        if HEALTH_FILE.exists():
            try:
                health = json.loads(HEALTH_FILE.read_text())
                print(f"Running (PID {pid})")
                print(f"  Uptime: {health.get('uptime_sec', 0) // 3600}h {(health.get('uptime_sec', 0) % 3600) // 60}m")
                print(f"  Open positions: {health.get('open_positions', 0)}")
                print(f"  Queued signals: {health.get('queued_signals', 0)}")
                print(f"  Total trades: {health.get('total_trades', 0)}")
                print(f"  Total P&L: ${health.get('total_pnl', 0):+,.2f}")
                print(f"  Circuit breaker: {'ACTIVE' if health.get('circuit_breaker') else 'OFF'}")
                print(f"  Last EDGAR check: {health.get('last_edgar_check', 'never')}")
            except Exception:
                print(f"Running (PID {pid}), health file unreadable")
        else:
            print(f"Running (PID {pid}), no health file yet")
    else:
        print("Not running")
        if PID_FILE.exists():
            cleanup_pid()


def cmd_stop():
    if not is_running():
        print("Not running")
        return

    pid = read_pid()
    print(f"Stopping PID {pid}...")
    try:
        os.kill(pid, signal.SIGTERM)
        # Wait up to 10s for graceful shutdown
        for _ in range(20):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except (OSError, ProcessLookupError):
                print("Stopped")
                cleanup_pid()
                return
        print("Force killing...")
        os.kill(pid, signal.SIGKILL)
        cleanup_pid()
    except (OSError, ProcessLookupError):
        print("Process already gone")
        cleanup_pid()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: paper_runner.py {start|status|stop}")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "start":
        cmd_start()
    elif cmd == "status":
        cmd_status()
    elif cmd == "stop":
        cmd_stop()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: paper_runner.py {start|status|stop}")
        sys.exit(1)

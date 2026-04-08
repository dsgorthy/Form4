#!/usr/bin/env python3
"""
Insider V3.4 — Paper Trading Daemon (Long + Short)

Monitors EDGAR for both buy and sell signals:
  BUY:  Tier 2+ insider, $2M+, quality >= 1.5 → buy shares
  SELL: 2+ DISCRETIONARY insiders selling $5M+ → shadow mode (log signals, track OOS)

V3.4 changes from V3.3:
  - Sell leg: filters out 10b5-1 planned sales (routine sells are noise)
  - Sell leg: shadow mode — logs all discretionary sell clusters for OOS validation
  - Sell leg: tracking both 7d/OTM and 60d/ITM configs for post-hoc comparison
  - Grid search on discretionary data shows ITM edge was from routine sells;
    best discretionary config is 5% OTM or 60d ITM (needs walk-forward validation)

Schedule:
  Every 10 min: poll EDGAR RSS → parse Form 4 → check triggers → queue signals
  At 9:31 ET:   submit queued orders (shares for buys, puts for sells)
  Every 5 min (9:30-16:00 ET): check positions for exits
  At 16:05 ET:  close expired positions, send daily summary

Usage:
  python paper_runner.py         # Run daemon (foreground)
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

# Add paths
STRATEGY_DIR = Path(__file__).resolve().parent
FRAMEWORK_ROOT = STRATEGY_DIR.parent.parent
sys.path.insert(0, str(FRAMEWORK_ROOT))
sys.path.insert(0, str(STRATEGY_DIR))
sys.path.insert(0, str(STRATEGY_DIR.parent / "insider_cluster_buy"))

from framework.execution.paper import PaperBackend
from edgar_monitor_v2 import (
    poll_edgar_rss,
    fetch_form4_xml,
    parse_form4_xml_v2,
    update_rolling_windows,
    check_buy_trigger,
    check_sell_trigger,
)
from put_leg import (
    submit_put_entry,
    check_put_exit,
    close_put_leg,
)
# Reuse V1's order manager for the buy (shares) leg
from order_manager import (
    can_open_position,
    submit_entry,
    check_stop_loss,
    check_time_exit,
    close_trade,
    get_vix,
)
from state import load_state, save_state, compute_rolling_dd

# ── Constants ────────────────────────────────────────────────────────────

STATE_FILE = STRATEGY_DIR / "state.json"
PID_FILE = STRATEGY_DIR / "runner.pid"
HEALTH_FILE = STRATEGY_DIR / "health.json"
LOG_DIR = STRATEGY_DIR / "logs"

logger = logging.getLogger("insider_v2")


# ── Time Helpers (copied from V1) ────────────────────────────────────────

def now_et() -> datetime:
    utc_now = datetime.utcnow()
    year = utc_now.year
    mar1 = datetime(year, 3, 1)
    dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)
    nov1 = datetime(year, 11, 1)
    dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    if dst_start <= utc_now.replace(tzinfo=None) < dst_end:
        return utc_now - timedelta(hours=4)
    else:
        return utc_now - timedelta(hours=5)


def is_market_hours(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    from datetime import time as dt_time
    return dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def is_market_open_window(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    from datetime import time as dt_time
    return dt_time(9, 31) <= now.time() <= dt_time(9, 35)


def is_market_close_window(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    from datetime import time as dt_time
    return dt_time(16, 5) <= now.time() <= dt_time(16, 10)


# ── Telegram ──────────────────────────────────────────────────────────────

class TelegramSync:
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
                logger.warning("Telegram send failed: %d", resp.status_code)
                return False
            return True
        except Exception as e:
            logger.error("Telegram error: %s", e)
            return False


# ── PID ───────────────────────────────────────────────────────────────────

def write_pid():
    PID_FILE.write_text(str(os.getpid()))

def cleanup_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()


# ── V2 Runner ─────────────────────────────────────────────────────────────

class InsiderV2Runner:
    def __init__(self, config_path: str):
        load_dotenv(config_path)

        # Alpaca
        self.api_key = os.environ["ALPACA_API_KEY"]
        self.api_secret = os.environ["ALPACA_API_SECRET"]
        self.base_url = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
        self.portfolio_value = float(os.environ.get("PORTFOLIO_VALUE", "30000"))

        # Buy leg config — V3.5 dual hold
        self.buy_size_pct = float(os.environ.get("BUY_POSITION_SIZE_PCT", "0.05"))
        self.buy_stop_loss_7d = float(os.environ.get("BUY_STOP_LOSS_7D", "-0.10"))
        self.buy_stop_loss_30d = float(os.environ.get("BUY_STOP_LOSS_30D", "-0.15"))
        self.buy_hold_days = int(os.environ.get("BUY_HOLD_DAYS", "7"))  # default, overridden per trade
        self.max_concurrent_longs = int(os.environ.get("MAX_CONCURRENT_LONGS", "3"))

        # Sell leg (ITM puts) config — V3.3
        self.put_size_pct = float(os.environ.get("PUT_SIZE_PCT", "0.005"))
        self.put_strike_mult = float(os.environ.get("PUT_STRIKE_MULT", "1.05"))
        self.put_min_dte = int(os.environ.get("PUT_MIN_DTE", "7"))
        self.put_max_dte = int(os.environ.get("PUT_MAX_DTE", "21"))
        self.put_hold_days = int(os.environ.get("PUT_HOLD_DAYS", "7"))
        self.put_stop_loss = float(os.environ.get("PUT_STOP_LOSS", "-0.25"))
        self.put_max_contracts = int(os.environ.get("PUT_MAX_CONTRACTS", "3"))
        self.put_spread_max = float(os.environ.get("PUT_SPREAD_MAX", "0.10"))
        self.put_min_oi = int(os.environ.get("PUT_MIN_OI", "100"))
        self.max_concurrent_puts = int(os.environ.get("MAX_CONCURRENT_PUTS", "3"))

        # Kill switch config (board-mandated)
        self.put_kill_trades = int(os.environ.get("PUT_KILL_SWITCH_TRADES", "50"))
        self.put_kill_min_sharpe = float(os.environ.get("PUT_KILL_SWITCH_MIN_SHARPE", "0.5"))

        # Risk
        self.vix_threshold = float(os.environ.get("VIX_THRESHOLD", "30"))
        self.vix_reduced_buy = float(os.environ.get("VIX_REDUCED_BUY_SIZE", "0.03"))
        self.vix_increased_put = float(os.environ.get("VIX_INCREASED_PUT_SIZE", "0.02"))
        self.circuit_breaker_dd = float(os.environ.get("CIRCUIT_BREAKER_DD_PCT", "0.08"))

        # Timing
        self.edgar_interval = int(os.environ.get("EDGAR_POLL_INTERVAL_SEC", "600"))
        self.position_interval = int(os.environ.get("POSITION_CHECK_INTERVAL_SEC", "300"))
        self.user_agent = os.environ.get("EDGAR_USER_AGENT", "InsiderV2Bot contact@example.com")

        # Backends
        self.backend = PaperBackend(
            api_key=self.api_key,
            api_secret=self.api_secret,
            base_url=self.base_url,
        )
        self.data_session = requests.Session()
        self.data_session.headers.update({
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        })

        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        self.telegram = TelegramSync(tg_token, tg_chat) if tg_token and tg_chat else None

        # State — V2 has separate buy/sell windows and position lists
        self.state = load_state(str(STATE_FILE))
        self._ensure_v2_state()
        self.running = True

        # Timing trackers
        self._last_edgar_poll = 0.0
        self._last_position_check = 0.0
        self._submitted_today = False
        self._closed_today = False
        self._start_time = time.monotonic()

    def _ensure_v2_state(self):
        """Ensure V2-specific state keys exist."""
        defaults = {
            "buy_rolling_window": {},
            "sell_rolling_window": {},
            "queued_buy_signals": [],
            "queued_sell_signals": [],
            "open_longs": [],
            "open_puts": [],
            "closed_longs": [],
            "closed_puts": [],
            "performance": {
                "long_trades": 0, "long_wins": 0, "long_pnl": 0.0, "long_returns": [],
                "put_trades": 0, "put_wins": 0, "put_pnl": 0.0, "put_returns": [],
                "max_dd": 0.0,
            },
            "last_seen_accession": None,
            "last_edgar_check": None,
            "circuit_breaker_active": False,
            "put_kill_switch_active": False,
        }
        for key, val in defaults.items():
            if key not in self.state:
                self.state[key] = val

    def _send_telegram(self, text: str):
        if self.telegram:
            self.telegram.send(text)

    def run(self):
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        write_pid()

        # Setup logging
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            LOG_DIR / "paper_runner.log", maxBytes=5_000_000, backupCount=3
        )
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s — %(message)s"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Validate Alpaca
        try:
            account = self.backend.get_account()
            equity = account["equity"]
            logger.info("Connected — equity=$%.2f", equity)
            self._send_telegram(
                f"*Insider V3.3 — Started*\n"
                f"Equity: ${equity:,.2f}\n"
                f"Open longs: {len(self.state['open_longs'])}\n"
                f"Open puts: {len(self.state['open_puts'])}\n"
                f"Queued buy: {len(self.state['queued_buy_signals'])}\n"
                f"Queued sell: {len(self.state['queued_sell_signals'])}"
            )
        except Exception as e:
            logger.error("Alpaca connection failed: %s", e)
            self._send_telegram(f"V2 STARTUP FAILED: {e}")
            cleanup_pid()
            sys.exit(1)

        logger.info("V3.3 Daemon started — PID %d", os.getpid())

        while self.running:
            try:
                self._tick()
            except Exception:
                logger.exception("Unhandled error in main loop")
            time.sleep(60)

        save_state(self.state, str(STATE_FILE))
        self._write_health()
        cleanup_pid()
        logger.info("V3.3 Daemon stopped")

    def _tick(self):
        now = now_et()
        mono = time.monotonic()

        if now.hour == 0 and now.minute < 2:
            self._submitted_today = False
            self._closed_today = False

        # 1. EDGAR poll
        if mono - self._last_edgar_poll >= self.edgar_interval:
            self._poll_edgar()
            self._last_edgar_poll = mono

        # 2. Submit queued orders at open
        if is_market_open_window(now) and not self._submitted_today:
            self._submit_queued_orders()
            self._submitted_today = True

        # 3. Position monitoring
        if is_market_hours(now) and mono - self._last_position_check >= self.position_interval:
            self._check_positions()
            self._last_position_check = mono

        # 4. EOD cleanup
        if is_market_close_window(now) and not self._closed_today:
            self._close_expired_positions()
            self._send_daily_summary()
            self._closed_today = True

        # 5. Health
        self._write_health()

    def _poll_edgar(self):
        """Poll EDGAR and check for both buy and sell triggers."""
        logger.info("Polling EDGAR...")

        new_filings, latest_accession = poll_edgar_rss(
            last_seen_accession=self.state.get("last_seen_accession"),
            user_agent=self.user_agent,
        )

        if latest_accession:
            self.state["last_seen_accession"] = latest_accession
        self.state["last_edgar_check"] = datetime.utcnow().isoformat()

        # Prune stale sell tickers (>30 days old)
        active_sells = self.state.get("active_sell_tickers", {})
        stale = [t for t, info in active_sells.items()
                 if (datetime.now() - datetime.fromisoformat(info["date"])).days > 30]
        for t in stale:
            del active_sells[t]

        if not new_filings:
            save_state(self.state, str(STATE_FILE))
            return

        # Parse ALL transactions (buys + sells)
        all_trades = []
        for filing in new_filings:
            cik = filing.get("cik", "")
            accession = filing.get("accession", "")
            if not cik or not accession:
                continue

            xml_text = fetch_form4_xml(cik, accession, self.user_agent)
            if not xml_text:
                continue

            trades = parse_form4_xml_v2(
                xml_text,
                cik=cik,
                filing_date=filing.get("filing_date", date.today().isoformat()),
                company=filing.get("company", ""),
            )
            all_trades.extend(trades)

        if all_trades:
            buys = [t for t in all_trades if t.get("direction") == "buy"]
            sells = [t for t in all_trades if t.get("direction") == "sell"]
            logger.info("Parsed %d buys + %d sells from %d filings",
                        len(buys), len(sells), len(new_filings))

            # Update separate rolling windows
            update_rolling_windows(
                all_trades,
                self.state["buy_rolling_window"],
                self.state["sell_rolling_window"],
            )

            # Check buy triggers
            tickers_checked = set()
            for trade in buys:
                ticker = trade["ticker"]
                if ticker in tickers_checked:
                    continue
                tickers_checked.add(ticker)
                signal = check_buy_trigger(ticker, self.state["buy_rolling_window"])
                if signal:
                    self._handle_buy_signal(signal)

            # Check sell triggers
            tickers_checked = set()
            for trade in sells:
                ticker = trade["ticker"]
                if ticker in tickers_checked:
                    continue
                tickers_checked.add(ticker)
                signal = check_sell_trigger(ticker, self.state["sell_rolling_window"])
                if signal:
                    self._handle_sell_signal(signal)

        save_state(self.state, str(STATE_FILE))

    def _handle_buy_signal(self, signal: dict):
        ticker = signal["ticker"]
        queued = {s["ticker"] for s in self.state["queued_buy_signals"]}
        open_tickers = {p["ticker"] for p in self.state["open_longs"]}

        if ticker in queued or ticker in open_tickers:
            return

        if len(self.state["open_longs"]) + len(self.state["queued_buy_signals"]) >= self.max_concurrent_longs:
            logger.info("Buy signal skipped %s: max concurrent longs", ticker)
            return

        if self.state.get("circuit_breaker_active"):
            logger.info("Buy signal skipped %s: circuit breaker", ticker)
            return

        # V3.5: Buy-avoidance — skip if discretionary sellers are active on this ticker
        active_sells = self.state.get("active_sell_tickers", {})
        if ticker in active_sells:
            sell_info = active_sells[ticker]
            # Only block if the sell signal is recent (within 30 days)
            try:
                sell_date = datetime.fromisoformat(sell_info["date"])
                if (datetime.now() - sell_date).days <= 30:
                    logger.info(
                        "Buy signal BLOCKED %s: active discretionary sell cluster "
                        "(%d insiders, $%.0f, high_conviction=%s)",
                        ticker, sell_info["n_insiders"], sell_info["total_value"],
                        sell_info.get("is_high_conviction", False),
                    )
                    self._send_telegram(
                        f"*BUY BLOCKED*: {ticker}\n"
                        f"Active sell cluster: {sell_info['n_insiders']} discretionary sellers, "
                        f"${sell_info['total_value']:,.0f}\n"
                        f"_Buy signal overridden by sell-side activity_"
                    )
                    return
            except (ValueError, KeyError):
                pass

        # V3.5: Determine hold period based on insider's best window
        hold_days = 7  # default
        stop_loss = self.buy_stop_loss_7d
        try:
            from config.database import get_connection
            _conn = get_connection(readonly=True)
            # Look up best_window for insiders in this signal
            for insider_name in signal.get("insiders", [])[:3]:
                row = _conn.execute("""
                    SELECT itr.best_window FROM insider_track_records itr
                    JOIN insiders i ON itr.insider_id = i.insider_id
                    WHERE i.name_normalized = ? OR COALESCE(i.display_name, i.name) = ?
                    LIMIT 1
                """, (insider_name.lower().strip(), insider_name)).fetchone()
                if row and row["best_window"] in ("30d", "90d"):
                    hold_days = 30
                    stop_loss = self.buy_stop_loss_30d
                    break
            _conn.close()
        except Exception:
            pass

        signal["hold_days"] = hold_days
        signal["stop_loss"] = stop_loss
        self.state["queued_buy_signals"].append(signal)
        hold_label = f"{hold_days}d" if hold_days > 7 else "7d"
        stop_label = f"{abs(stop_loss)*100:.0f}%"
        msg = (
            f"*BUY SIGNAL [{hold_label} hold]*: {ticker}\n"
            f"Insiders: {signal['n_insiders']} ({', '.join(signal['insiders'][:3])})\n"
            f"Value: ${signal['total_value']:,.0f}\n"
            f"Hold: {hold_label}, Stop: -{stop_label}\n"
            f"_Queued for next open_"
        )
        self._send_telegram(msg)

    def _handle_sell_signal(self, signal: dict):
        ticker = signal["ticker"]

        # V3.4: Shadow mode — log every discretionary sell signal for OOS validation
        self._log_shadow_signal(signal)

        queued = {s["ticker"] for s in self.state["queued_sell_signals"]}
        open_tickers = {p.get("ticker", "") for p in self.state["open_puts"]}

        if ticker in queued or ticker in open_tickers:
            return

        if self.state.get("put_kill_switch_active"):
            logger.info("Sell signal skipped %s: put kill switch active", ticker)
            return

        if len(self.state["open_puts"]) + len(self.state["queued_sell_signals"]) >= self.max_concurrent_puts:
            logger.info("Sell signal skipped %s: max concurrent puts", ticker)
            return

        self.state["queued_sell_signals"].append(signal)

        is_hc = signal.get("is_high_conviction", False)
        acc = signal.get("avg_sell_accuracy", 0)
        tier_label = "HIGH CONVICTION" if is_hc else "Standard"
        msg = (
            f"*SELL SIGNAL [{tier_label}]*: {ticker}\n"
            f"Sellers: {signal['n_insiders']} ({', '.join(signal['insiders'][:3])})\n"
            f"Value: ${signal['total_value']:,.0f}\n"
            f"Sell accuracy: {acc*100:.0f}%\n"
            f"{'C-suite: Yes' if signal.get('has_csuite') else ''}\n"
            f"_{'Queued for put entry' if is_hc else 'Shadow mode — tracking only'}_"
        )
        self._send_telegram(msg)

    def _log_shadow_signal(self, signal: dict):
        """Log sell signal for shadow mode validation (V3.5).
        Records every discretionary sell cluster with conviction tier."""
        shadow_entry = {
            "date": datetime.now().isoformat(),
            "ticker": signal["ticker"],
            "company": signal.get("company", ""),
            "n_insiders": signal["n_insiders"],
            "insiders": signal.get("insiders", []),
            "total_value": signal["total_value"],
            "confidence": signal.get("confidence", 0),
            "quality_score": signal.get("quality_score", 0),
            "avg_sell_accuracy": signal.get("avg_sell_accuracy", 0),
            "has_csuite": signal.get("has_csuite", False),
            "is_high_conviction": signal.get("is_high_conviction", False),
        }
        shadow_log = self.state.setdefault("shadow_sell_signals", [])
        shadow_log.append(shadow_entry)

        # Also track in the active sell tickers for buy-avoidance
        active_sells = self.state.setdefault("active_sell_tickers", {})
        active_sells[signal["ticker"]] = {
            "date": datetime.now().isoformat(),
            "n_insiders": signal["n_insiders"],
            "total_value": signal["total_value"],
            "is_high_conviction": signal.get("is_high_conviction", False),
        }

        tier = "HIGH CONVICTION" if signal.get("is_high_conviction") else "standard"
        logger.info("Shadow sell signal [%s]: %s ($%.0f, %d insiders, acc=%.0f%%)",
                    tier, signal["ticker"], signal["total_value"],
                    signal["n_insiders"], signal.get("avg_sell_accuracy", 0) * 100)

    def _submit_queued_orders(self):
        """Submit queued buy (shares) and sell (puts) orders."""
        try:
            account = self.backend.get_account()
            equity = account["equity"]
        except Exception as e:
            logger.error("Failed to get account: %s", e)
            return

        today_str = date.today().isoformat()

        # Submit buy orders (shares)
        buy_submitted = []
        for signal in self.state["queued_buy_signals"]:
            if signal.get("entry_date", today_str) > today_str:
                continue

            # Adjust size for VIX
            vix = get_vix(self.data_session)
            size = self.vix_reduced_buy if vix and vix > self.vix_threshold else self.buy_size_pct

            trade = submit_entry(
                signal=signal,
                backend=self.backend,
                session=self.data_session,
                equity=equity,
                size_pct=size,
                vix_threshold=self.vix_threshold,
                reduced_size_pct=self.vix_reduced_buy,
            )
            if trade:
                self.state["open_longs"].append(trade)
                buy_submitted.append(signal["ticker"])
                msg = (
                    f"*LONG FILLED*: {trade['ticker']}\n"
                    f"Shares: {trade['qty']} @ ${trade['entry_price']:.2f}\n"
                    f"Stop: ${trade['stop_price']:.2f}\n"
                    f"Exit target: {trade['exit_date_target']}"
                )
                self._send_telegram(msg)

        # Submit sell orders (puts)
        sell_submitted = []
        for signal in self.state["queued_sell_signals"]:
            if signal.get("entry_date", today_str) > today_str:
                continue

            # Get current stock price for put strike calculation
            ticker = signal["ticker"]
            try:
                quote_url = f"https://data.alpaca.markets/v2/stocks/{ticker}/quotes/latest"
                resp = self.data_session.get(quote_url, timeout=10)
                current_price = resp.json().get("quote", {}).get("ap", 0)
                if not current_price:
                    current_price = resp.json().get("quote", {}).get("bp", 0)
            except Exception as e:
                logger.warning("Failed to get quote for %s: %s", ticker, e)
                continue

            if not current_price or current_price <= 0:
                logger.warning("No valid price for %s, skipping put", ticker)
                continue

            # Adjust put size for VIX (increase in high vol)
            vix = get_vix(self.data_session)
            size = self.vix_increased_put if vix and vix > self.vix_threshold else self.put_size_pct

            put = submit_put_entry(
                signal=signal,
                backend=self.backend,
                current_price=current_price,
                portfolio_value=equity,
                strike_mult=self.put_strike_mult,
                min_dte=self.put_min_dte,
                max_dte=self.put_max_dte,
                hold_days=self.put_hold_days,
                size_pct=size,
                max_contracts=self.put_max_contracts,
                max_spread_pct=self.put_spread_max,
                min_oi=self.put_min_oi,
            )
            if put:
                put["ticker"] = ticker  # Ensure ticker is on the put record
                put["company"] = signal.get("company", ticker)
                self.state["open_puts"].append(put)
                sell_submitted.append(ticker)
                spread_info = f", spread={put.get('spread_at_entry', 0):.1%}" if put.get('spread_at_entry') else ""
                msg = (
                    f"*PUT FILLED*: {put['occ_symbol']}\n"
                    f"Ticker: {ticker}\n"
                    f"Contracts: {put['qty']} @ ${put['entry_price']:.2f}\n"
                    f"Strike: ${put['strike']:.2f}, Expiry: {put['expiry']}\n"
                    f"Stop: -25% premium{spread_info}"
                )
                self._send_telegram(msg)

        # Clean queues
        self.state["queued_buy_signals"] = [
            s for s in self.state["queued_buy_signals"]
            if s["ticker"] not in buy_submitted and s.get("entry_date", "") > today_str
        ]
        self.state["queued_sell_signals"] = [
            s for s in self.state["queued_sell_signals"]
            if s["ticker"] not in sell_submitted and s.get("entry_date", "") > today_str
        ]

        save_state(self.state, str(STATE_FILE))

    def _check_positions(self):
        """Check longs for stop/time exit, puts for profit/stop/time exit."""
        # Check longs
        to_close_longs = []
        for trade in self.state["open_longs"]:
            if check_stop_loss(trade, self.backend):
                closed = close_trade(trade, "stop_loss", self.backend, self.portfolio_value)
                to_close_longs.append((trade, closed))
                self._send_telegram(
                    f"*LONG STOP*: {trade['ticker']}\n"
                    f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.2%})"
                )
            elif check_time_exit(trade, self.backend):
                closed = close_trade(trade, "time_exit", self.backend, self.portfolio_value)
                to_close_longs.append((trade, closed))
                self._send_telegram(
                    f"*LONG EXIT (T+7)*: {trade['ticker']}\n"
                    f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.2%})"
                )

        for original, closed in to_close_longs:
            self.state["open_longs"].remove(original)
            self.state["closed_longs"].append(closed)
            perf = self.state["performance"]
            perf["long_trades"] += 1
            perf["long_pnl"] += closed["pnl"]
            if closed["pnl"] > 0:
                perf["long_wins"] += 1
            perf["long_returns"].append(closed.get("portfolio_return", closed["pnl_pct"]))

        # Check puts
        to_close_puts = []
        for put in self.state["open_puts"]:
            should_exit, reason = check_put_exit(
                put, self.backend,
                stop_loss=self.put_stop_loss,
            )
            if should_exit:
                put["exit_reason"] = reason
                closed = close_put_leg(put, self.backend, self.portfolio_value)
                to_close_puts.append((put, closed))
                self._send_telegram(
                    f"*PUT EXIT*: {put.get('occ_symbol', '?')}\n"
                    f"Reason: {reason}\n"
                    f"P&L: ${closed['pnl']:+.2f} ({closed['pnl_pct']:+.1%})"
                )

        for original, closed in to_close_puts:
            if original in self.state["open_puts"]:
                self.state["open_puts"].remove(original)
            self.state["closed_puts"].append(closed)
            perf = self.state["performance"]
            perf["put_trades"] += 1
            perf["put_pnl"] += closed["pnl"]
            if closed["pnl"] > 0:
                perf["put_wins"] += 1
            perf["put_returns"].append(closed["pnl_pct"])

        # Update max DD across all returns
        all_returns = self.state["performance"]["long_returns"] + self.state["performance"]["put_returns"]
        if all_returns:
            self.state["performance"]["max_dd"] = max(
                self.state["performance"]["max_dd"],
                compute_rolling_dd(all_returns),
            )

        # Kill switch: halt put leg if rolling Sharpe drops below threshold
        self._check_put_kill_switch()

        if to_close_longs or to_close_puts:
            save_state(self.state, str(STATE_FILE))

    def _check_put_kill_switch(self):
        """
        Board-mandated kill switch: halt put leg if rolling N-trade Sharpe < threshold.

        Checks rolling window of last put_kill_trades (default 50) put returns.
        If Sharpe drops below put_kill_min_sharpe (default 0.5), sets
        put_kill_switch_active=True and stops entering new puts.
        """
        put_returns = self.state["performance"].get("put_returns", [])
        if len(put_returns) < self.put_kill_trades:
            return

        # Rolling window of last N trades
        recent = put_returns[-self.put_kill_trades:]
        mean_ret = sum(recent) / len(recent)
        if len(recent) < 2:
            return
        variance = sum((r - mean_ret) ** 2 for r in recent) / (len(recent) - 1)
        std_ret = variance ** 0.5

        if std_ret > 0:
            rolling_sharpe = mean_ret / std_ret * (252 ** 0.5)  # Annualized
        else:
            rolling_sharpe = 0

        if rolling_sharpe < self.put_kill_min_sharpe:
            if not self.state.get("put_kill_switch_active"):
                self.state["put_kill_switch_active"] = True
                msg = (
                    f"*PUT KILL SWITCH TRIGGERED*\n"
                    f"Rolling {self.put_kill_trades}-trade Sharpe: {rolling_sharpe:.2f}\n"
                    f"Threshold: {self.put_kill_min_sharpe:.2f}\n"
                    f"Put leg HALTED — no new put entries"
                )
                logger.warning(msg.replace("*", ""))
                self._send_telegram(msg)
                save_state(self.state, str(STATE_FILE))

    def _close_expired_positions(self):
        """Force-close expired longs and puts at EOD."""
        today_str = date.today().isoformat()

        # Expired longs
        for trade in list(self.state["open_longs"]):
            if trade.get("exit_date_target", "") <= today_str:
                closed = close_trade(trade, "eod_time_exit", self.backend, self.portfolio_value)
                self.state["open_longs"].remove(trade)
                self.state["closed_longs"].append(closed)
                perf = self.state["performance"]
                perf["long_trades"] += 1
                perf["long_pnl"] += closed["pnl"]
                if closed["pnl"] > 0:
                    perf["long_wins"] += 1
                perf["long_returns"].append(closed.get("portfolio_return", closed["pnl_pct"]))

        # Expired puts
        for put in list(self.state["open_puts"]):
            if put.get("exit_date_target", "") <= today_str:
                put["exit_reason"] = "eod_time_exit"
                closed = close_put_leg(put, self.backend, self.portfolio_value)
                if put in self.state["open_puts"]:
                    self.state["open_puts"].remove(put)
                self.state["closed_puts"].append(closed)
                perf = self.state["performance"]
                perf["put_trades"] += 1
                perf["put_pnl"] += closed["pnl"]
                if closed["pnl"] > 0:
                    perf["put_wins"] += 1
                perf["put_returns"].append(closed["pnl_pct"])

        save_state(self.state, str(STATE_FILE))

    def _send_daily_summary(self):
        perf = self.state["performance"]
        lt = perf.get("long_trades", 0)
        lw = perf.get("long_wins", 0)
        pt = perf.get("put_trades", 0)
        pw = perf.get("put_wins", 0)

        msg = (
            f"*V3.3 Daily Summary — {date.today().isoformat()}*\n\n"
            f"*Longs:* {len(self.state['open_longs'])} open, "
            f"{len(self.state['queued_buy_signals'])} queued\n"
            f"  Trades: {lt}, WR: {lw/lt*100:.0f}%, P&L: ${perf.get('long_pnl',0):+,.2f}\n\n"
            if lt > 0 else
            f"*V3.3 Daily Summary — {date.today().isoformat()}*\n\n"
            f"*Longs:* {len(self.state['open_longs'])} open, "
            f"{len(self.state['queued_buy_signals'])} queued\n"
            f"  No trades yet\n\n"
        )
        if pt > 0:
            msg += (
                f"*Puts:* {len(self.state['open_puts'])} open, "
                f"{len(self.state['queued_sell_signals'])} queued\n"
                f"  Trades: {pt}, WR: {pw/pt*100:.0f}%, P&L: ${perf.get('put_pnl',0):+,.2f}\n\n"
            )
        else:
            msg += (
                f"*Puts:* {len(self.state['open_puts'])} open, "
                f"{len(self.state['queued_sell_signals'])} queued\n"
                f"  No trades yet\n\n"
            )

        total_pnl = perf.get("long_pnl", 0) + perf.get("put_pnl", 0)
        msg += f"*Combined P&L:* ${total_pnl:+,.2f}\nMax DD: {perf.get('max_dd',0):.1%}"
        self._send_telegram(msg)

    def _write_health(self):
        health = {
            "pid": os.getpid(),
            "uptime_sec": int(time.monotonic() - self._start_time),
            "last_heartbeat": datetime.utcnow().isoformat(),
            "open_longs": len(self.state.get("open_longs", [])),
            "open_puts": len(self.state.get("open_puts", [])),
            "queued_buy": len(self.state.get("queued_buy_signals", [])),
            "queued_sell": len(self.state.get("queued_sell_signals", [])),
            "total_long_trades": self.state["performance"].get("long_trades", 0),
            "total_put_trades": self.state["performance"].get("put_trades", 0),
            "total_pnl": (
                self.state["performance"].get("long_pnl", 0) +
                self.state["performance"].get("put_pnl", 0)
            ),
            "circuit_breaker": self.state.get("circuit_breaker_active", False),
            "put_kill_switch": self.state.get("put_kill_switch_active", False),
            "last_edgar_check": self.state.get("last_edgar_check"),
        }
        try:
            HEALTH_FILE.write_text(json.dumps(health, indent=2))
        except Exception:
            pass

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received")
        self.running = False


def main():
    config = str(STRATEGY_DIR / "config.env")
    if len(sys.argv) > 1:
        config = sys.argv[1]
    runner = InsiderV2Runner(config)
    runner.run()


if __name__ == "__main__":
    main()

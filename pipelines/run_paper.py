"""
Paper trading daemon for a single market day.

At entry_time:
  1. Load live bars → generate signal
  2. Submit paper order to Alpaca
  3. Monitor bar-by-bar until exit signal or time stop
  4. Submit exit order
  5. Send Telegram alerts at entry and exit

Usage:
    python pipelines/run_paper.py --strategy spy_0dte_reversal
    python pipelines/run_paper.py --strategy spy_0dte_reversal --dry-run
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pytz
import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.data.alpaca_client import AlpacaClient
from framework.data.storage import DataStorage
from framework.data.calendar import MarketCalendar
from framework.execution.paper import PaperBackend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

EASTERN = pytz.timezone("US/Eastern")


def get_env(key: str, required: bool = True) -> str:
    val = os.environ.get(key, "")
    if required and not val:
        logger.error("Missing required env var: %s", key)
        sys.exit(1)
    return val


def load_config(strategy_name: str) -> dict:
    config_path = Path(__file__).parent.parent / "strategies" / strategy_name / "config.yaml"
    if not config_path.exists():
        logger.error("No config.yaml at %s", config_path)
        sys.exit(1)
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_strategy(strategy_name: str, config: dict, storage: DataStorage):
    """Dynamically load and initialize the strategy."""
    module_path = f"strategies.{strategy_name}.strategy"
    module = importlib.import_module(module_path)
    strategy_class = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (isinstance(attr, type) and attr_name != "BaseStrategy"
                and hasattr(attr, "generate_signal")):
            strategy_class = attr
            break
    if strategy_class is None:
        logger.error("No strategy class in %s", module_path)
        sys.exit(1)
    strategy = strategy_class(config, storage=storage)
    return strategy


def wait_until(target_et: str, date_str: str) -> None:
    """Sleep until the given HH:MM ET time on date_str."""
    h, m = map(int, target_et.split(":"))
    target_dt = EASTERN.localize(
        datetime.strptime(f"{date_str} {h:02d}:{m:02d}:00", "%Y-%m-%d %H:%M:%S")
    )
    now = datetime.now(EASTERN)
    wait_secs = (target_dt - now).total_seconds()
    if wait_secs > 0:
        logger.info("Waiting %.0fs until %s ET...", wait_secs, target_et)
        time.sleep(wait_secs)


def fetch_and_store_bars(client: AlpacaClient, storage: DataStorage,
                          symbol: str, date_str: str) -> None:
    """Fetch today's 1-min bars from Alpaca and store them."""
    logger.info("Fetching %s bars for %s...", symbol, date_str)
    df = client.get_bars_df(symbol, date_str, date_str, timeframe="1Min")
    if df.empty:
        logger.warning("No bars returned for %s on %s", symbol, date_str)
        return
    storage.save_minute_bars(symbol, date_str, df)
    logger.info("Stored %d bars for %s", len(df), symbol)


def send_telegram(bot_token: str, chat_id: str, message: str) -> None:
    """Send a Telegram message (best-effort, never raises)."""
    if not bot_token or not chat_id:
        return
    try:
        from framework.alerts.telegram import TelegramAlerts
        alerts = TelegramAlerts(bot_token=bot_token, chat_id=chat_id)
        alerts.send_message(message)
    except Exception as exc:
        logger.warning("Telegram alert failed: %s", exc)


def main():
    parser = argparse.ArgumentParser(description="Paper trading daemon")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute signal but do not submit orders")
    parser.add_argument("--date", help="Override date (YYYY-MM-DD), default=today")
    args = parser.parse_args()

    # Load env
    api_key = get_env("ALPACA_API_KEY")
    api_secret = get_env("ALPACA_API_SECRET")
    bot_token = get_env("TELEGRAM_BOT_TOKEN", required=False)
    chat_id = get_env("TELEGRAM_CHAT_ID", required=False)

    # Load strategy
    config = load_config(args.strategy)
    storage = DataStorage()
    strategy = load_strategy(args.strategy, config, storage)

    # Determine trade date
    calendar = MarketCalendar()
    today = args.date or datetime.now(EASTERN).strftime("%Y-%m-%d")
    if not calendar.is_market_open(today):
        logger.info("Market is closed on %s — nothing to do.", today)
        sys.exit(0)

    logger.info("=== Paper Trading: %s on %s ===", args.strategy, today)

    # Alpaca clients
    data_client = AlpacaClient(api_key=api_key, api_secret=api_secret)
    execution = PaperBackend(api_key=api_key, api_secret=api_secret)

    # Get strategy config
    entry_time = config.get("entry", {}).get("time", "15:29")
    time_stop = config.get("exit", {}).get("time_stop", "15:55")
    symbols = config.get("data", {}).get("symbols", ["SPY", "VIXY"])

    # Wait until entry time
    wait_until(entry_time, today)

    # Fetch current bars for all symbols
    for symbol in symbols:
        fetch_and_store_bars(data_client, storage, symbol, today)

    # Load bars and generate signal
    from framework.data.loader import DataLoader
    loader = DataLoader(storage=storage)
    req = strategy.data_requirements()
    bars = loader.load_bars_for_date(today, req, up_to_time=entry_time)

    signal = strategy.generate_signal(bars, today)
    if not signal.is_valid():
        logger.info("No signal today: %s", signal.metadata.get("skip_reason", "no_signal"))
        send_telegram(bot_token, chat_id,
                      f"📊 <b>{args.strategy}</b> — No trade today ({today})\n"
                      f"Reason: {signal.metadata.get('skip_reason', 'no signal')}")
        sys.exit(0)

    # Select instrument
    instrument = strategy.select_instrument(signal, bars, today)
    if not instrument or not instrument.get("entry_price"):
        logger.warning("Instrument selection failed")
        sys.exit(1)

    entry_price = instrument["entry_price"]
    option_type = instrument.get("option_type", "?")
    strike = instrument.get("strike", 0)
    symbol_traded = instrument.get("symbol", "")

    logger.info("Signal: %s | %s @ $%.2f | Strike $%.0f",
                signal.direction.upper(), option_type.upper(), entry_price, strike)

    # Sizing
    account = execution.get_account()
    capital = account.get("equity", config.get("sizing", {}).get("starting_capital", 30000))
    size_pct = config.get("sizing", {}).get("position_size_pct", 3.0)
    max_alloc = capital * size_pct / 100.0
    cost_per = entry_price * 100
    num_contracts = max(1, int(max_alloc / cost_per))

    logger.info("Position: %d contracts | cost $%.2f | allocation %.1f%%",
                num_contracts, num_contracts * cost_per, size_pct)

    # Send entry alert
    send_telegram(bot_token, chat_id,
                  f"🔔 <b>TRADE ALERT — {today}</b>\n"
                  f"Strategy: {args.strategy}\n"
                  f"Signal: {signal.direction.upper()} {option_type.upper()} ${strike:.0f}\n"
                  f"Entry: ${entry_price:.2f} × {num_contracts} contracts = ${num_contracts * cost_per:,.0f}\n"
                  f"Direction: {signal.direction}")

    # Submit order (unless dry run)
    order_result = None
    if not args.dry_run:
        order_result = execution.submit_order(
            symbol=symbol_traded,
            qty=num_contracts,
            side="buy",
        )
        if order_result.is_error:
            logger.error("Order failed: %s", order_result.error)
            send_telegram(bot_token, chat_id, f"⚠️ Order failed: {order_result.error}")
            sys.exit(1)
        logger.info("Order submitted: %s", order_result.order_id)
    else:
        logger.info("[DRY RUN] Would buy %d contracts of %s", num_contracts, symbol_traded)

    # Position tracking dict
    position = {
        "entry_price": entry_price,
        "entry_time": entry_time,
        "direction": signal.direction,
        "instrument": instrument,
        "num_units": num_contracts,
        "multiplier": 100,
        "date": today,
    }

    # Bar-by-bar exit monitoring
    exit_reason = None
    exit_time_str = time_stop

    logger.info("Monitoring position until %s ET...", time_stop)

    while True:
        now = datetime.now(EASTERN)
        current_time = now.strftime("%H:%M")

        # Fetch latest bars
        for symbol in symbols:
            fetch_and_store_bars(data_client, storage, symbol, today)

        # Load current bars
        current_bars = loader.load_bars_for_date(today, req)

        # Check exit
        exit_reason = strategy.should_exit(position, current_bars)

        if exit_reason:
            exit_time_str = current_time
            logger.info("Exit triggered: %s at %s", exit_reason, exit_time_str)
            break

        # Time stop check
        h, m = map(int, time_stop.split(":"))
        if now.hour * 60 + now.minute >= h * 60 + m:
            exit_reason = "time_stop"
            exit_time_str = current_time
            logger.info("Time stop at %s", exit_time_str)
            break

        # Wait 1 minute
        time.sleep(60)

    # Submit exit order
    if not args.dry_run and order_result and order_result.is_filled:
        exit_order = execution.submit_order(
            symbol=symbol_traded,
            qty=num_contracts,
            side="sell",
        )
        logger.info("Exit order: %s status=%s", exit_order.order_id, exit_order.status)
        exit_price = exit_order.filled_price or entry_price
    else:
        exit_price = entry_price  # fallback for dry run
        logger.info("[DRY RUN] Would sell %d contracts of %s", num_contracts, symbol_traded)

    # Compute P&L
    pnl = (exit_price - entry_price) * 100 * num_contracts
    pnl_pct = pnl / (entry_price * 100 * num_contracts) * 100 if entry_price > 0 else 0

    logger.info("Exit: %s @ $%.2f | P&L $%.2f (%.1f%%)",
                exit_reason, exit_price, pnl, pnl_pct)

    # Send exit alert
    result_emoji = "✅" if pnl >= 0 else "❌"
    send_telegram(bot_token, chat_id,
                  f"📊 <b>Trade Result — {today}</b>\n"
                  f"{result_emoji} {exit_reason.upper()}: "
                  f"{'+' if pnl >= 0 else ''}{pnl:,.0f} ({pnl_pct:+.1f}%)\n"
                  f"Entry: ${entry_price:.2f} → Exit: ${exit_price:.2f}\n"
                  f"Contracts: {num_contracts}")


if __name__ == "__main__":
    main()

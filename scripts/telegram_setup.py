#!/usr/bin/env python3
"""Helper to fetch your Telegram chat_id after setting up a bot.

One-time use during initial Telegram alert setup. Steps:

    1. In Telegram: message @BotFather → /newbot → name + username
       → BotFather returns a bot token (looks like 123456:ABC...)
    2. Search for your new bot in Telegram, open it, tap /start
       (Telegram requires this so the bot is allowed to message you)
    3. Run this script with the bot token:
           python3 scripts/telegram_setup.py <BOT_TOKEN>
       It prints every chat the bot has received a message from. Find your
       own chat (will be labeled with your name) and copy the `chat_id`.
    4. Add to .env:
           TELEGRAM_ALERT_BOT_TOKEN=<BOT_TOKEN>
           TELEGRAM_ALERT_CHAT_ID=<CHAT_ID>
    5. Test:
           python3 -c "from framework.alerts.telegram import send_self_test; \\
               print('ok' if send_self_test() else 'FAILED')"
"""
from __future__ import annotations

import argparse
import json
import sys

import httpx


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("bot_token", help="Bot token from @BotFather")
    args = p.parse_args()

    url = f"https://api.telegram.org/bot{args.bot_token}/getUpdates"
    r = httpx.get(url, timeout=10.0)
    if r.status_code != 200:
        print(f"ERROR: HTTP {r.status_code} from Telegram", file=sys.stderr)
        print(r.text, file=sys.stderr)
        sys.exit(1)

    data = r.json()
    if not data.get("ok"):
        print(f"ERROR: Telegram returned ok=false: {data}", file=sys.stderr)
        sys.exit(1)

    updates = data.get("result") or []
    if not updates:
        print("No messages found. Make sure you tapped /start on your bot first.",
              file=sys.stderr)
        sys.exit(1)

    # Dedup by chat — print one row per unique chat
    seen: dict[int, dict] = {}
    for upd in updates:
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat = msg.get("chat") or {}
        cid = chat.get("id")
        if cid and cid not in seen:
            seen[cid] = chat

    print(f"\nFound {len(seen)} chat(s) that have messaged this bot:\n")
    print(f"{'chat_id':>14}  type        from")
    print(f"{'-' * 14}  ----------  ----")
    for cid, chat in seen.items():
        kind = chat.get("type", "?")
        name = (chat.get("first_name") or chat.get("title") or "—")
        username = chat.get("username")
        if username:
            name += f" (@{username})"
        print(f"{cid:>14}  {kind:<10}  {name}")
    print("\nAdd the chat_id you want to alert to your .env:")
    print(f"  TELEGRAM_ALERT_BOT_TOKEN={args.bot_token}")
    print(f"  TELEGRAM_ALERT_CHAT_ID=<chat_id from above>")


if __name__ == "__main__":
    main()

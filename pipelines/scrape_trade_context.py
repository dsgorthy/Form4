#!/usr/bin/env python3
"""Scrape web context for insider trades — company news, insider background, recent events.

Returns structured context + source URLs for each trade.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def scrape_trade_context(ticker: str, company: str, insider_name: str, trade_type: str, value: float) -> dict:
    """Scrape web for context about a trade. Returns {context: str, sources: list[dict]}."""
    import requests

    context_parts = []
    sources = []

    headers = {"User-Agent": "Form4App/1.0 (derek@sidequestgroup.com)"}

    # 1. Yahoo Finance — recent news headlines
    try:
        resp = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"range": "5d", "interval": "1d"},
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice")
            prev_close = meta.get("previousClose")
            if price and prev_close:
                day_change = ((price - prev_close) / prev_close) * 100
                if abs(day_change) > 2:
                    context_parts.append(f"The stock moved {day_change:+.1f}% today")
                    sources.append({"type": "price", "source": "Yahoo Finance", "url": f"https://finance.yahoo.com/quote/{ticker}"})
    except Exception as exc:
        logger.debug("Yahoo Finance failed for %s: %s", ticker, exc)

    # 2. Yahoo Finance news headlines
    try:
        resp = requests.get(
            f"https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": ticker, "newsCount": 3, "quotesCount": 0},
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            news = resp.json().get("news", [])
            recent_headlines = []
            for n in news[:5]:
                title = n.get("title", "")
                link = n.get("link", "")
                if not title:
                    continue
                # Skip generic aggregator headlines and SEC filing restatements
                skip_words = ["vickers", "top buyers", "top sellers", "top insider picks",
                              "daily –", "according to a recent sec filing", "insider sold shares worth",
                              "insider bought shares worth"]
                if any(sw in title.lower() for sw in skip_words):
                    continue
                recent_headlines.append(title)
                sources.append({"type": "news", "source": "Yahoo Finance", "title": title, "url": link})
            if recent_headlines:
                # Clean headline for TTS — no quotes, no special chars
                headline = recent_headlines[0].replace('"', '').replace("'", "").strip()
                if len(headline) > 80:
                    headline = headline[:77] + "..."
                context_parts.append(f"In recent news, {headline}")
    except Exception as exc:
        logger.debug("Yahoo news failed for %s: %s", ticker, exc)

    time.sleep(0.3)

    # 3. Finviz — company snapshot (market cap, sector, recent performance)
    try:
        resp = requests.get(
            f"https://finviz.com/quote.ashx?t={ticker}",
            headers={**headers, "User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        if resp.status_code == 200:
            html = resp.text
            # Extract market cap
            import re
            mc_match = re.search(r'Market Cap</td[^>]*>.*?<b>([^<]+)</b>', html, re.DOTALL)
            if mc_match:
                mc = mc_match.group(1).strip()
                # Convert to clean spoken format — round to avoid "48.20 billion"
                mc_spoken = mc
                try:
                    if mc.endswith("B"):
                        val = float(mc[:-1])
                        if val == int(val):
                            mc_spoken = f"a {int(val)} billion dollar company"
                        elif val >= 10:
                            mc_spoken = f"a {val:.0f} billion dollar company"
                        else:
                            mc_spoken = f"a {val:.1f} billion dollar company"
                    elif mc.endswith("M"):
                        val = float(mc[:-1])
                        if val == int(val):
                            mc_spoken = f"a {int(val)} million dollar company"
                        else:
                            mc_spoken = f"a {val:.0f} million dollar company"
                except ValueError:
                    mc_spoken = ""
                if mc_spoken:
                    context_parts.append(mc_spoken)
                sources.append({"type": "fundamentals", "source": "Finviz", "url": f"https://finviz.com/quote.ashx?t={ticker}"})

            # Extract analyst recommendation
            rec_match = re.search(r'Recom</td[^>]*>.*?<b>([^<]+)</b>', html, re.DOTALL)
            if rec_match:
                rec = rec_match.group(1).strip()
                try:
                    rec_val = float(rec)
                    if rec_val <= 1.5:
                        context_parts.append("Analysts rate this a Strong Buy")
                    elif rec_val <= 2.5:
                        context_parts.append("Analysts rate this a Buy")
                    elif rec_val >= 4.0:
                        context_parts.append("Analysts rate this a Sell")
                except ValueError:
                    pass

            # Extract earnings date
            earn_match = re.search(r'Earnings</td[^>]*>.*?<b>([^<]+)</b>', html, re.DOTALL)
            if earn_match:
                earn = earn_match.group(1).strip()
                if earn and earn != "-":
                    context_parts.append(f"Next earnings: {earn}")
    except Exception as exc:
        logger.debug("Finviz failed for %s: %s", ticker, exc)

    time.sleep(0.3)

    # 4. Build a natural spoken context from raw data using templates
    spoken_context = _build_natural_context(
        trade_type=trade_type,
        context_parts=context_parts,
        sources=sources,
    )

    return {
        "ticker": ticker,
        "company": company,
        "insider_name": insider_name,
        "trade_type": trade_type,
        "context_parts": context_parts,
        "spoken_context": spoken_context,
        "sources": sources,
    }


def _build_natural_context(
    trade_type: str,
    context_parts: list[str],
    sources: list[dict],
) -> str:
    """Build natural spoken context from raw data using smart templates."""
    if not context_parts:
        return ""

    is_buy = trade_type == "buy"
    parts = []

    # Extract structured data from context_parts
    mcap = ""
    analyst = ""
    earnings = ""
    news = ""
    for p in context_parts:
        if "billion dollar" in p or "million dollar" in p:
            mcap = p
        elif "Analysts rate" in p:
            analyst = p.replace("Analysts rate this ", "").lower()
        elif "Next earnings" in p:
            earnings = p.replace("Next earnings: ", "")
        elif "In recent news" in p:
            # Only use if it seems relevant (mentions earnings, revenue, acquisition, etc)
            relevant_keywords = ["earn", "revenue", "profit", "acqui", "merger", "layoff",
                                 "guidance", "beat", "miss", "upgrade", "downgrade", "FDA", "approval"]
            if any(kw in p.lower() for kw in relevant_keywords):
                news = p

    # Build natural sentences based on what we have
    if mcap:
        parts.append(f"It's {mcap}")

    if analyst:
        if is_buy and "sell" in analyst:
            parts.append("even though analysts rate it a sell")
        elif not is_buy and "buy" in analyst:
            parts.append("despite analysts still rating it a buy")
        elif not is_buy and "strong buy" in analyst:
            parts.append("while analysts still have it rated as a strong buy")

    if earnings:
        parts.append(f"with earnings coming up on {earnings}")

    if news:
        parts.append(news)

    if not parts:
        return ""

    return ". ".join(parts[:2]).capitalize() + "."


def scrape_all_trades(trades: list[dict]) -> list[dict]:
    """Scrape context for all trades. Returns list of context dicts."""
    results = []
    for t in trades:
        logger.info("Scraping context for %s (%s)", t["ticker"], t.get("company", ""))
        ctx = scrape_trade_context(
            ticker=t["ticker"],
            company=t.get("company", ""),
            insider_name=t.get("insider_name", ""),
            trade_type=t.get("trade_type", "buy"),
            value=t.get("total_value", 0),
        )
        results.append(ctx)
    return results

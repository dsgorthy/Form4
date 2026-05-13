"""News fetchers: Finnhub (per-ticker) + NewsAPI (macro narratives).

Both are best-effort. If keys are missing or APIs error, return [] and
the monitor still runs. Keys (in .env):
  FINNHUB_API_KEY  — https://finnhub.io/dashboard
  NEWSAPI_KEY      — https://newsapi.org/account
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

log = logging.getLogger(__name__)


@dataclass
class NewsItem:
    source: str
    headline: str
    url: str
    published: str
    summary: str = ""

    def line(self) -> str:
        return f"- {self.headline} ({self.source}, {self.published})"


# ─── Finnhub: per-ticker company news ──────────────────────────────────

def finnhub_company_news(ticker: str, *, days: int = 1, limit: int = 3) -> list[NewsItem]:
    key = os.getenv("FINNHUB_API_KEY", "")
    if not key:
        return []
    now = datetime.now(timezone.utc).date()
    start = now - timedelta(days=days)
    params = {"symbol": ticker, "from": start.isoformat(), "to": now.isoformat(), "token": key}
    try:
        r = httpx.get("https://finnhub.io/api/v1/company-news", params=params, timeout=12)
        r.raise_for_status()
        rows = r.json() or []
    except httpx.HTTPError as exc:
        log.warning("Finnhub %s: %s", ticker, exc)
        return []
    items: list[NewsItem] = []
    for row in rows[:limit]:
        items.append(
            NewsItem(
                source=row.get("source") or "Finnhub",
                headline=row.get("headline") or "",
                url=row.get("url") or "",
                published=datetime.fromtimestamp(row.get("datetime", 0), tz=timezone.utc).strftime("%H:%M UTC"),
                summary=(row.get("summary") or "")[:200],
            )
        )
    return items


# ─── NewsAPI: macro narrative searches ─────────────────────────────────

def newsapi_search(query: str, *, hours: int = 24, limit: int = 4) -> list[NewsItem]:
    key = os.getenv("NEWSAPI_KEY", "")
    if not key:
        return []
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")
    params = {
        "q": query,
        "from": since,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": limit,
        "apiKey": key,
    }
    try:
        r = httpx.get("https://newsapi.org/v2/everything", params=params, timeout=12)
        r.raise_for_status()
        articles = r.json().get("articles", [])
    except httpx.HTTPError as exc:
        log.warning("NewsAPI %s: %s", query, exc)
        return []
    items: list[NewsItem] = []
    for a in articles[:limit]:
        items.append(
            NewsItem(
                source=(a.get("source") or {}).get("name", "NewsAPI"),
                headline=a.get("title") or "",
                url=a.get("url") or "",
                published=(a.get("publishedAt") or "")[:16],
                summary=(a.get("description") or "")[:200],
            )
        )
    return items


# ─── Bundle ────────────────────────────────────────────────────────────

OIL_QUERIES = [
    "Strait of Hormuz Iran tanker",
    "OPEC oil production",
    "war risk insurance Gulf",
]

DC_QUERIES = [
    "China rare earth export",
    "copper supply data center",
    "hyperscaler capex AI",
]


def fetch_all_news(tickers: list[str]) -> dict[str, list[NewsItem]]:
    """Return a dict with keys: 'oil_macro', 'dc_macro', and one per ticker."""
    out: dict[str, list[NewsItem]] = {}
    for t in tickers:
        items = finnhub_company_news(t)
        if items:
            out[t] = items
    out["oil_macro"] = []
    for q in OIL_QUERIES:
        out["oil_macro"].extend(newsapi_search(q, limit=2))
    out["dc_macro"] = []
    for q in DC_QUERIES:
        out["dc_macro"].extend(newsapi_search(q, limit=2))
    return out

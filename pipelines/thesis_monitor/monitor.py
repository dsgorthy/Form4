"""Daily thesis monitor — entrypoint for the launchd job.

Steps:
  1. Load positions from positions.yaml
  2. Pull prices (yfinance), macro (FRED), news (Finnhub + NewsAPI)
  3. Check stops
  4. Build a raw data block
  5. Send to GLM-4.7-flash on Studio Ollama for a concise daily brief
  6. Email the brief (Resend, reused from api.email)

Usage:
  python3 -m pipelines.thesis_monitor.monitor              # full run + send
  python3 -m pipelines.thesis_monitor.monitor --dry-run    # build + print, no email
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Project root on path (so `from pipelines... import` works when launchd calls us)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

_env = Path(__file__).resolve().parents[2] / ".env"
if _env.exists():
    load_dotenv(_env)

from pipelines.thesis_monitor.email_sender import send_brief
from pipelines.thesis_monitor.macro import MacroPoint, fetch_macro
from pipelines.thesis_monitor.news import NewsItem, fetch_all_news
from pipelines.thesis_monitor.ollama_client import summarize
from pipelines.thesis_monitor.positions import Position, load_positions, underlyings
from pipelines.thesis_monitor.prices import Quote, get_quotes
from pipelines.thesis_monitor.stops import StopHit, check_stops

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("thesis_monitor")


SYSTEM_PROMPT = """You are an investment analyst writing a concise daily brief for a $25,000 portfolio split between two theses:
  (1) Iran / Strait-of-Hormuz oil convexity (~$16k — tanker equities + FRO and XOP options)
  (2) Data center supply chain (~$8.3k — MP, COPX, USAR, UUUU + MP and FCX options)

You will be given the day's prices, macro signals, news, and stop-check results.

Write a brief with exactly these four sections:

HEADLINE: One sentence on whether anything material changed today.

OIL THESIS: 2-3 sentences. P&L pulse for the leg. Any news that matters (Hormuz, OPEC, insurance, tanker rates). Skip noise.

DATA CENTER THESIS: 2-3 sentences. P&L pulse. News on rare earths, copper, hyperscaler capex.

WATCH TOMORROW: 2-3 specific bullet items (catalysts, levels to watch, decisions pending).

Rules:
- Be terse. No hedging. No "consult a financial advisor."
- Cite specific tickers and prices.
- If a stop is tripped, lead with it.
- If news is thin or just noise, say "nothing material" — do not fabricate.
"""


def _fmt_pos_row(p: Position, q: Quote | None) -> str:
    """One line per position with current price + P&L."""
    if q is None:
        return f"  {p.label():<32} | qty {p.qty:<6g} | basis ${p.basis:<7.2f} | <price unavailable>"
    if p.type == "equity":
        unreal = (q.last - p.basis) * p.qty
        pct = (q.last - p.basis) / p.basis * 100 if p.basis else 0.0
        return (
            f"  {p.label():<32} | qty {p.qty:<6g} | basis ${p.basis:<7.2f} | "
            f"last ${q.last:<7.2f} ({q.day_pct:+.2f}% day) | unreal ${unreal:+,.0f} ({pct:+.1f}%)"
        )
    # option: show underlying state + moneyness
    moneyness = "n/a"
    if p.strike:
        diff_pct = (q.last - p.strike) / p.strike * 100
        if p.side == "call":
            moneyness = f"{'ITM' if q.last > p.strike else 'OTM'} {abs(diff_pct):.1f}%"
        else:
            moneyness = f"{'ITM' if q.last < p.strike else 'OTM'} {abs(diff_pct):.1f}%"
    return (
        f"  {p.label():<32} | qty {p.qty:<6g} | basis ${p.basis:<7.2f} | "
        f"underlying ${q.last:<7.2f} ({q.day_pct:+.2f}% day) | {moneyness} | exp {p.expiry}"
    )


def _fmt_thesis_block(name: str, positions: list[Position], quotes: dict[str, Quote]) -> str:
    if not positions:
        return f"{name}:\n  (no positions)\n"
    total_cost = sum(p.cost() for p in positions)
    lines = [f"{name} (cost basis ${total_cost:,.0f}):"]
    for p in positions:
        ticker_key = p.underlying if p.type == "option" else p.ticker
        lines.append(_fmt_pos_row(p, quotes.get(ticker_key)))
    return "\n".join(lines) + "\n"


def _fmt_macro_block(macro: dict[str, MacroPoint]) -> str:
    if not macro:
        return "MACRO: (FRED data unavailable)\n"
    parts = []
    for key in ("brent", "wti", "ovx"):
        m = macro.get(key)
        if not m:
            continue
        pct = f"{m.pct_change:+.2f}%" if m.pct_change is not None else "n/a"
        parts.append(f"{key.upper()} ${m.value:.2f} ({pct}, as of {m.date})")
    return "MACRO: " + " | ".join(parts) + "\n"


def _fmt_news_block(news: dict[str, list[NewsItem]]) -> str:
    parts = ["NEWS:"]
    has_any = False

    # Oil macro
    if news.get("oil_macro"):
        has_any = True
        parts.append("  --- Oil / Hormuz / OPEC ---")
        for item in news["oil_macro"][:6]:
            parts.append("  " + item.line())
    # DC macro
    if news.get("dc_macro"):
        has_any = True
        parts.append("  --- Data center / rare earth / copper ---")
        for item in news["dc_macro"][:6]:
            parts.append("  " + item.line())
    # Per-ticker
    per_ticker = {k: v for k, v in news.items() if k not in {"oil_macro", "dc_macro"} and v}
    if per_ticker:
        has_any = True
        parts.append("  --- Per-position ---")
        for ticker, items in per_ticker.items():
            parts.append(f"  {ticker}:")
            for item in items[:3]:
                parts.append("    " + item.line())

    if not has_any:
        parts.append("  (no news retrieved — check FINNHUB_API_KEY and NEWSAPI_KEY)")
    return "\n".join(parts) + "\n"


def _fmt_stops_block(hits: list[StopHit], positions: list[Position]) -> str:
    if not hits:
        return "STOPS: none tripped today\n"
    parts = ["STOPS — ACTION REQUIRED:"]
    for h in hits:
        parts.append(f"  [{h.thesis}] {h.label}: {h.detail}  →  {h.action}")
    return "\n".join(parts) + "\n"


def build_raw_block(
    positions: list[Position],
    quotes: dict[str, Quote],
    macro: dict[str, MacroPoint],
    news: dict[str, list[NewsItem]],
    hits: list[StopHit],
) -> str:
    oil = [p for p in positions if p.thesis == "oil"]
    dc = [p for p in positions if p.thesis == "data_center"]
    blocks = [
        f"=== THESIS MONITOR — {date.today().isoformat()} ===\n",
        "=== POSITIONS ===",
        _fmt_thesis_block("Oil / Iran-Hormuz", oil, quotes),
        _fmt_thesis_block("Data Center Supply Chain", dc, quotes),
        "=== " + _fmt_macro_block(macro).rstrip() + " ===",
        "=== " + _fmt_stops_block(hits, positions).rstrip() + " ===",
        "=== " + _fmt_news_block(news).rstrip() + " ===",
    ]
    return "\n".join(blocks)


def run(*, dry_run: bool = False) -> int:
    positions = load_positions()
    if not positions:
        log.warning("No positions loaded. Add some with `thesis-add-fill` first.")
        return 0  # not an error — first-run state

    tickers = underlyings(positions)
    log.info("Fetching prices for %d tickers: %s", len(tickers), ", ".join(tickers))
    quotes = get_quotes(tickers)
    log.info("Got %d quotes", len(quotes))

    log.info("Fetching macro signals from FRED…")
    macro = fetch_macro()

    log.info("Fetching news…")
    news = fetch_all_news(tickers)

    hits = check_stops(quotes, macro)
    log.info("%d stop(s) tripped", len(hits))

    raw_block = build_raw_block(positions, quotes, macro, news, hits)
    log.info("Calling Ollama (GLM-4.7-flash)…")
    brief = summarize(SYSTEM_PROMPT, raw_block)

    if dry_run:
        print("=" * 70)
        print("RAW BLOCK")
        print("=" * 70)
        print(raw_block)
        print("=" * 70)
        print("LLM BRIEF")
        print("=" * 70)
        print(brief)
        return 0

    send_brief(brief, raw_block, hits=hits)
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true",
                   help="Print to stdout instead of emailing")
    args = p.parse_args()
    sys.exit(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()

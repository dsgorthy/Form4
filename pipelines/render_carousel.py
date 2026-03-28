#!/usr/bin/env python3
"""Render Instagram carousel images (1080x1080) from daily trade data.

Outputs numbered slides counting down from #N to #1 (strongest signal last).

Usage:
    python3 pipelines/render_carousel.py --date 2026-03-25
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONTENT_DIR = Path(__file__).resolve().parent / "data" / "content"
BRAND_DIR = Path(__file__).resolve().parent.parent / "brand"
_DB_DIR = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog"
PRICES_DB = _DB_DIR / "prices.db"  # daily_prices, option_prices
WIDTH = 1080
HEIGHT = 1080

BASE_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  width: 1080px; height: 1080px;
  background: #0A0A0F;
  color: #E8E8ED;
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif;
  padding: 50px;
  display: flex; flex-direction: column;
  justify-content: center;
}
.brand { color: #3B82F6; font-weight: 700; }
.green { color: #22C55E; }
.red { color: #EF4444; }
.amber { color: #F59E0B; }
.muted { color: #55556A; }
.card {
  background: #12121A;
  border: 1px solid #2A2A3A;
  border-radius: 16px;
  padding: 36px;
  width: 100%;
}
.grade {
  display: inline-flex; align-items: center; justify-content: center;
  width: 48px; height: 48px;
  border-radius: 10px;
  font-size: 24px; font-weight: 700; font-family: monospace;
}
.grade-a { background: rgba(34,197,94,0.15); color: #22C55E; border: 1px solid rgba(34,197,94,0.3); }
.grade-b { background: rgba(59,130,246,0.15); color: #3B82F6; border: 1px solid rgba(59,130,246,0.3); }
.grade-c { background: rgba(136,136,160,0.15); color: #8888A0; border: 1px solid rgba(136,136,160,0.3); }
.tag {
  display: inline-block; padding: 6px 14px; border-radius: 8px;
  font-size: 16px; font-weight: 500; margin-right: 8px; margin-top: 8px;
}
"""


def _img_data_uri(path: Path) -> str:
    import base64
    if not path.exists():
        return ""
    return f"data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}"


def _mini_chart_svg(ticker: str, db_path: Path, days: int = 120, width: int = 940, height: int = 180) -> str:
    """Render a mini SVG sparkline for a ticker."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    # daily_prices is in prices.db (split from insiders.db 2026-03-27)
    prices_db = db_path.parent / "prices.db"
    if prices_db.exists():
        conn.execute(f"ATTACH DATABASE 'file:{prices_db}?mode=ro' AS prices")
    prices = conn.execute(
        "SELECT close FROM daily_prices WHERE ticker = ? ORDER BY date DESC LIMIT ?",
        (ticker, days)
    ).fetchall()
    conn.close()

    if len(prices) < 10:
        return ""

    closes = [p["close"] for p in reversed(prices)]
    min_p, max_p = min(closes) * 0.98, max(closes) * 1.02
    p_range = max_p - min_p if max_p > min_p else 1
    pad = 6

    def x(i): return pad + (i / (len(closes) - 1)) * (width - pad * 2)
    def y(v): return pad + (1 - (v - min_p) / p_range) * (height - pad * 2)

    points = " ".join(f"{x(i):.1f},{y(c):.1f}" for i, c in enumerate(closes))
    area = points + f" {x(len(closes)-1):.1f},{height - pad} {pad},{height - pad}"
    up = closes[-1] >= closes[0]
    color = "#22C55E" if up else "#EF4444"
    fill = "rgba(34,197,94,0.08)" if up else "rgba(239,68,68,0.08)"
    price_label = f'<text x="{width - pad}" y="{y(closes[-1]) - 6}" text-anchor="end" font-size="16" font-weight="600" font-family="monospace" fill="{color}">${closes[-1]:.2f}</text>'

    return f"""<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
      <polygon points="{area}" fill="{fill}" />
      <polyline points="{points}" fill="none" stroke="{color}" stroke-width="2" />
      {price_label}
    </svg>"""


def slide_title(date_str: str, num_trades: int) -> str:
    _d = datetime.strptime(date_str, "%Y-%m-%d")
    _day = _d.day
    _suffix = "th" if 11 <= _day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(_day % 10, "th")
    date_fmt = f"{_d.strftime('%B')} {_day}{_suffix}"

    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4.png")
    logo_html = f'<img src="{logo_uri}" style="width:60%;max-height:120px;object-fit:contain" />' if logo_uri else '<div style="font-size:72px;font-weight:800">Form<span class="brand">4</span></div>'

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}</style></head><body>
    <div style="text-align:center">
      {logo_html}
      <div style="font-size:48px;font-weight:700;margin-top:48px">Top {num_trades} Insider Trades</div>
      <div style="font-size:32px;color:#55556A;margin-top:16px">{date_fmt}</div>
      <div style="font-size:24px;color:#F59E0B;margin-top:48px;font-weight:600">Swipe for the countdown →</div>
      <div style="font-size:20px;color:#55556A;margin-top:12px">#1 is the strongest signal</div>
    </div>
    </body></html>"""


def slide_trade(trade: dict, rank: int, total: int, db_path: Path) -> str:
    from pipelines.generate_daily_content import fmt_value, fmt_title, fmt_company_spoken, build_context_line

    action = "BOUGHT" if trade["trade_type"] == "buy" else "SOLD"
    action_color = "green" if trade["trade_type"] == "buy" else "red"
    grade = trade.get("signal_grade", "?")
    grade_class = f"grade-{grade.lower()}" if grade in ("A", "B", "C") else ""
    company = fmt_company_spoken(trade.get("company"), trade["ticker"])
    title = fmt_title(trade.get("title", ""))
    value = fmt_value(trade["total_value"])
    ctx = build_context_line(trade)

    # Tags
    tags_html = ""
    if trade.get("is_rare_reversal"):
        tags_html += '<span class="tag" style="background:rgba(245,158,11,0.1);color:#F59E0B;border:1px solid rgba(245,158,11,0.2)">Rare Reversal</span>'
    if trade.get("week52_proximity") is not None:
        w52 = trade["week52_proximity"]
        if w52 >= 0.8 and trade["trade_type"] == "buy":
            tags_html += '<span class="tag" style="background:rgba(34,197,94,0.1);color:#22C55E;border:1px solid rgba(34,197,94,0.2)">Near 52w High</span>'
        elif w52 <= 0.2:
            tags_html += '<span class="tag" style="background:rgba(239,68,68,0.1);color:#EF4444;border:1px solid rgba(239,68,68,0.2)">Near 52w Low</span>'

    # Mini chart
    chart = _mini_chart_svg(trade["ticker"], db_path)
    chart_html = f'<div style="margin-top:16px">{chart}</div>' if chart else ""

    # Rank styling
    is_top = rank == 1
    rank_color = "#F59E0B" if is_top else "#55556A"
    rank_label = f"#{rank} — Strongest Signal" if is_top else f"#{rank}"

    # Stock performance
    perf = trade.get("_stock_perf")
    perf_html = ""
    if perf:
        pct = perf["pct_change"]
        bar_color = "#22C55E" if pct > 0 else "#EF4444"
        perf_html = f"""
        <div style="display:flex;align-items:center;gap:12px;margin-top:12px">
          <span style="font-size:14px;color:#55556A">90d:</span>
          <span style="font-family:monospace;font-size:20px;font-weight:700;color:{bar_color}">{pct:+.1f}%</span>
          <span style="font-size:14px;color:#55556A">@ ${perf['current_price']:.2f}</span>
        </div>"""

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}</style></head><body>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:24px">
      <div style="font-size:28px;font-weight:700;color:{rank_color}">{rank_label}</div>
      <div style="font-size:16px;color:#55556A">{rank} of {total}</div>
    </div>
    <div class="card">
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:20px">
        <span class="grade {grade_class}">{grade}</span>
        <div>
          <div style="font-size:44px;font-weight:800;font-family:monospace;letter-spacing:-1px">${trade["ticker"]}</div>
          <div style="font-size:18px;color:#8888A0">{company}</div>
        </div>
      </div>
      <div style="font-size:24px;font-weight:600">{title}</div>
      <div style="margin-top:24px">
        <span class="{action_color}" style="font-size:32px;font-weight:700">{action}</span>
        <span style="font-size:44px;font-weight:800;font-family:monospace;margin-left:12px">{value}</span>
      </div>
      {f'<div style="margin-top:16px">{tags_html}</div>' if tags_html else ''}
      {f'<div style="font-size:18px;color:#8888A0;margin-top:12px">{ctx}</div>' if ctx else ''}
      {perf_html}
    </div>
    {chart_html}
    {f'<div style="text-align:center;margin-top:16px;font-size:18px;color:#55556A">Swipe for #{rank-1} →</div>' if rank > 1 else ''}
    </body></html>"""


def slide_cta() -> str:
    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4_tagline.png")
    logo_html = f'<img src="{logo_uri}" style="width:70%;max-height:300px;object-fit:contain" />' if logo_uri else '<div style="font-size:64px;font-weight:800">Form<span class="brand">4</span></div>'

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}</style></head><body>
    <div style="text-align:center">
      {logo_html}
      <div style="font-size:26px;color:#22C55E;margin-top:40px;font-weight:600">7-day free trial — no credit card</div>
      <div style="font-size:36px;font-weight:700;margin-top:16px;color:#3B82F6">form4.app</div>
      <div style="font-size:18px;color:#55556A;margin-top:32px">Daily insider trade signals • AI signal grading • Portfolio alerts</div>
    </div>
    </body></html>"""


def render_carousel(date_str: str) -> list[Path]:
    from playwright.sync_api import sync_playwright
    from pipelines.generate_daily_content import (
        get_top_trades, get_daily_stats, build_context_line, fmt_value, fmt_title,
        get_stock_performance, DB_PATH,
    )

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    conn.row_factory = sqlite3.Row
    trades = get_top_trades(conn, date_str)
    conn.close()

    if not trades:
        logger.warning("No trades for %s", date_str)
        return []

    # Enrich
    for t in trades:
        t["_fmt_value"] = fmt_value(t["total_value"])
        t["_fmt_title"] = fmt_title(t["title"] or "")
        t["_context_line"] = build_context_line(t)

    num_trades = min(len(trades), 5)
    trades = trades[:num_trades]

    # Build slides: title, then trades counting down (#N to #1)
    slides_html = []
    slides_html.append(("00_title", slide_title(date_str, num_trades)))

    # Reverse order: weakest first (#5), strongest last (#1)
    for i, t in enumerate(reversed(trades)):
        rank = num_trades - i
        slides_html.append((f"{i+1:02d}_trade_{rank}", slide_trade(t, rank, num_trades, DB_PATH)))

    slides_html.append((f"{num_trades+1:02d}_cta", slide_cta()))

    # Render
    date_slug = date_str.replace("-", "")
    output_dir = CONTENT_DIR / f"{date_slug}_carousel"
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})

        for name, html in slides_html:
            page.set_content(html, wait_until="networkidle")
            path = output_dir / f"{name}.png"
            page.screenshot(path=str(path), full_page=False)
            paths.append(path)
            logger.info("Rendered %s", path.name)

        browser.close()

    return paths


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    paths = render_carousel(args.date)
    if paths:
        logger.info("Carousel: %d slides in %s", len(paths), paths[0].parent)


if __name__ == "__main__":
    main()

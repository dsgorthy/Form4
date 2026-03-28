#!/usr/bin/env python3
"""Render vertical video assets (1080x1920) for daily content.

Outputs per-trade asset folders with trade cards, charts, logos,
headshots (or silhouette fallback), mystery/reveal cards, and more.

Usage:
    python3 pipelines/render_video_assets.py --date 2026-03-27

Called automatically by generate_daily_content.py during the daily pipeline.
"""
from __future__ import annotations

import base64
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONTENT_DIR = Path(__file__).resolve().parent / "data" / "content"
BRAND_DIR = Path(__file__).resolve().parent.parent / "brand"
DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
WIDTH = 1080
HEIGHT = 1920

# Brand palette
MIDNIGHT = "#0A0A0F"
SLATE = "#12121A"
CLOUD = "#E8E8ED"
STEEL = "#8888A0"
FOG = "#55556A"
SIGNAL_BLUE = "#3B82F6"
ALPHA_GREEN = "#22C55E"
RISK_RED = "#EF4444"
AMBER = "#F59E0B"

BASE_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    width: 1080px; height: 1920px;
    background: """ + MIDNIGHT + """;
    color: """ + CLOUD + """;
    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Inter', sans-serif;
    display: flex; flex-direction: column;
    justify-content: center;
    overflow: hidden;
}
.grade {
    display: inline-flex; align-items: center; justify-content: center;
    width: 64px; height: 64px;
    border-radius: 14px;
    font-size: 32px; font-weight: 700; font-family: monospace;
}
.grade-a { background: rgba(34,197,94,0.15); color: #22C55E; border: 2px solid rgba(34,197,94,0.3); }
.grade-b { background: rgba(59,130,246,0.15); color: #3B82F6; border: 2px solid rgba(59,130,246,0.3); }
.grade-c { background: rgba(136,136,160,0.15); color: #8888A0; border: 2px solid rgba(136,136,160,0.3); }
.tag {
    display: inline-block; padding: 8px 18px; border-radius: 10px;
    font-size: 18px; font-weight: 600; margin-right: 10px; margin-top: 10px;
}
.card {
    background: """ + SLATE + """;
    border: 1px solid #2A2A3A;
    border-radius: 20px;
    padding: 44px;
    width: 100%;
}
"""


# ---------------------------------------------------------------------------
# Asset fetching — logos and headshots
# ---------------------------------------------------------------------------

def _get_company_domain(company: str, ticker: str) -> str | None:
    """Find company domain via Clearbit Autocomplete API (free, no auth)."""
    import requests
    try:
        clean = re.sub(
            r'\b(Inc|Corp|Corporation|Ltd|Limited|LLC|Holdings?|Group|Co|Company|PLC|N\.?V\.?|S\.?A\.?)\b\.?',
            '', company, flags=re.IGNORECASE
        ).strip().rstrip(',').strip()
        if not clean:
            clean = ticker
        resp = requests.get(
            "https://autocomplete.clearbit.com/v1/companies/suggest",
            params={"query": clean},
            timeout=5,
        )
        if resp.status_code == 200:
            results = resp.json()
            if results:
                return results[0]["domain"]
    except Exception as exc:
        logger.debug("Domain lookup failed for %s: %s", ticker, exc)
    return None


def fetch_company_logo(company: str, ticker: str, output_path: Path) -> bool:
    """Download company logo. Tries icon.horse (high-res), falls back to Google favicon."""
    import requests
    domain = _get_company_domain(company, ticker)
    if not domain:
        return False

    # icon.horse — high quality icons, free, no auth
    try:
        resp = requests.get(f"https://icon.horse/icon/{domain}", timeout=10)
        if resp.status_code == 200 and len(resp.content) > 500:
            output_path.write_bytes(resp.content)
            logger.info("Logo (icon.horse): %s (%s)", ticker, domain)
            return True
    except Exception:
        pass

    # Fallback: Google favicon at max size
    try:
        resp = requests.get(
            f"https://www.google.com/s2/favicons?domain={domain}&sz=256",
            timeout=10,
        )
        if resp.status_code == 200 and len(resp.content) > 200:
            output_path.write_bytes(resp.content)
            logger.info("Logo (Google): %s (%s)", ticker, domain)
            return True
    except Exception as exc:
        logger.debug("Logo fetch failed for %s: %s", ticker, exc)
    return False


def _wikipedia_photo_url(name: str) -> str | None:
    """Get headshot URL from Wikipedia REST API for a named person.

    Tries multiple name variants: full name, without middle initial,
    first+last only.
    """
    import requests

    # Build name variants to try
    variants = [name]
    parts = name.split()
    if len(parts) >= 3:
        # "Mark A. Stevens" → "Mark Stevens"
        variants.append(f"{parts[0]} {parts[-1]}")
    # Strip middle initials like "A." or "J."
    stripped = re.sub(r'\s+[A-Z]\.?\s+', ' ', name).strip()
    if stripped != name and stripped not in variants:
        variants.append(stripped)

    person_keywords = [
        "executive", "businessman", "businesswoman", "ceo", "officer",
        "investor", "entrepreneur", "american", "chairman", "founder",
        "director", "president", "manager", "banker", "financier",
    ]

    for variant in variants:
        try:
            slug = variant.replace(" ", "_")
            resp = requests.get(
                f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}",
                timeout=8,
                headers={"User-Agent": "Form4App/1.0 (derek@sidequestgroup.com)"},
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            desc = (data.get("description") or "").lower()
            page_type = data.get("type", "")
            # Skip disambiguation pages
            if "disambiguation" in desc or page_type == "disambiguation":
                continue
            if any(kw in desc for kw in person_keywords):
                thumb = data.get("thumbnail", {}).get("source")
                if thumb:
                    return re.sub(r'/\d+px-', '/400px-', thumb)
        except Exception:
            continue

    return None


def fetch_insider_headshot(name: str, company: str, output_path: Path) -> bool:
    """Try to download insider headshot. Wikipedia first, then fallback."""
    import requests
    url = _wikipedia_photo_url(name)
    if url:
        try:
            resp = requests.get(url, timeout=10,
                                headers={"User-Agent": "Form4App/1.0"})
            if resp.status_code == 200 and len(resp.content) > 500:
                output_path.write_bytes(resp.content)
                logger.info("Headshot (Wikipedia): %s", name)
                return True
        except Exception:
            pass
    return False


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------

def _img_data_uri(path: Path) -> str:
    """Convert image file to data URI for embedding in HTML."""
    if not path.exists():
        return ""
    ext = path.suffix.lower()
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "svg": "image/svg+xml", "webp": "image/webp"}.get(ext.lstrip("."), "image/png")
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode()}"


def _mini_chart_svg(ticker: str, days: int = 120, width: int = 940, height: int = 280) -> str:
    """Render SVG sparkline chart for a ticker from daily_prices."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    prices = conn.execute(
        "SELECT close FROM daily_prices WHERE ticker = ? ORDER BY date DESC LIMIT ?",
        (ticker, days),
    ).fetchall()
    conn.close()

    if len(prices) < 10:
        return ""

    closes = [p["close"] for p in reversed(prices)]
    min_p, max_p = min(closes) * 0.98, max(closes) * 1.02
    p_range = max_p - min_p if max_p > min_p else 1
    pad = 8

    def x(i):
        return pad + (i / (len(closes) - 1)) * (width - pad * 2)

    def y(v):
        return pad + (1 - (v - min_p) / p_range) * (height - pad * 2)

    points = " ".join(f"{x(i):.1f},{y(c):.1f}" for i, c in enumerate(closes))
    area = points + f" {x(len(closes)-1):.1f},{height - pad} {pad},{height - pad}"
    up = closes[-1] >= closes[0]
    color = ALPHA_GREEN if up else RISK_RED
    fill = "rgba(34,197,94,0.08)" if up else "rgba(239,68,68,0.08)"

    price_lbl = (
        f'<text x="{width - pad}" y="{y(closes[-1]) - 10}" text-anchor="end" '
        f'font-size="26" font-weight="600" font-family="monospace" fill="{color}">'
        f'${closes[-1]:.2f}</text>'
    )

    return (
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">'
        f'<polygon points="{area}" fill="{fill}" />'
        f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3" />'
        f'{price_lbl}</svg>'
    )


def _tags_html(trade: dict) -> str:
    """Render signal tags (rare reversal, 52-week proximity, etc.)."""
    tags = []
    if trade.get("is_rare_reversal"):
        tags.append(f'<span class="tag" style="background:rgba(245,158,11,0.12);color:{AMBER};'
                    f'border:1px solid rgba(245,158,11,0.25)">Rare Reversal</span>')
    w52 = trade.get("week52_proximity")
    if w52 is not None:
        if w52 >= 0.8 and trade["trade_type"] == "buy":
            tags.append(f'<span class="tag" style="background:rgba(34,197,94,0.12);color:{ALPHA_GREEN};'
                        f'border:1px solid rgba(34,197,94,0.25)">Near 52w High</span>')
        elif w52 <= 0.2:
            tags.append(f'<span class="tag" style="background:rgba(239,68,68,0.12);color:{RISK_RED};'
                        f'border:1px solid rgba(239,68,68,0.25)">Near 52w Low</span>')
    if trade.get("signal_grade") == "A":
        tags.append(f'<span class="tag" style="background:rgba(34,197,94,0.12);color:{ALPHA_GREEN};'
                    f'border:1px solid rgba(34,197,94,0.25)">A-Grade</span>')
    return "\n".join(tags)


# ---------------------------------------------------------------------------
# Card templates — all 1080x1920 (9:16 vertical)
# ---------------------------------------------------------------------------

def card_hook(hook_text: str, date_str: str) -> str:
    """Render hook/title card."""
    _d = datetime.strptime(date_str, "%Y-%m-%d")
    _day = _d.day
    _sfx = "th" if 11 <= _day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(_day % 10, "th")
    date_fmt = f"{_d.strftime('%B')} {_day}{_sfx}"

    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4.png")
    logo_html = (
        f'<img src="{logo_uri}" style="width:50%;max-height:100px;object-fit:contain" />'
        if logo_uri else
        f'<div style="font-size:72px;font-weight:800">Form<span style="color:{SIGNAL_BLUE}">4</span></div>'
    )

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 80px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:100px">{logo_html}</div>
    <div style="font-size:54px;font-weight:800;line-height:1.25;max-width:900px">{hook_text}</div>
    <div style="margin-top:80px">
        <div style="font-size:22px;color:{FOG};text-transform:uppercase;letter-spacing:4px;font-weight:600">Daily Signal</div>
        <div style="font-size:28px;color:{STEEL};margin-top:12px">{date_fmt}</div>
    </div>
    </body></html>"""


def card_trade(trade: dict, rank: int, total: int,
               logo_path: Path | None = None, headshot_path: Path | None = None) -> str:
    """Render trade info card."""
    from pipelines.generate_daily_content import fmt_value, fmt_title, fmt_company_spoken, build_context_line

    action = "BOUGHT" if trade["trade_type"] == "buy" else "SOLD"
    action_color = ALPHA_GREEN if trade["trade_type"] == "buy" else RISK_RED
    grade = trade.get("signal_grade", "?")
    grade_cls = f"grade-{grade.lower()}" if grade in ("A", "B", "C") else ""
    company = fmt_company_spoken(trade.get("company"), trade["ticker"])
    title = fmt_title(trade.get("title", ""))
    value = fmt_value(trade.get("_combined_value", trade["total_value"]))
    ctx = build_context_line(trade)

    # Logo
    logo_html = ""
    if logo_path and logo_path.exists():
        logo_uri = _img_data_uri(logo_path)
        logo_html = f'<img src="{logo_uri}" style="width:80px;height:80px;border-radius:16px;object-fit:contain;background:#1a1a25;padding:8px" />'

    # Headshot
    headshot_html = ""
    if headshot_path and headshot_path.exists():
        hs_uri = _img_data_uri(headshot_path)
        headshot_html = f'<img src="{hs_uri}" style="width:100px;height:100px;border-radius:50%;object-fit:cover;border:3px solid #2A2A3A" />'

    # Tags
    tags = _tags_html(trade)

    # Performance
    perf = trade.get("_stock_perf")
    perf_html = ""
    if perf:
        pct = perf["pct_change"]
        bar_color = ALPHA_GREEN if pct > 0 else RISK_RED
        perf_html = f"""
        <div style="display:flex;align-items:center;gap:16px;margin-top:20px">
            <span style="font-size:18px;color:{FOG}">90d:</span>
            <span style="font-family:monospace;font-size:28px;font-weight:700;color:{bar_color}">{pct:+.1f}%</span>
            <span style="font-size:18px;color:{FOG}">@ ${perf['current_price']:.2f}</span>
        </div>"""

    # Chart
    chart = _mini_chart_svg(trade["ticker"])
    chart_html = f'<div style="margin-top:24px">{chart}</div>' if chart else ""

    # Rank label
    is_top = rank == 1
    rank_color = AMBER if is_top else FOG
    rank_label = f"#{rank} — Strongest Signal" if is_top else f"#{rank}"

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 60px; }}
    </style></head><body>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:32px">
        <div style="font-size:32px;font-weight:700;color:{rank_color}">{rank_label}</div>
        <div style="font-size:20px;color:{FOG}">{rank} of {total}</div>
    </div>

    <div class="card">
        <div style="display:flex;align-items:center;gap:20px;margin-bottom:28px">
            {logo_html}
            <span class="grade {grade_cls}">{grade}</span>
            <div>
                <div style="font-size:52px;font-weight:800;font-family:monospace;letter-spacing:-1px">${trade["ticker"]}</div>
                <div style="font-size:22px;color:{STEEL}">{company}</div>
            </div>
        </div>

        <div style="font-size:26px;font-weight:600;color:{STEEL}">{title}</div>

        <div style="margin-top:32px">
            <span style="color:{action_color};font-size:36px;font-weight:700">{action}</span>
            <span style="font-size:52px;font-weight:800;font-family:monospace;margin-left:16px">{value}</span>
        </div>

        {f'<div style="margin-top:20px">{tags}</div>' if tags else ''}
        {f'<div style="font-size:20px;color:{STEEL};margin-top:16px">{ctx}</div>' if ctx else ''}
        {perf_html}
    </div>

    {chart_html}

    {f'<div style="display:flex;justify-content:center;margin-top:24px">{headshot_html}</div>' if headshot_html else ''}

    <div style="text-align:center;margin-top:auto;padding-top:24px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_mystery(trade: dict, total: int) -> str:
    """Render mystery card for reveal — ticker and company hidden."""
    from pipelines.generate_daily_content import fmt_value, fmt_title

    action = "BOUGHT" if trade["trade_type"] == "buy" else "SOLD"
    action_color = ALPHA_GREEN if trade["trade_type"] == "buy" else RISK_RED
    grade = trade.get("signal_grade", "?")
    grade_cls = f"grade-{grade.lower()}" if grade in ("A", "B", "C") else ""
    title = fmt_title(trade.get("title", ""))
    value = fmt_value(trade.get("_combined_value", trade["total_value"]))
    tags = _tags_html(trade)

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 60px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:48px">
        <div style="font-size:28px;font-weight:700;color:{AMBER};text-transform:uppercase;letter-spacing:3px">Biggest Signal Today</div>
    </div>

    <div class="card">
        <div style="display:flex;align-items:center;gap:20px;margin-bottom:28px">
            <span class="grade {grade_cls}">{grade}</span>
            <div>
                <div style="font-size:52px;font-weight:800;font-family:monospace;letter-spacing:-1px;color:{AMBER}">????</div>
                <div style="font-size:22px;color:{FOG}">Stay to the end</div>
            </div>
        </div>

        <div style="font-size:26px;font-weight:600;color:{STEEL}">{title}</div>

        <div style="margin-top:32px">
            <span style="color:{action_color};font-size:36px;font-weight:700">{action}</span>
            <span style="font-size:52px;font-weight:800;font-family:monospace;margin-left:16px">{value}</span>
        </div>

        {f'<div style="margin-top:20px">{tags}</div>' if tags else ''}
    </div>

    <div style="text-align:center;margin-top:60px">
        <div style="font-size:72px">?</div>
        <div style="font-size:24px;color:{AMBER};margin-top:16px;font-weight:600">Who made this trade?</div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:24px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_chart(trade: dict) -> str:
    """Render standalone chart card (1080x1920)."""
    from pipelines.generate_daily_content import fmt_company_spoken

    ticker = trade["ticker"]
    company = fmt_company_spoken(trade.get("company"), ticker)
    chart = _mini_chart_svg(ticker, days=120, width=940, height=500)
    if not chart:
        return ""

    perf = trade.get("_stock_perf")
    perf_html = ""
    if perf:
        pct = perf["pct_change"]
        color = ALPHA_GREEN if pct > 0 else RISK_RED
        perf_html = f"""
        <div style="display:flex;align-items:center;gap:20px;margin-top:32px">
            <span style="font-family:monospace;font-size:56px;font-weight:800;color:{color}">{pct:+.1f}%</span>
            <span style="font-size:24px;color:{FOG}">last 90 days</span>
        </div>"""

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 60px; align-items: center; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:40px">
        <div style="font-size:64px;font-weight:800;font-family:monospace">${ticker}</div>
        <div style="font-size:24px;color:{STEEL};margin-top:8px">{company}</div>
    </div>

    <div style="width:100%">{chart}</div>

    {perf_html}

    <div style="text-align:center;margin-top:auto;padding-top:24px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_track_record(trade: dict, headshot_path: Path | None = None) -> str:
    """Render track record card if insider has enough history."""
    from pipelines.generate_daily_content import fmt_title, fmt_company_spoken

    track = trade.get("_track_record", [])
    with_ret = [t for t in track if t.get("return_30d") is not None]
    if len(with_ret) < 3:
        return ""

    is_buy = trade["trade_type"] == "buy"
    wins = sum(1 for t in with_ret if (t["return_30d"] > 0) == is_buy)
    wr = (wins / len(with_ret)) * 100
    avg_ret = sum(t["return_30d"] for t in with_ret) / len(with_ret)

    if wr < 60 or abs(avg_ret) < 1:
        return ""  # not impressive enough to show

    company = fmt_company_spoken(trade.get("company"), trade["ticker"])
    title = fmt_title(trade.get("title", ""))

    # Headshot
    hs_html = ""
    if headshot_path and headshot_path.exists():
        hs_uri = _img_data_uri(headshot_path)
        hs_html = f'<img src="{hs_uri}" style="width:120px;height:120px;border-radius:50%;object-fit:cover;border:3px solid #2A2A3A" />'
    else:
        # Silhouette placeholder
        hs_html = f"""
        <div style="width:120px;height:120px;border-radius:50%;background:#1a1a25;border:3px solid #2A2A3A;
                    display:flex;align-items:center;justify-content:center">
            <svg width="60" height="60" viewBox="0 0 24 24" fill="{FOG}">
                <circle cx="12" cy="8" r="4"/><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/>
            </svg>
        </div>"""

    # Win rate bar
    bar_w = 600
    filled_w = int(bar_w * wr / 100)
    bar_color = ALPHA_GREEN if wr >= 70 else SIGNAL_BLUE if wr >= 60 else STEEL

    # Trade history rows
    trade_rows = ""
    for t in with_ret[:5]:
        ret = t["return_30d"]
        is_win = (ret > 0) == is_buy
        icon = f'<span style="color:{ALPHA_GREEN}">&#10003;</span>' if is_win else f'<span style="color:{RISK_RED}">&#10007;</span>'
        ret_color = ALPHA_GREEN if ret > 0 else RISK_RED
        trade_rows += f"""
        <div style="display:flex;align-items:center;gap:16px;padding:12px 0;border-bottom:1px solid #1e1e2a">
            <span style="font-size:24px">{icon}</span>
            <span style="font-size:18px;color:{FOG};width:100px">{t.get('trade_date','')[:10]}</span>
            <span style="font-family:monospace;font-size:22px;font-weight:600;color:{ret_color}">{ret:+.1f}%</span>
            <span style="font-size:16px;color:{FOG}">30d return</span>
        </div>"""

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 60px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:48px">
        <div style="font-size:22px;color:{FOG};text-transform:uppercase;letter-spacing:3px;font-weight:600">Insider Track Record</div>
    </div>

    <div style="display:flex;align-items:center;gap:24px;margin-bottom:40px">
        {hs_html}
        <div>
            <div style="font-size:28px;font-weight:700">{title}</div>
            <div style="font-size:22px;color:{STEEL}">{company}</div>
        </div>
    </div>

    <div class="card">
        <div style="margin-bottom:32px">
            <div style="font-size:20px;color:{FOG};margin-bottom:12px">Win Rate</div>
            <div style="display:flex;align-items:center;gap:20px">
                <div style="width:{bar_w}px;height:20px;background:#1a1a25;border-radius:10px;overflow:hidden">
                    <div style="width:{filled_w}px;height:100%;background:{bar_color};border-radius:10px"></div>
                </div>
                <span style="font-family:monospace;font-size:28px;font-weight:700;color:{bar_color}">{wr:.0f}%</span>
            </div>
        </div>

        <div style="margin-bottom:32px">
            <div style="font-size:20px;color:{FOG};margin-bottom:8px">Avg 30-Day Return</div>
            <span style="font-family:monospace;font-size:40px;font-weight:800;color:{ALPHA_GREEN if avg_ret > 0 else RISK_RED}">{avg_ret:+.1f}%</span>
        </div>

        <div>
            <div style="font-size:20px;color:{FOG};margin-bottom:12px">Recent Trades ({len(with_ret)})</div>
            {trade_rows}
        </div>
    </div>

    <div style="text-align:center;margin-top:auto;padding-top:24px">
        <span style="font-size:18px;color:{FOG}">form4.app</span>
    </div>
    </body></html>"""


def card_cta() -> str:
    """Render CTA card."""
    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4_tagline.png")
    logo_html = (
        f'<img src="{logo_uri}" style="width:65%;max-height:250px;object-fit:contain" />'
        if logo_uri else
        f'<div style="font-size:72px;font-weight:800">Form<span style="color:{SIGNAL_BLUE}">4</span></div>'
    )

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 80px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="margin-bottom:80px">{logo_html}</div>

    <div style="font-size:36px;font-weight:700;margin-bottom:16px">Get real-time insider alerts</div>
    <div style="font-size:28px;color:{ALPHA_GREEN};font-weight:600">7-day free trial — no credit card</div>

    <div style="margin-top:60px">
        <div style="font-size:48px;font-weight:800;color:{SIGNAL_BLUE}">form4.app</div>
    </div>

    <div style="margin-top:60px;color:{STEEL};font-size:20px;line-height:1.8">
        Daily insider trade signals<br/>
        AI signal grading<br/>
        Portfolio alerts &amp; tracking
    </div>

    </body></html>"""


def card_silhouette(name: str, title: str, company: str, logo_path: Path | None = None) -> str:
    """Render branded silhouette card (headshot fallback)."""
    logo_html = ""
    if logo_path and logo_path.exists():
        logo_uri = _img_data_uri(logo_path)
        logo_html = f'<img src="{logo_uri}" style="width:60px;height:60px;border-radius:12px;object-fit:contain;background:#1a1a25;padding:4px;margin-top:16px" />'

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 80px; align-items: center; text-align: center; }}
    </style></head><body>
    <div style="width:200px;height:200px;border-radius:50%;background:linear-gradient(135deg, #1a1a25, #12121A);
                border:3px solid #2A2A3A;display:flex;align-items:center;justify-content:center;margin-bottom:32px">
        <svg width="100" height="100" viewBox="0 0 24 24" fill="{FOG}">
            <circle cx="12" cy="8" r="4"/><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/>
        </svg>
    </div>
    <div style="font-size:32px;font-weight:700">{name}</div>
    <div style="font-size:24px;color:{STEEL};margin-top:8px">{title}</div>
    <div style="font-size:22px;color:{FOG};margin-top:8px">{company}</div>
    {logo_html}
    </body></html>"""


# ---------------------------------------------------------------------------
# Main renderer
# ---------------------------------------------------------------------------

def _extract_hook_text(storyboard: str) -> str:
    """Extract the hook spoken line from a storyboard script."""
    in_hook = False
    for line in storyboard.split("\n"):
        if "HOOK" in line and "---" in line:
            in_hook = True
            continue
        if in_hook and line.strip().startswith('"') and line.strip().endswith('"'):
            return line.strip()[1:-1]
    return ""


def render_all_assets(trades: list[dict], date_str: str, storyboard: str = "") -> Path:
    """Render all video assets for a day's content.

    Creates:
      content/YYYYMMDD/assets/
        hook_text.png, cta.png
        trade_{rank}_{TICKER}/ — card, chart, logo, headshot
        reveal_{TICKER}/ — card, mystery_card, chart, logo, headshot, stats
    """
    from playwright.sync_api import sync_playwright
    from pipelines.generate_daily_content import (
        fmt_title, fmt_company_spoken, DB_PATH,
    )

    date_slug = date_str.replace("-", "")
    assets_dir = CONTENT_DIR / date_slug / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    blockbuster = trades[0]
    supporting = list(reversed(trades[1:5]))
    all_shown = supporting + [blockbuster]
    total = len(all_shown)

    # Extract hook text from storyboard
    hook_text = _extract_hook_text(storyboard) if storyboard else "Top insider trades today."

    # Pre-fetch logos and headshots for all trades
    logger.info("Fetching logos and headshots for %d trades...", len(all_shown))
    trade_assets: dict[str, dict] = {}  # ticker -> {logo_path, headshot_path}
    for t in all_shown:
        ticker = t["ticker"]
        if ticker in trade_assets:
            continue
        company = t.get("company", "")
        insider_name = t.get("insider_name", "")
        title_clean = fmt_title(t.get("title", ""))
        company_spoken = fmt_company_spoken(company, ticker)

        # Determine the asset folder
        is_reveal = (t is blockbuster)
        if is_reveal:
            t_dir = assets_dir / f"reveal_{ticker}"
        else:
            rank = total - all_shown.index(t)
            t_dir = assets_dir / f"trade_{rank}_{ticker}"
        t_dir.mkdir(parents=True, exist_ok=True)

        info: dict = {"dir": t_dir, "logo_path": None, "headshot_path": None}

        # Logo
        logo_path = t_dir / "logo.png"
        if fetch_company_logo(company, ticker, logo_path):
            info["logo_path"] = logo_path

        # Headshot
        headshot_path = t_dir / "headshot.png"
        if insider_name and fetch_insider_headshot(insider_name, company, headshot_path):
            info["headshot_path"] = headshot_path
        elif insider_name:
            # Render silhouette fallback
            sil_html = card_silhouette(insider_name, title_clean, company_spoken, info["logo_path"])
            if sil_html:
                info["_silhouette_html"] = sil_html
                info["headshot_path"] = headshot_path  # will be rendered by Playwright

        trade_assets[ticker] = info
        time.sleep(0.2)  # rate limit

    # Render all HTML cards via Playwright
    logger.info("Rendering cards with Playwright...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})

        def _render(html: str, path: Path):
            if not html:
                return
            page.set_content(html, wait_until="networkidle")
            page.screenshot(path=str(path), full_page=False)
            logger.info("  Rendered %s", path.relative_to(CONTENT_DIR))

        # Global cards
        _render(card_hook(hook_text, date_str), assets_dir / "hook_text.png")
        _render(card_cta(), assets_dir / "cta.png")

        # Per-trade cards
        for i, t in enumerate(all_shown):
            ticker = t["ticker"]
            is_reveal = (t is blockbuster)
            info = trade_assets.get(ticker, {})
            t_dir = info.get("dir")
            if not t_dir:
                continue

            logo_p = info.get("logo_path")
            hs_p = info.get("headshot_path")

            if is_reveal:
                rank = 1
                # Mystery card (pre-blurred — no ticker shown)
                _render(card_mystery(t, total), t_dir / "mystery_card.png")
            else:
                rank = total - i

            # Trade card
            _render(card_trade(t, rank, total, logo_p, hs_p), t_dir / "card.png")

            # Chart card
            chart_html = card_chart(t)
            if chart_html:
                _render(chart_html, t_dir / "chart.png")

            # Silhouette fallback (if no real headshot)
            sil_html = info.get("_silhouette_html")
            if sil_html and hs_p and not hs_p.exists():
                _render(sil_html, hs_p)

            # Track record card (reveal only, if data is strong enough)
            if is_reveal:
                tr_html = card_track_record(t, hs_p)
                if tr_html:
                    _render(tr_html, t_dir / "stats.png")

        browser.close()

    logger.info("All assets rendered to %s", assets_dir)
    return assets_dir


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Render video assets for daily content")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    # Load trades (same query as generate_daily_content)
    from pipelines.generate_daily_content import get_top_trades, get_stock_performance, DB_PATH as db
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    conn.row_factory = sqlite3.Row
    trades = get_top_trades(conn, args.date)
    conn.close()

    if not trades:
        logger.warning("No trades for %s", args.date)
        return

    render_all_assets(trades, args.date)


if __name__ == "__main__":
    main()

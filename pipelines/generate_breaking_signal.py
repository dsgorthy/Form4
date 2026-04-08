#!/usr/bin/env python3
"""Generate breaking signal content for high-profile insider trades.

Detects trades that qualify as "breaking" — mega-cap C-suite, large value,
rare reversals, cluster activity — and generates urgent-tone social content
with visual assets.

Usage:
    python3 pipelines/generate_breaking_signal.py                    # today
    python3 pipelines/generate_breaking_signal.py --date 2026-03-25  # specific date
    python3 pipelines/generate_breaking_signal.py --dry-run           # detect only, no content
    python3 pipelines/generate_breaking_signal.py --audio             # include ElevenLabs audio

Can be run manually or scheduled via launchd every 15-30 min during market hours.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_connection

try:
    from pipelines.portfolio_simulator import compute_signal_quality
except ImportError:
    from portfolio_simulator import compute_signal_quality

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "content"
SENT_LOG = Path(__file__).resolve().parent / "data" / "content" / "breaking_sent.json"

# ---------------------------------------------------------------------------
# Breaking signal criteria
# ---------------------------------------------------------------------------

# Tickers with high retail interest — trades here get engagement
MEGA_CAP_TICKERS = {
    # Magnificent 7 + mega tech
    "AAPL", "NVDA", "MSFT", "AMZN", "GOOG", "GOOGL", "META", "TSLA", "NFLX",
    # Semiconductor
    "AMD", "AVGO", "INTC", "QCOM", "MU", "ARM", "TSM",
    # Finance
    "JPM", "GS", "BAC", "WFC", "V", "MA", "BRK.B", "C", "MS",
    # Healthcare
    "UNH", "JNJ", "PFE", "LLY", "ABBV", "MRK", "BMY",
    # Consumer / retail
    "WMT", "COST", "HD", "NKE", "DIS", "SBUX", "MCD", "TGT",
    # Energy
    "XOM", "CVX", "COP",
    # Other high-interest
    "BA", "CRM", "UBER", "ABNB", "SQ", "COIN", "PLTR", "SNOW",
    "RIVN", "LCID", "GME", "AMC", "SOFI", "HOOD", "RKLB",
    # AI / hot sector
    "SMCI", "DELL", "APP", "CRWD", "NET", "DDOG", "ZS",
}

# C-suite title patterns
CSUITE_PATTERNS = [
    "chief executive", "chief financial", "chief operating", "chief technology",
    "ceo", "cfo", "coo", "cto", "president", "chairman", "chair",
    "general counsel",
]


def is_csuite(title: str) -> bool:
    """Check if title indicates C-suite or equivalent."""
    t = (title or "").lower()
    return any(p in t for p in CSUITE_PATTERNS)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_breaking_signals(conn, target_date: str) -> list[dict]:
    """Find trades from target_date that qualify as breaking signals.

    Criteria (any ONE triggers):
    1. Mega-cap C-suite trade >= $1M (buy) or >= $5M (sell)
    2. Any ticker: C-suite BUY >= $5M
    3. Any ticker: rare reversal at signal_grade A or B
    4. Mega-cap cluster: 3+ insiders same direction same day
    5. Any ticker: trade value >= $20M (buy) or >= $50M (sell)
    """
    # Get all non-routine, non-duplicate trades for the day
    rows = conn.execute("""
        SELECT
            t.trade_id, t.ticker, t.company, t.insider_id,
            COALESCE(i.display_name, i.name) AS insider_name,
            t.title, t.trade_type, t.filing_date, t.trade_date,
            SUM(t.value) AS total_value,
            SUM(t.qty) AS total_qty,
            t.signal_grade, t.is_rare_reversal, t.is_csuite,
            t.week52_proximity, t.cohen_routine, t.is_10b5_1,
            t.shares_owned_after, t.filing_key,
            COALESCE(t.pit_win_rate_7d, itr.buy_win_rate_7d) AS pit_win_rate_7d,
            COALESCE(t.pit_n_trades, itr.buy_count) AS pit_n_trades,
            t.insider_switch_rate,
            t.is_rare_reversal
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE t.filing_date = ?
          AND t.trans_code IN ('P', 'S')
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND (t.is_routine = 0 OR t.is_routine IS NULL)
          AND (t.is_10b5_1 = 0 OR t.is_10b5_1 IS NULL)
        GROUP BY t.insider_id, t.ticker, t.trade_type, t.filing_key
        ORDER BY SUM(t.value) DESC
    """, (target_date,)).fetchall()

    trades = [dict(r) for r in rows]
    # Filter out bad tickers
    trades = [t for t in trades if t["ticker"] and t["ticker"] not in ("NONE", "", None)]

    # Compute V4 quality and filter
    qualified = []
    for r in trades:
        title = (r.get("title") or "").lower()
        csuite = any(kw in title for kw in ["ceo", "chief exec", "president", "cfo", "chief financial", "coo", "evp", "svp", "vp", "vice pres"])
        quality, _ = compute_signal_quality(
            pit_wr=r.get("pit_win_rate_7d"),
            pit_n=r.get("pit_n_trades"),
            is_csuite=csuite,
            holdings_pct_change=None,
            is_10pct_owner=False,
            title=r.get("title"),
            is_rare_reversal=bool(r.get("is_rare_reversal")),
            switch_rate=r.get("insider_switch_rate"),
        )
        r["_quality"] = quality
        if quality < 6.0:  # content floor — show Q6+ for volume
            continue
        qualified.append(r)
    trades = qualified

    signals: list[dict] = []
    seen_tickers: set[str] = set()  # one signal per ticker per direction

    for t in trades:
        ticker = t["ticker"]
        is_buy = t["trade_type"] == "buy"
        val = t["total_value"] or 0
        grade = t["signal_grade"]
        title_str = t["title"] or ""
        csuite = t["is_csuite"] or is_csuite(title_str)
        is_mega = ticker in MEGA_CAP_TICKERS
        dedup_key = f"{ticker}:{t['trade_type']}"

        if dedup_key in seen_tickers:
            continue  # one entry per ticker+direction

        reason = None

        # 1. Mega-cap C-suite
        if is_mega and csuite:
            if is_buy and val >= 1_000_000:
                reason = "mega_cap_csuite_buy"
            elif not is_buy and val >= 5_000_000:
                reason = "mega_cap_csuite_sell"

        # 2. Any C-suite large buy
        if not reason and csuite and is_buy and val >= 5_000_000:
            reason = "large_csuite_buy"

        # 3. Rare reversal with good grade
        if not reason and t["is_rare_reversal"] and grade in ("A", "B"):
            if is_mega or val >= 500_000:
                reason = "rare_reversal"

        # 5. Massive trade at any company
        if not reason:
            if is_buy and val >= 20_000_000:
                reason = "massive_buy"
            elif not is_buy and val >= 50_000_000:
                reason = "massive_sell"

        if reason:
            seen_tickers.add(dedup_key)
            t["_break_reason"] = reason
            t["_is_mega"] = is_mega
            signals.append(t)

    # 4. Cluster detection — 3+ insiders same ticker same direction
    from collections import Counter
    ticker_dir_counts: dict[str, list[dict]] = {}
    for t in trades:
        k = f"{t['ticker']}:{t['trade_type']}"
        ticker_dir_counts.setdefault(k, []).append(t)

    for k, group in ticker_dir_counts.items():
        ticker = k.split(":")[0]
        direction = k.split(":")[1]
        dedup_key = f"{ticker}:{direction}"
        if ticker in ("NONE", "", None):
            continue
        if len(group) >= 3 and dedup_key not in seen_tickers:
            is_mega = ticker in MEGA_CAP_TICKERS
            if is_mega or sum(t["total_value"] or 0 for t in group) >= 5_000_000:
                rep = max(group, key=lambda x: x["total_value"] or 0)
                seen_tickers.add(dedup_key)
                rep["_break_reason"] = "cluster"
                rep["_is_mega"] = is_mega
                rep["_cluster_count"] = len(group)
                rep["_cluster_total"] = sum(t["total_value"] or 0 for t in group)
                rep["_cluster_insiders"] = [
                    {"name": t["insider_name"], "title": t["title"], "value": t["total_value"]}
                    for t in sorted(group, key=lambda x: -(x["total_value"] or 0))[:5]
                ]
                signals.append(rep)

    # Deduplicate and prioritize
    # Sort: rare_reversal > mega_cap_csuite_buy > cluster > large values
    priority = {
        "rare_reversal": 0,
        "mega_cap_csuite_buy": 1,
        "cluster": 2,
        "large_csuite_buy": 3,
        "massive_buy": 4,
        "mega_cap_csuite_sell": 5,
        "massive_sell": 6,
    }
    signals.sort(key=lambda x: (priority.get(x["_break_reason"], 99), -(x["total_value"] or 0)))

    return signals


# ---------------------------------------------------------------------------
# Already-sent tracking (avoid duplicate posts)
# ---------------------------------------------------------------------------

def _load_sent() -> set[str]:
    """Load set of already-sent signal keys."""
    if SENT_LOG.exists():
        try:
            data = json.loads(SENT_LOG.read_text())
            # Prune entries older than 7 days
            cutoff = (datetime.now() - timedelta(days=7)).isoformat()
            return {k for k, v in data.items() if v > cutoff}
        except Exception:
            pass
    return set()


def _mark_sent(key: str):
    """Mark a signal key as sent."""
    sent = {}
    if SENT_LOG.exists():
        try:
            sent = json.loads(SENT_LOG.read_text())
        except Exception:
            pass
    sent[key] = datetime.now().isoformat()
    # Prune old entries
    cutoff = (datetime.now() - timedelta(days=7)).isoformat()
    sent = {k: v for k, v in sent.items() if v > cutoff}
    SENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    SENT_LOG.write_text(json.dumps(sent, indent=2))


# ---------------------------------------------------------------------------
# Content generation — urgent tone, single trade focus
# ---------------------------------------------------------------------------

def generate_breaking_x_post(signal: dict) -> str:
    """Generate urgent X/Twitter post for a breaking signal."""
    from pipelines.generate_daily_content import (
        fmt_value, fmt_title, fmt_company_spoken, build_context_line,
    )

    ticker = signal["ticker"]
    company = fmt_company_spoken(signal.get("company"), ticker)
    title = fmt_title(signal.get("title", ""))
    action = "bought" if signal["trade_type"] == "buy" else "sold"
    value = fmt_value(signal.get("total_value", 0))
    grade = signal.get("signal_grade", "")
    reason = signal["_break_reason"]

    lines = []

    if reason == "rare_reversal":
        if signal["trade_type"] == "buy":
            lines.append(f"RARE REVERSAL: {company}'s {title} just {action} {value} in ${ticker}")
            lines.append(f"First buy after years of only selling.")
        else:
            lines.append(f"RARE REVERSAL: {company}'s {title} just {action} {value} in ${ticker}")
            lines.append(f"Long-time buyer just flipped to selling.")
        lines.append(f"In our data, rare reversals beat the market by 3.6% within 30 days.")
    elif reason == "cluster":
        n = signal.get("_cluster_count", 3)
        total = fmt_value(signal.get("_cluster_total", 0))
        lines.append(f"CLUSTER {action.upper()}: {n} insiders at {company} (${ticker}) all {action} today")
        lines.append(f"Combined value: {total}")
        lines.append(f"When {n}+ insiders move together, it's not a coincidence.")
    elif "csuite" in reason:
        lines.append(f"BREAKING: {company}'s {title} just {action} {value} in ${ticker}")
        ctx = build_context_line(signal)
        if ctx:
            lines.append(ctx)
    elif "massive" in reason:
        lines.append(f"LARGE TRADE: {value} {action} in ${ticker} ({company})")
        lines.append(f"Filed today by {signal.get('insider_name', 'an insider')} ({title})")
    else:
        lines.append(f"{company}'s {title} just {action} {value} in ${ticker}")

    if grade in ("A", "B"):
        lines.append(f"Signal grade: {grade}")

    lines.append(f"\nReal-time insider signals at form4.app")

    return "\n".join(lines)


def generate_breaking_script(signal: dict) -> str:
    """Generate 15-sec video storyboard for a breaking signal."""
    from pipelines.generate_daily_content import (
        fmt_value_natural, fmt_title, fmt_company_spoken, fmt_insider_spoken,
    )

    ticker = signal["ticker"]
    company = fmt_company_spoken(signal.get("company"), ticker)
    title = fmt_title(signal.get("title", ""))
    insider = fmt_insider_spoken(signal.get("insider_name", ""), title, company)
    action = "bought" if signal["trade_type"] == "buy" else "sold"
    val = fmt_value_natural(signal.get("total_value", 0))
    reason = signal["_break_reason"]

    lines = []
    lines.append(f"=== FORM4 BREAKING — ${ticker} ===")
    lines.append(f"~15 sec | single trade")
    lines.append("")

    # Hook — immediate, no buildup
    lines.append("--- HOOK (0:00–0:03) ---")
    lines.append("[SHOW: assets/breaking_card.png]")

    if reason == "rare_reversal" and signal["trade_type"] == "buy":
        lines.append(f'"Breaking. An insider at {company} just bought stock for the first time in years."')
    elif reason == "rare_reversal":
        lines.append(f'"Breaking. A long-time buyer at {company} just flipped to selling."')
    elif reason == "cluster":
        n = signal.get("_cluster_count", 3)
        lines.append(f'"Breaking. {n} insiders at {company} all {action} stock today."')
    elif signal["total_value"] >= 10_000_000:
        lines.append(f'"Breaking. {company}\'s {title} just {action} {val}."')
    else:
        lines.append(f'"Breaking. A {title} at {company} just made a big move."')

    lines.append("")

    # Body — the details
    lines.append("--- DETAILS (0:03–0:10) ---")
    lines.append("[SHOW: assets/trade_card.png]")
    lines.append(f"[OVERLAY: assets/logo.png]")

    if reason == "cluster":
        total = fmt_value_natural(signal.get("_cluster_total", 0))
        insiders = signal.get("_cluster_insiders", [])
        lines.append(f'"{insider} led the group with {val}."')
        lines.append(f'"Combined, that\'s {total} in one day."')
    else:
        lines.append(f'"{insider} {action} {val}."')

    # Why it matters
    lines.append("[SHOW: assets/chart_card.png]")
    perf = signal.get("_stock_perf")
    if perf and abs(perf["pct_change"]) > 5:
        direction = "up" if perf["pct_change"] > 0 else "down"
        pct = abs(perf["pct_change"])
        lines.append(f'"Stock is {direction} {pct:.0f}% this quarter."')

    if reason == "rare_reversal":
        lines.append('"Rare reversals beat the market by 3.6% within a month in our data."')
    elif reason == "cluster":
        lines.append('"When multiple insiders move together, the data says pay attention."')
    elif signal.get("signal_grade") == "A":
        lines.append('"A-grade signal — top tier in our quality system."')

    lines.append("")

    # CTA
    lines.append("--- CTA (0:10–0:15) ---")
    lines.append("[SHOW: assets/cta.png]")
    lines.append('"Get alerts like this in real time. form4.app, link in bio."')

    return "\n".join(lines)


def generate_breaking_captions(signal: dict) -> str:
    """Generate platform captions for breaking signal."""
    from pipelines.generate_daily_content import (
        fmt_value, fmt_title, fmt_company_spoken,
    )

    ticker = signal["ticker"]
    company = fmt_company_spoken(signal.get("company"), ticker)
    title = fmt_title(signal.get("title", ""))
    action = "bought" if signal["trade_type"] == "buy" else "sold"
    value = fmt_value(signal.get("total_value", 0))
    reason = signal["_break_reason"]

    emoji = "🟢" if signal["trade_type"] == "buy" else "🔴"
    hashtags = f"#InsiderTrading #{ticker} #StockMarket #SEC #SmartMoney #Form4"

    if reason == "rare_reversal":
        hook = f"RARE REVERSAL at {company} (${ticker})"
        detail = f"First {'buy' if signal['trade_type'] == 'buy' else 'sell'} after years — {value}"
    elif reason == "cluster":
        n = signal.get("_cluster_count", 3)
        hook = f"{n} insiders at {company} (${ticker}) all {action} today"
        detail = f"Combined value: {fmt_value(signal.get('_cluster_total', 0))}"
    else:
        hook = f"{company}'s {title} just {action} {value} (${ticker})"
        detail = f"Signal grade: {signal.get('signal_grade', '?')}"

    lines = []
    lines.append("=== INSTAGRAM / TIKTOK ===\n")
    lines.append(f"{emoji} BREAKING: {hook}")
    lines.append(f"{detail}")
    lines.append(f"\nFollow for real-time insider trade alerts")
    lines.append(f"\n{hashtags}")

    lines.append("\n\n=== X/TWITTER ===\n")
    lines.append(generate_breaking_x_post(signal))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Asset rendering
# ---------------------------------------------------------------------------

def render_breaking_assets(signal: dict, date_str: str, storyboard: str = "") -> Path:
    """Render visual assets for a breaking signal."""
    from pipelines.render_video_assets import (
        render_all_assets, card_trade, card_chart, card_cta,
        fetch_company_logo, fetch_insider_headshot,
        _img_data_uri, _mini_chart_svg, _tags_html,
        BASE_CSS, WIDTH, HEIGHT, BRAND_DIR,
        MIDNIGHT, SLATE, CLOUD, STEEL, FOG, SIGNAL_BLUE, ALPHA_GREEN, RISK_RED, AMBER,
    )
    from pipelines.generate_daily_content import (
        fmt_value, fmt_title, fmt_company_spoken, build_context_line,
        get_stock_performance, get_insider_track_record,
    )

    ticker = signal["ticker"]
    date_slug = date_str.replace("-", "")
    sig_dir = OUTPUT_DIR / f"{date_slug}_breaking_{ticker}"
    assets_dir = sig_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    # Enrich with stock perf and track record
    conn = get_connection(readonly=True)
    signal["_stock_perf"] = get_stock_performance(conn, ticker)
    signal["_track_record"] = get_insider_track_record(
        conn, signal["insider_id"], ticker, signal["trade_type"]
    )
    conn.close()

    # Fetch logo + headshot
    company = signal.get("company", "")
    logo_path = assets_dir / "logo.png"
    headshot_path = assets_dir / "headshot.png"
    fetch_company_logo(company, ticker, logo_path)
    if signal.get("insider_name"):
        fetch_insider_headshot(signal["insider_name"], company, headshot_path)

    logo_p = logo_path if logo_path.exists() else None
    hs_p = headshot_path if headshot_path.exists() else None

    # Render cards via Playwright
    from playwright.sync_api import sync_playwright

    action = "BOUGHT" if signal["trade_type"] == "buy" else "SOLD"
    action_color = ALPHA_GREEN if signal["trade_type"] == "buy" else RISK_RED
    value = fmt_value(signal.get("total_value", 0))
    company_spoken = fmt_company_spoken(company, ticker)
    title = fmt_title(signal.get("title", ""))
    grade = signal.get("signal_grade", "?")
    grade_cls = f"grade-{grade.lower()}" if grade in ("A", "B", "C") else ""
    reason = signal["_break_reason"]
    tags = _tags_html(signal)

    # Logo + headshot data URIs
    logo_html = ""
    if logo_p and logo_p.exists():
        logo_uri = _img_data_uri(logo_p)
        logo_html = f'<img src="{logo_uri}" style="width:80px;height:80px;border-radius:16px;object-fit:contain;background:#1a1a25;padding:8px" />'

    # Breaking banner card — the urgent "BREAKING" header version
    reason_label = {
        "rare_reversal": "RARE REVERSAL",
        "cluster": f"CLUSTER {'BUY' if signal['trade_type'] == 'buy' else 'SELL'}",
        "mega_cap_csuite_buy": "C-SUITE BUY",
        "mega_cap_csuite_sell": "C-SUITE SELL",
        "large_csuite_buy": "LARGE C-SUITE BUY",
        "massive_buy": "MASSIVE BUY",
        "massive_sell": "MASSIVE SELL",
    }.get(reason, "BREAKING SIGNAL")

    # Cluster detail
    cluster_html = ""
    if reason == "cluster":
        insiders = signal.get("_cluster_insiders", [])
        cluster_rows = ""
        for ins in insiders[:4]:
            ins_title = fmt_title(ins.get("title", ""))
            ins_val = fmt_value(ins.get("value", 0))
            cluster_rows += f'<div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1e1e2a"><span style="color:{STEEL}">{ins_title}</span><span style="font-family:monospace;font-weight:600">{ins_val}</span></div>'
        total_val = fmt_value(signal.get("_cluster_total", 0))
        cluster_html = f"""
        <div style="margin-top:24px">
            <div style="font-size:18px;color:{FOG};margin-bottom:8px">{signal.get('_cluster_count', 0)} insiders — combined {total_val}</div>
            {cluster_rows}
        </div>"""

    # Chart
    chart_svg = _mini_chart_svg(ticker)
    chart_html = f'<div style="margin-top:24px">{chart_svg}</div>' if chart_svg else ""

    # Performance
    perf = signal.get("_stock_perf")
    perf_html = ""
    if perf:
        pct = perf["pct_change"]
        bar_color = ALPHA_GREEN if pct > 0 else RISK_RED
        perf_html = f"""<div style="display:flex;align-items:center;gap:16px;margin-top:16px">
            <span style="font-size:18px;color:{FOG}">90d:</span>
            <span style="font-family:monospace;font-size:28px;font-weight:700;color:{bar_color}">{pct:+.1f}%</span>
            <span style="font-size:18px;color:{FOG}">@ ${perf['current_price']:.2f}</span>
        </div>"""

    breaking_card_html = f"""<!DOCTYPE html><html><head><style>{BASE_CSS}
    body {{ padding: 60px; }}
    </style></head><body>
    <div style="text-align:center;margin-bottom:32px">
        <div style="display:inline-block;padding:12px 32px;border-radius:12px;background:rgba(239,68,68,0.15);border:2px solid rgba(239,68,68,0.3)">
            <span style="font-size:20px;font-weight:800;color:{RISK_RED};text-transform:uppercase;letter-spacing:4px">{reason_label}</span>
        </div>
    </div>

    <div class="card">
        <div style="display:flex;align-items:center;gap:20px;margin-bottom:24px">
            {logo_html}
            <span class="grade {grade_cls}">{grade}</span>
            <div>
                <div style="font-size:52px;font-weight:800;font-family:monospace;letter-spacing:-1px">${ticker}</div>
                <div style="font-size:22px;color:{STEEL}">{company_spoken}</div>
            </div>
        </div>
        <div style="font-size:26px;font-weight:600;color:{STEEL}">{title}</div>
        <div style="margin-top:28px">
            <span style="color:{action_color};font-size:36px;font-weight:700">{action}</span>
            <span style="font-size:52px;font-weight:800;font-family:monospace;margin-left:16px">{value}</span>
        </div>
        {f'<div style="margin-top:16px">{tags}</div>' if tags else ''}
        {perf_html}
        {cluster_html}
    </div>
    {chart_html}
    <div style="text-align:center;margin-top:auto;padding-top:24px">
        <span style="font-size:18px;color:{FOG}">form4.app — real-time insider alerts</span>
    </div>
    </body></html>"""

    # Also render a standard trade card and chart card
    trade_card_html = card_trade(signal, 1, 1, logo_p, hs_p)
    chart_card_html = card_chart(signal)
    cta_html = card_cta()

    # Silhouette fallback if no headshot
    sil_html = None
    if not (hs_p and hs_p.exists()) and signal.get("insider_name"):
        from pipelines.render_video_assets import card_silhouette
        sil_html = card_silhouette(signal["insider_name"], title, company_spoken, logo_p)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})

        def _render(html, path):
            if not html:
                return
            page.set_content(html, wait_until="networkidle")
            page.screenshot(path=str(path), full_page=False)
            logger.info("  Rendered %s", path.name)

        _render(breaking_card_html, assets_dir / "breaking_card.png")
        _render(trade_card_html, assets_dir / "trade_card.png")
        if chart_card_html:
            _render(chart_card_html, assets_dir / "chart_card.png")
        _render(cta_html, assets_dir / "cta.png")
        if sil_html and hs_p and not hs_p.exists():
            _render(sil_html, hs_p)

        browser.close()

    return sig_dir


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Detect and generate breaking insider signals")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--dry-run", action="store_true", help="Detect only, no content generation")
    parser.add_argument("--audio", action="store_true", help="Generate ElevenLabs audio")
    parser.add_argument("--all", action="store_true", help="Generate for ALL qualifying signals (not just unsent)")
    parser.add_argument("--limit", type=int, default=3, help="Max signals to generate content for")
    args = parser.parse_args()

    conn = get_connection(readonly=True)

    signals = detect_breaking_signals(conn, args.date)
    conn.close()

    if not signals:
        logger.info("No breaking signals for %s", args.date)
        return

    logger.info("Found %d breaking signal(s) for %s:", len(signals), args.date)
    for s in signals:
        logger.info("  %s $%s — %s (%s) %s $%s [%s]",
                     s["_break_reason"], s["ticker"], s.get("insider_name", ""),
                     s.get("title", "")[:30], s["trade_type"],
                     f'{s["total_value"]/1e6:.1f}M' if s["total_value"] else "?",
                     s.get("signal_grade", "?"))

    if args.dry_run:
        return

    # Filter out already-sent signals (unless --all)
    sent = _load_sent() if not args.all else set()
    unsent = []
    for s in signals:
        key = f"{args.date}:{s['ticker']}:{s['trade_type']}:{s['_break_reason']}"
        if key not in sent:
            unsent.append((s, key))

    if not unsent:
        logger.info("All signals already sent")
        return

    # Generate content for top N unsent signals
    for signal, sent_key in unsent[:args.limit]:
        ticker = signal["ticker"]
        logger.info("Generating content for %s %s (%s)...",
                     signal["_break_reason"], ticker, signal["trade_type"])

        # Normalize field names for card_trade compatibility
        signal.setdefault("shares_after", signal.get("shares_owned_after"))
        signal.setdefault("total_qty", signal.get("total_qty", 0))

        # Scrape web context
        try:
            from pipelines.scrape_trade_context import scrape_trade_context
            ctx = scrape_trade_context(
                ticker, signal.get("company", ""), signal.get("insider_name", ""),
                signal["trade_type"], signal.get("total_value", 0),
            )
            signal["_web_context"] = ctx.get("spoken_context", "")
        except Exception:
            signal["_web_context"] = ""

        # Generate text content
        x_post = generate_breaking_x_post(signal)
        storyboard = generate_breaking_script(signal)
        captions = generate_breaking_captions(signal)

        # Generate SRT
        from pipelines.generate_daily_content import generate_srt
        srt = generate_srt(storyboard)

        # Render visual assets
        sig_dir = render_breaking_assets(signal, args.date, storyboard)

        # Save text outputs
        (sig_dir / "x_post.txt").write_text(x_post)
        (sig_dir / "storyboard.txt").write_text(storyboard)
        (sig_dir / "captions.srt").write_text(srt)
        (sig_dir / "captions_platforms.txt").write_text(captions)

        # Audio
        if args.audio:
            from pipelines.generate_daily_content import generate_audio
            generate_audio(storyboard, sig_dir / "narration.mp3")

        logger.info("Breaking signal content saved to %s", sig_dir)

        # Mark as sent
        _mark_sent(sent_key)

    logger.info("Done — %d breaking signal(s) generated", min(len(unsent), args.limit))


if __name__ == "__main__":
    main()

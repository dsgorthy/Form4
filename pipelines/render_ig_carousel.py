#!/usr/bin/env python3
"""Generate Instagram carousel assets — charts, logos, and structured trade data.

Outputs clean, high-res assets for Canva assembly:
  - charts/{TICKER}_chart.png — clean price chart (transparent text, dark bg)
  - logos/{TICKER}_logo.png — highest-res company logo available
  - trades.json — structured data for all trades (paste into Canva template)
  - cover_info.json — title, date, accent color, top tickers (for cover design)
  - caption.txt — copy-paste Instagram caption with hashtags
  - cta.png — reusable CTA slide

Event groupings: top_buys, top_sells, dip_buys, rip_sells, rare_reversals

Usage:
    python3 pipelines/render_ig_carousel.py --date 2026-03-26
    python3 pipelines/render_ig_carousel.py --date 2026-03-26 --group dip_buys
"""
from __future__ import annotations

import argparse
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

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "content"
BRAND_DIR = Path(__file__).resolve().parent.parent / "brand"

# Brand colors
SIGNAL_BLUE = "#3B82F6"
ALPHA_GREEN = "#22C55E"
RISK_RED = "#EF4444"
AMBER = "#F59E0B"
MIDNIGHT = "#0A0A0F"
SLATE = "#12121A"
CLOUD = "#E8E8ED"
STEEL = "#8888A0"
FOG = "#55556A"


# ---------------------------------------------------------------------------
# Data loading + grouping (unchanged)
# ---------------------------------------------------------------------------

def load_trades(conn: sqlite3.Connection, target_date: str) -> list[dict]:
    """Load trades for a date, compute V4 quality, filter Q7+, dedup by txn_group."""
    from pipelines.portfolio_simulator import compute_signal_quality

    # Content quality floor — only show trades we'd stake brand reputation on
    MIN_CONTENT_QUALITY = 7.0

    rows = conn.execute("""
        SELECT
            t.trade_id, t.ticker, t.company, t.insider_id,
            COALESCE(i.display_name, i.name) AS insider_name,
            t.title, t.trade_type, t.filing_date, t.trade_date,
            SUM(t.value) AS total_value,
            SUM(t.qty) AS total_qty,
            t.signal_grade, t.is_rare_reversal, t.is_csuite,
            -- Use live track record if PIT columns not backfilled
            COALESCE(t.pit_win_rate_7d, itr.buy_win_rate_7d) AS pit_win_rate_7d,
            COALESCE(t.pit_n_trades, itr.buy_count) AS pit_n_trades,
            t.insider_switch_rate,
            t.week52_proximity, t.cohen_routine, t.is_10b5_1,
            MAX(CASE WHEN t.direct_indirect = 'D' OR t.direct_indirect IS NULL
                THEN t.shares_owned_after ELSE NULL END) AS shares_owned_after,
            t.filing_key, t.txn_group_id
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE t.filing_date = ?
          AND t.trans_code IN ('P', 'S')
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND (t.is_routine = 0 OR t.is_routine IS NULL)
          AND (t.is_10b5_1 = 0 OR t.is_10b5_1 IS NULL)
          AND t.ticker NOT IN ('NONE', '')
        GROUP BY t.insider_id, t.ticker, t.trade_type, t.filing_key
        ORDER BY SUM(t.value) DESC
    """, (target_date,)).fetchall()

    trades = []
    seen_groups = set()
    for r in rows:
        r = dict(r)
        title = (r["title"] or "").lower()

        # Skip 10% owners
        if "10%" in title and not any(kw in title for kw in ["ceo", "cfo", "president", "chair", "officer"]):
            continue

        # Dedup by txn_group_id
        gid = r.get("txn_group_id")
        if gid and gid in seen_groups:
            continue
        if gid:
            seen_groups.add(gid)

        # Compute V4 quality
        csuite = any(kw in title for kw in ["ceo", "chief exec", "president", "pres",
                      "cfo", "chief financial", "coo", "evp", "svp", "vp", "vice pres"])
        holdings_pct = None
        if r["shares_owned_after"] and r["total_qty"] and r["trade_type"] == "buy":
            before = r["shares_owned_after"] - r["total_qty"]
            if before > 0:
                holdings_pct = r["total_qty"] / before

        quality, _ = compute_signal_quality(
            pit_wr=r.get("pit_win_rate_7d"),
            pit_n=r.get("pit_n_trades"),
            is_csuite=csuite,
            holdings_pct_change=holdings_pct,
            is_10pct_owner=False,
            title=r.get("title"),
            is_rare_reversal=bool(r.get("is_rare_reversal")),
            switch_rate=r.get("insider_switch_rate"),
        )

        r["_quality"] = quality
        if quality < MIN_CONTENT_QUALITY:
            continue

        # Skip tiny trades (< $10K)
        if (r["total_value"] or 0) < 10000:
            continue

        trades.append(r)

    for t in trades:
        t["_stock_perf"] = _get_stock_perf(conn, t["ticker"])
        if t["shares_owned_after"] and t["total_qty"] and t["trade_type"] == "buy":
            before = t["shares_owned_after"] - t["total_qty"]
            t["_holdings_pct_change"] = (t["total_qty"] / before) * 100 if before > 0 else 100
        else:
            t["_holdings_pct_change"] = None

        # Market cap tier from trade_context
        mc_row = conn.execute("""
            SELECT metadata FROM trade_context
            WHERE trade_id = ? AND context_type = 'market_cap_tier'
            LIMIT 1
        """, (t["trade_id"],)).fetchone()
        if mc_row and mc_row["metadata"]:
            import json as _json
            try:
                mc_data = _json.loads(mc_row["metadata"])
                t["_market_cap_tier"] = mc_data.get("tier", "")
                t["_market_cap"] = mc_data.get("market_cap", 0)
            except Exception:
                t["_market_cap_tier"] = ""
                t["_market_cap"] = 0
        else:
            t["_market_cap_tier"] = ""
            t["_market_cap"] = 0

    return trades


def _get_stock_perf(conn, ticker, days=30):
    rows = conn.execute(
        "SELECT date, close FROM daily_prices WHERE ticker = ? ORDER BY date DESC LIMIT ?",
        (ticker, days)
    ).fetchall()
    if len(rows) < 2:
        return None
    latest, oldest = rows[0]["close"], rows[-1]["close"]
    return {"current_price": latest, "pct_change": round(((latest - oldest) / oldest) * 100, 1)}


def group_trades(trades: list[dict]) -> dict[str, list[dict]]:
    """Group trades into carousel categories."""
    groups = {}
    buys = [t for t in trades if t["trade_type"] == "buy"]
    sells = [t for t in trades if t["trade_type"] == "sell"]

    # Sort by quality (primary), then value (secondary)
    top_buys = sorted(buys, key=lambda x: (-x.get("_quality", 0), -(x["total_value"] or 0)))[:8]
    if len(top_buys) >= 3:
        groups["top_buys"] = top_buys

    top_sells = sorted(sells, key=lambda x: (-x.get("_quality", 0), -(x["total_value"] or 0)))[:8]
    if len(top_sells) >= 3:
        groups["top_sells"] = top_sells

    dip_buys = [t for t in buys if t.get("_stock_perf") and t["_stock_perf"]["pct_change"] < -10]
    dip_buys.sort(key=lambda x: x["_stock_perf"]["pct_change"])
    if len(dip_buys) >= 3:
        groups["dip_buys"] = dip_buys[:8]

    rip_sells = [t for t in sells if t.get("_stock_perf") and t["_stock_perf"]["pct_change"] > 10]
    rip_sells.sort(key=lambda x: -x["_stock_perf"]["pct_change"])
    if len(rip_sells) >= 3:
        groups["rip_sells"] = rip_sells[:8]

    reversals = [t for t in trades if t.get("is_rare_reversal")]
    if len(reversals) >= 2:
        groups["rare_reversals"] = reversals[:6]

    return groups


GROUP_TITLES = {
    "top_buys": "The Largest Insider Buys",
    "top_sells": "The Largest Insider Sales",
    "dip_buys": "Large Dip Buys",
    "rip_sells": "Large Rip Sells",
    "rare_reversals": "Rare Reversals",
}

GROUP_COLORS = {
    "top_buys": ALPHA_GREEN,
    "top_sells": RISK_RED,
    "dip_buys": SIGNAL_BLUE,
    "rip_sells": AMBER,
    "rare_reversals": AMBER,
}


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fetch_sector(ticker: str) -> dict:
    """Fetch sector + industry from Yahoo Finance. Returns {sector, industry}."""
    import requests
    try:
        resp = requests.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": ticker, "quotesCount": 1, "newsCount": 0},
            headers={"User-Agent": "Form4App/1.0"},
            timeout=5,
        )
        if resp.status_code == 200:
            quotes = resp.json().get("quotes", [])
            if quotes:
                return {
                    "sector": quotes[0].get("sector", ""),
                    "industry": quotes[0].get("industry", ""),
                }
    except Exception:
        pass
    return {"sector": "", "industry": ""}


def _fmt_value(v: float) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:,.0f}"


def _fmt_title(title: str) -> str:
    if not title:
        return ""
    title = re.sub(r',?\s*10%.*$', '', title, flags=re.IGNORECASE).strip()
    expansions = {"dir": "Director", "ceo": "CEO", "cfo": "CFO", "coo": "COO",
                  "svp": "SVP", "evp": "EVP", "vp": "VP", "pres": "President"}
    words = [expansions.get(w.lower().rstrip(",."), w) for w in title.split()]
    result = " ".join(words)
    for long, short in {"Chief Executive Officer": "CEO", "Chief Financial Officer": "CFO",
                        "Chief Operating Officer": "COO",
                        "President and Chief Executive Officer": "President & CEO"}.items():
        if long.lower() in result.lower():
            return short
    return result[:40] + "..." if len(result) > 40 else result


def _fmt_company(company: str | None, ticker: str) -> str:
    if not company:
        return ticker
    cleaned = re.sub(
        r'\s*,?\s*\b(Inc\.?|Corp\.?|Corporation|Ltd\.?|Limited|LLC|PLC|plc|N\.?V\.?|S\.?A\.?|Co\.?|Company|Holdings?|Group)\s*\.?\s*$',
        '', company, flags=re.IGNORECASE
    ).strip().rstrip(',').strip()
    if cleaned and cleaned == cleaned.upper() and len(cleaned) > 3:
        cleaned = cleaned.title()
    return cleaned or ticker


# ---------------------------------------------------------------------------
# Logo fetching — highest resolution available
# ---------------------------------------------------------------------------

def _get_company_domain(company: str, ticker: str) -> str | None:
    """Find company domain via Clearbit Autocomplete."""
    import requests
    try:
        clean = re.sub(
            r'\b(Inc|Corp|Corporation|Ltd|Limited|LLC|Holdings?|Group|Co|Company|PLC)\b\.?',
            '', company, flags=re.IGNORECASE
        ).strip().rstrip(',').strip() or ticker
        resp = requests.get(
            "https://autocomplete.clearbit.com/v1/companies/suggest",
            params={"query": clean}, timeout=5,
        )
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]["domain"]
    except Exception:
        pass
    return None


def fetch_logo(company: str, ticker: str, output_path: Path) -> bool:
    """Download highest-resolution company logo available.

    Priority: apple-touch-icon (180px+) > icon.horse > google favicon.
    """
    import requests

    domain = _get_company_domain(company, ticker)
    if not domain:
        return _generate_ticker_logo(ticker, output_path)

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

    # 1. apple-touch-icon — usually 180x180, sometimes larger
    for path in ["/apple-touch-icon.png", "/apple-touch-icon-precomposed.png",
                 "/apple-touch-icon-180x180.png", "/apple-touch-icon-152x152.png"]:
        try:
            resp = requests.get(f"https://{domain}{path}", headers=headers,
                                timeout=5, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 1000:
                # Verify it's a real image, not an HTML error page
                if resp.content[:4] in (b'\x89PNG', b'\xff\xd8\xff\xe0', b'\xff\xd8\xff\xe1'):
                    output_path.write_bytes(resp.content)
                    logger.info("Logo (apple-touch-icon): %s (%s)", ticker, domain)
                    return True
        except Exception:
            pass

    # 2. icon.horse — variable quality but good coverage
    try:
        resp = requests.get(f"https://icon.horse/icon/{domain}", timeout=8)
        if resp.status_code == 200 and len(resp.content) > 500:
            # Check if it's at least 128px (skip tiny favicons)
            from PIL import Image
            import io
            img = Image.open(io.BytesIO(resp.content))
            if img.size[0] >= 128:
                # Convert to PNG if ICO
                if img.format == "ICO":
                    buf = io.BytesIO()
                    # Get the largest frame from ICO
                    if hasattr(img, 'n_frames') and img.n_frames > 1:
                        best_size = 0
                        for frame in range(img.n_frames):
                            img.seek(frame)
                            if img.size[0] > best_size:
                                best_size = img.size[0]
                                best_frame = frame
                        img.seek(best_frame)
                    img.save(buf, format="PNG")
                    output_path.write_bytes(buf.getvalue())
                else:
                    output_path.write_bytes(resp.content)
                logger.info("Logo (icon.horse %dx%d): %s (%s)", img.size[0], img.size[1], ticker, domain)
                return True
    except Exception:
        pass

    # 3. Google favicon at max size
    try:
        resp = requests.get(f"https://www.google.com/s2/favicons?domain={domain}&sz=128", timeout=5)
        if resp.status_code == 200 and len(resp.content) > 200:
            output_path.write_bytes(resp.content)
            logger.info("Logo (google 128px): %s (%s)", ticker, domain)
            return True
    except Exception:
        pass

    # 4. Generate ticker-text fallback (always succeeds)
    return _generate_ticker_logo(ticker, output_path)


def _generate_ticker_logo(ticker: str, output_path: Path, size: int = 180) -> bool:
    """Generate a dark rounded square with ticker text as fallback logo."""
    html = f"""<!DOCTYPE html><html><head><style>
    * {{ margin:0; padding:0; }}
    body {{ width:{size}px; height:{size}px; background:transparent;
           display:flex; align-items:center; justify-content:center; }}
    .logo {{ width:{size}px; height:{size}px; border-radius:{size//5}px; background:{SLATE};
             display:flex; align-items:center; justify-content:center;
             font-family:-apple-system,BlinkMacSystemFont,sans-serif;
             font-size:{size//max(len(ticker),2)}px; font-weight:800; color:{CLOUD}; letter-spacing:-1px; }}
    </style></head><body><div class="logo">{ticker}</div></body></html>"""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": size, "height": size}, device_scale_factor=2)
            page.set_content(html, wait_until="networkidle")
            page.screenshot(path=str(output_path), full_page=False, omit_background=True)
            browser.close()
        logger.info("Logo (ticker fallback): %s", ticker)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Chart rendering — clean, no text overlays, Canva-ready
# ---------------------------------------------------------------------------

def render_chart(ticker: str, trade_type: str, output_path: Path,
                 browser=None, page=None) -> bool:
    """Screenshot the product chart from form4.app for a ticker.

    Uses the real lightweight-charts candlestick chart with insider trade markers.
    Falls back to a simple SVG if form4.app is unreachable.
    """
    return _screenshot_product_chart(ticker, output_path, browser, page)


def _screenshot_product_chart(ticker: str, output_path: Path,
                              browser=None, page=None) -> bool:
    """Screenshot the chart from form4.app/company/{ticker}."""
    own_browser = False
    try:
        if not page:
            from playwright.sync_api import sync_playwright
            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1200, "height": 900}, device_scale_factor=2)
            own_browser = True

        page.goto(f"https://form4.app/company/{ticker}", wait_until="networkidle", timeout=20000)
        import time as _time
        _time.sleep(3)

        chart_el = page.query_selector('[class*="chart"], [class*="tv-lightweight"]')
        if chart_el:
            box = chart_el.bounding_box()
            if box and box["width"] > 100:
                # Screenshot full chart, then crop+resize to ~4:3 via PIL
                import io as _io
                raw = page.screenshot(
                    clip={"x": box["x"], "y": box["y"],
                          "width": box["width"], "height": box["height"]},
                )
                from PIL import Image as _Img
                img = _Img.open(_io.BytesIO(raw))
                w, h = img.size
                # Crop right 55% to focus on recent action
                crop_left = int(w * 0.45)
                img = img.crop((crop_left, 0, w, h))
                # Resize to 20:9 (1000x450) — fits 4:5 carousel with text above
                target_w = 1000
                target_h = 450
                img = img.resize((target_w, target_h), _Img.LANCZOS)
                img.save(str(output_path), "PNG")
                logger.info("Chart (form4.app): %s → %dx%d", ticker, target_w, target_h)
                return True

    except Exception as exc:
        logger.debug("Product chart failed for %s: %s", ticker, exc)
    finally:
        if own_browser and browser:
            browser.close()

    # Fallback: skip chart if form4.app unavailable (no nested Playwright)
    logger.warning("Chart unavailable for %s (form4.app unreachable)", ticker)
    return False


def _render_fallback_chart(ticker: str, output_path: Path,
                           days: int = 40, width: int = 1000, height: int = 450,
                           trade_value: float | None = None,
                           trade_date: str | None = None,
                           trade_type: str = "buy",
                           transparent_bg: bool = False) -> bool:
    """Fallback SVG chart with price (right axis), dates (bottom), and trade bar (left axis).

    Args:
        trade_value: if set, renders a single bar for this specific trade on trade_date.
                     If None, queries all insider trades for this ticker in the date range.
        trade_date: the date of the specific trade (required if trade_value is set).
        trade_type: 'buy' or 'sell' — determines bar color (green/red).
    """
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    rows = conn.execute(
        "SELECT date, close FROM daily_prices WHERE ticker = ? ORDER BY date DESC LIMIT ?",
        (ticker, days)
    ).fetchall()

    rows_list = list(reversed(rows))

    # Build trade bars data
    available_dates = {r["date"] for r in rows_list}
    trades_by_date: dict[str, list[dict]] = {}
    if trade_value and trade_date:
        # Single trade mode — snap to nearest available date if trade_date is outside range
        bar_date = trade_date
        if bar_date not in available_dates and rows_list:
            # Use the closest date (prefer last date if trade is after range)
            all_dates = sorted(available_dates)
            if bar_date > all_dates[-1]:
                bar_date = all_dates[-1]
            elif bar_date < all_dates[0]:
                bar_date = all_dates[0]
            else:
                bar_date = min(all_dates, key=lambda d: abs(
                    (datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(bar_date, "%Y-%m-%d")).days
                ))
        trades_by_date[bar_date] = [{"total_value": trade_value, "trade_type": trade_type}]
    elif len(rows_list) >= 2:
        # Auto mode — query all insider trades in the date range
        date_start = rows_list[0]["date"]
        date_end = rows_list[-1]["date"]
        trade_rows = conn.execute("""
            SELECT trade_date, SUM(value) as total_value, trade_type
            FROM trades
            WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?
              AND trans_code IN ('P', 'S')
              AND (is_duplicate = 0 OR is_duplicate IS NULL)
            GROUP BY trade_date, trade_type
        """, (ticker, date_start, date_end)).fetchall()
        for tr in trade_rows:
            trades_by_date.setdefault(tr["trade_date"], []).append(dict(tr))

    conn.close()
    rows = rows_list

    if len(rows) < 10:
        return False

    dates = [r["date"] for r in rows]
    closes = [r["close"] for r in rows]

    min_p, max_p = min(closes) * 0.96, max(closes) * 1.04
    p_range = max_p - min_p if max_p > min_p else 1

    # Layout: left for trade value labels, right for price labels, bottom for dates
    has_trades = bool(trades_by_date)
    pad_l = 75 if has_trades else 20
    pad_r = 110
    pad_t = 25
    pad_b = 45
    chart_w = width - pad_l - pad_r
    chart_h = height - pad_t - pad_b

    def cx(i):
        return pad_l + (i / (len(closes) - 1)) * chart_w
    def cy(v):
        return pad_t + (1 - (v - min_p) / p_range) * chart_h

    # Price line + area
    points = " ".join(f"{cx(i):.1f},{cy(c):.1f}" for i, c in enumerate(closes))
    area = points + f" {cx(len(closes)-1):.1f},{pad_t + chart_h} {pad_l},{pad_t + chart_h}"

    svg_parts = [
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">',
        # Gradient definition for area fill
        f'<defs><linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{SIGNAL_BLUE}" stop-opacity="0.25"/>'
        f'<stop offset="100%" stop-color="{SIGNAL_BLUE}" stop-opacity="0.02"/>'
        f'</linearGradient></defs>',
        f'<rect width="{width}" height="{height}" rx="12" fill="{"none" if transparent_bg else MIDNIGHT}"/>',
        # Area fill with gradient
        f'<polygon points="{area}" fill="url(#areaGrad)"/>',
        # Price line
        f'<polyline points="{points}" fill="none" stroke="{SIGNAL_BLUE}" stroke-width="2.5"/>',
    ]

    # Right Y-axis: price labels at round intervals
    import math
    def _nice_step(range_val, target_ticks=4):
        """Find a 'nice' step size (1, 2, 5, 10, 20, 50, 0.1, 0.2, 0.5, etc.)."""
        raw = range_val / target_ticks
        mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
        residual = raw / mag
        if residual <= 1.5:
            return mag
        elif residual <= 3.5:
            return 2 * mag
        elif residual <= 7.5:
            return 5 * mag
        else:
            return 10 * mag

    step = _nice_step(p_range)
    tick_start = math.ceil(min_p / step) * step
    ticks = []
    v = tick_start
    while v <= max_p:
        ticks.append(v)
        v += step
        v = round(v, 10)  # avoid float drift

    for price_val in ticks:
        ypos = pad_t + (1 - (price_val - min_p) / p_range) * chart_h
        # Format based on step size
        if step >= 1 and price_val >= 10:
            label = f"${price_val:,.0f}"
        elif step >= 0.1:
            label = f"${price_val:.1f}"
        elif step >= 0.01:
            label = f"${price_val:.2f}"
        else:
            label = f"${price_val:.3f}"
        # Price label
        svg_parts.append(
            f'<text x="{pad_l + chart_w + 20}" y="{ypos + 4:.1f}" '
            f'fill="{CLOUD}" font-family="JetBrains Mono,ui-monospace,monospace" font-size="22" '
            f'text-anchor="start">{label}</text>'
        )

    # Bottom X-axis: date labels (show ~5 evenly spaced dates)
    n_dates = min(5, len(dates))
    for i in range(n_dates):
        idx = int(i / (n_dates - 1) * (len(dates) - 1))
        d = dates[idx]
        # Format: "Mar 5" or "Feb 20"
        from datetime import datetime as _dt
        dt = _dt.strptime(d, "%Y-%m-%d")
        label = str(dt.day)
        xpos = cx(idx)
        svg_parts.append(
            f'<text x="{xpos:.1f}" y="{pad_t + chart_h + 20}" '
            f'fill="{CLOUD}" font-family="JetBrains Mono,ui-monospace,monospace" font-size="20" '
            f'text-anchor="middle">{label}</text>'
        )

    # Trade value bars (green=buy, red=sell) with left Y-axis
    if has_trades:
        # Build date->index lookup
        date_to_idx = {d: i for i, d in enumerate(dates)}

        # Find max trade value for left Y-axis scaling
        all_values = [t["total_value"] for tlist in trades_by_date.values() for t in tlist]
        max_trade_val = max(all_values) if all_values else 1
        bar_max_h = chart_h * 0.5  # bars reach ~halfway up

        def _fmt_value(v):
            if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
            if v >= 1_000: return f"${v/1_000:.0f}K"
            return f"${v:.0f}"

        bar_width = max(8, chart_w / len(dates) * 1.2)

        for trade_date, trade_list in trades_by_date.items():
            idx = date_to_idx.get(trade_date)
            if idx is None:
                continue
            for t in trade_list:
                val = t["total_value"]
                bar_h = (val / max_trade_val) * bar_max_h
                bx = cx(idx) - bar_width / 2
                by = pad_t + chart_h - bar_h
                color = "#22C55E" if t["trade_type"] == "buy" else "#EF4444"
                svg_parts.append(
                    f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_width:.1f}" '
                    f'height="{bar_h:.1f}" rx="3" fill="{color}"/>'
                )

        # Left Y-axis labels — span full chart height, target 3-4 labels
        label_max = max_trade_val * 1.8

        # Find the nice step that gives closest to 3-4 ticks
        def _best_step(mx):
            import math as _m
            mag = 10 ** _m.floor(_m.log10(mx)) if mx > 0 else 1
            candidates = [mag * m for m in [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50]]
            best = candidates[0]
            best_diff = 999
            for s in candidates:
                n = len([1 for i in range(1, 20) if s * i <= mx * 0.95])
                diff = abs(n - 3.5)
                if diff < best_diff and n >= 3:
                    best = s
                    best_diff = diff
            return best

        trade_step = _best_step(label_max)
        # Collect tick values first to check formatting
        tick_vals = []
        tv = trade_step
        while tv <= label_max * 0.95:
            tick_vals.append(tv)
            tv += trade_step
            tv = round(tv, 2)

        # Use consistent units across all labels — pick M if any tick >= 1M, else K
        any_millions = any(tv >= 1_000_000 for tv in tick_vals)

        def _fmt_trade_label(v):
            if any_millions:
                raw = v / 1_000_000
                # Drop decimal if all ticks are whole millions
                if all(tv / 1_000_000 == int(tv / 1_000_000) for tv in tick_vals):
                    return f"${raw:.0f}M"
                return f"${raw:.1f}M"
            raw = v / 1_000
            if all(tv / 1_000 == int(tv / 1_000) for tv in tick_vals):
                return f"${raw:.0f}K"
            return f"${raw:.1f}K"

        for tv in tick_vals:
            label_y = pad_t + chart_h - (tv / label_max) * chart_h
            if label_y >= pad_t - 5:
                svg_parts.append(
                    f'<text x="{pad_l - 8}" y="{label_y + 5:.1f}" '
                    f'fill="{CLOUD}" font-family="JetBrains Mono,ui-monospace,monospace" font-size="18" '
                    f'text-anchor="end">{_fmt_trade_label(tv)}</text>'
                )

    # Current price dot (skip if trade bar is present — it would overlap)
    if not has_trades:
        last_price = closes[-1]
        last_x = cx(len(closes) - 1)
        last_y = cy(last_price)
        svg_parts.append(
            f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="4" fill="{SIGNAL_BLUE}"/>'
        )

    svg_parts.append('</svg>')
    svg = "\n".join(svg_parts)

    bg_css = "background:transparent" if transparent_bg else f"background:{MIDNIGHT}"
    html = (f'<!DOCTYPE html><html><head>'
            f'<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">'
            f'<style>*{{margin:0;padding:0}}html,body{{width:{width}px;height:{height}px;{bg_css}}}</style>'
            f'</head><body>{svg}</body></html>')

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": width, "height": height})
        page.set_content(html, wait_until="networkidle")
        page.screenshot(path=str(output_path), full_page=False,
                        omit_background=transparent_bg)
        browser.close()

    return True


# ---------------------------------------------------------------------------
# CTA slide — rendered once, reused everywhere
# ---------------------------------------------------------------------------

def render_cta(output_path: Path):
    """Render the reusable CTA slide."""
    from pipelines.render_video_assets import _img_data_uri

    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4_tagline.png")
    logo_html = (
        f'<img src="{logo_uri}" style="width:55%;max-height:180px;object-fit:contain" />'
        if logo_uri else
        f'<div style="font-size:56px;font-weight:800">Form<span style="color:{SIGNAL_BLUE}">4</span></div>'
    )

    html = f"""<!DOCTYPE html><html><head><style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{ width:1080px; height:1080px; background:#FFFFFF;
           font-family:'Inter',-apple-system,sans-serif;
           display:flex; flex-direction:column; justify-content:center; align-items:center; text-align:center; padding:80px; }}
    </style></head><body>
    <div style="margin-bottom:60px">{logo_html}</div>
    <div style="font-size:36px;font-weight:800;color:#1a1a1a;line-height:1.3;max-width:800px">
        Real-time insider trade alerts.<br/>AI signal grading.
    </div>
    <div style="margin-top:40px">
        <span style="font-size:28px;font-weight:700;color:{ALPHA_GREEN}">7-day free trial</span>
    </div>
    <div style="margin-top:24px">
        <span style="font-size:36px;font-weight:800;color:{SIGNAL_BLUE}">form4.app</span>
    </div>
    </body></html>"""

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1080, "height": 1080})
        page.set_content(html, wait_until="networkidle")
        page.screenshot(path=str(output_path), full_page=False)
        browser.close()


# ---------------------------------------------------------------------------
# Main pipeline — output clean assets + structured data
# ---------------------------------------------------------------------------

def generate_carousel_assets(trades: list[dict], group_name: str, date_str: str) -> Path:
    """Generate all assets for one carousel group."""
    date_slug = date_str.replace("-", "")
    group_dir = OUTPUT_DIR / f"{date_slug}_carousel_{group_name}"
    charts_dir = group_dir / "charts"
    logos_dir = group_dir / "logos"
    charts_dir.mkdir(parents=True, exist_ok=True)
    logos_dir.mkdir(parents=True, exist_ok=True)

    # Date formatting
    _d = datetime.strptime(date_str, "%Y-%m-%d")
    _day = _d.day
    _sfx = "th" if 11 <= _day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(_day % 10, "th")
    date_fmt = f"{_d.strftime('%B')} {_day}{_sfx}, {_d.year}"

    # Fetch sector/industry for each ticker (Finviz, lightweight)
    logger.info("Fetching sector data...")
    sector_cache: dict[str, dict] = {}
    for t in trades:
        ticker = t["ticker"]
        if ticker not in sector_cache:
            sector_cache[ticker] = _fetch_sector(ticker)
            time.sleep(0.2)

    # Fetch logos (deduplicated by ticker)
    seen_tickers = set()
    for t in trades:
        ticker = t["ticker"]
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        logo_path = logos_dir / f"{ticker}.png"
        fetch_logo(t.get("company", ""), ticker, logo_path)
        time.sleep(0.15)

    # Render charts — use single browser instance for speed
    logger.info("Rendering charts from form4.app...")
    from playwright.sync_api import sync_playwright
    rendered_tickers = set()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1200, "height": 900}, device_scale_factor=2)
        for t in trades:
            ticker = t["ticker"]
            if ticker in rendered_tickers:
                continue
            rendered_tickers.add(ticker)
            chart_path = charts_dir / f"{ticker}.png"
            render_chart(ticker, t["trade_type"], chart_path, browser=browser, page=page)
        browser.close()

    # Render CTA
    cta_path = group_dir / "cta.png"
    render_cta(cta_path)

    # Build structured trade data (for Canva paste)
    trades_data = []
    for i, t in enumerate(trades):
        ticker = t["ticker"]
        company = _fmt_company(t.get("company"), ticker)
        title = _fmt_title(t.get("title", ""))
        value = _fmt_value(t.get("total_value", 0))
        action = "purchased" if t["trade_type"] == "buy" else "sold"
        perf = t.get("_stock_perf")
        hpc = t.get("_holdings_pct_change")

        entry = {
            "slide_number": i + 2,  # 1 = cover (Canva)
            "ticker": ticker,
            "company": company,
            "title": title,
            "action": action,
            "value": value,
            "signal_grade": t.get("signal_grade", ""),
        }

        # Tags (sector, industry, market cap tier) — for Canva template
        sec = sector_cache.get(ticker, {})
        entry["sector"] = sec.get("sector", "")
        entry["industry"] = sec.get("industry", "")
        entry["market_cap_tier"] = t.get("_market_cap_tier", "")
        entry["tags"] = [x for x in [sec.get("sector", ""), sec.get("industry", ""), t.get("_market_cap_tier", "")] if x]

        # Stock performance
        if perf:
            direction = "up" if perf["pct_change"] > 0 else "down"
            entry["stock_perf"] = f"{direction} {abs(perf['pct_change']):.0f}% last month"
            entry["current_price"] = f"${perf['current_price']:.2f}"
        else:
            entry["stock_perf"] = ""
            entry["current_price"] = ""

        # Holdings change
        if hpc and hpc > 1:
            if t["trade_type"] == "buy":
                entry["holdings_change"] = f"+{hpc:.0f}%"
            else:
                entry["holdings_change"] = f"-{hpc:.0f}%"
        else:
            entry["holdings_change"] = ""

        # Context tags
        tags = []
        if t.get("is_rare_reversal"):
            tags.append("Rare Reversal" if t["trade_type"] == "buy" else "Rare Reversal (Sell)")
        if perf and t["trade_type"] == "buy" and perf["pct_change"] < -10:
            tags.append("Dip Buy")
        if perf and t["trade_type"] == "sell" and perf["pct_change"] > 10:
            tags.append("Rip Sell")
        entry["context_tags"] = tags

        # Description (one paragraph, CEO Watcher style)
        desc_parts = [f'{title} at {company} ${ticker} {action} {value}']
        if hpc and hpc > 5:
            verb = "increased" if t["trade_type"] == "buy" else "decreased"
            desc_parts.append(f"This {verb} their listed holdings by {hpc:.0f}%")
        if perf and abs(perf["pct_change"]) > 5:
            direction = "up" if perf["pct_change"] > 0 else "down"
            if t["trade_type"] == "buy" and perf["pct_change"] < -10:
                desc_parts.append(f"Dip Buy: stock was {direction} {abs(perf['pct_change']):.0f}% in the previous month")
            elif t["trade_type"] == "sell" and perf["pct_change"] > 10:
                desc_parts.append(f"Rip Sell: stock {direction} {abs(perf['pct_change']):.0f}% in the previous month")
        if t.get("is_rare_reversal"):
            if t["trade_type"] == "buy":
                desc_parts.append("First purchase after years of only selling")
            else:
                desc_parts.append("First sale after years of only buying")
        entry["description"] = ". ".join(desc_parts) + "."

        # Asset paths (relative to group_dir)
        entry["chart_file"] = f"charts/{ticker}.png"
        entry["logo_file"] = f"logos/{ticker}.png"
        entry["logo_exists"] = (logos_dir / f"{ticker}.png").exists()
        entry["chart_exists"] = (charts_dir / f"{ticker}.png").exists()
        entry["quality"] = t.get("_quality", 0)

        # Drop trades without a rendered chart — consistency matters
        if not entry["chart_exists"]:
            logger.info(f"Dropping {ticker} — no chart rendered")
            continue

        trades_data.append(entry)

    # Renumber slides after dropping chartless trades
    for i, entry in enumerate(trades_data):
        entry["slide_number"] = i + 2  # 1 = cover

    # Write trades.json
    (group_dir / "trades.json").write_text(json.dumps(trades_data, indent=2))
    logger.info(f"{len(trades_data)} trades with charts (Q7+) for {group_name}")

    # Write cover_info.json
    cover_info = {
        "group": group_name,
        "title": GROUP_TITLES.get(group_name, group_name),
        "date": date_str,
        "date_display": date_fmt,
        "accent_color": GROUP_COLORS.get(group_name, SIGNAL_BLUE),
        "num_trades": len(trades),
        "total_slides": len(trades) + 2,  # cover + trades + CTA
        "top_tickers": [t["ticker"] for t in trades[:3]],
        "top_values": [_fmt_value(t["total_value"]) for t in trades[:3]],
        "top_companies": [_fmt_company(t.get("company"), t["ticker"]) for t in trades[:3]],
    }
    (group_dir / "cover_info.json").write_text(json.dumps(cover_info, indent=2))

    # Write caption
    caption = _generate_caption(trades, group_name, date_fmt)
    (group_dir / "caption.txt").write_text(caption)

    logger.info("Carousel assets: %s — %d trades, charts + logos + data in %s",
                group_name, len(trades), group_dir.name)
    return group_dir


def _generate_caption(trades: list[dict], group_name: str, date_fmt: str) -> str:
    title = GROUP_TITLES.get(group_name, group_name)
    hashtags = "#InsiderTrading #StockMarket #SEC #SmartMoney #Form4 #Investing"
    lines = [f"{title} — {date_fmt}\n"]
    seen = set()
    for t in trades[:5]:
        if t["ticker"] in seen:
            continue
        seen.add(t["ticker"])
        emoji = "\U0001f7e2" if t["trade_type"] == "buy" else "\U0001f534"
        company = _fmt_company(t.get("company"), t["ticker"])
        title_str = _fmt_title(t.get("title", ""))
        val = _fmt_value(t["total_value"])
        action = "bought" if t["trade_type"] == "buy" else "sold"
        lines.append(f"{emoji} ${t['ticker']} — {title_str} {action} {val}")
    lines.append(f"\nReal-time alerts + signal grading at form4.app")
    lines.append(f"Link in bio for 7-day free trial")
    lines.append(f"\n{hashtags}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate Instagram carousel assets")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--group", help="Only this group (top_buys, top_sells, dip_buys, rip_sells, rare_reversals)")
    parser.add_argument("--min-trades", type=int, default=3)
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    trades = load_trades(conn, args.date)
    conn.close()

    if not trades:
        logger.warning("No trades for %s", args.date)
        return

    logger.info("Loaded %d trades for %s", len(trades), args.date)
    groups = group_trades(trades)

    if args.group:
        if args.group not in groups:
            available = ", ".join(groups.keys()) if groups else "none"
            logger.warning("Group '%s' not available. Available: %s", args.group, available)
            return
        generate_carousel_assets(groups[args.group][:6], args.group, args.date)
        return

    if not groups:
        logger.warning("No groups with enough trades for %s", args.date)
        return

    for name, group_list in groups.items():
        if len(group_list) < args.min_trades:
            continue
        generate_carousel_assets(group_list[:6], name, args.date)

    logger.info("Done — %d carousel group(s)", len([g for g in groups.values() if len(g) >= args.min_trades]))


if __name__ == "__main__":
    main()

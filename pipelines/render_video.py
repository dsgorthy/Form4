#!/usr/bin/env python3
"""Render daily content video using actual product screenshots from form4.app."""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONTENT_DIR = Path(__file__).resolve().parent / "data" / "content"
DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"  # daily_prices, option_prices
SITE_URL = "https://form4.app"
WIDTH = 1080
HEIGHT = 1920

# Only the hook, reveal text overlay, and CTA use custom HTML.
# Everything else is real product screenshots.

BASE_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  width: 1080px; height: 1920px;
  background: #0A0A0F;
  color: #E8E8ED;
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif;
  padding: 60px 50px;
  display: flex; flex-direction: column;
  justify-content: center;
}
.brand { color: #3B82F6; font-weight: 700; }
"""


def _topbar_html(date_str: str) -> str:
    date_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d, %Y")
    return f"""<div style="position:absolute;top:40px;left:50px;right:50px;display:flex;justify-content:space-between;align-items:center">
      <div style="font-size:22px;font-weight:700">Form<span style="color:#3B82F6">4</span> Daily Signal</div>
      <div style="font-size:18px;color:#55556A">{date_fmt}</div>
    </div>"""


BRAND_DIR = Path(__file__).resolve().parent.parent / "brand"


def _img_data_uri(path: Path, max_w: int = 800) -> str:
    """Convert an image file to a base64 data URI for embedding in HTML."""
    import base64
    if not path.exists():
        return ""
    data = base64.b64encode(path.read_bytes()).decode()
    return f"data:image/png;base64,{data}"


def slide_hook_html(hook_text: str, date_str: str, stats: dict, num_trades: int) -> str:
    logo_path = BRAND_DIR / "wordmark_form4.png"
    logo_uri = _img_data_uri(logo_path)
    logo_html = f'<img src="{logo_uri}" style="width:85%;max-height:300px;object-fit:contain" />' if logo_uri else '<div style="font-size:140px;font-weight:800">Form<span style="color:#3B82F6">4</span></div>'

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}</style></head><body>
    <div style="text-align:center">
      {logo_html}
      <div style="font-size:48px;color:#55556A;margin-top:60px">Top Insider Trades</div>
    </div>
    </body></html>"""


def slide_cta_html() -> str:
    tagline_logo = BRAND_DIR / "wordmark_form4_tagline.png"
    logo_uri = _img_data_uri(tagline_logo)
    logo_html = f'<img src="{logo_uri}" style="width:80%;max-height:400px;object-fit:contain" />' if logo_uri else '<div style="font-size:64px;font-weight:800">Form<span style="color:#3B82F6">4</span></div>'

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}</style></head><body>
    <div style="text-align:center">
      {logo_html}
      <div style="font-size:28px;color:#22C55E;margin-top:48px;font-weight:600">7-day free trial — no credit card</div>
      <div style="font-size:44px;font-weight:700;margin-top:20px;color:#3B82F6">form4.app</div>
    </div>
    </body></html>"""


def _build_scorecard_slide(trade: dict, date_str: str) -> str | None:
    """Build an insider scorecard slide showing past trade outcomes as a clean list."""
    import sqlite3

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    conn.row_factory = sqlite3.Row

    # Pull ALL trades by this insider across all tickers (broader track record)
    past_trades = conn.execute("""
        SELECT MIN(t.trade_date) as trade_date, t.ticker, t.trade_type, SUM(t.value) as value,
               tr.return_7d, tr.return_30d, tr.return_90d
        FROM trades t LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.insider_id = ?
          AND t.trans_code IN ('P','S') AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        GROUP BY t.filing_key ORDER BY t.trade_date DESC LIMIT 10
    """, (trade["insider_id"],)).fetchall()

    # Stock performance
    perf = conn.execute(
        "SELECT close FROM daily_prices WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        (trade["ticker"],),
    ).fetchone()
    conn.close()

    past_trades = [dict(t) for t in past_trades]
    # Only show scorecard if insider has a GOOD track record worth showcasing
    trades_with_returns = [t for t in past_trades if t.get("return_30d") is not None]
    if len(trades_with_returns) < 3:
        return None
    wins = sum(1 for t in trades_with_returns if (t["return_30d"] > 0) == (t["trade_type"] == trade["trade_type"]))
    win_rate = wins / len(trades_with_returns)
    avg_ret = sum(t["return_30d"] for t in trades_with_returns) / len(trades_with_returns)
    # Skip if win rate < 60% or average return is negligible
    if win_rate < 0.6 or abs(avg_ret) < 1.0:
        return None

    from pipelines.generate_daily_content import fmt_company_spoken, fmt_value
    company = fmt_company_spoken(trade.get("company"), trade["ticker"])

    # Build trade rows
    trades_with_ret = [t for t in past_trades if t.get("return_30d") is not None]
    wins = sum(1 for t in trades_with_ret if (t["return_30d"] > 0) == (t["trade_type"] == "buy"))
    total = len(trades_with_ret)
    avg_ret = sum(t["return_30d"] for t in trades_with_ret) / total if total else 0

    rows_html = ""
    for t in past_trades[:6]:
        action = "BUY" if t["trade_type"] == "buy" else "SELL"
        action_color = "#22C55E" if t["trade_type"] == "buy" else "#EF4444"
        val = fmt_value(t["value"] or 0)
        ret_30 = t.get("return_30d")
        tkr = t.get("ticker", "")

        if ret_30 is not None:
            is_win = (ret_30 > 0) == (t["trade_type"] == "buy")
            ret_color = "#22C55E" if is_win else "#EF4444"
            ret_str = f'{ret_30:+.1f}%'
            icon = "✓" if is_win else "✗"
        else:
            ret_color = "#55556A"
            ret_str = "—"
            icon = ""

        rows_html += f"""
        <div style="display:flex;align-items:center;padding:18px 0;border-bottom:1px solid #2A2A3A">
          <div style="width:80px;font-size:16px;color:#55556A">{t['trade_date'][:10]}</div>
          <div style="width:70px;font-size:18px;font-weight:800;font-family:monospace;color:#E8E8ED">{tkr}</div>
          <div style="width:60px;font-size:18px;font-weight:700;color:{action_color}">{action}</div>
          <div style="flex:1;font-size:18px;color:#8888A0;font-family:monospace">{val}</div>
          <div style="width:90px;text-align:right;font-size:22px;font-weight:700;font-family:monospace;color:{ret_color}">{ret_str}</div>
          <div style="width:32px;text-align:center;font-size:20px;color:{ret_color}">{icon}</div>
        </div>"""

    # Summary stats
    wr_color = "#22C55E" if total and wins > total / 2 else "#EF4444" if total else "#55556A"
    ret_color = "#22C55E" if avg_ret > 0 else "#EF4444"

    insider_name = trade.get("insider_name", "")
    title_clean = trade.get("_fmt_title", "")

    return f"""<!DOCTYPE html><html><head><style>{BASE_CSS}</style></head><body>
    <div style="text-align:center;margin-bottom:32px">
      <div style="font-size:30px;font-weight:600;color:#F59E0B;text-transform:uppercase;letter-spacing:3px">Track Record</div>
      <div style="font-size:44px;font-weight:700;margin-top:20px">{insider_name}</div>
      <div style="font-size:24px;color:#8888A0;margin-top:6px">{title_clean} at {company}</div>
    </div>

    <div style="background:#12121A;border:1px solid #2A2A3A;border-radius:16px;padding:28px 32px">
      <div style="display:flex;align-items:center;padding-bottom:14px;border-bottom:2px solid #2A2A3A;margin-bottom:4px">
        <div style="width:80px;font-size:13px;color:#55556A;text-transform:uppercase">Date</div>
        <div style="width:70px;font-size:13px;color:#55556A;text-transform:uppercase">Ticker</div>
        <div style="width:60px;font-size:13px;color:#55556A;text-transform:uppercase">Type</div>
        <div style="flex:1;font-size:13px;color:#55556A;text-transform:uppercase">Value</div>
        <div style="width:90px;text-align:right;font-size:13px;color:#55556A;text-transform:uppercase">30d Return</div>
        <div style="width:32px"></div>
      </div>
      {rows_html}
    </div>

    <div style="display:flex;gap:48px;justify-content:center;margin-top:36px">
      <div style="text-align:center">
        <div style="font-size:18px;color:#55556A">Win Rate</div>
        <div style="font-size:44px;font-weight:800;font-family:monospace;color:{wr_color}">{wins}/{total}</div>
      </div>
      <div style="text-align:center">
        <div style="font-size:18px;color:#55556A">Avg 30d Return</div>
        <div style="font-size:44px;font-weight:800;font-family:monospace;color:{ret_color}">{avg_ret:+.1f}%</div>
      </div>
    </div>
    </body></html>"""


def render_all(trades: list[dict], hook_text: str, date_str: str, stats: dict, output_dir: Path) -> list[Path]:
    """Render slides: hook + CTA as HTML, trade cards as real product screenshots."""
    from playwright.sync_api import sync_playwright

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        # -- HTML slides (hook + CTA) --
        html_page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})

        # Hook
        num_trades = min(len(trades), 5)
        html_page.set_content(slide_hook_html(hook_text, date_str, stats, num_trades), wait_until="networkidle")
        hook_path = output_dir / "hook.png"
        html_page.screenshot(path=str(hook_path), full_page=False)
        paths.append(hook_path)
        logger.info("Rendered hook.png")

        html_page.close()

        # -- Product screenshots for each trade --
        # Use mobile viewport to get the mobile-optimized layout
        product_page = browser.new_page(
            viewport={"width": 430, "height": 932},  # iPhone 15 Pro Max
            device_scale_factor=2,
        )

        # Screenshot each trade's filing page and company chart
        supporting = list(reversed(trades[1:5]))
        blockbuster = trades[0]
        all_trades_ordered = supporting + [blockbuster]

        for i, t in enumerate(all_trades_ordered):
            trade_id = t.get("_encoded_trade_id") or t.get("trade_id_encoded")
            ticker = t["ticker"]

            # Filing detail page
            if trade_id:
                try:
                    product_page.goto(f"{SITE_URL}/filing/{trade_id}", timeout=15000, wait_until="networkidle")
                    product_page.wait_for_timeout(1500)  # let charts render
                    path = output_dir / f"trade_{i+1}_filing.png"
                    product_page.screenshot(path=str(path), full_page=False)
                    paths.append(path)
                    logger.info("Rendered trade_%d_filing.png (%s)", i + 1, ticker)
                except Exception as exc:
                    logger.warning("Failed to screenshot filing %s: %s", trade_id, exc)

        # Insider scorecard for the blockbuster
        chart_html = _build_scorecard_slide(blockbuster, date_str)
        if chart_html:
            html_page2 = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})
            html_page2.set_content(chart_html, wait_until="networkidle")
            chart_path = output_dir / "chart.png"
            html_page2.screenshot(path=str(chart_path), full_page=False)
            paths.append(chart_path)
            html_page2.close()
            logger.info("Rendered chart.png (%s)", blockbuster["ticker"])

        product_page.close()

        # CTA slide
        cta_page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})
        cta_page.set_content(slide_cta_html(), wait_until="networkidle")
        cta_path = output_dir / "cta.png"
        cta_page.screenshot(path=str(cta_path), full_page=False)
        paths.append(cta_path)
        logger.info("Rendered cta.png")
        cta_page.close()

        browser.close()

    # Resize product screenshots to 1080x1920 (they're iPhone viewport, need to scale up)
    for path in paths:
        if "filing" in path.name or "chart" in path.name:
            _resize_to_video(path)

    return paths


def _resize_to_video(path: Path) -> None:
    """Resize a mobile screenshot to fit within 1080x1920, centered with dark padding."""
    tmp = path.with_suffix(".tmp.png")
    try:
        subprocess.run([
            "ffmpeg", "-y", "-i", str(path),
            "-vf", (
                f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,"
                f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=0x0A0A0F"
            ),
            str(tmp),
        ], capture_output=True, timeout=10)
        if tmp.exists():
            tmp.rename(path)
    except Exception:
        if tmp.exists():
            tmp.unlink()


def assemble_video(slide_paths: list[Path], audio_path: Path, output_path: Path) -> bool:
    if not audio_path.exists():
        logger.warning("No audio file, skipping video")
        return False

    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", str(audio_path)],
        capture_output=True, text=True,
    )
    audio_duration = float(result.stdout.strip())
    logger.info("Audio duration: %.1f seconds", audio_duration)

    n_slides = len(slide_paths)

    # Parse the script to get per-slide character counts for timing.
    # Script structure: [HOOK] -> Trade 1/2/3/4 -> [REVEAL] -> [CTA]
    # Map directly to slide order.
    date_slug = Path(output_path.stem).stem.replace("_video", "")
    script_path = CONTENT_DIR / f"{date_slug}_video_script.txt"

    slide_chars = []
    if script_path.exists():
        script = script_path.read_text()

        # Extract spoken text per logical section
        hook_chars = 0
        trade_chars: list[int] = []
        reveal_chars = 0
        cta_chars = 0
        current_section = ""
        current_trade_chars = 0

        for line in script.split("\n"):
            line = line.strip()
            if line.startswith("[HOOK"):
                current_section = "hook"
            elif line.startswith("[BODY"):
                current_section = "body"
            elif line.startswith("[REVEAL"):
                if current_section == "body" and current_trade_chars > 0:
                    trade_chars.append(current_trade_chars)
                current_section = "reveal"
            elif line.startswith("[CTA"):
                current_section = "cta"
            elif line.startswith("Trade ") and line.endswith(":"):
                if current_trade_chars > 0:
                    trade_chars.append(current_trade_chars)
                current_trade_chars = 0
            elif line.startswith('"') and line.endswith('"'):
                chars = len(line) - 2
                if current_section == "hook":
                    hook_chars += chars
                elif current_section == "body":
                    current_trade_chars += chars
                elif current_section == "reveal":
                    reveal_chars += chars
                elif current_section == "cta":
                    cta_chars += chars
        if current_section == "body" and current_trade_chars > 0:
            trade_chars.append(current_trade_chars)

        # Map to slides: hook(1) + filing slides(N) + scorecard?(1) + reveal filing(last trade) + cta(1)
        # The reveal filing slide is the last trade_N_filing, scorecard is chart.png
        n_filing_slides = len([p for p in slide_paths if "filing" in p.stem])
        has_scorecard = any("chart" in p.stem for p in slide_paths)

        for p in slide_paths:
            name = p.stem
            if "hook" in name:
                slide_chars.append(max(hook_chars, 30))
            elif "filing" in name:
                # Which trade index is this?
                # trade_1_filing = supporting[0], ... trade_N_filing = blockbuster
                idx_str = name.split("_")[1]
                try:
                    idx = int(idx_str) - 1
                except ValueError:
                    idx = 0
                if idx == n_filing_slides - 1:
                    # This is the blockbuster/reveal filing — gets reveal narration
                    if has_scorecard:
                        slide_chars.append(reveal_chars * 0.5)
                    else:
                        slide_chars.append(reveal_chars)
                elif idx < len(trade_chars):
                    slide_chars.append(trade_chars[idx])
                else:
                    slide_chars.append(80)
            elif "chart" in name:
                # Scorecard gets the other half of reveal narration
                slide_chars.append(reveal_chars * 0.5)
            elif "cta" in name:
                slide_chars.append(max(cta_chars, 30))
            else:
                slide_chars.append(80)

    if not slide_chars or len(slide_chars) != len(slide_paths):
        slide_chars = [1.0] * len(slide_paths)

    total_c = sum(slide_chars)
    # Add 2s buffer to total to prevent visuals cutting out before audio ends
    durations = [(c / total_c) * (audio_duration + 2.0) for c in slide_chars]

    # Log timing for debugging
    for p, d in zip(slide_paths, durations):
        logger.info("  %s: %.1fs", p.stem, d)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        for path, dur in zip(slide_paths, durations):
            f.write(f"file '{path}'\n")
            f.write(f"duration {dur:.2f}\n")
        f.write(f"file '{slide_paths[-1]}'\n")
        concat_file = f.name

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0", "-i", concat_file,
        "-i", str(audio_path),
        "-vf", f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=0x0A0A0F,format=yuv420p",
        "-c:v", "libx264", "-preset", "medium", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        str(output_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    os.unlink(concat_file)

    if result.returncode == 0:
        logger.info("Video saved: %s (%.1f MB)", output_path, output_path.stat().st_size / 1e6)
        return True
    logger.error("ffmpeg failed: %s", result.stderr[-500:])
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--slides-only", action="store_true")
    args = parser.parse_args()

    date_slug = args.date.replace("-", "")

    from pipelines.generate_daily_content import (
        get_top_trades, get_daily_stats, build_context_line, fmt_value, fmt_title,
        DB_PATH as GEN_DB_PATH,
    )
    from api.id_encoding import encode_trade_id
    import sqlite3

    conn = sqlite3.connect(f"file:{GEN_DB_PATH}?mode=ro", uri=True)
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    conn.row_factory = sqlite3.Row
    trades = get_top_trades(conn, args.date)
    stats = get_daily_stats(conn, args.date)
    conn.close()

    if not trades:
        print(f"No trades for {args.date}")
        return

    for t in trades:
        t["_fmt_value"] = fmt_value(t["total_value"])
        t["_fmt_title"] = fmt_title(t["title"] or "")
        t["_context_line"] = build_context_line(t)
        # Encode trade_id for URL
        if t.get("trade_id"):
            t["_encoded_trade_id"] = encode_trade_id(t["trade_id"])

    bb = trades[0]
    from pipelines.generate_daily_content import fmt_value_spoken, fmt_company_spoken
    bb_value_spoken = fmt_value_spoken(bb["total_value"])
    bb_company = fmt_company_spoken(bb.get("company"), bb["ticker"])
    date_spoken = datetime.strptime(args.date, "%Y-%m-%d").strftime("%B %d")

    # Tease
    if bb["is_rare_reversal"]:
        tease = "The last one is a rare reversal — the strongest signal type in our data."
    elif bb["total_value"] >= 10_000_000:
        tease = f"The last one is a {bb_value_spoken} move you need to see."
    elif bb.get("signal_grade") == "A":
        tease = "The last one is the strongest buy signal this week."
    else:
        tease = f"The last one is a {bb_value_spoken} bet that could mean something big."

    hook = f"Top insider trades for {date_spoken}. {tease}"

    slide_dir = CONTENT_DIR / f"{date_slug}_slides"
    slide_paths = render_all(trades, hook, args.date, stats, slide_dir)

    if args.slides_only:
        return

    audio_path = CONTENT_DIR / f"{date_slug}_narration.mp3"
    video_path = CONTENT_DIR / f"{date_slug}_video.mp4"
    assemble_video(slide_paths, audio_path, video_path)


if __name__ == "__main__":
    main()

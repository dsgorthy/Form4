#!/usr/bin/env python3
"""Generate weekly performance snapshot for Form4 A/B-grade insider signals.

Shows how signals from 7-14 days ago actually performed, generating a branded
scorecard image, social copy, and a short video storyboard.

Usage:
    python3 pipelines/generate_weekly_snapshot.py                        # last complete week
    python3 pipelines/generate_weekly_snapshot.py --end-date 2026-03-21  # specific week ending

Outputs to: pipelines/data/content/weekly_{YYYYMMDD}/
    scorecard.png, x_post.txt, captions_platforms.txt, storyboard.txt
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from pipelines.portfolio_simulator import compute_signal_quality
except ImportError:
    from portfolio_simulator import compute_signal_quality

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from pipelines.render_video_assets import _img_data_uri, BASE_CSS, WIDTH, HEIGHT, BRAND_DIR

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
CONTENT_DIR = Path(__file__).resolve().parent / "data" / "content"

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


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _compute_week_range(end_date: date) -> tuple[date, date]:
    """Compute the 7-day window of filing dates to evaluate.

    We look at signals filed 7-14 days before end_date so that 7-day returns
    have had time to be computed.
    """
    window_end = end_date - timedelta(days=7)
    window_start = end_date - timedelta(days=13)
    return window_start, window_end


def _is_win(trade_type: str, return_7d: float | None) -> bool | None:
    """Determine if a trade was a win based on direction.

    Buys win when return_7d > 0, sells win when return_7d < 0.
    """
    if return_7d is None:
        return None
    if trade_type == "buy":
        return return_7d > 0
    else:  # sell
        return return_7d < 0


def _effective_return(trade_type: str, return_val: float | None) -> float | None:
    """Return the directional return (flip sign for sells so positive = good)."""
    if return_val is None:
        return None
    if trade_type == "sell":
        return -return_val
    return return_val


def load_weekly_signals(conn: sqlite3.Connection, window_start: date, window_end: date) -> dict:
    """Load A/B grade signals from the window and compute performance stats."""

    start_str = window_start.isoformat()
    end_str = window_end.isoformat()

    # V4 quality-scored signals with returns
    rows = conn.execute("""
        SELECT
            t.trade_id,
            t.ticker,
            t.company,
            t.trade_type,
            t.signal_grade,
            t.value,
            t.filing_date,
            t.is_csuite,
            t.title,
            tr.return_7d,
            tr.return_30d,
            COALESCE(t.pit_win_rate_7d, itr.buy_win_rate_7d) AS pit_win_rate_7d,
            COALESCE(t.pit_n_trades, itr.buy_count) AS pit_n_trades,
            t.insider_switch_rate,
            t.is_rare_reversal
        FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE t.filing_date BETWEEN ? AND ?
          AND t.trans_code IN ('P', 'S')
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        ORDER BY t.value DESC
    """, (start_str, end_str)).fetchall()

    # Compute V4 quality and filter
    signals = []
    for r in [dict(r) for r in rows]:
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
        signals.append(r)

    # Count routine/planned trades that were filtered out
    routine_count = conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE filing_date BETWEEN ? AND ?
          AND trans_code IN ('P', 'S')
          AND (is_duplicate = 0 OR is_duplicate IS NULL)
          AND (is_routine = 1 OR is_10b5_1 = 1 OR cohen_routine = 1)
    """, (start_str, end_str)).fetchone()[0]

    # Compute stats
    with_returns = [s for s in signals if s["return_7d"] is not None]
    total_signals = len(signals)
    total_with_returns = len(with_returns)

    wins = [s for s in with_returns if _is_win(s["trade_type"], s["return_7d"])]
    win_count = len(wins)
    win_rate = (win_count / total_with_returns * 100) if total_with_returns > 0 else 0.0

    # Average 7d return (directional: positive = good for both buys and sells)
    directional_returns = [
        _effective_return(s["trade_type"], s["return_7d"])
        for s in with_returns
        if _effective_return(s["trade_type"], s["return_7d"]) is not None
    ]
    avg_return_7d = (sum(directional_returns) / len(directional_returns)) if directional_returns else 0.0

    # Top and bottom performers by directional 7d return
    ranked = sorted(
        with_returns,
        key=lambda s: _effective_return(s["trade_type"], s["return_7d"]) or 0,
        reverse=True,
    )
    top_3 = ranked[:3] if len(ranked) >= 3 else ranked
    best = ranked[0] if ranked else None
    worst = ranked[-1] if ranked else None

    return {
        "signals": signals,
        "total_signals": total_signals,
        "total_with_returns": total_with_returns,
        "win_count": win_count,
        "win_rate": win_rate,
        "avg_return_7d": avg_return_7d,
        "top_3": top_3,
        "best": best,
        "worst": worst,
        "routine_filtered": routine_count,
    }


# ---------------------------------------------------------------------------
# Scorecard HTML
# ---------------------------------------------------------------------------

def _fmt_pct(val: float | None) -> str:
    """Format a decimal return as a percentage string."""
    if val is None:
        return "N/A"
    return f"{val * 100:+.1f}%"


def _return_color(val: float | None) -> str:
    if val is None:
        return STEEL
    return ALPHA_GREEN if val > 0 else RISK_RED


def build_scorecard_html(stats: dict, window_start: date, window_end: date) -> str:
    """Build the weekly scorecard HTML for Playwright rendering."""

    logo_uri = _img_data_uri(BRAND_DIR / "wordmark_form4.png")
    date_range = f"{window_start.strftime('%b %d')} - {window_end.strftime('%b %d, %Y')}"

    # Top performers rows
    top_rows = ""
    for t in stats["top_3"]:
        eff_ret = _effective_return(t["trade_type"], t["return_7d"])
        color = _return_color(eff_ret)
        grade = t["signal_grade"]
        grade_class = "grade-a" if grade == "A" else "grade-b"
        direction = "BUY" if t["trade_type"] == "buy" else "SELL"
        top_rows += f"""
        <div class="performer-row">
            <div class="perf-left">
                <span class="grade-sm {grade_class}">{grade}</span>
                <span class="perf-ticker">{t['ticker']}</span>
                <span class="perf-dir" style="color:{STEEL}">{direction}</span>
            </div>
            <span class="perf-return" style="color:{color}">{_fmt_pct(eff_ret)}</span>
        </div>"""

    # Win rate color
    wr = stats["win_rate"]
    wr_color = ALPHA_GREEN if wr >= 55 else (AMBER if wr >= 45 else RISK_RED)

    # Avg return color
    avg_color = _return_color(stats["avg_return_7d"])

    html = f"""<!DOCTYPE html>
<html><head><style>
{BASE_CSS}
body {{
    padding: 60px 50px;
    justify-content: flex-start;
    gap: 0;
}}
.logo {{
    width: 220px;
    margin-bottom: 24px;
}}
.header {{
    font-size: 24px;
    font-weight: 700;
    letter-spacing: 6px;
    text-transform: uppercase;
    color: {SIGNAL_BLUE};
    margin-bottom: 8px;
}}
.date-range {{
    font-size: 22px;
    color: {STEEL};
    margin-bottom: 48px;
}}
.big-stat {{
    text-align: center;
    margin-bottom: 48px;
}}
.big-number {{
    font-size: 88px;
    font-weight: 800;
    line-height: 1;
    margin-bottom: 8px;
}}
.big-label {{
    font-size: 28px;
    color: {STEEL};
}}
.stat-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 28px 36px;
    background: {SLATE};
    border: 1px solid #2A2A3A;
    border-radius: 16px;
    margin-bottom: 16px;
}}
.stat-label {{
    font-size: 22px;
    color: {STEEL};
}}
.stat-value {{
    font-size: 32px;
    font-weight: 700;
}}
.section-label {{
    font-size: 20px;
    font-weight: 700;
    letter-spacing: 4px;
    text-transform: uppercase;
    color: {FOG};
    margin-top: 32px;
    margin-bottom: 16px;
}}
.performer-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 20px 28px;
    background: {SLATE};
    border: 1px solid #2A2A3A;
    border-radius: 14px;
    margin-bottom: 10px;
}}
.perf-left {{
    display: flex;
    align-items: center;
    gap: 16px;
}}
.grade-sm {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 44px; height: 44px;
    border-radius: 10px;
    font-size: 22px; font-weight: 700; font-family: monospace;
}}
.perf-ticker {{
    font-size: 28px;
    font-weight: 700;
    color: {CLOUD};
}}
.perf-dir {{
    font-size: 18px;
    font-weight: 600;
}}
.perf-return {{
    font-size: 30px;
    font-weight: 700;
}}
.routine-bar {{
    text-align: center;
    padding: 24px;
    background: rgba(136,136,160,0.08);
    border: 1px solid #2A2A3A;
    border-radius: 14px;
    margin-top: 32px;
    margin-bottom: 32px;
}}
.routine-text {{
    font-size: 20px;
    color: {FOG};
}}
.routine-num {{
    color: {STEEL};
    font-weight: 700;
}}
.cta {{
    text-align: center;
    margin-top: auto;
    padding-top: 24px;
}}
.cta-text {{
    font-size: 26px;
    font-weight: 600;
    color: {SIGNAL_BLUE};
}}
</style></head><body>

<img class="logo" src="{logo_uri}" alt="Form4" />
<div class="header">Weekly Signal Report</div>
<div class="date-range">{date_range}</div>

<div class="big-stat">
    <div class="big-number" style="color:{wr_color}">{stats['win_count']}/{stats['total_with_returns']}</div>
    <div class="big-label">signals in the green &mdash; {stats['win_rate']:.0f}% win rate</div>
</div>

<div class="stat-row">
    <span class="stat-label">Avg 7-Day Return</span>
    <span class="stat-value" style="color:{avg_color}">{_fmt_pct(stats['avg_return_7d'])}</span>
</div>

<div class="stat-row">
    <span class="stat-label">Total Signals</span>
    <span class="stat-value">{stats['total_signals']}</span>
</div>

<div class="section-label">Top Performers</div>
{top_rows}

<div class="routine-bar">
    <div class="routine-text">
        <span class="routine-num">{stats['routine_filtered']}</span> routine/planned trades filtered out this week
    </div>
</div>

<div class="cta">
    <div class="cta-text">form4.app</div>
</div>

</body></html>"""

    return html


# ---------------------------------------------------------------------------
# Social copy
# ---------------------------------------------------------------------------

def generate_x_post(stats: dict, window_start: date, window_end: date) -> str:
    """Generate X/Twitter post text."""

    date_range = f"{window_start.strftime('%b %d')}-{window_end.strftime('%b %d')}"
    lines = []

    lines.append(f"Form4 Weekly Signal Report ({date_range})")
    lines.append("")
    lines.append(
        f"{stats['win_count']}/{stats['total_with_returns']} insider signals landed in the green "
        f"({stats['win_rate']:.0f}% win rate)"
    )
    lines.append(f"Avg 7-day return: {_fmt_pct(stats['avg_return_7d'])}")
    lines.append("")

    if stats["top_3"]:
        lines.append("Top performers:")
        for t in stats["top_3"]:
            eff = _effective_return(t["trade_type"], t["return_7d"])
            direction = "bought" if t["trade_type"] == "buy" else "sold"
            lines.append(f"  ${t['ticker']} ({direction}) {_fmt_pct(eff)}")
        lines.append("")

    lines.append(
        f"{stats['routine_filtered']} routine/planned trades filtered out."
    )
    lines.append("")
    lines.append("Every signal graded. Every trade tracked.")
    lines.append("form4.app")

    return "\n".join(lines)


def generate_captions(stats: dict, window_start: date, window_end: date) -> str:
    """Generate Instagram/TikTok captions."""

    date_range = f"{window_start.strftime('%b %d')}-{window_end.strftime('%b %d')}"
    sections = []

    # Instagram
    ig_lines = []
    ig_lines.append(f"WEEKLY INSIDER SIGNAL REPORT | {date_range}")
    ig_lines.append("")
    ig_lines.append(
        f"{stats['win_count']} out of {stats['total_with_returns']} insider signals "
        f"finished in the green this week ({stats['win_rate']:.0f}% win rate)."
    )
    ig_lines.append(f"Average 7-day return: {_fmt_pct(stats['avg_return_7d'])}")
    ig_lines.append("")

    if stats["top_3"]:
        ig_lines.append("Top 3:")
        for t in stats["top_3"]:
            eff = _effective_return(t["trade_type"], t["return_7d"])
            ig_lines.append(f"${t['ticker']} {_fmt_pct(eff)}")
        ig_lines.append("")

    ig_lines.append(
        f"We filtered out {stats['routine_filtered']} routine/planned trades "
        "so you only see the ones that matter."
    )
    ig_lines.append("")
    ig_lines.append("Link in bio: form4.app")
    ig_lines.append("")
    ig_lines.append("#insidertrading #stockmarket #form4 #tradingsignals #investing")
    sections.append("=== INSTAGRAM ===\n" + "\n".join(ig_lines))

    # TikTok
    tk_lines = []
    tk_lines.append(
        f"{stats['win_count']}/{stats['total_with_returns']} insider signals hit this week "
        f"({stats['win_rate']:.0f}%). Here are the top 3."
    )
    tk_lines.append("")
    if stats["top_3"]:
        for t in stats["top_3"]:
            eff = _effective_return(t["trade_type"], t["return_7d"])
            direction = "bought" if t["trade_type"] == "buy" else "sold"
            tk_lines.append(f"${t['ticker']} insider {direction} - {_fmt_pct(eff)} in 7 days")
    tk_lines.append("")
    tk_lines.append("Follow for weekly signal reports. form4.app")
    tk_lines.append("")
    tk_lines.append("#insidertrading #stockmarket #form4 #tradingsignals")
    sections.append("=== TIKTOK ===\n" + "\n".join(tk_lines))

    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Video storyboard
# ---------------------------------------------------------------------------

def generate_storyboard(stats: dict, window_start: date, window_end: date) -> str:
    """Generate a 15-second video storyboard script."""

    date_range = f"{window_start.strftime('%b %d')} to {window_end.strftime('%b %d')}"

    sections = []
    sections.append("=== WEEKLY SIGNAL REPORT - 15s STORYBOARD ===")
    sections.append("")

    # Beat 1: Hook (0-3s)
    sections.append("[0s-3s] HOOK")
    sections.append(f"Visual: Form4 logo fade-in, then big number: {stats['win_count']}/{stats['total_with_returns']}")
    sections.append(
        f"\"This week, {stats['win_count']} out of {stats['total_with_returns']} "
        f"insider signals landed in the green.\""
    )
    sections.append("")

    # Beat 2: Stats (3-7s)
    sections.append("[3s-7s] STATS")
    sections.append(
        f"Visual: Win rate {stats['win_rate']:.0f}% animates in, "
        f"avg return {_fmt_pct(stats['avg_return_7d'])} slides up"
    )
    sections.append(
        f"\"{stats['win_rate']:.0f} percent win rate. "
        f"Average seven-day return: {_fmt_pct(stats['avg_return_7d'])}.\""
    )
    sections.append("")

    # Beat 3: Top performers (7-12s)
    sections.append("[7s-12s] TOP PERFORMERS")
    if stats["top_3"]:
        tickers = ", ".join(
            f"${t['ticker']} {_fmt_pct(_effective_return(t['trade_type'], t['return_7d']))}"
            for t in stats["top_3"]
        )
        sections.append(f"Visual: Top 3 cards slide in: {tickers}")
        top = stats["top_3"][0]
        eff = _effective_return(top["trade_type"], top["return_7d"])
        sections.append(
            f"\"Top performer: ${top['ticker']}, up {_fmt_pct(eff)} in just seven days.\""
        )
    sections.append("")

    # Beat 4: CTA (12-15s)
    sections.append("[12s-15s] CTA")
    sections.append(
        f"Visual: \"{stats['routine_filtered']} routine trades filtered\" text, "
        "then form4.app logo"
    )
    sections.append("\"We filter out the noise. See every graded signal at form4.app.\"")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_scorecard(html: str, output_path: Path) -> None:
    """Render scorecard HTML to PNG via Playwright."""
    from playwright.sync_api import sync_playwright

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": HEIGHT})
        page.set_content(html, wait_until="networkidle")
        page.screenshot(path=str(output_path), full_page=False)
        browser.close()

    logger.info("Scorecard saved to %s", output_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate weekly performance snapshot")
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Week ending date (YYYY-MM-DD). Defaults to last Friday.",
    )
    args = parser.parse_args()

    # Determine end date: default to most recent Friday
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        today = date.today()
        # Walk back to most recent Friday (weekday 4)
        days_since_friday = (today.weekday() - 4) % 7
        if days_since_friday == 0 and today.weekday() != 4:
            days_since_friday = 7
        end_date = today - timedelta(days=days_since_friday)

    window_start, window_end = _compute_week_range(end_date)
    logger.info(
        "Weekly snapshot: signals filed %s to %s (returns measured by %s)",
        window_start, window_end, end_date,
    )

    # Output directory
    date_slug = end_date.strftime("%Y%m%d")
    out_dir = CONTENT_DIR / f"weekly_{date_slug}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    stats = load_weekly_signals(conn, window_start, window_end)
    conn.close()

    logger.info(
        "Found %d A/B signals (%d with returns), %d wins (%.0f%% WR), avg 7d: %s",
        stats["total_signals"],
        stats["total_with_returns"],
        stats["win_count"],
        stats["win_rate"],
        _fmt_pct(stats["avg_return_7d"]),
    )
    logger.info("Routine/planned filtered: %d", stats["routine_filtered"])

    if stats["total_with_returns"] == 0:
        logger.warning("No signals with returns found for this window. Generating with zeros.")

    # Generate scorecard image
    scorecard_html = build_scorecard_html(stats, window_start, window_end)
    render_scorecard(scorecard_html, out_dir / "scorecard.png")

    # Generate social copy
    x_post = generate_x_post(stats, window_start, window_end)
    (out_dir / "x_post.txt").write_text(x_post)
    logger.info("X post saved to %s", out_dir / "x_post.txt")

    captions = generate_captions(stats, window_start, window_end)
    (out_dir / "captions_platforms.txt").write_text(captions)
    logger.info("Captions saved to %s", out_dir / "captions_platforms.txt")

    # Generate storyboard
    storyboard = generate_storyboard(stats, window_start, window_end)
    (out_dir / "storyboard.txt").write_text(storyboard)
    logger.info("Storyboard saved to %s", out_dir / "storyboard.txt")

    # Summary
    logger.info("All outputs in %s", out_dir)
    print(f"\n{'='*60}")
    print(f"Weekly Snapshot: {window_start} to {window_end}")
    print(f"{'='*60}")
    print(f"Signals:    {stats['total_signals']} ({stats['total_with_returns']} with returns)")
    print(f"Win rate:   {stats['win_count']}/{stats['total_with_returns']} = {stats['win_rate']:.0f}%")
    print(f"Avg 7d:     {_fmt_pct(stats['avg_return_7d'])}")
    print(f"Filtered:   {stats['routine_filtered']} routine/planned trades")
    if stats["best"]:
        eff = _effective_return(stats["best"]["trade_type"], stats["best"]["return_7d"])
        print(f"Best:       ${stats['best']['ticker']} {_fmt_pct(eff)}")
    if stats["worst"]:
        eff = _effective_return(stats["worst"]["trade_type"], stats["worst"]["return_7d"])
        print(f"Worst:      ${stats['worst']['ticker']} {_fmt_pct(eff)}")
    print(f"Output:     {out_dir}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

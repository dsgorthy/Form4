#!/usr/bin/env python3
"""Generate slides.txt cheat sheet and carousel_caption.txt for each carousel group.

Called by run_daily_content.sh after render_ig_carousel.py generates trades.json.
Also generates weekly aggregation on Fridays.

Usage:
    python3 pipelines/generate_slides_txt.py --date 2026-03-25
    python3 pipelines/generate_slides_txt.py --date 2026-03-28 --weekly  # weekend: Mon-Fri aggregate
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

CONTENT_DIR = Path(__file__).resolve().parent / "data" / "content"

# Rotating CTA hooks — pipeline picks one per carousel, no repeat within a week
CTA_HOOKS = [
    "{total_filings} insider trades were filed today. We filtered it down to {n_trades}.",
    "We scored {total_filings} insider filings today. Only {n_trades} passed our quality threshold.",
    "Not all insider trades are equal. We filtered out {routine_count} routine trades to find these {n_trades}.",
    "Most insider trading data is useless. We spent 2 years figuring out which signals actually predict returns.",
    "The SEC publishes every insider trade. We grade them.",
    "These trades were filed with the SEC hours ago. You're seeing them before most of Wall Street.",
    "Following every insider buy loses money. Following the right ones beats the S&P. Here's how we tell the difference.",
]


def _date_fmt(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    day = dt.day
    sfx = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{dt.strftime('%B')} {day}{sfx}, {dt.year}"


def generate_slides_txt(group_dir: Path, date_str: str, group_name: str) -> None:
    """Generate slides.txt and carousel_caption.txt for one carousel group."""
    trades_path = group_dir / "trades.json"
    if not trades_path.exists():
        return

    trades = json.loads(trades_path.read_text())
    if not trades:
        return

    date_fmt = _date_fmt(date_str)
    is_buy = "buy" in group_name
    action_word = "Buys" if is_buy else "Sells"
    emoji = "🟢" if is_buy else "🔴"
    total_slides = len(trades) + 2  # cover + CTA

    # Title
    if "weekly" in group_name:
        title = f"Top Insider {action_word} This Week"
    else:
        title = f"Top Insider {action_word}"

    # Build slides.txt
    out = []
    out.append(f"{title.upper()} — {date_fmt}")
    out.append(f"{len(trades)} slides + cover + CTA = {total_slides} total")
    out.append("")
    out.append(f"COVER SLIDE (1/{total_slides})")
    out.append(f"  Title: {title}")
    out.append(f"  Date: {date_fmt}")
    out.append("  Logo: brand/form4_wordmark_lightmode.png")
    out.append("")

    for t in trades:
        out.append(f"SLIDE {t['slide_number']}/{total_slides} — {t['company']} ${t['ticker']}")
        out.append(f"  Chart: charts/{t['ticker']}.png")
        out.append(f"  Logo: logos/{t['ticker']}.png")
        out.append(f"  Chips: {' · '.join(t.get('tags', []))}")
        color = "green" if is_buy else "red"
        out.append(f"  Value callout: {t['value']} ({color})")
        out.append(f"  Signal quality: {t.get('quality', 0):.0f}/10")
        out.append("")

        # Build copy without repeating holdings info
        role = t.get("title", "")
        company = t.get("company", "")
        ticker = t.get("ticker", "")
        value = t.get("value", "")
        action = t.get("action", "purchased" if is_buy else "sold")

        parts = [f"{role} at {company} ${ticker} {action} {value}."]

        stock_perf = t.get("stock_perf", "")
        holdings = t.get("holdings_change", "")

        if stock_perf and "down" in stock_perf and is_buy:
            hld = f" Holdings increased by {holdings}." if holdings else ""
            parts.append(f"Dip buy:{hld} Stock {stock_perf}.")
        elif holdings and stock_perf:
            verb = "increased" if is_buy else "decreased"
            parts.append(f"Holdings {verb} by {holdings}. Stock {stock_perf}.")
        elif holdings:
            verb = "increased" if is_buy else "decreased"
            parts.append(f"Holdings {verb} by {holdings}.")
        elif stock_perf:
            parts.append(f"Stock {stock_perf}.")

        out.append("  COPY:")
        out.append(f"  {' '.join(parts)}")
        out.append("")

    out.append(f"CTA SLIDE ({total_slides}/{total_slides})")
    out.append("  Logo: brand/form4_wordmark_darkmode.png (on dark bg)")
    out.append("  Hook: [pick from rotating hooks below]")
    out.append("  CTA: Comment \"signals\" and we'll send you our insider research.")
    out.append("")

    # Pick a hook — use day-of-year mod to rotate
    day_idx = datetime.strptime(date_str, "%Y-%m-%d").timetuple().tm_yday
    hook = CTA_HOOKS[day_idx % len(CTA_HOOKS)]
    out.append("  HOOK OPTIONS:")
    for i, h in enumerate(CTA_HOOKS[:4]):
        marker = " <<<" if i == day_idx % len(CTA_HOOKS) else ""
        out.append(f"    {i+1}. \"{h}\"{marker}")
    out.append("")

    (group_dir / "slides.txt").write_text("\n".join(out))
    logger.info(f"  slides.txt: {len(trades)} slides for {group_name}")

    # Build carousel_caption.txt (5 hashtags max, padded for iPhone)
    caption_lines = [f"{title} — {date_fmt}", ""]
    for t in trades:
        caption_lines.append(f"{emoji} ${t['ticker']} — {t.get('title', '')} {t.get('action', 'traded')} {t['value']}")
    caption_lines.append("")
    caption_lines.append("Real-time alerts + signal grading at form4.app")
    caption_lines.append("Link in bio for 7-day free trial")
    caption_lines.append("")
    caption_lines.append("#InsiderTrading #StockMarket #SEC #SmartMoney #Form4")
    # Padding for iPhone copy issue
    caption_lines.extend(["", "", "", "."])
    caption_lines.append("")

    (group_dir / "carousel_caption.txt").write_text("\n".join(caption_lines))
    logger.info(f"  carousel_caption.txt written")


def generate_weekly(date_str: str) -> None:
    """Aggregate Mon-Fri Q7+ trades into weekend carousel.

    Scans the past 5 weekdays of carousel outputs and merges the best trades.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")

    # Find the past 5 weekdays
    weekdays = []
    d = dt
    while len(weekdays) < 5:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            weekdays.append(d.strftime("%Y-%m-%d"))

    logger.info(f"Weekly aggregation: {weekdays[-1]} to {weekdays[0]}")

    for group_type in ["top_buys", "top_sells"]:
        all_trades = []
        for day in weekdays:
            slug = day.replace("-", "")
            day_dir = CONTENT_DIR / f"{slug}_carousel_{group_type}"
            trades_path = day_dir / "trades.json"
            if trades_path.exists():
                trades = json.loads(trades_path.read_text())
                for t in trades:
                    t["_source_date"] = day
                all_trades.extend(trades)

        if not all_trades:
            logger.info(f"  No {group_type} trades for weekly")
            continue

        # Dedup by ticker (keep highest quality)
        seen = {}
        for t in sorted(all_trades, key=lambda x: -x.get("quality", 0)):
            if t["ticker"] not in seen:
                seen[t["ticker"]] = t
        deduped = list(seen.values())[:8]

        if len(deduped) < 3:
            logger.info(f"  Only {len(deduped)} unique {group_type} — skipping weekly")
            continue

        # Renumber
        for i, t in enumerate(deduped):
            t["slide_number"] = i + 2

        # Write to weekly folder
        week_end = weekdays[0]
        week_slug = week_end.replace("-", "")
        weekly_dir = CONTENT_DIR / f"{week_slug}_carousel_weekly_{group_type}"
        weekly_dir.mkdir(parents=True, exist_ok=True)

        # Copy charts and logos from source days
        charts_dir = weekly_dir / "charts"
        logos_dir = weekly_dir / "logos"
        charts_dir.mkdir(exist_ok=True)
        logos_dir.mkdir(exist_ok=True)

        import shutil
        for t in deduped:
            src_slug = t["_source_date"].replace("-", "")
            src_dir = CONTENT_DIR / f"{src_slug}_carousel_{group_type}"

            src_chart = src_dir / "charts" / f"{t['ticker']}.png"
            src_logo = src_dir / "logos" / f"{t['ticker']}.png"

            if src_chart.exists():
                shutil.copy2(src_chart, charts_dir / f"{t['ticker']}.png")
                t["chart_exists"] = True
            if src_logo.exists():
                shutil.copy2(src_logo, logos_dir / f"{t['ticker']}.png")
                t["logo_exists"] = True

        # Drop trades without charts
        deduped = [t for t in deduped if t.get("chart_exists")]
        for i, t in enumerate(deduped):
            t["slide_number"] = i + 2

        (weekly_dir / "trades.json").write_text(json.dumps(deduped, indent=2))

        # Generate slides.txt and caption
        week_label = f"{_date_fmt(weekdays[-1]).split(',')[0]} – {_date_fmt(weekdays[0]).split(',')[0]}, {dt.year}"
        generate_slides_txt(weekly_dir, week_end, f"weekly_{group_type}")

        logger.info(f"  Weekly {group_type}: {len(deduped)} trades → {weekly_dir.name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--weekly", action="store_true", help="Generate weekend weekly aggregation")
    args = parser.parse_args()

    date_slug = args.date.replace("-", "")

    # Generate slides.txt for each carousel group that exists
    for group_dir in sorted(CONTENT_DIR.glob(f"{date_slug}_carousel_*")):
        if not group_dir.is_dir():
            continue
        group_name = group_dir.name.replace(f"{date_slug}_carousel_", "")
        if group_name.startswith("weekly_"):
            continue  # don't re-process weekly dirs
        generate_slides_txt(group_dir, args.date, group_name)

    # Weekly aggregation (for weekends)
    if args.weekly:
        generate_weekly(args.date)


if __name__ == "__main__":
    main()

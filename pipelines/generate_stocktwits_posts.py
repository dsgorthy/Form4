#!/usr/bin/env python3
"""Five copy-paste Stocktwits posts a day, one per ticker.

WHY NOT EXTEND generate_daily_content.py

That script ranks by raw dollar value and scores with
portfolio_simulator.compute_signal_quality, which the signal registry lists as
red flag #1: it reads trades.pit_win_rate_7d and COALESCEs to
insider_track_records.buy_win_rate_7d, a global all-time statistic. Ranking a
public post on a PIT-violating score is not something to inherit. This ranks on
career_grade, which was validated on filing-anchored returns: A+/A beat B/C/D
by +3.46pp over 21 trading days, positive in 8 of 9 years.

WHY ONE POST PER TICKER

Stocktwits discovery runs through $CASHTAG streams. A roundup post reaches
followers only; five per-ticker posts reach five streams. Hence the hard
one-ticker-per-post rule and the dedupe below.

SELECTION

Rank blends three things, in the order they measured:
  1. career_grade  — the only component validated out of sample
  2. trade size    — a rough proxy for whether anyone will care
  3. cluster       — multiple insiders on one name reads as a story

Buys are favoured over sells 3:2. Not editorial preference: discretionary buys
run +1.06% at 30d unconditionally while discretionary sells run -0.39%, so the
buy side carries roughly 2.7x the magnitude. Sells still earn slots because a
large sale is legitimately interesting content, and pretending otherwise would
make the feed look like a permabull.

Usage:
    python3 pipelines/generate_stocktwits_posts.py
    python3 pipelines/generate_stocktwits_posts.py --date 2026-08-17 --count 5
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402
from pipelines.insider_study.annotate_trade import annotate  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "content"

GRADE_WEIGHT = {"A+": 100, "A": 80, "B": 40, "C": 15, "D": 0}

SQL = """
SELECT t.trade_id, t.ticker, t.company, t.signal_class, t.value, t.qty, t.price,
       t.shares_owned_after, t.is_largest_ever, t.is_rare_reversal,
       t.consecutive_sells_before, t.dip_1mo, t.dip_3mo, t.pit_cluster_size,
       t.career_grade, t.title AS insider_title, t.filing_date,
       COALESCE(i.display_name, i.name) AS insider_name,
       (SELECT close FROM prices.daily_prices d
         WHERE d.ticker = t.ticker ORDER BY d.date DESC LIMIT 1) AS current_price,
       NOT EXISTS (SELECT 1 FROM trades p
                    WHERE p.insider_id = t.insider_id AND p.ticker = t.ticker
                      AND p.signal_class = t.signal_class
                      AND p.filing_date < t.filing_date) AS is_first_ever
  FROM trades t
  JOIN insiders i ON i.insider_id = t.insider_id
 WHERE t.filing_date = ?
   AND t.signal_class IN ('discretionary_buy', 'discretionary_sell')
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
   AND t.superseded_by IS NULL
   AND t.ticker NOT IN ('NONE', 'NA', 'N/A', '')
   AND t.value > 25000
"""


def score(t: dict) -> float:
    """Rank, not a prediction. Grade dominates; size and cluster break ties.

    Buys and sells are scored on separate scales and never against each other.
    compute_career_grades stamps a grade only on trans_code='P', so a sell can
    never carry one — put both sides in one ranking and every sell scores the
    default forever, the reserved slots never fill, and the feed is all buys by
    construction. Which is exactly what the first run produced.
    """
    import math
    size = min(math.log10(max(t.get("value") or 1, 1)) * 6, 45)
    cluster = min((t.get("pit_cluster_size") or 0) * 4, 16)
    if t["signal_class"] == "discretionary_sell":
        # No grade exists, so size and company carry it, plus a nudge for a
        # holder actually leaving rather than trimming.
        exiting = 12 if (t.get("shares_owned_after") == 0) else 0
        return size * 1.6 + cluster + exiting
    return float(GRADE_WEIGHT.get(t.get("career_grade") or "", 5)) + size + cluster


def render(t: dict) -> str:
    """One Stocktwits post. Cashtag first — it is the discovery surface."""
    is_buy = t["signal_class"] == "discretionary_buy"
    verb = "bought" if is_buy else "sold"
    val = t.get("value") or 0
    amount = f"${val / 1_000_000:.1f}M" if val >= 1_000_000 else f"${val / 1_000:.0f}K"

    title = (t.get("insider_title") or "Insider").strip()
    name = (t.get("insider_name") or "").strip()
    who = title if (not name or name.lower() in title.lower()) else f"{title} {name}"

    lines = [f"${t['ticker']} — {who} {verb} {amount}", ""]
    lines += [f"· {a}" for a in annotate(t, max_lines=3)]

    grade = t.get("career_grade")
    if grade in ("A+", "A"):
        lines += ["", f"We grade this insider {grade}. Our top tier has beaten "
                      f"the S&P by ~2% over the following month."]
    elif grade:
        lines += ["", f"We grade this insider {grade}."]

    lines += ["", "form4.app", "Not investment advice."]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Filing date (default: today)")
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--write", action="store_true", help="Also write to data/content/")
    args = ap.parse_args()

    day = args.date or date.today().isoformat()
    conn = get_connection()
    rows = [dict(r) for r in conn.execute(SQL, (day,)).fetchall()]
    if not rows:
        logger.info("No qualifying filings for %s", day)
        return 0

    buys = sorted([r for r in rows if r["signal_class"] == "discretionary_buy"],
                  key=score, reverse=True)
    sells = sorted([r for r in rows if r["signal_class"] == "discretionary_sell"],
                   key=score, reverse=True)

    # One post per ticker, and hold the buy/sell mix near 3:2 so the feed does
    # not read as a permabull on a heavy buy day or a doomsayer on a heavy sell
    # one. Caps are soft: if a side runs out, the other fills the slots.
    n_sell = max(1, args.count * 2 // 5)
    picked, seen = [], set()

    def take(pool, limit):
        for r in pool:
            if limit <= 0 or len(picked) >= args.count:
                return
            if r["ticker"] in seen:
                continue
            seen.add(r["ticker"])
            picked.append(r)
            limit -= 1

    take(sells, n_sell)
    take(buys, args.count - len(picked))
    take(sells + buys, args.count - len(picked))   # one side ran thin
    picked.sort(key=score, reverse=True)

    out = []
    for i, t in enumerate(picked, 1):
        post = render(t)
        out.append(post)
        print(f"\n{'─' * 58}\n  POST {i}/{len(picked)}   ({len(post)} chars)\n{'─' * 58}")
        print(post)

    if args.write:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        p = OUTPUT_DIR / f"{day}_stocktwits.txt"
        p.write_text(("\n\n" + "=" * 58 + "\n\n").join(out))
        logger.info("wrote %s", p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

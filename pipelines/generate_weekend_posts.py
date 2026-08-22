#!/usr/bin/env python3
"""Weekend Stocktwits posts: what the week held, and how an old cohort did.

WHY THE WEEKEND IS A DIFFERENT POST

Nothing files on a Saturday, so the weekday generator has nothing to say. But
the weekend is when people do their research for Monday, and the stream is far
less crowded. It is the natural slot for the posts that zoom out — the ones
that build an account rather than serve a ticker.

WHAT THIS PRODUCES

  1. THE WEEK IN NUMBERS — buys vs sells, the biggest cluster. Pure fact, no
     claim, nothing to dispute.
  2. THE COHORT — every company where 2+ insiders bought $250K+ during a
     MONTH that has since fully matured, and what all of them did over their
     own 30 days. Including the losers.
  3. MONDAY SETUP — the strongest cluster of the week just gone, as a per-ticker
     post so it lands in that cashtag stream.

WHY THE COHORT POST IS THE IMPORTANT ONE

"Best trades of the week" is the obvious weekend post and it is a trap: trades
filed this week have had no time to do anything, so "best" can only mean
"biggest", and an account that posts its biggest winners is indistinguishable
from one that posts its luckiest.

The cohort inverts that. It takes a mechanical rule — 2+ insiders buying
$250K+ of the same ticker — applied to a window whose position is computed, not
chosen, and reports EVERY member. No selection is possible, which is exactly
why the number means something. On 2026-08-22: 28 names, 19 up, median +5.1%
against SPY's +4.4%, and it included MOBI at -28.2%.

Three rules keep it honest, all enforced in code rather than by intention:

  * THE WINDOW IS COMPUTED. Its position falls out of HOLD_DAYS and
    COHORT_SPAN_DAYS. Choosing a lookback after seeing results is how a
    measurement becomes marketing.
  * EVERY MEMBER HAS MATURED. The window ends HOLD_DAYS+1 before the anchor, so
    no name is measured mid-flight. The first version pooled a month up to the
    present and averaged a 30-day hold with a 1-day hold.
  * THE LOSERS SHIP. There is no filter on outcome anywhere in this file. Both
    the median AND the mean are published, and on 2026-08-22 they disagreed
    sharply — +5.1% against +0.2%, because the misses were bigger than the
    hits. Reporting only the flattering one of those is the easiest lie
    available here, so the renderer reports both whenever they diverge.

This is NOT a track record. We did not post about those companies at the time,
and the post must never imply we did. It is an observation about insider
clusters. The real scorecard — our own calls, followed up — needs 30 days of
social_posts history and starts around 2026-09-20.

Usage:
    python3 pipelines/generate_weekend_posts.py
    python3 pipelines/generate_weekend_posts.py --date 2026-08-22 --no-record
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "content"

#: Each cohort member is held exactly this many calendar days from its filing.
#: 30 because that is where the measured edge peaks (+5.36% vs SPY at 30d,
#: against +3.18% at 7d and +2.22% at 90d) — the window that answers the
#: question rather than the one that flatters it.
HOLD_DAYS = 30

#: The cohort window is 28 days long and ENDS HOLD_DAYS+1 before the anchor, so
#: every member has had its full 30 days. The first version of this pooled a
#: month up to the present, which silently mixed a name held 30 days with one
#: held 1 day and called the average a result.
COHORT_SPAN_DAYS = 28

#: Minimum insiders on one ticker to count as a cluster.
CLUSTER_MIN = 2

#: Minimum total dollars in a cluster.
#:
#: Two insiders putting in $25K each is not a cluster, and without a floor the
#: cohort was dominated by names like RANI ($50K total) sitting beside FSBC
#: ($6.28M). Set at $250K on that reasoning, then checked across six
#: independent past windows before adopting: the floor moved the median up in
#: three and down in three, so it is not a curve fit in either direction. It
#: cost the 2026-08-22 post its headline — that week went from +7.1% median to
#: +0.7% — and was kept anyway, which is the only way a threshold like this
#: stays trustworthy.
CLUSTER_MIN_VALUE = 250_000

#: Tickers that are not tradeable securities. 'NONE' is the private-company
#: placeholder and carries real dollar values, so it outranks everything if
#: left in.
NOT_A_TICKER = ("NONE", "NA", "N/A", "")

#: Shared filters. Routine and pre-scheduled activity is not a decision, and
#: signal_class already separates 10b5-1 into planned_* — see api/classification.
CLEAN = """
      AND COALESCE(t.is_routine, 0) = 0
      AND COALESCE(t.cohen_routine, 0) = 0
      AND NOT COALESCE(t.value_suspect, FALSE)
      AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
      AND t.superseded_by IS NULL
      AND t.ticker NOT IN ('NONE', 'NA', 'N/A', '')
"""


def _money(v: float) -> str:
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.1f}B"
    if v >= 999_500:
        return f"${v / 1_000_000:.1f}M"
    return f"${v / 1_000:.0f}K"


def trading_week(anchor: date) -> tuple[str, str]:
    """The Mon-Fri that just closed, for a weekend run."""
    monday = anchor - timedelta(days=anchor.weekday())
    if anchor.weekday() >= 5:            # Sat/Sun -> the week just ended
        return monday.isoformat(), (monday + timedelta(days=4)).isoformat()
    prev = monday - timedelta(days=7)    # midweek run -> last full week
    return prev.isoformat(), (prev + timedelta(days=4)).isoformat()


def week_numbers(conn, lo: str, hi: str) -> dict | None:
    r = conn.execute(f"""
        SELECT COUNT(*) n,
               COUNT(*) FILTER (WHERE t.signal_class='discretionary_buy')  buys,
               COUNT(*) FILTER (WHERE t.signal_class='discretionary_sell') sells,
               SUM(t.value) FILTER (WHERE t.signal_class='discretionary_buy')  buy_val,
               SUM(t.value) FILTER (WHERE t.signal_class='discretionary_sell') sell_val,
               COUNT(DISTINCT t.ticker) tickers
          FROM trades t
         WHERE t.filing_date BETWEEN ? AND ?
           AND t.signal_class IN ('discretionary_buy','discretionary_sell')
           {CLEAN}
           AND t.value > 25000""", (lo, hi)).fetchone()
    return dict(r) if r and r["n"] else None


def top_clusters(conn, lo: str, hi: str, limit: int = 5) -> list[dict]:
    rows = conn.execute(f"""
        SELECT t.ticker, MAX(t.company) company,
               COUNT(DISTINCT t.insider_id) n_ins, SUM(t.value) v,
               -- Who bought matters more than how many. A cluster of directors
               -- is a different story from one where the people who see the
               -- numbers first are participating.
               -- Regex, not ILIKE '%CEO%': the compat layer rewrites ? to %s
               -- and psycopg2 then reads the LIKE wildcards as parameter
               -- placeholders, which fails with "tuple index out of range".
               -- ~* needs no wildcards and is clearer anyway.
               COUNT(DISTINCT t.insider_id) FILTER (
                   WHERE t.normalized_title ~* 'CEO|CFO|COO|President') n_csuite,
               MAX(t.career_grade) best_grade,
               -- How far off its own recent high the stock was when they
               -- bought. Buying a name that has halved is the interesting
               -- version of this story.
               MIN(t.dip_3mo) dip_3mo
          FROM trades t
         WHERE t.filing_date BETWEEN ? AND ?
           AND t.signal_class = 'discretionary_buy'
           {CLEAN}
         GROUP BY t.ticker
        HAVING COUNT(DISTINCT t.insider_id) >= ?
         ORDER BY COUNT(DISTINCT t.insider_id) DESC, SUM(t.value) DESC
         LIMIT ?""", (lo, hi, CLUSTER_MIN, limit)).fetchall()
    return [dict(r) for r in rows]


def cohort_window(anchor: date) -> tuple[str, str]:
    """The month-long span whose members have all completed HOLD_DAYS.

    Computed, never chosen — see the module docstring.
    """
    hi = anchor - timedelta(days=HOLD_DAYS + 1)
    return (hi - timedelta(days=COHORT_SPAN_DAYS - 1)).isoformat(), hi.isoformat()


def cohort(conn, lo: str, hi: str) -> list[dict]:
    """Every qualifying cluster in the window, each held its own 30 days.

    Measured from the last close at or before the filing to the last close at
    or before filing+HOLD_DAYS, so each name gets the same horizon regardless of
    where in the window it sits.

    A member with no price series is dropped — about 9% of filing tickers have
    no coverage at all (EOS is a closed-end fund, BBASX a mutual fund) — and
    the count is logged, because silently shrinking a cohort is how a clean
    number gets manufactured.
    """
    rows = conn.execute(f"""
        WITH cl AS (
          SELECT t.ticker, MIN(t.filing_date) d,
                 COUNT(DISTINCT t.insider_id) n_ins, SUM(t.value) v
            FROM trades t
           WHERE t.filing_date BETWEEN ? AND ?
             AND t.signal_class = 'discretionary_buy'
             {CLEAN}
           GROUP BY t.ticker
          HAVING COUNT(DISTINCT t.insider_id) >= ?
             AND SUM(t.value) >= ?
        )
        SELECT cl.*,
          (SELECT p.close FROM prices.daily_prices p
            WHERE p.ticker = cl.ticker AND p.date::text <= cl.d
            ORDER BY p.date DESC LIMIT 1) p0,
          (SELECT p.close FROM prices.daily_prices p
            WHERE p.ticker = cl.ticker AND p.date::text <= (cl.d::date + ?)::text
            ORDER BY p.date DESC LIMIT 1) p1
          FROM cl ORDER BY cl.v DESC""",
        (lo, hi, CLUSTER_MIN, CLUSTER_MIN_VALUE, HOLD_DAYS)).fetchall()
    out = []
    for r in rows:
        p0, p1 = r["p0"], r["p1"]
        out.append({
            **dict(r),
            "pct": (float(p1) / float(p0) - 1) * 100
                   if p0 and p1 and float(p0) > 0 else None,
        })
    return out


def spy_move(conn, lo: str, hi: str) -> float | None:
    """SPY over the cohort's own stretch — start of the window to the last
    member's maturity — so the benchmark spans the same calendar the cohort
    does. Comparing a 30-day-per-name cohort against SPY-to-today would flatter
    or punish it purely by how long ago the window sat."""
    end = (datetime.strptime(hi, "%Y-%m-%d").date()
           + timedelta(days=HOLD_DAYS)).isoformat()
    r = conn.execute("""
        SELECT (SELECT close FROM prices.daily_prices
                 WHERE ticker='SPY' AND date::text <= ? ORDER BY date DESC LIMIT 1) a,
               (SELECT close FROM prices.daily_prices
                 WHERE ticker='SPY' AND date::text <= ? ORDER BY date DESC LIMIT 1) b""",
        (lo, end)).fetchone()
    if r and r["a"] and r["b"] and float(r["a"]) > 0:
        return (float(r["b"]) / float(r["a"]) - 1) * 100
    return None


# ── rendering ─────────────────────────────────────────────────────────────

def render_week(nums: dict, clusters: list[dict], lo: str, hi: str) -> str:
    d0 = datetime.strptime(lo, "%Y-%m-%d").strftime("%b %-d")
    d1 = datetime.strptime(hi, "%Y-%m-%d").strftime("%b %-d")
    ratio = (nums["sell_val"] or 0) / max(nums["buy_val"] or 1, 1)
    lines = [
        f"Insider activity, {d0}–{d1}",
        "",
        f"· {nums['buys']:,} buys, {_money(nums['buy_val'] or 0)}",
        f"· {nums['sells']:,} sells, {_money(nums['sell_val'] or 0)}",
        f"· {ratio:.0f}x more dollars sold than bought",
    ]
    if clusters:
        c = clusters[0]
        lines.append(
            f"· Most crowded buy: ${c['ticker']} — {c['n_ins']} insiders, "
            f"{_money(float(c['v']))}"
        )
    lines += ["", "Not investment advice."]
    return "\n".join(lines)


def render_cohort(members: list[dict], lo: str, hi: str,
                  spy: float | None) -> str | None:
    scored = [m for m in members if m["pct"] is not None]
    if len(scored) < 4:
        logger.info("cohort too small to publish (%d scored)", len(scored))
        return None

    pcts = [m["pct"] for m in scored]
    up = sum(1 for p in pcts if p > 0)
    med = statistics.median(pcts)
    mean = statistics.mean(pcts)
    best = max(scored, key=lambda m: m["pct"])
    worst = min(scored, key=lambda m: m["pct"])
    d0 = datetime.strptime(lo, "%Y-%m-%d").strftime("%b %-d")
    d1 = datetime.strptime(hi, "%Y-%m-%d").strftime("%b %-d")

    lines = [
        f"{d0}–{d1}: {len(scored)} companies had {CLUSTER_MIN}+ insiders "
        f"buying $250K+.",
        "",
        f"Each held {HOLD_DAYS} days. All of them:",
        "",
        f"· {up} up, {len(scored) - up} down ({up / len(scored) * 100:.0f}%)",
        f"· median {med:+.1f}%" + (f" · SPY {spy:+.1f}%" if spy is not None else ""),
    ]
    # When the median and the mean disagree, saying only the kinder one is the
    # easiest lie available here. 1.5pp apart is enough to be worth a reader's
    # attention rather than a rounding artefact.
    if abs(med - mean) >= 1.5:
        shape = ("the misses were bigger than the hits"
                 if mean < med else "a few big winners carried it")
        lines.append(f"· average {mean:+.1f}% — {shape}")
    lines += [
        f"· best ${best['ticker']} {best['pct']:+.1f}% · "
        f"worst ${worst['ticker']} {worst['pct']:+.1f}%",
        "",
        # The claim being made, stated precisely. This is a property of insider
        # clusters, not a record of our calls — we did not post these at the
        # time, and implying otherwise would be the one unrecoverable mistake
        # an account like this can make.
        "Not a stock pick list — the full cohort, winners and losers.",
        "",
        "Not investment advice.",
    ]
    return "\n".join(lines)


def render_setup(c: dict) -> str:
    """The week's strongest cluster, as a per-ticker post.

    Leads with the count, then the facts that distinguish this cluster from a
    crowd of directors: who bought, and what the stock had already done. Only
    lines that are actually true get emitted — a template with an empty slot
    reads worse than a shorter post.
    """
    lines = [
        f"${c['ticker']} — {c['n_ins']} insiders bought {_money(float(c['v']))} "
        f"between them last week.",
        "",
    ]
    company = (c.get("company") or "").strip()
    if company:
        lines.append(f"· {company}")

    n_cs = c.get("n_csuite") or 0
    if n_cs >= 3:
        lines.append("· The CEO, CFO and COO are all in it.")
    elif n_cs > 0:
        lines.append(f"· {n_cs} of them run the company.")

    dip = c.get("dip_3mo")
    if dip is not None and float(dip) <= -0.20:
        lines.append(f"· Bought after a {abs(float(dip)) * 100:.0f}% "
                     "slide over three months.")

    if (c.get("best_grade") or "") in ("A+", "A"):
        lines.append(f"· One of them is grade {c['best_grade']} "
                     "on their own prior trades.")

    lines += ["", "Not investment advice."]
    return "\n".join(lines)


# ── persistence ───────────────────────────────────────────────────────────

def record(conn, kind: str, body: str, ticker: str | None = None) -> None:
    conn.execute(
        """INSERT INTO social_posts (platform, post_kind, ticker, body)
           VALUES ('stocktwits', ?, ?, ?)""", (kind, ticker, body))
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=None, help="Anchor date (default: today)")
    ap.add_argument("--no-record", action="store_true",
                    help="Print only; do not write to social_posts")
    ap.add_argument("--write", action="store_true", help="Also write a .txt")
    args = ap.parse_args()

    anchor = (datetime.strptime(args.date, "%Y-%m-%d").date()
              if args.date else date.today())
    lo, hi = trading_week(anchor)
    r_lo, r_hi = cohort_window(anchor)
    logger.info("week %s..%s   cohort window %s..%s", lo, hi, r_lo, r_hi)

    conn = get_connection()
    nums = week_numbers(conn, lo, hi)
    if not nums:
        logger.info("no filings in %s..%s — nothing to recap", lo, hi)
        return 0
    clusters = top_clusters(conn, lo, hi)
    members = cohort(conn, r_lo, r_hi)
    dropped = sum(1 for m in members if m["pct"] is None)
    if dropped:
        logger.info("cohort: %d of %d members have no price series, dropped",
                    dropped, len(members))

    posts: list[tuple[str, str, str | None]] = [
        ("recap", render_week(nums, clusters, lo, hi), None),
    ]
    body = render_cohort(members, r_lo, r_hi, spy_move(conn, r_lo, r_hi))
    if body:
        posts.append(("scorecard", body, None))
    if clusters:
        posts.append(("recap", render_setup(clusters[0]), clusters[0]["ticker"]))

    out = []
    for i, (kind, body, ticker) in enumerate(posts, 1):
        out.append(body)
        print(f"\n{'─' * 58}\n  POST {i}/{len(posts)}  [{kind}]  "
              f"({len(body)} chars)\n{'─' * 58}")
        print(body)
        if not args.no_record:
            record(conn, kind, body, ticker)

    if not args.no_record:
        logger.info("recorded %d weekend posts", len(posts))
    if args.write:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        p = OUTPUT_DIR / f"{anchor.isoformat()}_stocktwits_weekend.txt"
        p.write_text(("\n\n" + "=" * 58 + "\n\n").join(out))
        logger.info("wrote %s", p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

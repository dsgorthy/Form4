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

from api.ownership import position_change  # noqa: E402
from config.database import get_connection  # noqa: E402
from pipelines.insider_study.annotate_trade import annotate, clean_title  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "content"

GRADE_WEIGHT = {"A+": 100, "A": 80, "B": 40, "C": 15, "D": 0}

SQL = """
-- One row per insider per ticker per day — the unit a person tells a story
-- about. A Form 4 routinely reports one decision as several execution lots,
-- and an insider can file more than one accession the same day, so querying
-- raw rows both understates the trade and misranks it.
--
-- Benjamin Wood's CDNL purchase was two lots, 13,627 at $39.22 and 12,073 at
-- $39.77. Ungrouped it posted as $534K; it is $1,014,594. Jeremy Spivey bought
-- $3.2M of the same stock across four lots and never surfaced at all, because
-- no single lot of his outranked anyone.
SELECT
    MIN(t.trade_id)              AS trade_id,
    t.ticker,
    MAX(t.company)               AS company,
    t.signal_class,
    SUM(t.value)                 AS value,
    SUM(t.qty)                   AS qty,
    -- Part of the grouping key, not decoration. A TSMC VP filed Taiwan-listed
    -- 2330.TW shares priced in TWD alongside US ADRs on the same day; summed
    -- together they produced a $72.73 blended "fill" against a $431 close and
    -- the post claimed the stock was up 493%. Class A and Series A Preference
    -- shares are the same mistake in a domestic filing (LILA, $27.9M blended
    -- across three classes). Different securities are different decisions.
    t.security_title             AS security_title,
    -- Volume-weighted, not the first lot's price: reporting one lot's price
    -- beside a summed value is how the site and the post disagreed.
    SUM(t.value) / NULLIF(SUM(t.qty), 0) AS price,
    -- Every lot, kept whole. The stake maths cannot be done with one
    -- aggregated balance: shares_owned_after is reported per ownership line,
    -- and a fund selling through seven partnerships reports seven of them.
    -- api.ownership.position_change reconciles them; see its module docstring
    -- for the two filings this got wrong in public.
    JSON_AGG(JSON_BUILD_OBJECT(
        'trade_id', t.trade_id, 'trade_date', t.trade_date, 'qty', t.qty,
        'shares_owned_after', t.shares_owned_after,
        'direct_indirect', t.direct_indirect,
        'nature_of_ownership', t.nature_of_ownership
    ) ORDER BY t.trade_date, t.trade_id) AS lots,
    MAX(t.is_largest_ever::int)  AS is_largest_ever,
    MAX(t.is_rare_reversal::int) AS is_rare_reversal,
    MAX(t.consecutive_sells_before) AS consecutive_sells_before,
    MIN(t.dip_1mo)               AS dip_1mo,
    MIN(t.dip_3mo)               AS dip_3mo,
    MAX(t.pit_cluster_size)      AS pit_cluster_size,
    -- Publication-time cluster, which is a different question from the PIT
    -- one. pit_cluster_size counts only insiders who filed *before* this row,
    -- so the first filer of the day scores 0 by construction — correct for a
    -- backtest, wrong for a post written that evening when all of them are
    -- known. On 2026-08-17 six Cardinal Infrastructure insiders bought $8.2M
    -- between them; the PIT counts were 0, 1, 2, 3, 4, 5 and the post led with
    -- one man's $1.0M.
    (SELECT COUNT(DISTINCT c.insider_id) FROM trades c
      WHERE c.ticker = t.ticker AND c.filing_date = t.filing_date
        AND c.signal_class = t.signal_class
        AND NOT COALESCE(c.value_suspect, FALSE)
        AND (c.is_duplicate = 0 OR c.is_duplicate IS NULL)
        AND c.superseded_by IS NULL)          AS day_cluster_n,
    -- How many insiders in this cluster filed MORE THAN ONE trade.
    --
    -- This is what separates a decision from a window opening. When a trading
    -- window opens after earnings, everyone sells once and the cluster is an
    -- artefact of the calendar, not of anybody's conviction. MEDP on
    -- 2026-08-21 was exactly that: three insiders, four filings, all traded
    -- 08-19/08-20, none carrying a routine marker — and it led the feed over
    -- genuinely interesting trades.
    (SELECT COUNT(*) FROM (
        SELECT c.insider_id FROM trades c
         WHERE c.ticker = t.ticker AND c.filing_date = t.filing_date
           AND c.signal_class = t.signal_class
           AND NOT COALESCE(c.value_suspect, FALSE)
           AND (c.is_duplicate = 0 OR c.is_duplicate IS NULL)
           AND c.superseded_by IS NULL
         GROUP BY c.insider_id HAVING COUNT(*) > 1) m)  AS cluster_multi_filers,
    (SELECT SUM(c.value) FROM trades c
      WHERE c.ticker = t.ticker AND c.filing_date = t.filing_date
        AND c.signal_class = t.signal_class
        AND NOT COALESCE(c.value_suspect, FALSE)
        AND (c.is_duplicate = 0 OR c.is_duplicate IS NULL)
        AND c.superseded_by IS NULL)          AS day_cluster_value,
    MAX(t.career_grade)          AS career_grade,
    MAX(t.title)                 AS insider_title,
    MAX(COALESCE(i.is_entity, 0)) AS is_entity,
    t.filing_date,
    MAX(COALESCE(i.display_name, i.name)) AS insider_name,
    MAX(i.slug)                  AS insider_slug,
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
   -- Routine and pre-scheduled activity is not a signal and should not be a
   -- post.
   --
   -- CORRECTION 2026-08-21: an earlier version of this comment claimed
   -- is_10b5_1 and aff_10b5_1 were "never populated" because both are 0 across
   -- all 33,533 discretionary sells. That was a scoping error, not a dead
   -- column. is_10b5_1 is set on 27,800 filings; it is zero here BY
   -- CONSTRUCTION, because signal_class routes 10b5-1 sells to `planned_sell`
   -- and this query only admits the discretionary classes. All 23,885 planned
   -- sells carry it.
   --
   -- The filters below are therefore belt-and-braces over the signal_class
   -- restriction rather than the primary defence: cohen_routine (100%
   -- populated) and is_routine (16%) both cut ACROSS signal_class and catch
   -- discretionary-looking activity that is really recurring.
   AND COALESCE(t.is_routine, 0) = 0
   AND COALESCE(t.cohen_routine, 0) = 0
   AND COALESCE(t.is_tax_sale, 0) = 0
 GROUP BY t.insider_id, t.ticker, t.signal_class, t.filing_date, t.security_title
HAVING SUM(t.value) > 25000
"""


def is_cluster_story(t: dict) -> bool:
    """Should this be told as "N insiders did X the same day"?

    A BUY cluster always qualifies — several people independently choosing to
    put money in on one day is the story, whatever size their individual
    cheques.

    A SELL cluster does not. Post-earnings the window opens and everyone sells
    once; the cluster is the calendar, not a decision. Derek's rule after MEDP
    led the 2026-08-21 feed: a selling cluster is only a story when at least
    two of its participants filed more than one trade, which is the signature
    of people acting rather than people being unblocked.
    """
    if (t.get("day_cluster_n") or 1) - 1 < 2:
        return False
    if t["signal_class"] == "discretionary_buy":
        return True
    return (t.get("cluster_multi_filers") or 0) >= 2


def hooks(t: dict) -> list[str]:
    """Why this trade is worth a post. Empty means it is not.

    The feed used to be a ranking with no floor: the top five of whatever
    filed that day got posted, so on a quiet day the fifth-best trade was
    still a post. This is the floor. A trade needs at least one concrete
    reason to exist beyond being large.
    """
    found: list[str] = []
    is_buy = t["signal_class"] == "discretionary_buy"

    if is_cluster_story(t) and (t.get("day_cluster_value") or 0) >= 1_000_000:
        found.append("cluster")

    if is_buy:
        if (t.get("career_grade") or "") in ("A+", "A", "B"):
            found.append("graded insider")
        if t.get("is_first_ever"):
            found.append("first ever purchase")
        if t.get("is_largest_ever"):
            found.append("largest ever purchase")
        if t.get("is_rare_reversal"):
            found.append("reversal")
        dip = t.get("dip_3mo")
        if dip is not None and dip <= -0.25:
            found.append("bought a drawdown")
        return found

    # Sells. No career grade exists on the sell side, so the hook has to come
    # from the position: someone leaving, or cutting deeply. Trimming is not
    # news no matter how many dollars it represents.
    pc = position_change(t.get("lots") or [t], is_buy=False)
    if pc is not None:
        if pc.is_full_exit:
            found.append("full exit")
        elif pc.fraction is not None and pc.fraction >= 0.33:
            # `fraction` is POSITIVE and of the prior position — 0.33 means the
            # stake was cut by a third. It is not a signed pct_change.
            found.append("cut a third of the stake")
    if t.get("is_largest_ever"):
        found.append("largest ever sale")
    return found


def score(t: dict) -> float:
    """Rank, not a prediction. Grade dominates; size and cluster break ties.

    Buys and sells are scored on separate scales and never against each other.
    compute_career_grades stamps a grade only on trans_code='P', so a sell can
    never carry one — put both sides in one ranking and every sell scores the
    default forever, the reserved slots never fill, and the feed is all buys by
    construction. Which is exactly what the first run produced.
    """
    import math
    # Two rankings in one number, because this score does two jobs: it orders
    # tickers against each other, and it picks which filer represents a ticker
    # (only one row per ticker survives the dedupe below).
    #
    # `size` is the STORY's scale — the whole cluster's dollars when the post
    # will lead with the cluster, since that is the figure the reader sees.
    # Ranking a cluster on one participant's cheque put a $750K day above an
    # $8.2M one. But story scale is identical for every filer on the ticker,
    # so on its own it makes the within-ticker pick arbitrary: EAT chose the
    # $442K director over the $3.8M one. `own` breaks that tie, and is scaled
    # small enough that it never reorders the tickers themselves.
    others = max((t.get("day_cluster_n") or 1) - 1, 0)
    story_value = (t.get("day_cluster_value") or 0) if others >= 2 else 0
    story_value = max(story_value, t.get("value") or 1)
    size = min(math.log10(max(story_value, 1)) * 6, 45)
    own = min(math.log10(max(t.get("value") or 1, 1)) * 2, 14)
    # Count alone is not a story — five insiders splitting $750K is a payroll
    # event. The bonus only applies once the day clears seven figures.
    cluster = min(others * 5, 20) if story_value >= 1_000_000 else 0

    # A fund vehicle trimming 1% is a worse representative of the day than a
    # sitting officer, and PBF picked the fund over four selling executives.
    # A penalty rather than an exclusion: when the entity is the only filer on
    # the name, it should still get the slot.
    title = (t.get("insider_title") or "").upper()
    if t.get("is_entity") or "10%" in title:
        own -= 10
    if t["signal_class"] == "discretionary_sell":
        # No grade exists, so size and company carry it, plus a nudge for a
        # holder actually leaving rather than trimming. "Leaving" has to come
        # from the reconciled position: a fund that sold 2.5% of a stake held
        # across seven partnerships reports a near-zero balance on the last
        # line, which is what used to earn this bonus.
        pc = position_change(t.get("lots") or [t], is_buy=False)
        exiting = 12 if (pc is not None and pc.is_full_exit) else 0
        return size * 1.6 + own + cluster + exiting
    return float(GRADE_WEIGHT.get(t.get("career_grade") or "", 5)) + size + own + cluster


def _amount(val: float) -> str:
    # Switch at the point where the K form would ROUND to four digits, not at
    # the million. $999,600 is "$1.0M", never "$1000K".
    return f"${val / 1_000_000:.1f}M" if val >= 999_500 else f"${val / 1_000:.0f}K"


def render(t: dict) -> str:
    """One Stocktwits post. Cashtag first — it is the discovery surface."""
    is_buy = t["signal_class"] == "discretionary_buy"
    verb = "bought" if is_buy else "sold"
    val = t.get("value") or 0

    title = clean_title(t.get("insider_title"))
    name = (t.get("insider_name") or "").strip()
    who = title if (not name or name.lower() in title.lower()) else f"{title} {name}"

    # Three or more insiders acting on one name in one day is a different
    # story from any one of them acting alone, and it is the one worth
    # leading with. The individual becomes the supporting detail rather than
    # the headline.
    others = max((t.get("day_cluster_n") or 1) - 1, 0)
    noun = "buying" if verb == "bought" else "selling"
    lines: list[str] = []
    if is_cluster_story(t):
        n = t["day_cluster_n"]
        # "the same day" meant the same FILING day — day_cluster_n and
        # day_cluster_value both group on filing_date — but a reader takes it
        # as the same trading day, and the two are routinely different. IRDM's
        # six sellers disclosed together on 2026-08-19 having traded across
        # 08-17 and 08-18; Fitzpatrick's $11.2M is a single accession covering
        # both. The totals were right and the sentence was not.
        #
        # Disclosure day is also the more interesting claim: six people
        # choosing to surface on one date is the story, and it is the date the
        # market actually learns.
        lines = [f"${t['ticker']} — {n} insiders disclosed "
                 f"{_amount(t.get('day_cluster_value') or val)} of {noun} the same day", ""]
        lines.append(f"· {who} {verb} {_amount(val)} of it.")
        lines += [f"· {a}" for a in annotate(t, max_lines=3 if others < 2 else 2)]
    else:
        # The blank separator belongs to the bullet block, not the headline.
        # Emitting it unconditionally left a doubled blank line on every post
        # that had no annotation to make — which is most of the small buys.
        lines = [f"${t['ticker']} — {who} {verb} {_amount(val)}"]
        bullets = [f"· {a}" for a in annotate(t, max_lines=3 if others < 2 else 2)]
        if bullets:
            lines += [""] + bullets

    # Stated as a data point, not a pitch. Stocktwits bans links used as
    # "direct advertisements or sales pitches for a paid product", and the
    # earlier phrasing ("our top tier has beaten the S&P by ~2%") was a
    # performance claim advertising the service. The grade itself is analysis,
    # which the rules explicitly allow, so keep the grade and drop the sell.
    grade = t.get("career_grade")
    if grade:
        lines += ["", f"Insider grade: {grade} (from their own prior trades)"]

    # NO LINK. Removed 2026-08-20: Stocktwits demotes posts containing links,
    # so the one link per post was costing far more reach than the clicks it
    # earned. The $CASHTAG is the distribution mechanism here, not the URL —
    # readers who want the filing history search the ticker. Do not reinstate
    # a link without measuring impressions on a linked vs unlinked cohort.
    lines += ["", "Not investment advice."]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Filing date (default: today)")
    ap.add_argument("--count", type=int, default=None,
                    help="Exact number of posts (overrides --min/--max)")
    ap.add_argument("--min-count", type=int, default=3,
                    help="Always post at least this many, even on a thin day")
    ap.add_argument("--max-count", type=int, default=10)
    ap.add_argument("--write", action="store_true", help="Also write to data/content/")
    args = ap.parse_args()

    day = args.date or date.today().isoformat()
    conn = get_connection()
    rows = [dict(r) for r in conn.execute(SQL, (day,)).fetchall()]
    if not rows:
        logger.info("No qualifying filings for %s", day)
        return 0

    # How many posts today is a property of the day, not a constant. A day
    # with eleven genuinely notable filings should produce eleven posts; a
    # quiet one should not be padded to five with whatever ranked fifth.
    lo = args.count or args.min_count
    hi = args.count or args.max_count

    notable = [r for r in rows if hooks(r)]
    filler = [r for r in rows if not hooks(r)]
    logger.info("%s: %d candidates, %d clear the notability bar",
                day, len(rows), len(notable))

    def split(pool):
        return (sorted([r for r in pool if r["signal_class"] == "discretionary_buy"],
                       key=score, reverse=True),
                sorted([r for r in pool if r["signal_class"] == "discretionary_sell"],
                       key=score, reverse=True))

    buys, sells = split(notable)

    # One post per ticker, and hold the buy/sell mix near 3:2 so the feed does
    # not read as a permabull on a heavy buy day or a doomsayer on a heavy sell
    # one. Caps are soft: if a side runs out, the other fills the slots.
    target = max(lo, min(hi, len(notable)))
    n_sell = max(1, target * 2 // 5)
    picked, seen = [], set()

    def take(pool, limit):
        for r in pool:
            if limit <= 0 or len(picked) >= target:
                return
            if r["ticker"] in seen:
                continue
            seen.add(r["ticker"])
            picked.append(r)
            limit -= 1

    take(sells, n_sell)
    take(buys, target - len(picked))
    take(sells + buys, target - len(picked))       # one side ran thin

    # The floor is a promise to post daily, so it is allowed to reach past the
    # notability bar — but only that far, and it is logged as what it is.
    if len(picked) < lo:
        short = lo - len(picked)
        fb, fs = split(filler)
        take(sorted(fb + fs, key=score, reverse=True), short)
        logger.info("only %d notable; backfilled %d to meet the %d floor",
                    len(notable), short, lo)

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

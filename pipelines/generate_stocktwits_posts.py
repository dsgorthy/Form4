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
    MIN(t.filing_key)            AS filing_key,
    t.insider_id                 AS insider_id,
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
    -- JOINT FILERS ARE ONE SELLER, NOT SEVERAL.
    --
    -- 2026-08-24, AVAH: J.H. Whitney Equity Partners VII sold, and its two
    -- managing members each filed their own Form 4 reporting the same
    -- underlying transactions as indirect beneficial owners. Three accessions,
    -- three insider_ids, one economic event — and the post said "6 insiders
    -- disclosed $558.5M of selling" when the real figure was about a third of
    -- that by one seller.
    --
    -- Accession does not dedupe them; each related person files separately.
    -- The signature is (trade_date, value), which is what
    -- api.filters.deduplicate_filers has always used for the same reason.
    -- THE CLUSTER SUBQUERIES CARRY THE SAME EXCLUSIONS AS THE MAIN WHERE.
    --
    -- They did not, so the headline counted activity that could never itself
    -- be a post. RBLX on 2026-08-24: 12 discretionary sells, 6 of them
    -- cohen_routine, and the post said "6 insiders disclosed $4.3M" using all
    -- twelve. A cluster is only a story if the things in it are stories.
    --
    -- DISTINCT PEOPLE, after collapsing joint filers.
    --
    -- Counting rows over-reports (three filers, one sale). Counting distinct
    -- (trade_date, value) events over-reports differently — AVAH's three
    -- related filers made nine separate sales between them, and "9 insiders"
    -- is as wrong as "6". So: reduce to one row per economic event, then count
    -- the distinct people left. That is the number of sellers.
    (SELECT COUNT(DISTINCT ev.insider_id) FROM (
        SELECT DISTINCT ON (c.trade_date, c.value) c.insider_id
          FROM trades c
         WHERE c.ticker = t.ticker AND c.filing_date = t.filing_date
           AND c.signal_class = t.signal_class
           AND NOT COALESCE(c.value_suspect, FALSE)
           AND (c.is_duplicate = 0 OR c.is_duplicate IS NULL)
           AND c.superseded_by IS NULL
           AND COALESCE(c.is_routine, 0) = 0
           AND COALESCE(c.cohen_routine, 0) = 0
           AND COALESCE(c.is_tax_sale, 0) = 0
         ORDER BY c.trade_date, c.value, c.insider_id) ev)  AS day_cluster_events,
    (SELECT COUNT(DISTINCT c.insider_id) FROM trades c
      WHERE c.ticker = t.ticker AND c.filing_date = t.filing_date
        AND c.signal_class = t.signal_class
        AND NOT COALESCE(c.value_suspect, FALSE)
        AND (c.is_duplicate = 0 OR c.is_duplicate IS NULL)
        AND c.superseded_by IS NULL
        AND COALESCE(c.is_routine, 0) = 0
        AND COALESCE(c.cohen_routine, 0) = 0
        AND COALESCE(c.is_tax_sale, 0) = 0
        )          AS day_cluster_n,
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
           AND COALESCE(c.is_routine, 0) = 0
           AND COALESCE(c.cohen_routine, 0) = 0
           AND COALESCE(c.is_tax_sale, 0) = 0
         GROUP BY c.insider_id HAVING COUNT(*) > 1) m)  AS cluster_multi_filers,
    -- Summed over DISTINCT (trade_date, value) for the same reason: adding
    -- every filer's row triples a transaction three people reported.
    (SELECT COALESCE(SUM(e.value), 0) FROM (
        SELECT DISTINCT c.trade_date, c.value FROM trades c
         WHERE c.ticker = t.ticker AND c.filing_date = t.filing_date
           AND c.signal_class = t.signal_class
           AND NOT COALESCE(c.value_suspect, FALSE)
           AND (c.is_duplicate = 0 OR c.is_duplicate IS NULL)
           AND c.superseded_by IS NULL
           AND COALESCE(c.is_routine, 0) = 0
           AND COALESCE(c.cohen_routine, 0) = 0
           AND COALESCE(c.is_tax_sale, 0) = 0
           ) e)     AS day_cluster_value,
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
   -- SELLING SHARES YOU WERE HANDED LAST WEEK IS NOT A DECISION.
   --
   -- Reads the `post_vest_dump` TAG rather than re-deriving the window here.
   -- The tag is the product's one definition of this behaviour
   -- (pipelines/insider_study/compute_signals.py) and it is rendered on the
   -- filing, so a post and a page cannot disagree about the same trade.
   --
   -- 2026-08-24: twelve Procter & Gamble executives filed sales on 08-20, all
   -- granted shares on 08-19. signal_class called every one of them
   -- `discretionary_sell`, because the classifier reads a row in isolation and
   -- cannot see the grant that landed the day before. It is not a PG quirk —
   -- 23.1% of discretionary sells in 180 days follow an A or M within 5 days.
   --
   -- The tag itself had been erroring since the Postgres migration (a SQLite
   -- `date(col, '+30 days')` the compat layer does not rewrite), which is why
   -- this could not simply have read it before.
   AND NOT EXISTS (
         SELECT 1 FROM trade_signals v
          WHERE v.trade_id = t.trade_id
            AND v.signal_type IN ('post_vest_dump', 'exercise_and_sell'))
   -- SUBSCRIBER GUARD. Never post a filing a live strategy traded around.
   --
   -- The case for posting freely is that the product is the SELECTION, not the
   -- ticker: 2026 had 9,340 postable filings against 41 strategy entries, a
   -- 228x pool, so a reader seeing ~200 posts a month cannot tell which two
   -- became positions. That holds in aggregate and fails for the individual
   -- name — a subscriber should never read their own alert here for free. At
   -- ~41 filings a year against ~3,600 posts this costs about 1% of volume,
   -- which is a cheap way to make "no posting delay" safe to commit to.
   --
   -- +/-3 sessions rather than same-day: entry can lag the filing by a session
   -- (after-bell filings fill at the next open) and the alert goes out first.
   AND NOT EXISTS (
       SELECT 1 FROM strategy_portfolio sp
        WHERE sp.ticker = t.ticker
          AND sp.execution_source = 'simulated'
          AND sp.strategy IN ('quality_notrend', 'quality_momentum', 'reversal_dip')
          AND sp.entry_date::date BETWEEN t.filing_date::date - 3
                                      AND t.filing_date::date + 3
   )
 GROUP BY t.insider_id, t.ticker, t.signal_class, t.filing_date, t.security_title
HAVING SUM(t.value) > 25000
"""

# ─── Multi-day context ───────────────────────────────────────────────────────
#
# EVERYTHING ABOVE IS ONE FILING DAY WIDE. INSIDER BEHAVIOUR IS NOT.
#
# A filing is a tranche of a decision that usually takes several days to
# execute and disclose. Ranking and describing one tranche in isolation
# produced four distinct failures in the 2026-08-25 batch alone:
#
#   $BABA  Joseph Tsai bought $10.3M on 08-24 and $10.4M on 08-25, and BOTH
#          were posted, each as an isolated "largest purchase they've ever
#          made". Both rows carry is_largest_ever and the second only wins by
#          the 1.3% price difference on an identical 720,000-share block, so
#          the flag was true, useless and repeated. The actual story — $20.7M
#          over two days, the CEO buying $5.0M alongside him, and the first
#          open-market purchases by any Alibaba insider on record — is
#          invisible to a query that cannot see past one filing_date.
#
#   $AUGO  Bruno Sousa's third consecutive selling disclosure in six days
#          ($22.9M, $10.75M, $19.6M). Posted twice, each time as though it
#          were the first, and each time with a stake percentage derived from
#          that day's lots alone.
#
#   $TTMI  Different insiders buying on consecutive days. day_cluster_n counts
#   $AMR   only within a single filing_date, so a cluster forming across the
#          week scores as two unrelated singles and neither leads.
#
#   $CPAY  A $49.3M "CEO sale" that is the tail of a 450,000-share option
#          exercise four days earlier. The exercise_and_sell tag spans three
#          days, so the last tranche of a multi-day liquidation escapes it.
#
# Fetched for the CANDIDATES ONLY, as a handful of small queries keyed on the
# candidate tickers, rather than as more correlated subqueries hung off the
# main SELECT — that statement already carries four and is at the limit of
# what anyone will read.

#: How far back a single insider's own accumulation/distribution is followed.
LOOKBACK_PROGRAM_DAYS = 30
#: How far back OTHER insiders at the same company are followed.
LOOKBACK_CLUSTER_DAYS = 30
#: How far back the opposite side is followed, to spot a regime change.
LOOKBACK_OPPOSITE_DAYS = 90
#: An M (exercise) or A (grant) this recently makes a sale partly mechanical.
LOOKBACK_EXERCISE_DAYS = 14
#: Posting the same ticker again inside this many days needs a reason.
REPOST_QUIET_DAYS = 10
#: ...and that reason is either a materially bigger program, or a new insider.
REPOST_GROWTH = 1.5

# ── Rate limits. These exist because the account was banned. ───────────────
#
# Stocktwits suspended us on 2026-08-26. Nothing in the content broke a
# published rule -- measured across the 53 posts: no links, no promotion, no
# irrelevant cashtags, and a median pairwise structural similarity of 45% with
# 50 of 50 distinct opening lines, so "the same message or nearly identical
# ones repeatedly" does not describe them either. Their rules explicitly
# welcome bots that are "data feeds".
#
# What did happen: on 2026-08-24 the generator RAN TWICE, at 18:08 and 19:10,
# and put out 20 posts across 19 cashtags in 62 minutes from an account that
# was four days old. record_posts is idempotent per FILING, so the second run
# posted no duplicates -- it simply took the next ten of the sixty candidates
# that had cleared the bar. The guard stopped repeats; nothing stopped a
# second batch.

#: Hard ceiling per CALENDAR DAY, counted against social_posts rather than
#: against this process, so a second invocation is a no-op instead of a
#: second batch.
MAX_POSTS_PER_DAY = 5

#: A ticker may not reappear inside this many days FOR ANY REASON. Distinct
#: from REPOST_QUIET_DAYS, which asks whether the same STORY has moved on and
#: has escape hatches (a new insider, a program 1.5x bigger). This one has
#: none: it is a floor under how often a follower can see the same cashtag.
#:
#: 14 rather than 7 because it is free. Applied to the 53 posts we made, a
#: 7-day cooldown would have blocked 5 and a 14-day cooldown also blocks 5 --
#: the repeats were clustered within days of each other. And supply is not the
#: constraint: ~156 distinct tickers carry a discretionary filing every day
#: against 5 slots, so a fortnight of cooldown still leaves ~2,000 ticker-days
#: of candidates for 70 slots.
TICKER_COOLDOWN_DAYS = 14

# The main query types these six predicates four times over (main WHERE plus
# three cluster subqueries) and a comment there records what happened the one
# time a subquery did not carry them: RBLX counted six cohen_routine sells
# into a headline that said "6 insiders disclosed $4.3M". The context queries
# are a fifth, sixth and seventh copy, so they share one constant.
_EXCL = """
   AND NOT COALESCE(x.value_suspect, FALSE)
   AND (x.is_duplicate = 0 OR x.is_duplicate IS NULL)
   AND x.superseded_by IS NULL
   AND COALESCE(x.is_routine, 0) = 0
   AND COALESCE(x.cohen_routine, 0) = 0
   AND COALESCE(x.is_tax_sale, 0) = 0
"""

# One insider's own run on one ticker, one direction. Counted in FILINGS, not
# rows — a Form 4 reports one decision as however many lots the broker filled,
# and tests/unit/test_filing_level_grouping.py exists because that has already
# been miscounted in four separate places. filing_key is the same grouping key
# /insiders/{id}/trades uses.
CTX_PROGRAM = f"""
SELECT x.insider_id, x.ticker, x.signal_class,
       COUNT(DISTINCT COALESCE(x.filing_key, x.accession, x.trade_date::text)) AS n_filings,
       SUM(x.value)            AS value,
       MIN(x.filing_date)::text AS first_filing,
       MAX(x.filing_date)::text AS last_filing
  FROM trades x
 WHERE x.ticker = ANY(?)
   AND x.signal_class IN ('discretionary_buy', 'discretionary_sell')
   AND x.filing_date::date >  ?::date - ?
   AND x.filing_date::date <= ?::date
   {_EXCL}
 GROUP BY 1, 2, 3
"""

# The per-filing breakdown behind that total, so a post can say "$10.3M on
# 8/24, then another $10.4M on 8/25" instead of only the sum. Values are summed
# per filing across lots for the same reason as above.
CTX_PROGRAM_LEGS = f"""
SELECT x.insider_id, x.ticker, x.signal_class,
       x.filing_date::text AS filing_date,
       SUM(x.value)        AS value
  FROM trades x
 WHERE x.ticker = ANY(?)
   AND x.signal_class IN ('discretionary_buy', 'discretionary_sell')
   AND x.filing_date::date >  ?::date - ?
   AND x.filing_date::date <= ?::date
   {_EXCL}
 GROUP BY 1, 2, 3, 4
 ORDER BY 1, 2, 3, 4
"""

# Everyone at the company, same direction, over the window — the cross-DAY
# equivalent of day_cluster_events. Joint filers are collapsed first: a fund
# and its two managing members each file the same underlying sale, and counting
# accessions turned one AVAH seller into "6 insiders disclosed $558.5M". The
# signature is (trade_date, value), which is what api.filters.deduplicate_filers
# has always used.
CTX_CLUSTER = f"""
SELECT e.ticker, e.signal_class,
       COUNT(DISTINCT e.insider_id) AS n,
       SUM(e.value)                 AS value,
       MIN(e.filing_date)::text     AS first_filing,
       MAX(e.filing_date)::text     AS last_filing
  FROM (SELECT DISTINCT ON (x.ticker, x.signal_class, x.trade_date, x.value)
               x.ticker, x.signal_class, x.insider_id, x.value, x.filing_date
          FROM trades x
         WHERE x.ticker = ANY(?)
           AND x.signal_class IN ('discretionary_buy', 'discretionary_sell')
           AND x.filing_date::date >  ?::date - ?
           AND x.filing_date::date <= ?::date
           {_EXCL}
         ORDER BY x.ticker, x.signal_class, x.trade_date, x.value, x.insider_id) e
 GROUP BY 1, 2
"""

# Who else is in that cluster, largest first. "2 insiders bought $25.7M" is a
# statistic; "the CEO bought $5.0M alongside him" is the reason to read it.
# Joint filers collapsed first, same as above, then summed per person.
CTX_CLUSTER_MEMBERS = f"""
SELECT e.ticker, e.signal_class, e.insider_id,
       MAX(COALESCE(i.display_name, i.name)) AS insider_name,
       MAX(e.title)  AS insider_title,
       SUM(e.value)  AS value
  FROM (SELECT DISTINCT ON (x.ticker, x.signal_class, x.trade_date, x.value)
               x.ticker, x.signal_class, x.insider_id, x.value, x.title
          FROM trades x
         WHERE x.ticker = ANY(?)
           AND x.signal_class IN ('discretionary_buy', 'discretionary_sell')
           AND x.filing_date::date >  ?::date - ?
           AND x.filing_date::date <= ?::date
           {_EXCL}
         ORDER BY x.ticker, x.signal_class, x.trade_date, x.value, x.insider_id) e
  JOIN insiders i ON i.insider_id = e.insider_id
 GROUP BY 1, 2, 3
 ORDER BY 6 DESC
"""

# What the other side of the book did over a longer window. This is the line
# that makes BABA a story rather than a large purchase: Alibaba insiders filed
# nothing but exercises and sales for five months, and then the chairman and
# the CEO bought. A buy into persistent selling is a regime change; a buy into
# more buying is a trend.
CTX_OPPOSITE = f"""
SELECT x.ticker, x.signal_class, COUNT(*) AS n, SUM(x.value) AS value
  FROM trades x
 WHERE x.ticker = ANY(?)
   AND x.signal_class IN ('discretionary_buy', 'discretionary_sell')
   AND x.filing_date::date >  ?::date - ?
   AND x.filing_date::date <= ?::date
   {_EXCL}
 GROUP BY 1, 2
"""

# Same-direction activity BEFORE the program window, used only to say "first
# on record". Deliberately unbounded backwards.
CTX_PRIOR = f"""
SELECT x.ticker, x.signal_class, COUNT(*) AS n
  FROM trades x
 WHERE x.ticker = ANY(?)
   AND x.signal_class IN ('discretionary_buy', 'discretionary_sell')
   AND x.filing_date::date <= ?::date - ?
   {_EXCL}
 GROUP BY 1, 2
"""

# Option exercises and grants near a sale. NO exclusions here and no
# signal_class filter: M and A rows are by definition not discretionary, which
# is exactly why they never show up in the query above and why a sale that is
# really an option cash-out can look like a decision.
CTX_MECHANICAL = """
SELECT x.insider_id, x.ticker,
       MAX(x.trade_date)::text AS last_date,
       SUM(CASE WHEN x.trans_code = 'M' THEN x.qty ELSE 0 END) AS exercised_qty
  FROM trades x
 WHERE x.ticker = ANY(?)
   AND x.trans_code IN ('M', 'A')
   AND x.trade_date::date >  ?::date - ?
   AND x.trade_date::date <= ?::date
 GROUP BY 1, 2
"""

# What we last told people about this ticker. Without it the feed repeats
# itself: BABA went out twice in two days with near-identical wording.
CTX_LAST_POST = """
SELECT DISTINCT ON (s.ticker)
       s.ticker, s.ref_date::text AS ref_date, s.value, s.insider_name, s.direction
  FROM social_posts s
 WHERE s.platform = 'stocktwits' AND s.ticker = ANY(?)
 ORDER BY s.ticker, s.created_at DESC
"""


#: Tickers that are still inside the cooldown, whatever the story.
CTX_COOLDOWN = """
SELECT DISTINCT s.ticker
  FROM social_posts s
 WHERE s.platform = 'stocktwits'
   AND s.ticker IS NOT NULL
   AND s.posted_at::date > ?::date - ?
   AND s.posted_at::date <= ?::date
"""

#: How many posts already went out on this calendar day.
CTX_TODAY_COUNT = """
SELECT COUNT(*) AS n
  FROM social_posts
 WHERE platform = 'stocktwits' AND posted_at::date = ?::date
"""


def tickers_in_cooldown(conn, day: str) -> set:
    rows = conn.execute(
        CTX_COOLDOWN, (day, TICKER_COOLDOWN_DAYS, day)).fetchall()
    return {r[0] for r in rows}


def posts_already_today(conn, day: str) -> int:
    row = conn.execute(CTX_TODAY_COUNT, (day,)).fetchone()
    return int(row[0]) if row else 0


def _days_between(a: str | None, b: str | None) -> int:
    """Inclusive span in days between two ISO dates. 8/24→8/25 is two days."""
    if not a or not b:
        return 1
    return abs((date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days) + 1


_SPELLED = {2: "two", 3: "three", 4: "four", 5: "five", 6: "six",
            7: "seven", 8: "eight", 9: "nine", 10: "ten"}


def span_phrase(days: int) -> str:
    """"two days", "six days", "three weeks" — never "2 days"."""
    if days <= 1:
        return "a single day"
    if days <= 10:
        return f"{_SPELLED[days]} days"
    if days <= 14:
        return "two weeks"
    if days <= 24:
        return f"{days} days"
    return "a month" if days <= 34 else f"{days} days"


def attach_context(conn, rows: list[dict], day: str) -> None:
    """Fill each candidate with what was happening around it. Mutates in place.

    Every field added here is OPTIONAL downstream. annotate() drops a line
    whose input is missing rather than guessing, so a context query that
    returns nothing degrades to the old one-day behaviour instead of
    fabricating a program that is not there.
    """
    tickers = sorted({r["ticker"] for r in rows})
    if not tickers:
        return

    def q(sql, params):
        return [dict(r) for r in conn.execute(sql, params).fetchall()]

    prog = {(r["insider_id"], r["ticker"], r["signal_class"]): r
            for r in q(CTX_PROGRAM, (tickers, day, LOOKBACK_PROGRAM_DAYS, day))}
    legs: dict[tuple, list[dict]] = {}
    for r in q(CTX_PROGRAM_LEGS, (tickers, day, LOOKBACK_PROGRAM_DAYS, day)):
        legs.setdefault((r["insider_id"], r["ticker"], r["signal_class"]), []).append(r)
    clust = {(r["ticker"], r["signal_class"]): r
             for r in q(CTX_CLUSTER, (tickers, day, LOOKBACK_CLUSTER_DAYS, day))}
    members: dict[tuple, list[dict]] = {}
    for r in q(CTX_CLUSTER_MEMBERS, (tickers, day, LOOKBACK_CLUSTER_DAYS, day)):
        members.setdefault((r["ticker"], r["signal_class"]), []).append(r)
    opp = {(r["ticker"], r["signal_class"]): r
           for r in q(CTX_OPPOSITE, (tickers, day, LOOKBACK_OPPOSITE_DAYS, day))}
    prior = {(r["ticker"], r["signal_class"]): r
             for r in q(CTX_PRIOR, (tickers, day, LOOKBACK_PROGRAM_DAYS))}
    mech = {(r["insider_id"], r["ticker"]): r
            for r in q(CTX_MECHANICAL, (tickers, day, LOOKBACK_EXERCISE_DAYS, day))}
    try:
        posted = {r["ticker"]: r for r in q(CTX_LAST_POST, (tickers,))}
    except Exception as e:                                     # noqa: BLE001
        logger.warning("social_posts lookup failed, repeat guard is off: %s", e)
        posted = {}

    other = {"discretionary_buy": "discretionary_sell",
             "discretionary_sell": "discretionary_buy"}

    for t in rows:
        key = (t["insider_id"], t["ticker"], t["signal_class"])
        tk = (t["ticker"], t["signal_class"])

        p = prog.get(key)
        if p and (p["n_filings"] or 0) >= 2:
            t["prog_n_filings"] = p["n_filings"]
            t["prog_value"] = p["value"]
            t["prog_span_days"] = _days_between(p["first_filing"], p["last_filing"])
            t["prog_legs"] = [(r["filing_date"], r["value"])
                              for r in legs.get(key, [])]

        c = clust.get(tk)
        if c and (c["n"] or 0) >= 2:
            t["win_cluster_n"] = c["n"]
            t["win_cluster_value"] = c["value"]
            t["win_cluster_span_days"] = _days_between(c["first_filing"], c["last_filing"])
            # The biggest participant who is NOT the filer this post is about.
            peers = [m for m in members.get(tk, [])
                     if m["insider_id"] != t["insider_id"]]
            if peers:
                t["peer_name"] = peers[0]["insider_name"]
                t["peer_title"] = peers[0]["insider_title"]
                t["peer_value"] = peers[0]["value"]

        o = opp.get((t["ticker"], other[t["signal_class"]]))
        if o:
            t["opp_n"] = o["n"]
            t["opp_value"] = o["value"]

        # "First on record" is only safe to say about a ticker we demonstrably
        # cover. Alibaba's first filing of any kind is 2026-03-25, so "first
        # ever" would be a claim about the dataset, not the company. Requiring
        # prior activity on the OTHER side proves coverage exists.
        t["is_first_on_record"] = (
            (prior.get(tk, {}).get("n") or 0) == 0 and (t.get("opp_n") or 0) >= 3
        )

        if t["signal_class"] == "discretionary_sell":
            m = mech.get((t["insider_id"], t["ticker"]))
            if m and m.get("last_date"):
                t["mech_date"] = m["last_date"]
                t["mech_qty"] = m.get("exercised_qty") or 0

        lp = posted.get(t["ticker"])
        if lp:
            t["last_posted_date"] = lp["ref_date"]
            t["last_posted_value"] = lp["value"]
            t["last_posted_name"] = lp["insider_name"]


def is_distribution_program(t: dict) -> bool:
    """Is this seller on a schedule rather than making a decision?

    REPEAT FILING MEANS OPPOSITE THINGS ON THE TWO SIDES, and treating it
    symmetrically inverted the whole feed the first time this ran. Buying
    again three days after buying is conviction shown twice. SELLING again
    three days after selling is how every executive diversifies, and it is
    mechanical almost by definition.

    Unfiltered, the 2026-08-25 batch led with CRWD's George Kurtz on his
    TWELFTH disclosure in a month — $3.67M, $3.61M, $3.71M, $7.39M ... a
    metronome, and none of it carrying is_routine or cohen_routine, because
    those flags describe a filing and this is a property of the sequence.
    BFST's 6 dribbles of $103K-$637K and MEDP's four came with it — and MEDP
    leading the feed is the precise regression is_cluster_story was written to
    stop (see its docstring).

    Four or more selling disclosures inside the window is a program. Three or
    fewer can still be a person getting out, which is why AUGO's Bruno Sousa
    survives this: three filings, escalating, and 84% of the stake gone.
    """
    return (t.get("signal_class") == "discretionary_sell"
            and (t.get("prog_n_filings") or 0) >= 4)


def is_repeat_worth_posting(t: dict, day: str) -> bool:
    """Have we already said this? If so, has it materially moved on?

    BABA went out on 08-24 as "$10.3M, largest purchase they've ever made" and
    was queued again on 08-25 as "$10.4M, largest purchase they've ever made".
    Two posts, one story, and the second reads as a correction of the first.

    A repeat earns its slot two ways: the program is materially bigger than the
    figure we last published, or somebody new has joined it. A different person
    buying is news even when the cheque is smaller — which is exactly the AMR
    case, where Gorzynski's $2.1M follows Courtis's $3.2M.
    """
    last = t.get("last_posted_date")
    if not last or _days_between(last, day) > REPOST_QUIET_DAYS:
        return True
    if (t.get("insider_name") or "") != (t.get("last_posted_name") or ""):
        return True
    now = t.get("prog_value") or t.get("value") or 0
    before = t.get("last_posted_value") or 0
    return before <= 0 or now >= before * REPOST_GROWTH


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
    if (t.get("day_cluster_events") or t.get("day_cluster_n") or 1) - 1 < 2:
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

    # Accumulation. BUY SIDE ONLY — see is_distribution_program for why the
    # mirror image is noise. Counted in filings, so a purchase that filled in
    # five lots is still one decision.
    if is_buy and (t.get("prog_n_filings") or 0) >= 2:
        found.append("repeat buyer")

    # A cluster that forms across the week rather than inside one day. TTMI and
    # AMR each had a second insider buy the following session and neither
    # registered, because day_cluster_n resets at midnight.
    #
    # The thresholds are not symmetric either. Three people buying $2M between
    # them is a decision; three people selling $2M over a month is a vesting
    # calendar, so the sell side has to clear four people and $5M before it
    # counts as anything. Without that, AMRZ posted on seven insiders splitting
    # $1.02M — a payroll event.
    n_win, v_win = (t.get("win_cluster_n") or 0), (t.get("win_cluster_value") or 0)
    if (n_win >= 3 and v_win >= 2_000_000) if is_buy else (n_win >= 4 and v_win >= 5_000_000):
        found.append("multi-day cluster")

    if is_buy:
        # Buying where the record shows only exercises and sales. This is the
        # whole of the BABA story and none of it was expressible before.
        if t.get("is_first_on_record"):
            found.append("first buy on record")
        # The grade is the one component validated out of sample, so it stays
        # the strongest hook — but it cannot be the ONLY thing carrying a
        # trade this small. HYNE ($26K) and HKHC ($54K) both reached the feed
        # on a B and nothing else. $100K is not a claim about significance,
        # just a floor under what a stranger is asked to read about.
        if (t.get("career_grade") or "") in ("A+", "A", "B") and (t.get("value") or 0) >= 100_000:
            found.append("graded insider")
        if t.get("is_first_ever"):
            found.append("first ever purchase")
        # Same floor as the grade, and for a stronger reason: annotate_trade
        # measures largest-ever at +0.19% against a +0.05% routine baseline,
        # so it is the weakest flag we publish. It kept HKHC's $54K purchase
        # in the feed on its own.
        if t.get("is_largest_ever") and (t.get("value") or 0) >= 100_000:
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
    # "Largest sale they've ever made" is true of almost every option cash-out,
    # because exercising 450,000 options and selling into it is the largest
    # thing most executives ever do in their own stock. CPAY reached the top of
    # the 2026-08-25 feed that way. It can still be SAID (annotate discloses
    # the exercise beside it) but it can no longer be the only reason to post.
    if t.get("is_largest_ever") and not t.get("mech_date"):
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
    # Deduped EVENTS, not filer rows — joint filers are one seller.
    others = max((t.get("day_cluster_events") or t.get("day_cluster_n") or 1) - 1, 0)
    story_value = (t.get("day_cluster_value") or 0) if others >= 2 else 0
    # An insider on their third disclosure in a week is telling a $53M story,
    # not a $19.6M one, and the reader is shown the larger figure — so rank on
    # it. Same for a cluster that assembled over several days.
    story_value = max(story_value, t.get("prog_value") or 0)
    if (t.get("win_cluster_n") or 0) >= 2:
        story_value = max(story_value, t.get("win_cluster_value") or 0)
    story_value = max(story_value, t.get("value") or 1)
    size = min(math.log10(max(story_value, 1)) * 6, 45)
    own = min(math.log10(max(t.get("value") or 1, 1)) * 2, 14)
    # Count alone is not a story — five insiders splitting $750K is a payroll
    # event. The bonus only applies once the day clears seven figures.
    others = max(others, (t.get("win_cluster_n") or 1) - 1)
    cluster = min(others * 5, 20) if story_value >= 1_000_000 else 0
    # BEING NEAR A BIG CLUSTER IS NOT THE SAME AS BEING THE STORY.
    #
    # story_value above is the cluster's total, which is right for ordering the
    # story but hands the same score to every filer standing in it. GEF's
    # $110K and CXW's $255K were ranked on $8.6M and $13.2M of other people's
    # selling and outranked AUGO's Bruno Sousa, who sold $54.9M of his own and
    # is 76% out. A filer contributing under a tenth of the cluster is a
    # supporting detail in it.
    win_v = t.get("win_cluster_value") or 0
    if (t.get("win_cluster_n") or 0) >= 2 and (t.get("value") or 0) < 0.10 * win_v:
        cluster -= 10
    # A repeat BUYER outranks a one-off of the same size: conviction shown
    # twice is worth more than conviction shown once. Gated on the same
    # seven-figure floor as the cluster bonus — HKHC's $54K purchase climbed
    # into the feed on three insiders splitting $66K.
    if t["signal_class"] == "discretionary_buy" and story_value >= 1_000_000:
        cluster += min(((t.get("prog_n_filings") or 1) - 1) * 6, 18)

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
        # A stake cut in half is the same story as an exit, told earlier, and
        # scoring only the literal full exit buried AUGO's Bruno Sousa — $54.9M
        # across three filings with 76% of the position gone — beneath a GEF
        # filer who sold $110K and happened to stand near a big cluster.
        if pc is None:
            exiting = 0
        elif pc.is_full_exit:
            exiting = 12
        elif (pc.fraction or 0) >= 0.5:
            exiting = 8
        else:
            exiting = 0
        return size * 1.6 + own + cluster + exiting
    return float(GRADE_WEIGHT.get(t.get("career_grade") or "", 5)) + size + own + cluster


def _amount(val: float) -> str:
    # Switch at the point where the K form would ROUND to four digits, not at
    # the million. $999,600 is "$1.0M", never "$1000K".
    return f"${val / 1_000_000:.1f}M" if val >= 999_500 else f"${val / 1_000:.0f}K"


def headline_mode(t: dict) -> str:
    """Which story does this post lead with — "cluster", "program" or "single"?

    Split out of render() so record_posts can store the figure we actually
    PUBLISHED. It stored t["value"] regardless, so BABA went into social_posts
    at $10.4M under a headline that said $20.7M. That number is not decoration:
    is_repeat_worth_posting compares tomorrow's program against it, so an
    understated baseline quietly makes the repeat guard too permissive, and any
    later "30 days ago we flagged this" would cite a smaller call than the one
    we made.
    """
    val = t.get("value") or 0
    win_n, win_v = (t.get("win_cluster_n") or 0), (t.get("win_cluster_value") or 0)
    if win_n >= 3 and win_v >= 3 * max(t.get("prog_value") or 0, val):
        return "cluster"
    if (t.get("prog_n_filings") or 0) >= 2:
        return "program"
    return "single"


def headline_value(t: dict) -> float:
    """The figure the headline actually states."""
    mode = headline_mode(t)
    if mode == "cluster":
        return t.get("win_cluster_value") or 0
    if mode == "program":
        return t.get("prog_value") or t.get("value") or 0
    return t.get("value") or 0


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
    others = max((t.get("day_cluster_events") or t.get("day_cluster_n") or 1) - 1, 0)
    noun = "buying" if verb == "bought" else "selling"
    lines: list[str] = []
    past = "bought" if is_buy else "sold"
    mode = headline_mode(t)
    win_n, win_v = (t.get("win_cluster_n") or 0), (t.get("win_cluster_value") or 0)
    if mode == "cluster":
        # The company is the story and today's filer is a bit part in it. GEF
        # led with "has sold $211K over 16 days" while the bullet underneath
        # said a colleague sold $3.70M and seven of them had sold $8.65M. When
        # the cluster is more than triple the individual, say so first.
        t["_cluster_led"] = True
        lines = [f"${t['ticker']} — {win_n} insiders have {past} {_amount(win_v)} "
                 f"here in {span_phrase(t.get('win_cluster_span_days') or 30)}", ""]
        lines.append(f"· {who} {verb} {_amount(val)} of it.")
        lines += [f"· {a}" for a in annotate(t, max_lines=2)]
    elif mode == "program":
        # The headline is the whole run. "bought $10.4M" and "has bought $20.7M
        # over two days" are the same filing described at two different
        # altitudes, and only one of them is news on the second day.
        lines = [f"${t['ticker']} — {who} has {verb} "
                 f"{_amount(t.get('prog_value') or val)} over "
                 f"{span_phrase(t.get('prog_span_days') or 2)}"]
        bullets = [f"· {a}" for a in annotate(t, max_lines=3)]
        if bullets:
            lines += [""] + bullets
    elif is_cluster_story(t):
        # The published count is the number of distinct transactions, not the
        # number of Form 4s. Three related persons reporting one sale is one
        # sale, and saying "3 insiders" about it is simply false.
        n = t.get("day_cluster_events") or t["day_cluster_n"]
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
    # BUYS ONLY. compute_career_grades scores an insider on how their own
    # PURCHASES worked out, so the letter says nothing whatsoever about their
    # selling — but "Insider grade: A" printed under "sold $57.3M" reads as a
    # rated sell recommendation. (The docstring on score() claims a sell can
    # never carry a grade; it is stale, the column is stamped on every row of
    # the insider's, which is exactly how this reached a rendered post.)
    grade = t.get("career_grade") if is_buy else None
    if grade:
        lines += ["", f"Insider grade: {grade} (from their own prior trades)"]

    # NO LINK. Removed 2026-08-20: Stocktwits demotes posts containing links,
    # so the one link per post was costing far more reach than the clicks it
    # earned. The $CASHTAG is the distribution mechanism here, not the URL —
    # readers who want the filing history search the ticker. Do not reinstate
    # a link without measuring impressions on a linked vs unlinked cohort.
    lines += ["", "Not investment advice."]
    return "\n".join(lines)


def record_posts(conn, picked: list[dict], bodies: list[str]) -> int:
    """Write each generated post to social_posts.

    Without this there is no follow-up: "30 days ago we flagged this" needs to
    know that we flagged it, when, and at what price. `ref_price` is the whole
    point — it freezes the claim at the moment it was made, so a later scorecard
    cannot quietly re-derive a kinder number than the one we published.

    Idempotent per filing via the one-alert-per-trade unique index, so
    re-running the generator for a day does not double-post.
    """
    written = 0
    for t, body in zip(picked, bodies):
        try:
            cur = conn.execute(
                """INSERT INTO social_posts
                     (platform, post_kind, ticker, trade_id, filing_key,
                      ref_price, ref_date, direction, insider_name, value, body)
                   VALUES ('stocktwits', 'alert', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT DO NOTHING""",
                (
                    t.get("ticker"),
                    t.get("trade_id"),
                    t.get("filing_key"),
                    # The mark a follow-up measures against: the latest close we
                    # had when the post was written. NOT the insider's fill — a
                    # reader acts on the post, not on the filing.
                    t.get("current_price"),
                    t.get("filing_date"),
                    "buy" if t.get("signal_class") == "discretionary_buy" else "sell",
                    t.get("insider_name"),
                    headline_value(t),
                    body,
                ),
            )
            # rowcount, not a blind increment. ON CONFLICT DO NOTHING makes a
            # second run a no-op, and counting attempts would report ten fresh
            # posts every time the job ran twice in a day.
            written += getattr(cur, "rowcount", 1) or 0
        except Exception as e:                     # noqa: BLE001
            logger.warning("could not record post for %s: %s", t.get("ticker"), e)
    conn.commit()
    logger.info("recorded %d posts to social_posts", written)
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Filing date (default: today)")
    ap.add_argument("--count", type=int, default=None,
                    help="Exact number of posts (overrides --min/--max)")
    ap.add_argument("--min-count", type=int, default=3,
                    help="Always post at least this many, even on a thin day")
    ap.add_argument("--max-count", type=int, default=MAX_POSTS_PER_DAY)
    ap.add_argument("--write", action="store_true", help="Also write to data/content/")
    ap.add_argument("--no-record", action="store_true",
                    help="Skip writing to social_posts (dry run)")
    args = ap.parse_args()

    day = args.date or date.today().isoformat()
    conn = get_connection()
    rows = [dict(r) for r in conn.execute(SQL, (day,)).fetchall()]
    if not rows:
        logger.info("No qualifying filings for %s", day)
        return 0

    attach_context(conn, rows, day)
    before = len(rows)
    rows = [r for r in rows if not is_distribution_program(r)]
    if before != len(rows):
        logger.info("dropped %d seller(s) on a disclosure schedule",
                    before - len(rows))
    before = len(rows)
    rows = [r for r in rows if is_repeat_worth_posting(r, day)]
    if before != len(rows):
        logger.info("repeat guard dropped %d candidate(s) already posted",
                    before - len(rows))

    # A ticker inside the cooldown is out, whatever the story says. This runs
    # AFTER is_repeat_worth_posting on purpose: that one can wave a candidate
    # through on a new insider or a bigger programme, and this is the floor
    # underneath it that nothing gets to argue with.
    cooling = tickers_in_cooldown(conn, day)
    if cooling:
        before = len(rows)
        rows = [r for r in rows if r["ticker"] not in cooling]
        if before != len(rows):
            logger.info("cooldown (%dd) dropped %d candidate(s) on %d recent ticker(s)",
                        TICKER_COOLDOWN_DAYS, before - len(rows), len(cooling))
    if not rows:
        logger.info("every candidate for %s is inside the %d-day ticker cooldown",
                    day, TICKER_COOLDOWN_DAYS)
        return 0

    # How many posts today is a property of the day, not a constant. A day
    # with eleven genuinely notable filings should produce eleven posts; a
    # quiet one should not be padded to five with whatever ranked fifth.
    lo = args.count or args.min_count
    hi = args.count or args.max_count

    # THE CALENDAR-DAY BUDGET, counted against what is already recorded rather
    # than against this process. Running the generator twice on 2026-08-24 is
    # what got the account suspended: the second run was not a duplicate, it
    # was simply the next ten candidates, and nothing in the code objected.
    already = posts_already_today(conn, day)
    budget = MAX_POSTS_PER_DAY - already
    if budget <= 0:
        logger.info("%d post(s) already recorded for %s and the daily cap is %d "
                    "— nothing to do", already, day, MAX_POSTS_PER_DAY)
        return 0
    if already:
        logger.info("%d post(s) already recorded for %s; topping up to the cap of %d",
                    already, day, MAX_POSTS_PER_DAY)
    hi = min(hi, budget)
    lo = min(lo, hi)

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

    if not args.no_record:
        record_posts(conn, picked, out)

    if args.write:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        p = OUTPUT_DIR / f"{day}_stocktwits.txt"
        p.write_text(("\n\n" + "=" * 58 + "\n\n").join(out))
        logger.info("wrote %s", p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

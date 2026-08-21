-- What we posted, when, and what we claimed at the time.
--
-- WHY
--
-- Nothing recorded that a post happened: no table, the generator only read,
-- and the .txt output was not even committed. Every follow-up in the Stocktwits
-- strategy — "30 days ago we flagged this, here is what it did" — is impossible
-- until a post leaves a row behind. This is the whole dependency.
--
-- ref_price IS THE LOAD-BEARING COLUMN. It freezes what we claimed at the
-- moment we claimed it. Reconstructing it later from "the close that day" would
-- quietly rewrite our own history, and a public scorecard is only worth
-- anything if it cannot be edited after the fact.

CREATE TABLE IF NOT EXISTS social_posts (
    post_id        BIGSERIAL PRIMARY KEY,
    platform       TEXT NOT NULL DEFAULT 'stocktwits',
    -- alert     — the original call, posted the evening of the filing
    -- followup  — revisits an alert at +7d (big movers) or +30d (always)
    -- scorecard — the weekly roundup of everything that matured
    post_kind      TEXT NOT NULL CHECK (post_kind IN ('alert','followup','scorecard')),
    posted_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    ticker         TEXT,
    trade_id       BIGINT,
    filing_key     TEXT,
    parent_post_id BIGINT REFERENCES social_posts(post_id),

    -- The claim, frozen at post time.
    ref_price      NUMERIC,
    ref_date       DATE,
    direction      TEXT CHECK (direction IN ('buy','sell')),
    insider_name   TEXT,
    value          NUMERIC,

    body           TEXT NOT NULL,
    -- Set once the post actually reaches Stocktwits. NULL means generated but
    -- not published, which is the normal state between the job running and a
    -- human (or Zapier) posting it.
    external_id    TEXT,
    followed_up_at TIMESTAMP,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Recent-first listing.
CREATE INDEX IF NOT EXISTS idx_social_posts_posted
    ON social_posts (posted_at DESC);
-- "have we already posted this ticker lately?"
CREATE INDEX IF NOT EXISTS idx_social_posts_ticker
    ON social_posts (ticker, posted_at DESC);
-- The follow-up queue: alerts still awaiting their 30-day revisit. Partial,
-- because that is the only rowset the follow-up job ever scans.
CREATE INDEX IF NOT EXISTS idx_social_posts_followup_queue
    ON social_posts (posted_at)
    WHERE post_kind = 'alert' AND followed_up_at IS NULL;
-- One alert per filing. The generator is idempotent per day, and a re-run
-- must not double-post or create two parents for one follow-up.
CREATE UNIQUE INDEX IF NOT EXISTS idx_social_posts_one_alert_per_trade
    ON social_posts (platform, trade_id)
    WHERE post_kind = 'alert' AND trade_id IS NOT NULL;

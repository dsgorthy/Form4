-- Narrow the follow-up queue to posts a follow-up can actually be computed for.
--
-- ref_price is the mark a follow-up measures against. About 9% of tickers that
-- file have no price coverage at all — EOS is Eaton Vance Enhanced Equity
-- Income Fund II, LYNX likewise — so those posts are recorded (they were real
-- posts and belong in the history) but can never produce a "here is what it
-- did" line. Leaving them in the queue means the 30-day job re-reads and skips
-- them forever.
DROP INDEX IF EXISTS idx_social_posts_followup_queue;
CREATE INDEX idx_social_posts_followup_queue
    ON social_posts (posted_at)
    WHERE post_kind = 'alert'
      AND followed_up_at IS NULL
      AND ref_price IS NOT NULL;

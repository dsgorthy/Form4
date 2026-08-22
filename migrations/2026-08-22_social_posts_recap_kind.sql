-- Allow post_kind = 'recap'.
--
-- The original three kinds were alert / followup / scorecard. A weekend "week
-- in numbers" post is none of them: it makes no performance claim (so it is
-- not a scorecard) and revisits no single filing (so it is not a followup).
-- Giving it its own kind keeps the follow-up queue and the scorecard series
-- clean — both filter on post_kind, and folding recaps into either would
-- pollute a query whose whole job is to be countable.
ALTER TABLE social_posts DROP CONSTRAINT IF EXISTS social_posts_post_kind_check;
ALTER TABLE social_posts ADD CONSTRAINT social_posts_post_kind_check
    CHECK (post_kind IN ('alert', 'followup', 'scorecard', 'recap'));

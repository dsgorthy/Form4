-- When a notification was actually DELIVERED, as opposed to created.
--
-- `notifications.emailed` is a state flag; it carries no time. That makes a
-- delivery-layer rate limit impossible to enforce honestly -- you cannot ask
-- "how many emails has this user had today" of a table that only knows when
-- the underlying events happened. The first version of the cap counted
-- distinct notification created_at values, which counts EVENTS, not SENDS: a
-- single digest covering forty notifications would have registered as forty
-- emails and gated the user out permanently.
--
-- Additive, nullable, IF NOT EXISTS. No existing reader changes behaviour:
-- everything today filters on `emailed`, which is untouched. Rows sent before
-- this migration keep NULL, which reads correctly as "sent, time unknown".
ALTER TABLE notifications.notifications
    ADD COLUMN IF NOT EXISTS emailed_at TEXT;

-- The cap query is "sends by this user in the last 24h", so the index is on
-- (user_id, emailed_at) and only needs rows that were actually sent.
CREATE INDEX IF NOT EXISTS idx_notifications_emailed_at
    ON notifications.notifications (user_id, emailed_at)
 WHERE emailed_at IS NOT NULL;

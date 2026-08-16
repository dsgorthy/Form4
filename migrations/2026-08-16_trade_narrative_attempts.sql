-- Bound the narrative job's retries.
--
-- trade_narrative rows that failed were retried on every run, by design: the
-- comment in demo_narratives.py reasoned that the usual failure is an Ollama
-- timeout on dense input, which often succeeds once load drops. That holds for
-- transient failures. It does not hold when the backend is unavailable, and
-- there was nothing to tell the two apart.
--
-- The job fires every 5 minutes over a 24h window, so a trade that cannot be
-- generated is retried ~288 times, each attempt burning up to the 120s Ollama
-- timeout, before it silently ages out of the window still unwritten. 259 of
-- the 272 currently-failed rows are timeouts accumulated exactly that way.
--
-- An explicit attempt counter lets the job stop at a bound and lets a human
-- see which rows gave up, instead of inferring it from a 13 MB log.

ALTER TABLE trade_narrative
    ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0;

-- Existing failures have been retried many times over; seed them at 1 rather
-- than 0 so they are not indistinguishable from a fresh row, but leave them
-- retryable, since the current failures are an outage rather than bad input.
UPDATE trade_narrative
   SET attempts = 1
 WHERE summary IS NULL
   AND attempts = 0;

-- The job's hot path is "unwritten or retryable rows in the recent window".
CREATE INDEX IF NOT EXISTS idx_trade_narrative_retryable
    ON trade_narrative (attempts)
 WHERE summary IS NULL;

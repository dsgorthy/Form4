-- Add 'abandoned' to pipeline_runs.status.
--
-- WHY
--
-- framework/observability/pipeline_run() catches BaseException, so any run
-- that gets as far as Python exiting writes a terminal status. What it cannot
-- survive is SIGKILL, an OOM kill, or the host going down — and those left 7
-- rows stuck in 'running' as of 2026-08-23, the oldest from 2026-05-24. They
-- render on /admin/pipelines as jobs that have been live for three months.
--
-- 'failed' is wrong: the job may well have succeeded and simply never got to
-- record it. 'timeout' is wrong: nothing timed out. The state being described
-- is "no terminal status was ever written", which is its own thing.
--
-- pipeline_run() now reaps its own service's stale rows on startup, scaled to
-- that service's observed max duration (refresh_ticker_metadata legitimately
-- runs 72 hours, so a flat threshold cannot work) with a 24-hour floor.
--
-- Apply:  psql -d form4 -f migrations/2026-08-23_pipeline_runs_abandoned.sql

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_status_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_status_check
    CHECK (status = ANY (ARRAY[
        'running'::text, 'ok'::text, 'failed'::text,
        'timeout'::text, 'partial'::text, 'abandoned'::text
    ]));

-- Reap the existing orphans with the same rule the runtime now applies.
UPDATE pipeline_runs SET status = 'abandoned', ended_at = NOW(),
       error_message = 'no terminal status recorded; reaped by '
                       '2026-08-23_pipeline_runs_abandoned.sql'
 WHERE status = 'running'
   AND started_at < NOW() - GREATEST(
         INTERVAL '24 hours',
         COALESCE((SELECT 3 * MAX(duration_ms) * INTERVAL '1 millisecond'
                     FROM pipeline_runs p2
                    WHERE p2.service = pipeline_runs.service
                      AND p2.status = 'ok'), INTERVAL '24 hours'));

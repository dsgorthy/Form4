-- The most recent observation of every (signal_id, ticker) pair.
-- Convenience surface for Claude / strategies that just want "what's the
-- current value of insider.career_grade.v3 for NVDA?" without writing
-- their own window query each time.

with ranked as (
    select
        signal_id,
        ticker,
        as_of_date,
        ingested_at,
        value,
        confidence,
        source_run_id,
        metadata,
        row_number() over (
            partition by signal_id, ticker
            order by as_of_date desc, ingested_at desc
        ) as rn
    from {{ source('dataplane', 'signal_observations') }}
)

select
    signal_id,
    ticker,
    as_of_date,
    ingested_at,
    value,
    confidence,
    source_run_id,
    metadata
from ranked
where rn = 1

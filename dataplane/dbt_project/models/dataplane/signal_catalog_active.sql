-- Operator-friendly view of the active signal catalog with row counts.
-- Powers the future /admin/signals page.

with obs_counts as (
    select
        signal_id,
        count(*)               as n_rows,
        max(as_of_date)        as latest_as_of,
        max(ingested_at)       as latest_ingested_at,
        count(distinct ticker) as n_tickers
    from {{ source('dataplane', 'signal_observations') }}
    group by signal_id
)

select
    d.signal_id,
    d.version,
    d.signal_class::text                as signal_class,
    d.owner,
    d.description,
    d.sla_hours,
    d.business_hours_only,
    d.status,
    d.registered_at,
    -- match signal_definitions.signal_id to signal_observations.signal_id
    -- via prefix (definitions store "insider.career_grade" while observations
    -- store "insider.career_grade.v3.0.1" — the version-suffixed form)
    coalesce(o.n_rows,             0) as n_rows,
    coalesce(o.n_tickers,          0) as n_tickers,
    o.latest_as_of                    as latest_as_of,
    o.latest_ingested_at              as latest_ingested_at
from {{ source('dataplane', 'signal_definitions') }} d
left join obs_counts o
    on o.signal_id like d.signal_id || '.' || d.version || '%'
   or  o.signal_id =    d.signal_id || '.' || d.version
where d.status = 'active'
order by d.signal_class, d.signal_id, d.version

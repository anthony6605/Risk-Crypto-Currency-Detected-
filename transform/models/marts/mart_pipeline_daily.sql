select
    pipeline_name,
    (started_at at time zone 'UTC')::date                       as run_date,
    count(*)                                                    as runs,
    count(*) filter (where status = 'FAILED')                   as failed_runs,
    round(avg((status <> 'FAILED')::int) * 100, 1)              as success_rate_pct,
    sum(rows_loaded)                                            as rows_loaded,
    sum(rows_rejected)                                          as rows_rejected,
    round(avg(duration_seconds)::numeric, 2)                    as avg_duration_seconds
from {{ ref('stg_pipeline_audit') }}
where status <> 'RUNNING'
group by 1, 2

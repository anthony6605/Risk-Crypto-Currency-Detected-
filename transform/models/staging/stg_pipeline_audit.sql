select
    audit_id,
    pipeline_name,
    coin_id,
    status,
    started_at,
    ended_at,
    extract(epoch from ended_at - started_at) as duration_seconds,
    coalesce(rows_extracted, 0) as rows_extracted,
    coalesce(rows_loaded, 0)    as rows_loaded,
    coalesce(rows_rejected, 0)  as rows_rejected,
    error_message
from {{ source('riskcoin', 'pipeline_audit') }}

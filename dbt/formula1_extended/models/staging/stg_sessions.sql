with ranked_sessions as (
    select
        session_key,
        meeting_key,
        session_name,
        session_type,
        date_start,
        date_end,
        year,
        ingested_at_utc,
        row_number() over (
            partition by session_key
            order by ingested_at_utc desc
        ) as session_rank
    from {{ source('raw', 'sessions') }}
)

select
    session_key,
    meeting_key,
    session_name,
    session_type,
    date_start,
    date_end,
    year,
    ingested_at_utc
from ranked_sessions
where session_rank = 1

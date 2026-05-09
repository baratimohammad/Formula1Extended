select
    session_key,
    driver_number,
    lap_number,
    lap_duration,
    date_start,
    ingested_at_utc
from {{ source('raw', 'laps') }}

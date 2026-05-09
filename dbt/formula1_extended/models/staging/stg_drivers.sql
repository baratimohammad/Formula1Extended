select
    session_key,
    driver_number,
    full_name,
    team_name,
    ingested_at_utc
from {{ source('raw', 'drivers') }}

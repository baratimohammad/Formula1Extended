with sessions as (
    select * from {{ ref('stg_sessions') }}
),
drivers as (
    select * from {{ ref('stg_drivers') }}
),
laps as (
    select * from {{ ref('stg_laps') }}
    where lap_duration is not null
)

select
    laps.session_key,
    sessions.session_name,
    sessions.session_type,
    sessions.year,
    laps.driver_number,
    drivers.full_name,
    drivers.team_name,
    count(*) as lap_count,
    min(laps.lap_duration) as fastest_lap_duration,
    avg(laps.lap_duration) as average_lap_duration,
    max(laps.ingested_at_utc) as latest_ingested_at_utc
from laps
left join drivers
    on laps.session_key = drivers.session_key
   and laps.driver_number = drivers.driver_number
left join sessions
    on laps.session_key = sessions.session_key
group by
    laps.session_key,
    sessions.session_name,
    sessions.session_type,
    sessions.year,
    laps.driver_number,
    drivers.full_name,
    drivers.team_name

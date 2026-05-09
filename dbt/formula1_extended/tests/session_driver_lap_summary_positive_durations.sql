select *
from {{ ref('session_driver_lap_summary') }}
where average_lap_duration <= 0
   or fastest_lap_duration <= 0

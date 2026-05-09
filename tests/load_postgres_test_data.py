from src.config_loader import get_postgres_raw_schema
from src.storage.postgres_writer import write_records_to_postgres


def main():
    raw_schema = get_postgres_raw_schema()

    session = {
        "session_key": 999,
        "meeting_key": 111,
        "session_name": "Test Race",
        "session_type": "Race",
        "date_start": "2026-01-01T00:00:00+00:00",
        "date_end": "2026-01-01T02:00:00+00:00",
        "year": 2026,
        "ingested_at_utc": "2026-01-01T00:00:00+00:00",
    }

    drivers = [
        {
            "session_key": 999,
            "driver_number": 1,
            "full_name": "Max Verstappen",
            "team_name": "Red Bull Racing",
            "ingested_at_utc": "2026-01-01T00:00:00+00:00",
        }
    ]

    laps = [
        {
            "session_key": 999,
            "driver_number": 1,
            "lap_number": 1,
            "lap_duration": 92.5,
            "date_start": "2026-01-01T00:10:00+00:00",
            "ingested_at_utc": "2026-01-01T00:00:00+00:00",
        }
    ]

    write_records_to_postgres(
        records=[session],
        table_name="sessions",
        schema_name=raw_schema,
        session_key=999,
    )
    write_records_to_postgres(
        records=drivers,
        table_name="drivers",
        schema_name=raw_schema,
        session_key=999,
    )
    write_records_to_postgres(
        records=laps,
        table_name="laps",
        schema_name=raw_schema,
        session_key=999,
    )


if __name__ == "__main__":
    main()

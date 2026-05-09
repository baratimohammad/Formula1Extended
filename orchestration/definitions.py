import subprocess
from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Backoff,
    Definitions,
    Jitter,
    MetadataValue,
    Output,
    Failure,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from src.config_loader import (
    PROJECT_ROOT,
    build_raw_output_path,
    configure_logging,
    get_pipeline_retry_config,
    get_pipeline_schedule_config,
    get_postgres_analytics_schema,
    get_postgres_raw_schema,
    get_storage_overwrite,
)
from src.ingestion.api_client import resolve_latest_session
from src.ingestion.drivers import ingest_drivers_for_session
from src.ingestion.laps import ingest_laps_for_session
from src.storage.parquet_writer import write_records_to_parquet
from src.storage.postgres_writer import write_records_to_postgres


configure_logging()

RETRY_BACKOFFS = {
    "EXPONENTIAL": Backoff.EXPONENTIAL,
    "LINEAR": Backoff.LINEAR,
}
RETRY_JITTERS = {
    "PLUS_MINUS": Jitter.PLUS_MINUS,
}

retry_config = get_pipeline_retry_config()
schedule_config = get_pipeline_schedule_config()
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "formula1_extended"

api_retry_policy = RetryPolicy(
    max_retries=int(retry_config["max_retries"]),
    delay=int(retry_config["delay_seconds"]),
    backoff=RETRY_BACKOFFS[str(retry_config["backoff"]).upper()],
    jitter=RETRY_JITTERS[str(retry_config["jitter"]).upper()],
)


@asset(
    retry_policy=api_retry_policy,
    description="Resolve the latest OpenF1 session and return its concrete session_key.",
)
def latest_session(context: AssetExecutionContext) -> Output[dict]:
    session = resolve_latest_session()

    session_key = session["session_key"]
    ingested_at_utc = datetime.now(timezone.utc).isoformat()
    session_record = {
        **session,
        "ingested_at_utc": ingested_at_utc,
    }

    write_records_to_postgres(
        records=[session_record],
        table_name="sessions",
        schema_name=get_postgres_raw_schema(),
        session_key=session_key,
    )

    context.log.info(f"Resolved latest session_key={session_key}")

    return Output(
        value=session_record,
        metadata={
            "session_key": session_key,
            "meeting_key": session.get("meeting_key"),
            "session_name": session.get("session_name"),
            "session_type": session.get("session_type"),
            "year": session.get("year"),
            "target_table": f'{get_postgres_raw_schema()}.sessions',
            "materialized_at_utc": MetadataValue.text(ingested_at_utc),
        },
    )


@asset(
    retry_policy=api_retry_policy,
    description="Ingest OpenF1 drivers for the latest resolved session.",
)
def drivers(
    context: AssetExecutionContext,
    latest_session: dict,
) -> Output[dict]:
    session_key = latest_session["session_key"]

    records = ingest_drivers_for_session(session_key)

    output_path = str(build_raw_output_path("drivers", session_key, "drivers.parquet"))

    write_records_to_parquet(
        records=records,
        output_path=output_path,
        overwrite=get_storage_overwrite(),
    )
    write_records_to_postgres(
        records=records,
        table_name="drivers",
        schema_name=get_postgres_raw_schema(),
        session_key=session_key,
    )

    materialized_at_utc = datetime.now(timezone.utc).isoformat()

    context.log.info(
        f"Wrote {len(records)} driver records for session_key={session_key} to {output_path}"
    )

    return Output(
        value={
            "session_key": session_key,
            "row_count": len(records),
            "output_path": output_path,
        },
        metadata={
            "session_key": session_key,
            "row_count": len(records),
            "output_path": MetadataValue.path(output_path),
            "target_table": f'{get_postgres_raw_schema()}.drivers',
            "materialized_at_utc": MetadataValue.text(materialized_at_utc),
        },
    )


@asset(
    retry_policy=api_retry_policy,
    description="Ingest OpenF1 laps for the latest resolved session. Depends on drivers to force sequential execution.",
)
def laps(
    context: AssetExecutionContext,
    latest_session: dict,
    drivers: dict,
) -> Output[dict]:
    session_key = latest_session["session_key"]

    records = ingest_laps_for_session(session_key)

    output_path = str(build_raw_output_path("laps", session_key, "laps.parquet"))

    write_records_to_parquet(
        records=records,
        output_path=output_path,
        overwrite=get_storage_overwrite(),
    )
    write_records_to_postgres(
        records=records,
        table_name="laps",
        schema_name=get_postgres_raw_schema(),
        session_key=session_key,
    )

    materialized_at_utc = datetime.now(timezone.utc).isoformat()

    context.log.info(
        f"Wrote {len(records)} lap records for session_key={session_key} to {output_path}"
    )

    return Output(
        value={
            "session_key": session_key,
            "row_count": len(records),
            "output_path": output_path,
            "upstream_drivers_row_count": drivers["row_count"],
        },
        metadata={
            "session_key": session_key,
            "row_count": len(records),
            "output_path": MetadataValue.path(output_path),
            "target_table": f'{get_postgres_raw_schema()}.laps',
            "materialized_at_utc": MetadataValue.text(materialized_at_utc),
        },
    )


@asset(
    description="Run dbt models and tests against the Postgres-backed raw layer.",
)
def dbt_build(
    context: AssetExecutionContext,
    latest_session: dict,
    laps: dict,
) -> Output[dict]:
    command = [
        "dbt",
        "build",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ]

    completed_process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed_process.stdout:
        context.log.info(completed_process.stdout)

    if completed_process.stderr:
        context.log.warning(completed_process.stderr)

    if completed_process.returncode != 0:
        raise Failure(
            description=(
                "dbt build failed.\n"
                f"stdout:\n{completed_process.stdout}\n"
                f"stderr:\n{completed_process.stderr}"
            )
        )

    return Output(
        value={
            "session_key": latest_session["session_key"],
            "row_count": laps["row_count"],
            "analytics_schema": get_postgres_analytics_schema(),
        },
        metadata={
            "session_key": latest_session["session_key"],
            "dbt_project_dir": MetadataValue.path(str(DBT_PROJECT_DIR)),
            "analytics_schema": get_postgres_analytics_schema(),
        },
    )


openf1_ingestion_job = define_asset_job(
    name="openf1_ingestion_job",
    selection=AssetSelection.all(),
)


daily_openf1_schedule = ScheduleDefinition(
    name="daily_openf1_schedule",
    job=openf1_ingestion_job,
    cron_schedule=str(schedule_config["cron"]),
    execution_timezone=str(schedule_config["timezone"]),
)


defs = Definitions(
    assets=[
        latest_session,
        drivers,
        laps,
        dbt_build,
    ],
    jobs=[
        openf1_ingestion_job,
    ],
    schedules=[
        daily_openf1_schedule,
    ],
)

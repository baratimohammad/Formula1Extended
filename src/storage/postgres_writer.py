import json
import os
from collections.abc import Iterable
from numbers import Integral, Real

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


def build_postgres_url() -> str:
    user = os.getenv("POSTGRES_USER", "formula1")
    password = os.getenv("POSTGRES_PASSWORD", "formula1")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "formula1")

    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def get_postgres_engine() -> Engine:
    return create_engine(build_postgres_url(), future=True)


def _is_missing(value) -> bool:
    if value is None:
        return True

    if isinstance(value, (list, dict, tuple, set)):
        return False

    return bool(pd.isna(value))


def _normalize_value(value):
    if isinstance(value, (list, dict, tuple, set)):
        return json.dumps(value, sort_keys=True)

    return value


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(lambda column: column.map(_normalize_value))


def _infer_postgres_type(series: pd.Series) -> str:
    non_null_values = [
        value
        for value in series.tolist()
        if not _is_missing(value)
    ]

    if not non_null_values:
        return "TEXT"

    if all(isinstance(value, bool) for value in non_null_values):
        return "BOOLEAN"

    if all(
        isinstance(value, Integral) and not isinstance(value, bool)
        for value in non_null_values
    ):
        return "BIGINT"

    if all(
        isinstance(value, Real) and not isinstance(value, bool)
        for value in non_null_values
    ):
        return "DOUBLE PRECISION"

    return "TEXT"


def _add_missing_columns(
    connection,
    df: pd.DataFrame,
    table_name: str,
    schema_name: str,
) -> None:
    existing_columns = {
        column["name"]
        for column in inspect(connection).get_columns(table_name, schema=schema_name)
    }

    missing_columns = [
        column_name
        for column_name in df.columns
        if column_name not in existing_columns
    ]

    for column_name in missing_columns:
        postgres_type = _infer_postgres_type(df[column_name])
        connection.execute(
            text(
                f'ALTER TABLE "{schema_name}"."{table_name}" '
                f'ADD COLUMN "{column_name}" {postgres_type}'
            )
        )


def write_records_to_postgres(
    records: Iterable[dict],
    table_name: str,
    schema_name: str,
    session_key: int | None = None,
) -> None:
    records = list(records)

    if not records:
        raise ValueError("No records to write")

    df = _normalize_dataframe(pd.DataFrame(records))
    engine = get_postgres_engine()

    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))

        table_exists = inspect(connection).has_table(table_name, schema=schema_name)

        if table_exists:
            _add_missing_columns(
                connection=connection,
                df=df,
                table_name=table_name,
                schema_name=schema_name,
            )

            if session_key is not None and "session_key" in df.columns:
                connection.execute(
                    text(
                        f'DELETE FROM "{schema_name}"."{table_name}" '
                        "WHERE session_key = :session_key"
                    ),
                    {"session_key": session_key},
                )

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists="append",
        index=False,
        method="multi",
    )

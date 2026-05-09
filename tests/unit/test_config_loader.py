from src.config_loader import (
    build_raw_output_path,
    configure_logging,
    get_api_base_url,
    get_api_timeout_seconds,
    get_pipeline_retry_config,
    get_postgres_analytics_schema,
    get_postgres_database,
    get_postgres_host,
    get_postgres_port,
    get_postgres_raw_schema,
    get_postgres_user,
    get_storage_overwrite,
    get_storage_raw_root,
    load_app_config,
)


def test_load_app_config_contains_expected_sections():
    config = load_app_config()

    assert "api" in config
    assert "storage" in config
    assert "database" in config
    assert "pipeline" in config


def test_config_loader_returns_expected_defaults():
    assert get_api_base_url() == "https://api.openf1.org/v1"
    assert get_api_timeout_seconds() == 30
    assert get_storage_raw_root().as_posix().endswith("/data/raw")
    assert get_storage_overwrite() is True
    assert get_postgres_host() == "localhost"
    assert get_postgres_port() == 5432
    assert get_postgres_database() == "formula1"
    assert get_postgres_user() == "formula1"
    assert get_postgres_raw_schema() == "raw"
    assert get_postgres_analytics_schema() == "analytics"
    assert get_pipeline_retry_config()["max_retries"] == 3


def test_build_raw_output_path_uses_configured_root():
    output_path = build_raw_output_path("drivers", 999, "drivers.parquet")

    assert output_path.as_posix().endswith(
        "/data/raw/drivers/session_key=999/drivers.parquet"
    )


def test_configure_logging_uses_yaml_without_error():
    configure_logging()

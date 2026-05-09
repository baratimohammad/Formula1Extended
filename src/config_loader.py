import logging.config
import os
from functools import lru_cache
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
APP_CONFIG_PATH = CONFIG_DIR / "config.yaml"
LOGGING_CONFIG_PATH = CONFIG_DIR / "logging.yaml"


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Configuration file must contain a mapping: {path}")

    return data


@lru_cache(maxsize=1)
def load_app_config() -> dict:
    return load_yaml(APP_CONFIG_PATH)


def configure_logging() -> None:
    logging.config.dictConfig(load_yaml(LOGGING_CONFIG_PATH))


def get_api_base_url() -> str:
    return str(load_app_config()["api"]["base_url"]).rstrip("/")


def get_api_timeout_seconds() -> int:
    return int(load_app_config()["api"]["timeout_seconds"])


def get_storage_raw_root() -> Path:
    raw_root = Path(load_app_config()["storage"]["raw_root"])
    if raw_root.is_absolute():
        return raw_root
    return PROJECT_ROOT / raw_root


def get_storage_overwrite() -> bool:
    return bool(load_app_config()["storage"]["overwrite"])


def _get_database_value(key: str, env_var: str) -> str:
    env_value = os.getenv(env_var)
    if env_value:
        return env_value

    return str(load_app_config()["database"][key])


def get_postgres_host() -> str:
    return _get_database_value("host", "POSTGRES_HOST")


def get_postgres_port() -> int:
    return int(_get_database_value("port", "POSTGRES_PORT"))


def get_postgres_database() -> str:
    return _get_database_value("name", "POSTGRES_DB")


def get_postgres_user() -> str:
    return _get_database_value("user", "POSTGRES_USER")


def get_postgres_password() -> str:
    return os.getenv("POSTGRES_PASSWORD", "formula1")


def get_postgres_raw_schema() -> str:
    return os.getenv(
        "POSTGRES_RAW_SCHEMA",
        str(load_app_config()["database"]["raw_schema"]),
    )


def get_postgres_analytics_schema() -> str:
    return os.getenv(
        "POSTGRES_ANALYTICS_SCHEMA",
        str(load_app_config()["database"]["analytics_schema"]),
    )


def get_pipeline_retry_config() -> dict:
    return load_app_config()["pipeline"]["retries"]


def get_pipeline_schedule_config() -> dict:
    return load_app_config()["pipeline"]["schedule"]


def build_raw_output_path(dataset: str, session_key: int, filename: str) -> Path:
    return get_storage_raw_root() / dataset / f"session_key={session_key}" / filename

import json
import time
from pathlib import Path

from filelock import FileLock

from src.config_loader import get_observability_state_path


COUNTER_METRICS = {
    "formula1_pipeline_run_success_total": {
        "description": "Successful pipeline runs.",
        "label_names": [],
    },
    "formula1_pipeline_run_failure_total": {
        "description": "Failed pipeline runs.",
        "label_names": [],
    },
    "formula1_pipeline_run_retry_total": {
        "description": "Retry attempts requested by pipeline steps.",
        "label_names": [],
    },
    "formula1_step_failure_total": {
        "description": "Step-level failures by step key.",
        "label_names": ["step"],
    },
    "formula1_dbt_failure_total": {
        "description": "dbt build failures.",
        "label_names": [],
    },
}

GAUGE_METRICS = {
    "formula1_pipeline_run_last_duration_seconds": {
        "description": "Duration in seconds of the latest completed pipeline run.",
        "label_names": [],
    },
    "formula1_pipeline_run_last_success_unixtime": {
        "description": "Unix timestamp of the latest successful pipeline run.",
        "label_names": [],
    },
    "formula1_pipeline_run_last_failure_unixtime": {
        "description": "Unix timestamp of the latest failed pipeline run.",
        "label_names": [],
    },
    "formula1_asset_last_materialization_unixtime": {
        "description": "Unix timestamp of the latest successful asset materialization.",
        "label_names": ["asset"],
    },
    "formula1_asset_rows_ingested": {
        "description": "Rows ingested in the latest successful materialization for an asset.",
        "label_names": ["asset"],
    },
    "formula1_step_last_failure_unixtime": {
        "description": "Unix timestamp of the latest failure for a step.",
        "label_names": ["step"],
    },
    "formula1_dbt_last_success_unixtime": {
        "description": "Unix timestamp of the latest successful dbt build.",
        "label_names": [],
    },
}


def _default_state() -> dict:
    return {
        "counters": {},
        "gauges": {},
        "run_starts": {},
        "updated_at": None,
    }


def _metric_storage_key(labels: dict[str, str] | None = None) -> str:
    if not labels:
        return "__default__"

    return "|".join(
        f"{label_name}={labels[label_name]}"
        for label_name in sorted(labels)
    )


def _read_state(path: Path) -> dict:
    if not path.exists():
        return _default_state()

    return json.loads(path.read_text(encoding="utf-8"))


def _write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def update_counter(metric_name: str, amount: float = 1.0, labels: dict[str, str] | None = None) -> None:
    if metric_name not in COUNTER_METRICS:
        raise KeyError(f"Unknown counter metric: {metric_name}")

    path = get_observability_state_path()
    lock = FileLock(f"{path}.lock")

    with lock:
        state = _read_state(path)
        metric_values = state["counters"].setdefault(metric_name, {})
        storage_key = _metric_storage_key(labels)
        existing_value = float(metric_values.get(storage_key, 0.0))
        metric_values[storage_key] = existing_value + amount
        state["updated_at"] = time.time()
        _write_state(path, state)


def set_gauge(metric_name: str, value: float, labels: dict[str, str] | None = None) -> None:
    if metric_name not in GAUGE_METRICS:
        raise KeyError(f"Unknown gauge metric: {metric_name}")

    path = get_observability_state_path()
    lock = FileLock(f"{path}.lock")

    with lock:
        state = _read_state(path)
        metric_values = state["gauges"].setdefault(metric_name, {})
        metric_values[_metric_storage_key(labels)] = value
        state["updated_at"] = time.time()
        _write_state(path, state)


def load_metrics_state() -> dict:
    path = get_observability_state_path()
    lock = FileLock(f"{path}.lock")

    with lock:
        return _read_state(path)


def set_run_start_if_absent(run_id: str, started_at: float) -> None:
    path = get_observability_state_path()
    lock = FileLock(f"{path}.lock")

    with lock:
        state = _read_state(path)
        run_starts = state.setdefault("run_starts", {})
        run_starts.setdefault(run_id, started_at)
        state["updated_at"] = time.time()
        _write_state(path, state)


def pop_run_start(run_id: str) -> float | None:
    path = get_observability_state_path()
    lock = FileLock(f"{path}.lock")

    with lock:
        state = _read_state(path)
        run_starts = state.setdefault("run_starts", {})
        started_at = run_starts.pop(run_id, None)
        state["updated_at"] = time.time()
        _write_state(path, state)
        return started_at

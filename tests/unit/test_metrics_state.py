import json

from src.observability.metrics_state import (
    load_metrics_state,
    pop_run_start,
    set_gauge,
    set_run_start_if_absent,
    update_counter,
)


def test_metrics_state_updates_counter_and_gauge(monkeypatch, tmp_path):
    state_path = tmp_path / "metrics_state.json"
    monkeypatch.setenv("OBSERVABILITY_STATE_PATH", str(state_path))

    update_counter("formula1_pipeline_run_success_total")
    update_counter("formula1_step_failure_total", labels={"step": "drivers"})
    set_gauge("formula1_pipeline_run_last_success_unixtime", 123.0)
    set_gauge("formula1_asset_rows_ingested", 22.0, labels={"asset": "drivers"})

    state = load_metrics_state()

    assert state["counters"]["formula1_pipeline_run_success_total"]["__default__"] == 1.0
    assert state["counters"]["formula1_step_failure_total"]["step=drivers"] == 1.0
    assert state["gauges"]["formula1_pipeline_run_last_success_unixtime"]["__default__"] == 123.0
    assert state["gauges"]["formula1_asset_rows_ingested"]["asset=drivers"] == 22.0

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["updated_at"] is not None


def test_metrics_state_tracks_run_start_fallback(monkeypatch, tmp_path):
    state_path = tmp_path / "metrics_state.json"
    monkeypatch.setenv("OBSERVABILITY_STATE_PATH", str(state_path))

    set_run_start_if_absent("run-1", 100.0)
    set_run_start_if_absent("run-1", 200.0)

    state = load_metrics_state()
    assert state["run_starts"]["run-1"] == 100.0

    assert pop_run_start("run-1") == 100.0
    assert pop_run_start("run-1") is None
    assert load_metrics_state()["run_starts"] == {}

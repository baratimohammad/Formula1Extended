import time
from threading import Event

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from src.config_loader import get_prometheus_metrics_port
from src.observability.metrics_state import (
    COUNTER_METRICS,
    GAUGE_METRICS,
    load_metrics_state,
)


class PipelineHealthCollector:
    def collect(self):
        state = load_metrics_state()
        counters = state.get("counters", {})
        gauges = state.get("gauges", {})

        for metric_name, metric_config in COUNTER_METRICS.items():
            family = CounterMetricFamily(
                metric_name,
                metric_config["description"],
                labels=metric_config["label_names"],
            )

            for storage_key, value in counters.get(metric_name, {}).items():
                if storage_key == "__default__":
                    family.add_metric([], value)
                    continue

                labels = [
                    segment.split("=", maxsplit=1)[1]
                    for segment in storage_key.split("|")
                ]
                family.add_metric(labels, value)

            yield family

        for metric_name, metric_config in GAUGE_METRICS.items():
            family = GaugeMetricFamily(
                metric_name,
                metric_config["description"],
                labels=metric_config["label_names"],
            )

            for storage_key, value in gauges.get(metric_name, {}).items():
                if storage_key == "__default__":
                    family.add_metric([], value)
                    continue

                labels = [
                    segment.split("=", maxsplit=1)[1]
                    for segment in storage_key.split("|")
                ]
                family.add_metric(labels, value)

            yield family

        freshness_family = GaugeMetricFamily(
            "formula1_asset_freshness_lag_seconds",
            "Freshness lag in seconds since the latest successful materialization.",
            labels=["asset"],
        )
        now = time.time()

        for storage_key, value in gauges.get(
            "formula1_asset_last_materialization_unixtime",
            {},
        ).items():
            if storage_key == "__default__":
                continue

            labels = {
                segment.split("=", maxsplit=1)[0]: segment.split("=", maxsplit=1)[1]
                for segment in storage_key.split("|")
            }
            freshness_family.add_metric(
                [labels["asset"]],
                max(0.0, now - float(value)),
            )

        yield freshness_family


def main() -> None:
    REGISTRY.register(PipelineHealthCollector())
    start_http_server(get_prometheus_metrics_port())
    Event().wait()


if __name__ == "__main__":
    main()

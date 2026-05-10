# OpenF1 Dagster + dbt Stack

This project ingests the latest OpenF1 session, drivers, and laps data with Dagster, stores raw snapshots under `data/raw/`, persists raw operational tables in Postgres, and builds dbt models on top of that raw layer.

## Local Runtime

Prerequisites:
- Docker with Docker Compose
- Internet access to `https://api.openf1.org`

Configuration:
- Create `.env` from `.env.example` before running Docker Compose:

```bash
cp .env.example .env
```

- `.env` contains the local Postgres and Dagster connection settings used by Docker Compose.
- Postgres is published on host port `5433` by default via `POSTGRES_HOST_PORT`, while the container still uses internal port `5432`.
- `config/config.yaml` controls API, storage, retry, schedule, and default database values.
- `config/logging.yaml` controls Python logging output.

Start the full stack:

```bash
docker compose up --build -d
```

Check the running services:

```bash
docker compose ps
```

Dagster UI:

```text
http://localhost:3002
```

Prometheus:

```text
http://localhost:9090
```

Pipeline metrics endpoint:

```text
http://localhost:9108/metrics
```

Run the end-to-end Dagster job inside Docker:

```bash
docker compose run --rm dagster-code python -m dagster job execute -f orchestration/definitions.py -j openf1_ingestion_job
```

Connect to Postgres from the host:

```bash
psql -h localhost -p 5433 -U formula1 -d formula1
```

Run dbt manually inside Docker:

```bash
docker compose run --rm dagster-code dbt build --project-dir dbt/formula1_extended --profiles-dir dbt
```

Stop the stack:

```bash
docker compose down
```

## Validation

Python checks:

```bash
python3.10 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/ruff check src tests orchestration
venv/bin/pytest tests/unit
venv/bin/python tests/create_test_data.py
venv/bin/python tests/run_sql_tests.py
```

Dockerized Postgres + dbt integration:

```bash
docker compose up -d postgres
docker compose run --rm dagster-code python tests/load_postgres_test_data.py
docker compose run --rm dagster-code dbt build --project-dir dbt/formula1_extended --profiles-dir dbt
docker compose down -v
```

Pipeline health monitoring:

```bash
docker compose up --build -d
docker compose run --rm dagster-code python -m dagster job execute -f orchestration/definitions.py -j openf1_ingestion_job
curl http://localhost:9108/metrics
```

Prometheus scrapes the Dagster metrics exporter every 15 seconds using [monitoring/prometheus/prometheus.yml](/root/11_Formula1Extended/Formula1Extended/monitoring/prometheus/prometheus.yml).
Key exported metrics include:
- `formula1_pipeline_run_success_total`
- `formula1_pipeline_run_failure_total`
- `formula1_pipeline_run_retry_total`
- `formula1_pipeline_run_last_duration_seconds`
- `formula1_pipeline_run_last_success_unixtime`
- `formula1_asset_last_materialization_unixtime`
- `formula1_asset_freshness_lag_seconds`
- `formula1_asset_rows_ingested`
- `formula1_step_failure_total`
- `formula1_dbt_failure_total`

## Project Layout

- `docker-compose.yml` provisions Postgres plus the Dagster code, webserver, and daemon containers.
- `monitoring/prometheus/prometheus.yml` configures Prometheus to scrape pipeline-health metrics from the Dagster code container.
- `src/observability/metrics_state.py` persists pipeline-health metrics to a shared state file under `dagster_home/observability/`.
- `src/observability/metrics_server.py` exposes those metrics to Prometheus on port `9108`.
- `dagster_home/dagster.yaml` configures Dagster instance storage in Postgres.
- `src/storage/postgres_writer.py` persists raw API records into Postgres schemas.
- `dbt/formula1_extended/` contains the dbt staging and mart models.
- `.github/workflows/ci.yml` validates Python checks, Docker Compose startup, and dbt builds.
- `.github/workflows/deploy.yml` runs the Dockerized pipeline on `main` and uploads raw, Dagster, and dbt artifacts.

## Troubleshooting

- `Connection refused` or `could not translate host name`: confirm `docker compose up -d postgres` completed and the `.env` values match the running service.
- `No records to write`: the upstream API returned no rows for the resolved session; inspect the Dagster materialization logs.
- `dbt build` failures: inspect the `dbt` step output or the uploaded `dbt-artifacts` workflow artifact for compiled SQL and test results.
- Dagster UI not loading: check `docker compose logs dagster-webserver dagster-daemon dagster-code`.
- Prometheus not scraping metrics: check `docker compose logs prometheus dagster-code` and confirm `http://localhost:9108/metrics` responds.

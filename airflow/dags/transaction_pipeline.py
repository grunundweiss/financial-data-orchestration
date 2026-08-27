# airflow/dags/transaction_pipeline.py
from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.observability.stats import Stats

from pipeline_tasks import ingest_raw_transactions, run_dbt

DB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/analytics_platform.db")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

# dbt's profiles.yml resolves the DuckDB path via env_var('DUCKDB_PATH', ...);
# pin it so ingestion and dbt always point at the same file.
os.environ["DUCKDB_PATH"] = DB_PATH

log = logging.getLogger(__name__)


def alert_on_failure(context: dict) -> None:
    """Placeholder failure hook - swap for a Slack/PagerDuty call in a real deployment."""
    ti = context["task_instance"]
    log.error("Task %s in dag %s failed on run %s", ti.task_id, ti.dag_id, context["run_id"])


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="transaction_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,               # safe now that ingestion is idempotent per logical_date
    max_active_runs=1,          # DuckDB takes an exclusive write lock; concurrent runs would deadlock
    default_args=default_args,
    tags=["finance", "dbt", "duckdb"],
)
def transaction_pipeline():
    """dbt deps -> ingestion -> dbt run (Silver/Gold modeling) -> dbt test (quality gate)."""

    @task
    def dbt_deps() -> None:
        run_dbt("deps", DBT_PROJECT_DIR, DBT_PROFILES_DIR)

    @task
    def ingest_raw_data(logical_date=None) -> int:
        row_count = ingest_raw_transactions(DB_PATH, logical_date)
        # Feeds the "rows ingested" panel on the Grafana pipeline dashboard.
        Stats.gauge("rows_ingested", row_count)
        return row_count

    @task
    def dbt_run(_row_count: int, logical_date=None) -> None:
        batch_date = logical_date.strftime("%Y-%m-%d")
        run_dbt("run", DBT_PROJECT_DIR, DBT_PROFILES_DIR, dbt_vars={"batch_date": batch_date})

    @task
    def dbt_test() -> None:
        run_dbt("test", DBT_PROJECT_DIR, DBT_PROFILES_DIR)

    deps = dbt_deps()
    row_count = ingest_raw_data()
    deps >> dbt_run(row_count) >> dbt_test()


transaction_pipeline()

# airflow/dags/transaction_pipeline.py
from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.observability.stats import Stats

from audit_ledger import record_event, record_task_failure, snapshot_gold
from pipeline_tasks import ingest_raw_transactions, run_dbt, validate_batch_date

DB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/analytics_platform.db")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

# dbt's profiles.yml resolves the DuckDB path via env_var('DUCKDB_PATH', ...);
# pin it so ingestion and dbt always point at the same file.
os.environ["DUCKDB_PATH"] = DB_PATH

log = logging.getLogger(__name__)


def alert_on_failure(context: dict) -> None:
    """Unpacks Airflow's context and hands off to the testable failure handler.

    The handling itself lives in audit_ledger.record_task_failure so it can be
    tested without installing Airflow. Swap the logger for Slack/PagerDuty in a
    real deployment - the ledger write should stay either way.
    """
    ti = context["task_instance"]
    Stats.incr("pipeline_task_failed")
    record_task_failure(
        DB_PATH,
        run_id=context["run_id"],
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        try_number=getattr(ti, "try_number", None),
        logger=log,
    )


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

    # There is deliberately no dbt_deps task: packages are baked into the image
    # at build time (see airflow/Dockerfile), so a scheduled run fetches nothing
    # from the dbt hub and cannot execute macro SQL from a version that did not
    # exist when this code was written.

    @task
    def resolve_batch_date(logical_date=None, params=None, dag_run=None) -> str:
        """Resolves the run's batch_date and validates it before anything uses it.

        An operator can override the date for a manual backfill via
        `dag_run.conf`, which makes this the pipeline's untrusted input: the
        value ends up interpolated into the gold model's SQL. Validating it in
        its own task means a bad value fails the run here, before ingestion
        writes anything or dbt executes.
        """
        override = (dag_run.conf or {}).get("batch_date") if dag_run else None
        return validate_batch_date(override if override is not None else logical_date)

    @task
    def ingest_raw_data(batch_date: str, run_id=None) -> int:
        row_count = ingest_raw_transactions(DB_PATH, batch_date)
        # Feeds the "rows ingested" panel on the Grafana pipeline dashboard.
        Stats.gauge("rows_ingested", row_count)
        record_event(
            DB_PATH,
            run_id=run_id,
            dag_id="transaction_pipeline",
            task_id="ingest_raw_data",
            batch_date=batch_date,
            event_type="ingest",
            row_count=row_count,
            payload={"partition_replaced": batch_date},
        )
        return row_count

    @task
    def dbt_run(_row_count: int, batch_date: str, run_id=None) -> None:
        run_dbt("run", DBT_PROJECT_DIR, DBT_PROFILES_DIR, dbt_vars={"batch_date": batch_date})
        # Commit to the gold rows this run produced, so a later edit to them is
        # detectable even though the ledger itself is untouched.
        record_event(
            DB_PATH,
            run_id=run_id,
            dag_id="transaction_pipeline",
            task_id="dbt_run",
            batch_date=batch_date,
            event_type="gold_snapshot",
            payload=snapshot_gold(DB_PATH, batch_date),
        )

    @task
    def dbt_test(batch_date: str, run_id=None) -> None:
        run_dbt("test", DBT_PROJECT_DIR, DBT_PROFILES_DIR)
        # Only reached when the gate passed: this row is the durable evidence
        # that it did, for this batch_date and this run.
        chain_head = record_event(
            DB_PATH,
            run_id=run_id,
            dag_id="transaction_pipeline",
            task_id="dbt_test",
            batch_date=batch_date,
            event_type="quality_gate_passed",
            payload={"outcome": "pass"},
        )
        # Published so a broken chain is visible on the Grafana dashboard.
        Stats.gauge("audit_chain_head", int(chain_head[:8], 16))

    batch_date = resolve_batch_date()
    row_count = ingest_raw_data(batch_date)
    dbt_run(row_count, batch_date) >> dbt_test(batch_date)


transaction_pipeline()

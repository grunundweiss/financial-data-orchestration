# airflow/dags/pipeline_tasks.py
"""Plain business-logic functions used by the transaction_pipeline DAG.

Kept free of Airflow imports so they can be unit-tested without installing
apache-airflow on the host.
"""
import random
import uuid
from datetime import UTC, datetime, timedelta

import duckdb


def ingest_raw_transactions(db_path: str, logical_date: datetime, n: int = 100) -> int:
    """Generates the transaction batch (NOK) for `logical_date` into bronze.raw_transactions.

    Seeded on the date so a re-run or backfill of the same interval produces
    byte-identical rows: the task is idempotent and safe to retry. The write
    is scoped to the `batch_date` partition, so re-running one date never
    touches another day's rows.
    """
    batch_date = logical_date.strftime("%Y-%m-%d")
    rng = random.Random(batch_date)
    day_start = datetime.strptime(batch_date, "%Y-%m-%d").replace(tzinfo=UTC)

    account_pool = [f"ACC-NOR-{rng.randint(1000, 9999)}" for _ in range(5)]

    mock_transactions = [
        {
            "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "account_id": rng.choice(account_pool),
            "amount": round(rng.uniform(-5000.0, 15000.0), 2),
            "currency": "NOK",
            "timestamp": (day_start + timedelta(seconds=rng.randint(0, 86399))).isoformat(),
            "merchant_category": rng.choice(["Retail", "Utilities", "Transfer", "Entertainment"]),
            "batch_date": batch_date,
        }
        for _ in range(n)
    ]

    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
            transaction_id TEXT,
            account_id TEXT,
            amount DOUBLE,
            currency TEXT,
            timestamp TIMESTAMP,
            merchant_category TEXT,
            batch_date DATE
        );
    """)
    conn.execute("DELETE FROM bronze.raw_transactions WHERE batch_date = ?;", [batch_date])
    conn.executemany("""
        INSERT INTO bronze.raw_transactions VALUES
        ($transaction_id, $account_id, $amount, $currency, $timestamp, $merchant_category, $batch_date);
    """, mock_transactions)

    count_row = conn.execute(
        "SELECT COUNT(*) FROM bronze.raw_transactions WHERE batch_date = ?;", [batch_date]
    ).fetchone()
    conn.close()
    return int(count_row[0]) if count_row else 0


def run_dbt(command: str, project_dir: str, profiles_dir: str, dbt_vars: dict | None = None) -> None:
    """Invokes a real dbt command (e.g. 'run' or 'test') against dbt_project."""
    import json

    from dbt.cli.main import dbtRunner

    args = [command, "--project-dir", project_dir, "--profiles-dir", profiles_dir]
    if dbt_vars:
        args += ["--vars", json.dumps(dbt_vars)]
    result = dbtRunner().invoke(args)

    if not result.success:
        raise RuntimeError(f"dbt {command} failed: {result.exception or result.result}")

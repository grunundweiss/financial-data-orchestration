# airflow/dags/pipeline_tasks.py
"""Plain business-logic functions used by the transaction_pipeline DAG.

Kept free of Airflow imports so they can be unit-tested without installing
apache-airflow on the host.
"""
import random
import uuid
from datetime import UTC, datetime, timedelta

import duckdb

from tokenization import register_tokens, tokenize_account


class InvalidBatchDate(ValueError):
    """Raised when an untrusted batch_date fails validation at the trust boundary."""


def validate_batch_date(value: object) -> str:
    """Returns `value` as a canonical YYYY-MM-DD string, or raises InvalidBatchDate.

    batch_date reaches the pipeline from callers we don't control (dag_run.conf
    on a manual backfill, a REST trigger, an operator-supplied param) and is
    interpolated into the gold model's SQL. Validation therefore parses the
    value into a real date rather than pattern-matching it: strptime accepts
    only an actual calendar date, so a SQL fragment, a date-shaped string that
    isn't a date, or a non-string can't survive this function.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if not isinstance(value, str):
        raise InvalidBatchDate(f"batch_date must be a string or datetime, got {type(value).__name__}")
    try:
        # Deliberately naive: this is a calendar date, not an instant, and
        # strptime is doing validation here rather than time arithmetic.
        return datetime.strptime(value.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")  # noqa: DTZ007
    except ValueError as exc:
        raise InvalidBatchDate(f"batch_date must be YYYY-MM-DD, got {value!r}") from exc


def ingest_raw_transactions(db_path: str, logical_date: datetime | str, n: int = 100) -> int:
    """Generates the transaction batch (NOK) for `logical_date` into bronze.raw_transactions.

    Accepts either a datetime or an already-validated YYYY-MM-DD string, and
    validates either way. Seeded on the date so a re-run or backfill of the
    same interval produces byte-identical rows: the task is idempotent and
    safe to retry. The write is scoped to the `batch_date` partition, so
    re-running one date never touches another day's rows.
    """
    batch_date = validate_batch_date(logical_date)
    rng = random.Random(batch_date)
    # batch_date is a calendar date with no time zone of its own; UTC is
    # attached explicitly on the next call rather than inferred from the host.
    day_start = datetime.strptime(batch_date, "%Y-%m-%d").replace(tzinfo=UTC)

    # The identifiers the "source feed" produces. They exist only in this
    # function and in the vault - what reaches Bronze is the token.
    account_pool = [f"ACC-NOR-{rng.randint(1000, 9999)}" for _ in range(5)]
    tokens = {account_id: tokenize_account(account_id) for account_id in account_pool}

    mock_transactions = [
        {
            "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "account_id": tokens[rng.choice(account_pool)],
            "amount": round(rng.uniform(-5000.0, 15000.0), 2),
            "currency": "NOK",
            "timestamp": (day_start + timedelta(seconds=rng.randint(0, 86399))).isoformat(),
            "merchant_category": rng.choice(["Retail", "Utilities", "Transfer", "Entertainment"]),
            "batch_date": batch_date,
        }
        for _ in range(n)
    ]

    conn = duckdb.connect(db_path)
    register_tokens(conn, account_pool, batch_date)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
            transaction_id TEXT,
            -- Tokenized at ingestion: this is never the source identifier.
            -- See airflow/dags/tokenization.py and vault.account_tokens.
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
    """Invokes a real dbt command (e.g. 'run' or 'test') against dbt_project.

    Re-validates batch_date here as well as at the DAG boundary: this function
    is the only path by which a var reaches dbt's templating, so validating it
    here means the model is safe no matter which caller supplied the value.
    """
    import json

    from dbt.cli.main import dbtRunner

    args = [command, "--project-dir", project_dir, "--profiles-dir", profiles_dir]
    if dbt_vars:
        dbt_vars = dict(dbt_vars)
        if "batch_date" in dbt_vars:
            dbt_vars["batch_date"] = validate_batch_date(dbt_vars["batch_date"])
        args += ["--vars", json.dumps(dbt_vars)]
    result = dbtRunner().invoke(args)

    if not result.success:
        raise RuntimeError(f"dbt {command} failed: {result.exception or result.result}")

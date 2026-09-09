# tests/test_analytics.py
import os
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from pipeline_tasks import ingest_raw_transactions, run_dbt

DBT_PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dbt_project'))


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    """Points dbt's profiles.yml at a fresh, per-test DuckDB file via DUCKDB_PATH."""
    db_path = str(tmp_path / "test_analytics.db")
    monkeypatch.setenv("DUCKDB_PATH", db_path)
    yield db_path


def run_pipeline(db_path, logical_date):
    ingest_raw_transactions(db_path, logical_date)
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR,
             dbt_vars={"batch_date": logical_date.strftime("%Y-%m-%d")})
    run_dbt("test", DBT_PROJECT_DIR, DBT_PROJECT_DIR)


def test_pipeline_end_to_end_execution(isolated_db):
    """Verifies ingestion -> real `dbt run` -> real `dbt test` executes cleanly end-to-end."""
    run_pipeline(isolated_db, datetime(2026, 6, 29, tzinfo=UTC))

    conn = duckdb.connect(isolated_db)

    bronze_count = conn.execute("SELECT COUNT(*) FROM bronze.raw_transactions;").fetchone()[0]
    assert bronze_count == 100

    silver_count = conn.execute("SELECT COUNT(*) FROM silver.stg_transactions;").fetchone()[0]
    assert silver_count == 100

    gold_count = conn.execute("SELECT COUNT(*) FROM gold.fct_account_risk_metrics;").fetchone()[0]
    assert gold_count > 0

    conn.close()


def test_dbt_risk_profiling_logic(isolated_db):
    """Explicitly tests the dbt model's conditional SQL logic for transaction risk classification."""
    logical_date = datetime(2026, 6, 29, tzinfo=UTC)
    ingest_raw_transactions(isolated_db, logical_date)

    # Inject deterministic rows directly into bronze to test boundary conditions
    conn = duckdb.connect(isolated_db)
    conn.execute("""
        INSERT INTO bronze.raw_transactions VALUES
        ('test-1', 'ACC-1', 15000.00, 'NOK', '2026-06-29T00:00:00Z', 'Retail', '2026-06-29'),
        ('test-2', 'ACC-1', -50.00, 'NOK', '2026-06-29T00:00:00Z', 'Utilities', '2026-06-29'),
        ('test-3', 'ACC-1', 500.00, 'NOK', '2026-06-29T00:00:00Z', 'Entertainment', '2026-06-29');
    """)
    conn.close()

    # Run the real dbt model (dbt_project/models/staging/stg_transactions.sql)
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": "2026-06-29"})

    conn = duckdb.connect(isolated_db)
    high_value = conn.execute(
        "SELECT transaction_risk_profile FROM silver.stg_transactions WHERE transaction_id='test-1';"
    ).fetchone()[0]
    outflow = conn.execute(
        "SELECT transaction_risk_profile FROM silver.stg_transactions WHERE transaction_id='test-2';"
    ).fetchone()[0]
    standard = conn.execute(
        "SELECT transaction_risk_profile FROM silver.stg_transactions WHERE transaction_id='test-3';"
    ).fetchone()[0]
    conn.close()

    assert high_value == "HIGH_VALUE"
    assert outflow == "OUTFLOW"
    assert standard == "STANDARD"


def test_dbt_schema_alignment(isolated_db):
    """Validates that dbt materializes models into the configured silver/gold schemas."""
    run_pipeline(isolated_db, datetime(2026, 6, 29, tzinfo=UTC))

    conn = duckdb.connect(isolated_db)

    silver_tables = [t[0].lower() for t in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE LOWER(table_schema)='silver';"
    ).fetchall()]
    gold_tables = [t[0].lower() for t in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE LOWER(table_schema)='gold';"
    ).fetchall()]

    conn.close()

    assert "stg_transactions" in silver_tables, \
        f"Expected dbt staging model in silver schema, got: {silver_tables}"
    assert "fct_account_risk_metrics" in gold_tables, \
        f"Expected dbt fact table in gold schema, got: {gold_tables}"


def test_ingestion_is_idempotent_per_logical_date(isolated_db):
    """Re-running the same logical_date must produce byte-identical rows, not a second copy."""
    logical_date = datetime(2026, 3, 1, tzinfo=UTC)

    first_count = ingest_raw_transactions(isolated_db, logical_date)
    conn = duckdb.connect(isolated_db)
    first_rows = conn.execute(
        "SELECT * FROM bronze.raw_transactions WHERE batch_date = '2026-03-01' ORDER BY transaction_id;"
    ).fetchall()
    conn.close()

    second_count = ingest_raw_transactions(isolated_db, logical_date)
    conn = duckdb.connect(isolated_db)
    second_rows = conn.execute(
        "SELECT * FROM bronze.raw_transactions WHERE batch_date = '2026-03-01' ORDER BY transaction_id;"
    ).fetchall()
    total_rows = conn.execute("SELECT COUNT(*) FROM bronze.raw_transactions;").fetchone()[0]
    conn.close()

    assert first_count == second_count == 100
    assert first_rows == second_rows
    assert total_rows == 100  # the re-run replaced the partition instead of duplicating it


def test_backfill_produces_one_partition_per_day(isolated_db):
    """A 3-day backfill must yield 3 distinct bronze partitions, 300 total rows, and one
    gold fact row per (account, day) rather than an overwritten lifetime rollup."""
    start = datetime(2026, 1, 15, tzinfo=UTC)
    logical_dates = [start + timedelta(days=i) for i in range(3)]

    for logical_date in logical_dates:
        run_pipeline(isolated_db, logical_date)

    conn = duckdb.connect(isolated_db)
    total_bronze_rows = conn.execute("SELECT COUNT(*) FROM bronze.raw_transactions;").fetchone()[0]
    distinct_partitions = conn.execute(
        "SELECT COUNT(DISTINCT batch_date) FROM bronze.raw_transactions;"
    ).fetchone()[0]
    distinct_gold_days = conn.execute(
        "SELECT COUNT(DISTINCT batch_date) FROM gold.fct_account_risk_metrics;"
    ).fetchone()[0]
    conn.close()

    assert total_bronze_rows == 300
    assert distinct_partitions == 3
    assert distinct_gold_days == 3

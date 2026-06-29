# tests/test_analytics.py
import os
import sys
import duckdb
import pytest

# Ensure the airflow directory can be found by Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../airflow/dags')))

from transaction_pipeline import FinancialDataOrchestrator

TEST_DB_PATH = "test_analytics_isolated.db"

@pytest.fixture
def clean_orchestrator():
    """Fixture to provision and tear down an isolated testing database lifecycle."""
    orchestrator = FinancialDataOrchestrator(db_path=TEST_DB_PATH)
    yield orchestrator
    # Clean up and delete the test database file after tests complete
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

def test_pipeline_end_to_end_execution(clean_orchestrator):
    """Verifies that the entire pipeline executes from Bronze to Gold without throwing errors."""
    clean_orchestrator.run_pipeline()

    conn = duckdb.connect(TEST_DB_PATH)

    # Assert Bronze layer populated
    bronze_count = conn.execute("SELECT COUNT(*) FROM bronze.stg_transactions;").fetchone()[0]
    assert bronze_count == 100

    # Assert Silver layer transformed properly
    silver_count = conn.execute("SELECT COUNT(*) FROM silver.int_transactions_cleansed;").fetchone()[0]
    assert silver_count == 100

    # Assert Gold layer aggregated unique accounts
    gold_count = conn.execute("SELECT COUNT(*) FROM gold.fct_account_risk_metrics;").fetchone()[0]
    assert gold_count > 0

    conn.close()

def test_silver_layer_risk_profiling_logic(clean_orchestrator):
    """Explicitly tests the conditional SQL logic for transaction risk classification."""
    clean_orchestrator.execute_task_1_ingest_raw_data()

    # Inject deterministic rows directly into bronze to test boundary conditions
    conn = duckdb.connect(TEST_DB_PATH)
    conn.execute("""
        INSERT INTO bronze.stg_transactions VALUES
        ('test-1', 'ACC-1', 15000.00, 'NOK', '2026-06-29T00:00:00Z', 'Retail'),
        ('test-2', 'ACC-1', -50.00, 'NOK', '2026-06-29T00:00:00Z', 'Utilities'),
        ('test-3', 'ACC-1', 500.00, 'NOK', '2026-06-29T00:00:00Z', 'Entertainment');
    """)
    conn.close()

    # Run transformations
    clean_orchestrator.execute_task_2_dbt_transformations()

    # Validate calculations
    conn = duckdb.connect(TEST_DB_PATH)
    high_value = conn.execute("SELECT transaction_risk_profile FROM silver.int_transactions_cleansed WHERE transaction_id='test-1';").fetchone()[0]
    outflow = conn.execute("SELECT transaction_risk_profile FROM silver.int_transactions_cleansed WHERE transaction_id='test-2';").fetchone()[0]
    standard = conn.execute("SELECT transaction_risk_profile FROM silver.int_transactions_cleansed WHERE transaction_id='test-3';").fetchone()[0]
    conn.close()

    assert high_value == "HIGH_VALUE"
    assert outflow == "OUTFLOW"
    assert standard == "STANDARD"

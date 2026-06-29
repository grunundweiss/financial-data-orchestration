# airflow/dags/transaction_pipeline.py
import os
import duckdb
from datetime import datetime, timezone
import random
import uuid

DB_PATH = "analytics_platform.db"

class FinancialDataOrchestrator:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def execute_task_1_ingest_raw_data(self):
        """Task 1: Simulates ingestion of raw daily banking transactions.

        Creates the Bronze/Staging layer directly from a generated stream.
        """
        print("Executing Task 1: Ingesting raw transactional stream...")

        # Generate mock transaction data
        mock_transactions = []
        account_pool = [f"ACC-NOR-{random.randint(1000, 9999)}" for _ in range(5)]

        for _ in range(100):
            mock_transactions.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id": random.choice(account_pool),
                "amount": round(random.uniform(-5000.0, 15000.0), 2),
                "currency": "NOK",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "merchant_category": random.choice(["Retail", "Utilities", "Transfer", "Entertainment"])
            })

        # Connect to DuckDB and initialize the Bronze (Staging) layer
        conn = duckdb.connect(self.db_path)

        # Create staging schema and table
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        conn.execute("""
            CREATE OR REPLACE TABLE bronze.stg_transactions (
                transaction_id TEXT,
                account_id TEXT,
                amount DOUBLE,
                currency TEXT,
                timestamp TIMESTAMP,
                merchant_category TEXT
            );
        """)

        # Batch insert data using DuckDB's native Python object integration
        conn.executemany("""
            INSERT INTO bronze.stg_transactions VALUES
            ($transaction_id, $account_id, $amount, $currency, $timestamp, $merchant_category);
        """, mock_transactions)

        row_count = conn.execute("SELECT COUNT(*) FROM bronze.stg_transactions;").fetchone()[0]
        conn.close()
        print(f"Task 1 Complete: Ingested {row_count} raw records into bronze.stg_transactions.")

    def execute_task_2_dbt_transformations(self):
        """Task 2: Executes the analytical SQL data modeling logic.

        Simulates dbt execution by building the Silver and Gold analytics layers.
        """
        print("Executing Task 2: Running data modeling transformations (dbt simulation)...")
        conn = duckdb.connect(self.db_path)

        # --- SILVER LAYER: Cleaned & Transformed Intermediate Data ---
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        conn.execute("""
            CREATE OR REPLACE TABLE silver.int_transactions_cleansed AS
            SELECT
                transaction_id,
                account_id,
                amount,
                currency,
                timestamp,
                merchant_category,
                CASE
                    WHEN amount > 10000.0 THEN 'HIGH_VALUE'
                    WHEN amount < 0.0 THEN 'OUTFLOW'
                    ELSE 'STANDARD'
                END AS transaction_risk_profile
            FROM bronze.stg_transactions
            WHERE transaction_id IS NOT NULL;
        """)

        # --- GOLD LAYER: Business-Ready Metrics (Aggregations) ---
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        conn.execute("""
            CREATE OR REPLACE TABLE gold.fct_account_risk_metrics AS
            SELECT
                account_id,
                COUNT(transaction_id) AS total_transactions,
                ROUND(SUM(amount), 2) AS net_balance,
                COUNT(CASE WHEN transaction_risk_profile = 'HIGH_VALUE' THEN 1 END) AS high_value_count,
                ROUND(AVG(amount), 2) AS average_transaction_size
            FROM silver.int_transactions_cleansed
            GROUP BY account_id;
        """)

        conn.close()
        print("Task 2 Complete: Silver and Gold layers successfully compiled.")

    def execute_task_3_data_quality_audit(self):
        """Task 3: SRE perspective data testing gate.

        Enforces constraints similar to dbt tests (unique, not_null, value thresholds).
        """
        print("Executing Task 3: Running data quality and integrity audits...")
        conn = duckdb.connect(self.db_path)

        # Test 1: Primary Key Uniqueness check
        duplicate_count = conn.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
            FROM silver.int_transactions_cleansed;
        """).fetchone()[0]

        # Test 2: Null boundary check
        null_accounts = conn.execute("""
            SELECT COUNT(*) FROM gold.fct_account_risk_metrics WHERE account_id IS NULL;
        """).fetchone()[0]

        conn.close()

        # Assertion matrix to simulate platform pipeline failure triggers
        if duplicate_count > 0:
            raise ValueError(f"CRITICAL: Primary Key violation detected. {duplicate_count} duplicates found.")
        if null_accounts > 0:
            raise ValueError(f"CRITICAL: Integrity violation. Null accounts leaked into Gold layer.")

        print("Task 3 Complete: All data quality assertions PASSED successfully.")

    def run_pipeline(self):
        """Master execution block mapping DAG task dependency structure."""
        print(f"--- Pipeline Execution Started: {datetime.now(timezone.utc)} ---")
        try:
            self.execute_task_1_ingest_raw_data()
            self.execute_task_2_dbt_transformations()
            self.execute_task_3_data_quality_audit()
            print("--- Pipeline Completed Successfully [GREEN] ---")
        except Exception as e:
            print(f"--- Pipeline Execution FAILED [RED]: {e} ---")
            raise

if __name__ == "__main__":
    orchestrator = FinancialDataOrchestrator()
    orchestrator.run_pipeline()

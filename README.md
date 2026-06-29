# Automated Financial Data Orchestration & Observability Platform

A production-grade transactional data pipeline implementing scheduled ingestion, multi-layered data modeling, and automated data quality validation gates. The platform uses DuckDB as an embedded high-performance analytical engine to execute structured data warehouse transformation strategies locally and instantly.

## Architectural Data Matrix

The platform orchestrates a three-tier medallion data model architecture to ensure separation of concerns and optimal analytical query execution:
* **Bronze Layer (`bronze.stg_transactions`):** Low-latency data landing tier that ingests raw transaction streams without structural mutations.
* **Silver Layer (`silver.int_transactions_cleansed`):** Intermediate transformation tier executing data cleaning, transaction classification logic, and risk profiling.
* **Gold Layer (`gold.fct_account_risk_metrics`):** Highly aggregated analytical fact layer tracking high-value exposure counts and rolling balances per account.

---

## Getting Started

### Prerequisites
* Python 3.12+
* Git

### Installation & Local Setup
1. **Clone the project infrastructure:**
   ```bash
   git clone [https://github.com/grunundweiss/financial-data-orchestration.git](https://github.com/grunundweiss/financial-data-orchestration.git)
   cd financial-data-orchestration
   ```

2. **Initialize isolated execution environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install application dependencies:**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

---

## Running the Platform

### 1. Execute the Data Pipeline Orchestrator
To manually trigger the master workflow sequence (Ingestion $\rightarrow$ Data Modeling Transformations $\rightarrow$ Quality Auditing):
```bash
python airflow/dags/transaction_pipeline.py
```

### 2. Run Automated Verification Tests
The repository runs a strict testing matrix using Pytest to validate data types, edge-case conditions, and conditional data routing rules:
```bash
pytest -v
```

---

## CI/CD Infrastructure
Every commit pushed to the `main` branch automatically triggers the integrated GitHub Actions workflow. The runner provisions an isolated environment, verifies package compilation stability, and ensures all analytical data transformations pass the regression test suite cleanly before deployment boundaries are met.

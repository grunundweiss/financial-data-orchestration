# Automated Financial Data Orchestration & Observability Platform

A production-grade transactional data pipeline implementing scheduled ingestion, multi-layered data modeling via dbt, and automated data quality validation gates. The platform uses DuckDB as an embedded analytical engine alongside a Dockerized SRE observability stack to monitor execution health and data integrity.

## Architectural Data & Observability Matrix

The platform orchestrates a three-tier medallion data model architecture alongside an infrastructure monitoring mesh:
* **Bronze Layer (`bronze.stg_transactions`):** Ingests raw transaction streams (simulating a live daily banking feed in NOK) without structural mutations.
* **Silver Layer (`dbt_project/models/staging/stg_transactions.sql`):** Intermediate dbt view executing data cleaning, risk profile classification logic, and structural validations.
* **Gold Layer (`dbt_project/models/intermediate/fct_account_risk_metrics.sql`):** Highly aggregated analytical fact table tracking high-value exposure counts and rolling balances per account.
* **Observability Tier (`monitoring/`):** Dedicated Prometheus scraping instance paired with Grafana dashboard services to track data flow metrics, assertion pass rates, and processing latency boundaries.

---

## Getting Started

### Prerequisites
* Python 3.12+
* Docker & Docker Compose
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

## Running and Testing the Platform

### 1. Execute the Data Pipeline
To manually trigger the master orchestrator sequence (Ingestion $\rightarrow$ dbt Data Modeling Transformations $\rightarrow$ Quality Auditing):
```bash
python airflow/dags/transaction_pipeline.py
```

### 2. Run Automated Verification Tests
The repository runs an isolated test suite using Pytest to validate data types, risk boundary logic, and database catalog compliance across schemas:
```bash
pytest -v -s
```

### 3. Launch the Monitoring Infrastructure
To spin up the Prometheus and Grafana containers in the background:
```bash
docker compose up -d
```
* **Prometheus Gateway:** Accessible locally at `http://localhost:9090`
* **Grafana Dashboard Engine:** Accessible locally at `http://localhost:3000` (Default credentials: `admin` / `secure_admin_pass`)

---

## CI/CD Infrastructure
Every commit pushed to the `main` branch automatically triggers the integrated GitHub Actions workflow. The runner provisions an isolated environment, verifies package compilation stability, and ensures all analytical data transformations pass the regression test suite cleanly before deployment boundaries are met.

# Financial Data Orchestration

[![CI](https://github.com/grunundweiss/financial-data-orchestration/actions/workflows/ci.yml/badge.svg)](https://github.com/grunundweiss/financial-data-orchestration/actions/workflows/ci.yml)

A daily transaction pipeline: Airflow schedules it, dbt models it in DuckDB, dbt tests gate it, and Prometheus/Grafana show whether it's healthy. Runs are idempotent and backfillable, in which an action of re-running any logical date reproduces that date's data exactly.

## How it works

```mermaid
flowchart LR
    subgraph airflow["Airflow DAG (transaction_pipeline, @daily)"]
        date["resolve_batch_date<br/>validates untrusted input"] --> ingest["ingest_raw_data<br/>seeded + tokenized"]
        ingest --> run["dbt run<br/>silver + gold models"]
        run --> test["dbt test<br/>quality gate"]
    end

    subgraph duckdb["DuckDB (medallion layers)"]
        bronze["bronze.raw_transactions<br/>partitioned by batch_date"]
        silver["silver.stg_transactions<br/>+ stg_transactions_rejects"]
        gold["gold.fct_account_risk_metrics<br/>incremental"]
        vault["vault.account_tokens"]
        ledger["audit.pipeline_events<br/>hash-chained"]
    end

    ingest --> bronze
    ingest -.-> vault
    run --> silver
    silver --> gold
    test -.-> ledger

    subgraph obs["Observability"]
        statsd["statsd-exporter"] --> prom["Prometheus"] --> graf["Grafana"]
    end

    airflow -. "statsd metrics" .-> statsd
```

**Data layers**
* **Bronze (`bronze.raw_transactions`)** — raw daily transactions in NOK, ingested unmodified and partitioned by `batch_date`. Written by the `ingest_raw_data` task ([airflow/dags/pipeline_tasks.py](airflow/dags/pipeline_tasks.py)).
* **Silver (`silver.stg_transactions`)** — dbt view: cleaning plus risk-profile classification. Rows with no `transaction_id` are excluded here but captured in `silver.stg_transactions_rejects`, so the drop is counted rather than silent.
* **Gold (`gold.fct_account_risk_metrics`)** — dbt incremental table: one row per account per day, with net balance and high-value exposure.
* **Quality gate** — `dbt test` runs 14 tests ([schema.yml](dbt_project/models/schema.yml) plus two custom singular tests in [dbt_project/tests/](dbt_project/tests/)) as the DAG's final task. The DAG fails if any fail, and the pass is recorded in the audit ledger.
* **Security layer** — account identifiers are tokenized at the Bronze boundary, and every run appends a hash-chained entry that makes later edits detectable. See [SECURITY.md](SECURITY.md).

The DAG code is split in two so the business logic stays testable without installing Airflow:
* [airflow/dags/pipeline_tasks.py](airflow/dags/pipeline_tasks.py) — plain functions, no Airflow import.
* [airflow/dags/transaction_pipeline.py](airflow/dags/transaction_pipeline.py) — the DAG (TaskFlow API) wiring those functions together.

## Idempotency and backfill

Every run is a pure function of its logical date.

`ingest_raw_transactions` seeds its random generator on `logical_date`, so the batch for 2026-01-15 is byte-identical whether it was generated on the day or backfilled six months later. The write is partition-scoped — `DELETE FROM bronze.raw_transactions WHERE batch_date = ?` followed by `INSERT` — so re-running one date replaces only that date's rows and never touches another day's.

The gold model follows the same rule. Its grain is one row per account **per `batch_date`**, materialized `incremental` with a `delete+insert` strategy on `['account_id', 'batch_date']`. Each run only aggregates the day it was given, so backfilling out of order can't double-count or overwrite neighbouring days.

Two consequences worth stating explicitly:

* **`catchup=True` is meaningful.** A three-day backfill produces three distinct partitions and 300 rows, asserted in [tests/test_analytics.py](tests/test_analytics.py).
* **Retries are only safe because of this.** `default_args` sets `retries=2` with exponential backoff. A retry re-executes the same logical date — which is harmless precisely because that operation is idempotent. Retries on a non-idempotent task would duplicate data instead of recovering from a failure.

## Design decisions

**Why DuckDB over Postgres.** The analytical workload here is columnar aggregation over a single node's worth of data. DuckDB gives that with zero infrastructure — the warehouse is a file — which keeps the repo cloneable and runnable in one command. The tradeoff is the next point.

**Why `max_active_runs=1`.** DuckDB takes an exclusive write lock on its database file. Concurrent DAG runs would contend for it and deadlock, so the scheduler is constrained to one run at a time. This is a case where the storage engine's concurrency model dictates the scheduler config; on Postgres or Snowflake this limit could be lifted.

**Why business logic lives outside the DAG file.** `pipeline_tasks.py` has no Airflow import, so the test suite exercises real ingestion and real dbt runs without installing or booting Airflow. The DAG file is left as thin wiring.

**Why rejects are quarantined rather than filtered.** A `WHERE ... IS NOT NULL` that silently drops rows lets a pipeline discard most of a day's data and still report success. `stg_transactions_rejects` captures exactly what was dropped, and `reject_rate_below_threshold.sql` fails the quality gate if rejects exceed 5% of a batch.

**Why a statsd mapping config.** Without one, Airflow's dotted metric names become one Prometheus metric per DAG and task, which can't be grouped or filtered. [monitoring/statsd-mapping.yml](monitoring/statsd-mapping.yml) lifts `dag_id` and `task_id` into labels so the dashboard queries stay stable as DAGs are added.

**Why tokens are deterministic.** The gold model groups by account and a dbt `relationships` test joins gold back to silver, so the same identifier must always produce the same token. Per-row salting would break the pipeline's own correctness tests. The cost is that an attacker who can submit known identifiers can confirm whether an account is present — a deliberate trade-off, written down in [the threat model](docs/threat-model.md#what-is-explicitly-not-defended) rather than left implicit.

**Why `dbt deps` is not a task in the DAG.** It used to be, which meant every scheduled run reached out to the dbt hub before the models executed — a network dependency at 03:00 and a third party's ability to change what your SQL does between runs. Packages are now exact-pinned and installed at image build time.

**Why validation happens three times.** `batch_date` is parsed at the DAG boundary, re-validated in `run_dbt`, and re-asserted in the model's own Jinja. That looks redundant until you notice the model can be run by hand from the dbt CLI, bypassing the first two entirely.

## Getting started

**Prerequisites:** Python 3.14+, Docker & Docker Compose, Git.

```bash
git clone https://github.com/grunundweiss/financial-data-orchestration.git
cd financial-data-orchestration
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Then generate local secrets:

```bash
python security/generate_env.py && python security/preflight.py
```

That writes a `.env` (mode 600, gitignored) with a fresh Fernet key, JWT secret, database password, UI and Grafana passwords, and the HMAC pepper used to tokenize account identifiers. The preflight then confirms it.

Every one of those values is **required**: `docker-compose.yml` uses `${VAR:?}` expansions with no fallbacks, so a missing or half-filled `.env` stops the stack rather than booting it with a default published in this repository. The preflight additionally rejects values that are present but still placeholders. Your Airflow and Grafana passwords are in the generated file.

To fill it in by hand instead, copy `.env.example` and run the preflight until it passes.

**Regenerating `.env` for a stack that has already run** (`--force`) rotates `POSTGRES_PASSWORD`, but Postgres only applies that variable when it initializes an empty data directory — it won't be reflected in the already-initialized `postgres-db-volume`, so every Airflow component starts failing with "password authentication failed" until you drop that volume:

```bash
docker compose down -v && docker compose up airflow-init && docker compose up -d --wait
```

That only removes Airflow's own metadata database; `./data` and `./airflow/dags` are host bind mounts and are untouched. `generate_env.py --force` prints this same reminder.

### Run the tests

Ingests mock data into DuckDB and runs a real `dbt run` / `dbt test` against it.

```bash
pytest -v
```

### Run the full platform

```bash
sudo systemctl start docker         # start the docker
docker compose up airflow-init      # one-time DB migration + admin user
docker compose up -d --wait         # Airflow, Postgres, statsd-exporter, Prometheus, Grafana
```

* **Airflow**  `http://localhost:8080` (login from `.env`). Unpause and trigger `transaction_pipeline`, or:
  ```bash
  docker compose exec airflow-apiserver airflow dags trigger transaction_pipeline
  ```
* **Prometheus**  `http://localhost:9090`; the `airflow_pipeline_metrics` target should be `UP`.
* **Grafana**  `http://localhost:3000`. The Prometheus datasource and the **Transaction Pipeline** dashboard are provisioned automatically from [monitoring/grafana/](monitoring/grafana/) , theremore no manual setup is required.

Tear down with `docker compose down -v`.

![Grafana dashboard showing rows ingested, task outcomes, and per-task duration for a real transaction_pipeline run](docs/grafana-dashboard.png)

Rows ingested, task duration, and task success/failure counts come from `airflow dags test` runs — the same command CI uses. "DAG run duration" and "scheduler delay" stay empty until the DAG has run through the actual scheduler (`airflow dags trigger`, or its `@daily` schedule firing), since `dags test` bypasses the scheduler path those two metrics are emitted from.

## Security

The interesting demo is ten seconds long. Ingest three days, quietly edit one
historical gold row directly in DuckDB, and ask the verifier:

```bash
python security/verify_chain.py --db data/analytics_platform.db
```

```
verify-chain: FAILED
  the ledger chain is intact across 9 entries, but the warehouse no longer matches what it recorded:
  - gold rows for 2026-01-16 no longer match the hash recorded when the run completed
    (recorded d8dcf3f49b1c..., now 29135f52421f...)
```

Every pipeline run appends a hash-chained entry to `audit.pipeline_events`, committing to the gold rows it produced. Editing the warehouse, editing the ledger, deleting an entry, or re-parenting one are all detected — and each has a test.

Account identifiers are HMAC-tokenized at the Bronze boundary, so the raw account number never reaches the warehouse file; re-identification is a lookup against `vault.account_tokens` rather than a join available to any query.

Nine controls, each mapped to a test that can fail, are in [security/controls.yaml](security/controls.yaml). CI fails if any control names a test that doesn't exist. Two are marked `partial`, with notes on what's missing.

* **[SECURITY.md](SECURITY.md)** — what was wrong with this repo, and the test that now catches each one.
* **[docs/threat-model.md](docs/threat-model.md)** — assets, trust boundaries, and what is explicitly *not* defended.

## CI

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push and PR to `main`:

1. **`test`** — `ruff check`, `mypy`, then `pytest` (real dbt run/test against DuckDB).
2. **`controls`** — fails if any control in the register has no test behind it.
3. **`secrets`** — gitleaks over the full history.
4. **`supply-chain`** — `pip-audit` and `bandit`.
5. **`airflow-smoke-test`** — builds the image, boots the full compose stack, asserts zero DAG import errors, runs the DAG end-to-end, **re-runs the same logical date and asserts the row count didn't change**, verifies the audit chain, **then tampers with the warehouse and asserts verify-chain rejects it**, and checks Grafana provisioned its datasource.

That last job runs the real scheduler, so a DAG that imports cleanly but fails at runtime, or an ingestion change that quietly breaks idempotency, fails CI rather than passing a mocked test. The tamper step matters for the same reason: a verifier that only ever runs on clean data proves nothing.

## Known limitations

* **Single node.** LocalExecutor and an embedded DuckDB file; no distributed execution.
* **DuckDB is single-writer**, which is why `max_active_runs=1`. Concurrent DAG runs are not possible without swapping the warehouse.
* **Mock data.** `ingest_raw_transactions` generates transactions rather than reading a real feed; the seeding makes it reproducible, not realistic.
* **No CDC and no SCD2.** Bronze is a full daily partition, not a change stream, and dimensions have no history tracking.
* **The failure callback logs and writes to the ledger.** It does not page anyone; wiring it to Slack or PagerDuty is left as a deployment concern.
* **No encryption at rest, and the audit ledger is append-only by convention.** The chain makes tampering detectable, not impossible. See [the threat model](docs/threat-model.md#what-is-explicitly-not-defended) for the full list of what isn't defended, including the deterministic-tokenization trade-off.

## License

MIT — see [LICENSE](LICENSE).

# tests/test_security_ledger.py
"""Proves C-05: every pipeline run appends a tamper-evident ledger entry.

The gate from milestone 5: run three days, mutate one historical gold row
directly in DuckDB, and assert verify-chain exits non-zero naming that row.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from audit_ledger import (
    ChainBreak,
    compute_payload_hash,
    record_event,
    snapshot_gold,
    verify_chain,
    verify_gold_against_ledger,
)
from pipeline_tasks import ingest_raw_transactions, run_dbt

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = str(REPO_ROOT / "dbt_project")
VERIFY_CLI = REPO_ROOT / "security" / "verify_chain.py"

BACKFILL_DAYS = ["2026-01-15", "2026-01-16", "2026-01-17"]


def release_dbt_connection() -> None:
    """Closes dbt-duckdb's cached connection so another process can open the file.

    dbt-duckdb caches its environment on a class attribute, and DuckDB permits
    a single writer per file. Nothing in production needs this - verify-chain
    runs when the pipeline is idle - but the tests drive dbt and the CLI from
    one session.
    """
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager

    with DuckDBConnectionManager._LOCK:
        if DuckDBConnectionManager._ENV is not None:
            DuckDBConnectionManager._ENV.close()
            DuckDBConnectionManager._ENV = None


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_ledger.db")
    monkeypatch.setenv("DUCKDB_PATH", db_path)
    yield db_path


def run_day(db_path: str, batch_date: str) -> None:
    """One full pipeline run for a date, writing the same ledger entries the DAG does."""
    row_count = ingest_raw_transactions(db_path, batch_date)
    record_event(db_path, run_id=f"test__{batch_date}", dag_id="transaction_pipeline",
                 task_id="ingest_raw_data", batch_date=batch_date, event_type="ingest",
                 row_count=row_count, payload={"partition_replaced": batch_date})

    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": batch_date})
    record_event(db_path, run_id=f"test__{batch_date}", dag_id="transaction_pipeline",
                 task_id="dbt_run", batch_date=batch_date, event_type="gold_snapshot",
                 payload=snapshot_gold(db_path, batch_date))

    run_dbt("test", DBT_PROJECT_DIR, DBT_PROJECT_DIR)
    record_event(db_path, run_id=f"test__{batch_date}", dag_id="transaction_pipeline",
                 task_id="dbt_test", batch_date=batch_date, event_type="quality_gate_passed",
                 payload={"outcome": "pass"})


@pytest.fixture
def three_day_warehouse(isolated_db):
    for batch_date in BACKFILL_DAYS:
        run_day(isolated_db, batch_date)
    return isolated_db


def run_cli(db_path: str) -> subprocess.CompletedProcess:
    """Invokes the real verify-chain CLI as a subprocess.

    dbt's adapter keeps a connection open after `dbt run`, and DuckDB is
    single-writer: the subprocess cannot open the file until this process lets
    go. An operator running verify-chain from a shell never hits this, but the
    tests run both in one session, so release the adapter first.
    """
    release_dbt_connection()
    return subprocess.run(
        [sys.executable, str(VERIFY_CLI), "--db", db_path],
        capture_output=True, text=True, check=False,
        env={**os.environ, "DUCKDB_PATH": db_path},
    )


def test_a_clean_three_day_run_verifies(three_day_warehouse):
    assert verify_chain(three_day_warehouse) == len(BACKFILL_DAYS) * 3
    assert verify_gold_against_ledger(three_day_warehouse) == []

    result = run_cli(three_day_warehouse)
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_mutating_a_historical_gold_row_is_detected(three_day_warehouse):
    """The demo: edit one gold row from the middle day, quietly and directly."""
    target_day = BACKFILL_DAYS[1]

    conn = duckdb.connect(three_day_warehouse)
    account = conn.execute(
        "SELECT account_id FROM gold.fct_account_risk_metrics WHERE batch_date = ? "
        "ORDER BY account_id LIMIT 1;", [target_day]
    ).fetchone()[0]
    conn.execute(
        "UPDATE gold.fct_account_risk_metrics SET net_balance = net_balance + 1000000 "
        "WHERE batch_date = ? AND account_id = ?;", [target_day, account]
    )
    conn.close()

    # The ledger itself was not touched, so the chain is still internally valid...
    assert verify_chain(three_day_warehouse) == len(BACKFILL_DAYS) * 3

    # ...but the warehouse no longer matches what the ledger committed to.
    mismatches = verify_gold_against_ledger(three_day_warehouse)
    assert len(mismatches) == 1
    assert target_day in mismatches[0]

    result = run_cli(three_day_warehouse)
    assert result.returncode == 1
    assert "FAILED" in result.stderr
    assert target_day in result.stderr, "the verifier must name the day that was edited"


def test_editing_a_ledger_row_breaks_the_chain(three_day_warehouse):
    conn = duckdb.connect(three_day_warehouse)
    conn.execute("UPDATE audit.pipeline_events SET row_count = 999999 WHERE seq = 1;")
    conn.close()

    with pytest.raises(ChainBreak) as exc:
        verify_chain(three_day_warehouse)
    assert "seq 1" in str(exc.value)
    assert "modified" in str(exc.value)

    assert run_cli(three_day_warehouse).returncode == 1


def test_deleting_a_ledger_row_breaks_the_chain(three_day_warehouse):
    """Deleting history must not look like it never happened."""
    conn = duckdb.connect(three_day_warehouse)
    conn.execute("DELETE FROM audit.pipeline_events WHERE seq = 4;")
    conn.close()

    with pytest.raises(ChainBreak) as exc:
        verify_chain(three_day_warehouse)
    assert "deleted" in str(exc.value)

    assert run_cli(three_day_warehouse).returncode == 1


def test_reparenting_an_entry_breaks_the_chain(three_day_warehouse):
    """Recomputing one row's own hash isn't enough - it must still chain."""
    conn = duckdb.connect(three_day_warehouse)
    conn.execute(
        "UPDATE audit.pipeline_events SET prev_hash = ? WHERE seq = 5;",
        ["f" * 64],
    )
    conn.close()

    with pytest.raises(ChainBreak) as exc:
        verify_chain(three_day_warehouse)
    assert "seq 5" in str(exc.value)


def test_rerunning_a_day_appends_rather_than_rewrites(three_day_warehouse):
    """Re-running a date replaces its data by design; the ledger must still
    show both runs, so the replacement is on the record."""
    before = verify_chain(three_day_warehouse)

    run_day(three_day_warehouse, BACKFILL_DAYS[0])

    after = verify_chain(three_day_warehouse)
    assert after == before + 3, "the re-run must append, not overwrite"
    assert verify_gold_against_ledger(three_day_warehouse) == []

    conn = duckdb.connect(three_day_warehouse)
    ingests = conn.execute(
        "SELECT COUNT(*) FROM audit.pipeline_events WHERE event_type = 'ingest' "
        "AND batch_date = ?;", [BACKFILL_DAYS[0]]
    ).fetchone()[0]
    conn.close()
    assert ingests == 2, "both the original run and the re-run must be recorded"


def run_dbt_subprocess(db_path: str, batch_date: str) -> subprocess.CompletedProcess:
    """Invokes `dbt run` as a genuine separate process.

    `run_dbt()` (used by run_day() above) calls dbt's Python API in-process,
    sharing this test session's process and thread pool across "runs" - it
    cannot reproduce cross-process variance. This is the only way to exercise
    the same boundary a real pipeline run crosses every time Airflow spawns
    one, which is what test_gold_is_deterministic_across_separate_processes
    below needs.
    """
    release_dbt_connection()
    return subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
         "--vars", f'{{"batch_date": "{batch_date}"}}'],
        capture_output=True, text=True, check=False,
        env={**os.environ, "DUCKDB_PATH": db_path},
    )


def test_gold_is_deterministic_across_separate_dbt_processes(isolated_db):
    """Regression test for a real incident, with an important caveat: two
    genuine `dbt run` processes, fed identical deterministic input (same
    seeded ingestion, same batch_date), produced different gold hashes five
    days apart. The leading theory is DuckDB's internal SUM/AVG parallelism -
    float addition isn't associative, so the order partial sums combine in
    isn't guaranteed stable run to run - which dbt_project.yml now guards
    against with `PRAGMA threads=1` via on-run-start.

    This test does NOT confirm that theory: it passed 5/5 times even with
    that pragma removed, on an 8-core machine, most likely because 100 rows
    is too small for DuckDB to bother parallelizing. It exists as insurance
    (the pragma is cheap and harmless either way) and as the one place that
    exercises two real separate dbt processes rather than one in-process
    Python session - run_day() above shares a single DuckDB thread pool
    across "runs" and structurally cannot catch this class of bug, regardless
    of whether the underlying cause is confirmed.
    """
    batch_date = "2026-02-01"
    ingest_raw_transactions(isolated_db, batch_date)

    first = run_dbt_subprocess(isolated_db, batch_date)
    assert first.returncode == 0, first.stdout + first.stderr
    first_hash = compute_payload_hash(snapshot_gold(isolated_db, batch_date))

    second = run_dbt_subprocess(isolated_db, batch_date)
    assert second.returncode == 0, second.stdout + second.stderr
    second_hash = compute_payload_hash(snapshot_gold(isolated_db, batch_date))

    assert first_hash == second_hash, (
        "the same deterministic input produced different gold output across two "
        "separate dbt processes - aggregation is not actually deterministic"
    )


def test_payload_hash_is_order_independent_but_content_sensitive():
    assert compute_payload_hash({"a": 1, "b": 2}) == compute_payload_hash({"b": 2, "a": 1})
    assert compute_payload_hash({"a": 1}) != compute_payload_hash({"a": 2})


def test_verify_cli_reports_missing_warehouse(tmp_path):
    result = run_cli(str(tmp_path / "nonexistent.db"))
    assert result.returncode == 2

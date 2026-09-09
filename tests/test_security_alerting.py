# tests/test_security_alerting.py
"""Proves C-06: a failed quality gate raises an alert and leaves a durable record.

A log line that rotates away is not evidence. The failure handler has to write
to the audit ledger, so that afterwards you can show the gate ran and what it
decided - the difference between being right and being able to demonstrate it.

The handler lives in audit_ledger rather than the DAG file precisely so it can
be tested here, without installing Airflow.
"""
import logging
from pathlib import Path

import duckdb
import pytest

from audit_ledger import record_task_failure, verify_chain

REPO_ROOT = Path(__file__).resolve().parent.parent
DAG_FILE = REPO_ROOT / "airflow" / "dags" / "transaction_pipeline.py"

log = logging.getLogger("test_alerting")


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_alerting.db")


def test_failure_writes_a_durable_ledger_entry(db_path):
    assert record_task_failure(
        db_path, run_id="manual__2026-01-15", dag_id="transaction_pipeline",
        task_id="dbt_test", try_number=3, logger=log,
    )

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT run_id, task_id, event_type FROM audit.pipeline_events "
        "WHERE event_type = 'task_failed';"
    ).fetchall()
    conn.close()

    assert rows == [("manual__2026-01-15", "dbt_test", "task_failed")]


def test_failure_alerts_with_run_context(db_path, caplog):
    """An alert with no context is a page nobody can act on."""
    with caplog.at_level(logging.ERROR):
        record_task_failure(
            db_path, run_id="manual__2026-01-15", dag_id="transaction_pipeline",
            task_id="dbt_test", logger=log,
        )

    assert "ALERT" in caplog.text
    assert "dbt_test" in caplog.text
    assert "transaction_pipeline" in caplog.text
    assert "manual__2026-01-15" in caplog.text


def test_failure_entries_extend_the_same_verifiable_chain(db_path):
    for i in range(3):
        record_task_failure(db_path, run_id=f"run-{i}", dag_id="transaction_pipeline",
                            task_id="dbt_test", logger=log)

    assert verify_chain(db_path) == 3


def test_a_broken_ledger_does_not_mask_the_original_failure(caplog):
    """If the ledger write fails, the handler must still alert and return
    cleanly rather than raising and burying what it was called to report."""
    with caplog.at_level(logging.ERROR):
        recorded = record_task_failure(
            "/nonexistent-directory/cannot-write.db",
            run_id="run-1", dag_id="transaction_pipeline", task_id="dbt_test", logger=log,
        )

    assert recorded is False
    assert "ALERT" in caplog.text, "the alert must fire even when the ledger write fails"


def test_handler_works_without_a_logger(db_path):
    """Airflow supplies one, but the function must not require it."""
    assert record_task_failure(db_path, run_id="r", dag_id="d", task_id="t")


def test_the_dag_registers_the_hook_on_every_task():
    """A callback nothing calls is not a control."""
    source = DAG_FILE.read_text()
    assert "alert_on_failure" in source
    assert "record_task_failure" in source, "the DAG hook must reach the durable handler"

    # In default_args, so it applies to every task rather than one.
    default_args_block = source.split("default_args = {", 1)[1].split("}", 1)[0]
    assert "on_failure_callback" in default_args_block

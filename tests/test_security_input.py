# tests/test_security_input.py
"""Proves C-04: untrusted parameters cannot reach templated SQL.

batch_date is the pipeline's untrusted input - an operator can set it via
dag_run.conf on a manual backfill - and it is interpolated into the gold
model's incremental filter. These tests assert it is rejected at the boundary,
and that a rejected run leaves the warehouse untouched.
"""
import os
from datetime import UTC, datetime

import duckdb
import pytest

from pipeline_tasks import (
    InvalidBatchDate,
    ingest_raw_transactions,
    run_dbt,
    validate_batch_date,
)

DBT_PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dbt_project'))

# Values a caller could realistically supply: SQL fragments that would change
# the meaning of the incremental filter, plus shapes that aren't dates at all.
MALICIOUS_BATCH_DATES = [
    "2026-01-15' OR '1'='1",
    "2026-01-15'; DROP TABLE gold.fct_account_risk_metrics; --",
    "2026-01-15' UNION SELECT * FROM bronze.raw_transactions --",
    "' OR 1=1 --",
    "2026-13-45",          # date-shaped, not a real date
    "2026/01/15",          # wrong separator
    "not-a-date",
    "",
    "   ",
]


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_security.db")
    monkeypatch.setenv("DUCKDB_PATH", db_path)
    yield db_path


@pytest.mark.parametrize("bad_value", MALICIOUS_BATCH_DATES)
def test_validate_batch_date_rejects_crafted_input(bad_value):
    with pytest.raises(InvalidBatchDate):
        validate_batch_date(bad_value)


@pytest.mark.parametrize("bad_value", [None, 20260115, 3.14, ["2026-01-15"], {"d": "2026-01-15"}])
def test_validate_batch_date_rejects_non_string_types(bad_value):
    with pytest.raises(InvalidBatchDate):
        validate_batch_date(bad_value)


def test_validate_batch_date_accepts_real_dates():
    assert validate_batch_date("2026-01-15") == "2026-01-15"
    assert validate_batch_date("  2026-01-15  ") == "2026-01-15"
    assert validate_batch_date(datetime(2026, 1, 15, 13, 45, tzinfo=UTC)) == "2026-01-15"


def test_crafted_batch_date_is_rejected_before_dbt_runs(isolated_db):
    """The gate from milestone 3: a crafted batch_date must be rejected, and
    gold must be byte-identical afterwards."""
    ingest_raw_transactions(isolated_db, "2026-01-15")
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": "2026-01-15"})

    conn = duckdb.connect(isolated_db)
    gold_before = conn.execute(
        "SELECT * FROM gold.fct_account_risk_metrics ORDER BY account_id, batch_date;"
    ).fetchall()
    conn.close()
    assert gold_before, "fixture should have produced gold rows to compare against"

    injection = "2026-01-15'; DROP TABLE gold.fct_account_risk_metrics; --"
    with pytest.raises(InvalidBatchDate):
        run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": injection})

    conn = duckdb.connect(isolated_db)
    gold_after = conn.execute(
        "SELECT * FROM gold.fct_account_risk_metrics ORDER BY account_id, batch_date;"
    ).fetchall()
    conn.close()

    assert gold_after == gold_before, "a rejected run must not modify the warehouse"


def test_valid_batch_date_still_compiles_in_incremental_mode(isolated_db):
    """Guards the guard: a too-strict shape check would reject every date and
    the rejection tests above would still pass. This asserts the happy path
    survives, specifically on the second (incremental) run where the Jinja
    branch containing the check is actually taken."""
    ingest_raw_transactions(isolated_db, "2026-01-15")
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": "2026-01-15"})

    ingest_raw_transactions(isolated_db, "2026-01-16")
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": "2026-01-16"})

    conn = duckdb.connect(isolated_db)
    days = conn.execute(
        "SELECT COUNT(DISTINCT batch_date) FROM gold.fct_account_risk_metrics;"
    ).fetchone()[0]
    conn.close()

    assert days == 2, "both days should be present after an incremental second run"


def test_model_rejects_crafted_var_even_if_validation_is_bypassed(isolated_db):
    """Defence in depth: the model's own Jinja must refuse a bad batch_date
    even when the value skips pipeline_tasks' validation entirely."""
    import json

    from dbt.cli.main import dbtRunner

    ingest_raw_transactions(isolated_db, "2026-01-15")
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": "2026-01-15"})

    # Call dbt directly, bypassing run_dbt's validation, the way a hand-run
    # `dbt run --vars` from a shell would.
    injection = "2026-01-15' OR '1'='1"
    result = dbtRunner().invoke([
        "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
        "--vars", json.dumps({"batch_date": injection}),
    ])

    assert not result.success, "the model must refuse to compile with a crafted batch_date"

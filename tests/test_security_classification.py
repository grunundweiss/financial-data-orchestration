# tests/test_security_classification.py
"""Proves C-01 (identifier columns are classified) and C-02 (identifiers are
tokenized before they reach Bronze).

C-02's test is the important one: it scans every text column in silver and
gold for the raw account pattern the source feed produces, and fails on a hit.
That is what would catch a future model that joins the vault back in, or an
ingestion change that forgets to tokenize.
"""
import re
from pathlib import Path

import duckdb
import pytest
import yaml

from pipeline_tasks import ingest_raw_transactions, run_dbt
from tokenization import MissingPepper, resolve_token, tokenize_account

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = str(REPO_ROOT / "dbt_project")
SCHEMA_YML = REPO_ROOT / "dbt_project" / "models" / "schema.yml"

# The shape the mock source feed emits: ACC-NOR-1234.
RAW_ACCOUNT_PATTERN = re.compile(r"ACC-NOR-\d{4}")

# Columns whose name implies they carry an identifier and therefore must be
# classified. Matched case-insensitively against declared column names.
IDENTIFIER_NAME_HINTS = ("account", "_id", "customer", "iban", "msisdn")


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    monkeypatch.setenv("ACCOUNT_TOKEN_PEPPER", "test-pepper-value-long-enough-to-pass")
    db_path = str(tmp_path / "test_classification.db")
    monkeypatch.setenv("DUCKDB_PATH", db_path)

    ingest_raw_transactions(db_path, "2026-01-15")
    run_dbt("run", DBT_PROJECT_DIR, DBT_PROJECT_DIR, dbt_vars={"batch_date": "2026-01-15"})
    return db_path


def declared_columns():
    schema = yaml.safe_load(SCHEMA_YML.read_text())
    for model in schema.get("models", []):
        for column in model.get("columns", []) or []:
            yield model["name"], column


# --- C-01 -------------------------------------------------------------------

def test_every_identifier_column_is_classified():
    unclassified = []
    for model_name, column in declared_columns():
        name = column["name"].lower()
        if not any(hint in name for hint in IDENTIFIER_NAME_HINTS):
            continue
        classification = (column.get("meta") or {}).get("classification")
        if not classification:
            unclassified.append(f"{model_name}.{column['name']}")

    assert not unclassified, (
        "identifier columns missing a meta.classification tag: " + ", ".join(unclassified)
    )


def test_account_columns_are_marked_tokenized():
    for model_name, column in declared_columns():
        if column["name"].lower() != "account_id":
            continue
        meta = column.get("meta") or {}
        assert meta.get("pii") is True, f"{model_name}.account_id must be marked pii"
        assert meta.get("tokenized") is True, f"{model_name}.account_id must be marked tokenized"


# --- C-02 -------------------------------------------------------------------

def scan_schema_for_raw_accounts(conn, schema: str) -> list[str]:
    """Returns 'table.column' for every text column holding a raw account id."""
    columns = conn.execute(
        """
        SELECT table_name, column_name FROM information_schema.columns
        WHERE LOWER(table_schema) = ? AND data_type IN ('VARCHAR', 'TEXT');
        """,
        [schema],
    ).fetchall()

    hits = []
    for table, column in columns:
        matched = conn.execute(
            f'SELECT COUNT(*) FROM {schema}."{table}" '
            f'WHERE regexp_matches(CAST("{column}" AS VARCHAR), ?);',
            [RAW_ACCOUNT_PATTERN.pattern],
        ).fetchone()[0]
        if matched:
            hits.append(f"{schema}.{table}.{column} ({matched} rows)")
    return hits


@pytest.mark.parametrize("schema", ["bronze", "silver", "gold"])
def test_no_raw_account_identifiers_reach_the_warehouse(warehouse, schema):
    """The gate from milestone 4. Bronze is included deliberately: the whole
    point of tokenizing at the boundary is that raw ids never land at all."""
    conn = duckdb.connect(warehouse)
    try:
        hits = scan_schema_for_raw_accounts(conn, schema)
    finally:
        conn.close()

    assert not hits, f"raw account identifiers found in {schema}: {hits}"


def test_the_scan_would_actually_catch_a_leak(warehouse):
    """Guards the guard: a scan that never fires proves nothing."""
    conn = duckdb.connect(warehouse)
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    conn.execute("CREATE TABLE silver.leaky AS SELECT 'ACC-NOR-4242' AS account_id;")
    try:
        hits = scan_schema_for_raw_accounts(conn, "silver")
    finally:
        conn.execute("DROP TABLE silver.leaky;")
        conn.close()

    assert any("leaky" in hit for hit in hits), "the scan failed to detect a planted raw identifier"


def test_tokens_are_deterministic_and_keyed():
    pepper_a = b"pepper-one-long-enough"
    pepper_b = b"pepper-two-long-enough"

    assert tokenize_account("ACC-NOR-1234", pepper_a) == tokenize_account("ACC-NOR-1234", pepper_a)
    assert tokenize_account("ACC-NOR-1234", pepper_a) != tokenize_account("ACC-NOR-9999", pepper_a)
    # A different key must produce a different token, or the pepper is decorative.
    assert tokenize_account("ACC-NOR-1234", pepper_a) != tokenize_account("ACC-NOR-1234", pepper_b)


def test_token_does_not_contain_the_source_identifier():
    token = tokenize_account("ACC-NOR-1234", b"pepper-long-enough-value")
    assert "1234" not in token
    assert "ACC-NOR" not in token


def test_tokenization_fails_closed_without_a_pepper(monkeypatch):
    """No default pepper: a shipped one would make every token reversible."""
    monkeypatch.delenv("ACCOUNT_TOKEN_PEPPER", raising=False)
    with pytest.raises(MissingPepper):
        tokenize_account("ACC-NOR-1234")

    monkeypatch.setenv("ACCOUNT_TOKEN_PEPPER", "short")
    with pytest.raises(MissingPepper):
        tokenize_account("ACC-NOR-1234")


def test_reidentification_requires_the_vault(warehouse):
    """The mapping exists, but only in one place, reachable by one function."""
    conn = duckdb.connect(warehouse)
    token, account_id = conn.execute(
        "SELECT token, account_id FROM vault.account_tokens LIMIT 1;"
    ).fetchone()
    conn.close()

    assert RAW_ACCOUNT_PATTERN.match(account_id), "vault should hold the real identifier"
    assert resolve_token(warehouse, token) == account_id
    assert resolve_token(warehouse, "ACT-does-not-exist") is None

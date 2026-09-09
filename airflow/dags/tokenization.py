# airflow/dags/tokenization.py
"""Turns account identifiers into deterministic tokens at the Bronze boundary.

The warehouse is a DuckDB file on a host bind mount with no encryption at rest
and no column-level access control: anyone who can read the folder can read
every account's full transaction history. Tokenizing at ingestion means the
identifier that lands in the file is not the identifier the source system uses.

HMAC-SHA256 with a keyed pepper, rather than a bare hash, because the account
space here is small and enumerable - a plain SHA-256 of "ACC-NOR-1234" is
reversible by anyone willing to hash ten thousand candidates. The pepper is
key material: it lives in .env, never in the repository, and rotating it
invalidates every existing token.

Deterministic on purpose. The gold model groups by account and a dbt
relationships test joins gold back to silver, so the same input must always
produce the same token. That is a real trade-off, written down in
docs/threat-model.md: determinism is what lets an attacker who can submit
known identifiers confirm their presence in the warehouse.

Kept free of Airflow imports so it can be unit-tested without installing
apache-airflow, matching pipeline_tasks.py.
"""
from __future__ import annotations

import hashlib
import hmac
import os

import duckdb

TOKEN_PREFIX = "ACT"
_PEPPER_VAR = "ACCOUNT_TOKEN_PEPPER"


class MissingPepper(RuntimeError):
    """Raised when the tokenization key is absent, rather than falling back to none."""


def get_pepper() -> bytes:
    """Reads the keyed pepper, failing closed if it is missing or trivial.

    There is deliberately no default. A default pepper would be published in
    this repository, which makes every token in the warehouse reversible by
    anyone who can read the source - the failure would be silent and total.
    """
    pepper = os.environ.get(_PEPPER_VAR, "")
    if not pepper:
        raise MissingPepper(
            f"{_PEPPER_VAR} is not set. Generate one with "
            f"`python security/generate_env.py`; there is no default because a "
            f"shipped default would make every token reversible."
        )
    if len(pepper) < 16:
        raise MissingPepper(f"{_PEPPER_VAR} is too short to be useful key material")
    return pepper.encode()


def tokenize_account(account_id: str, pepper: bytes | None = None) -> str:
    """Returns the stable token for an account identifier."""
    key = pepper if pepper is not None else get_pepper()
    digest = hmac.new(key, account_id.encode(), hashlib.sha256).hexdigest()
    return f"{TOKEN_PREFIX}-{digest[:32]}"


def ensure_vault(conn: duckdb.DuckDBPyConnection) -> None:
    """Creates the token vault.

    Re-identification is a privileged operation against this table, not a join
    available to every query that touches the warehouse. Keeping the mapping
    here - rather than carrying the raw identifier alongside the token in
    Bronze - is what makes that distinction real.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS vault;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vault.account_tokens (
            token TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            first_seen_batch_date DATE NOT NULL
        );
    """)


def register_tokens(
    conn: duckdb.DuckDBPyConnection,
    account_ids: list[str],
    batch_date: str,
    pepper: bytes | None = None,
) -> dict[str, str]:
    """Tokenizes each account, records unseen ones in the vault, returns the mapping."""
    ensure_vault(conn)
    key = pepper if pepper is not None else get_pepper()

    mapping = {account_id: tokenize_account(account_id, key) for account_id in set(account_ids)}

    for account_id, token in sorted(mapping.items()):
        # Deterministic tokens mean a re-run produces the same rows; ignoring
        # the conflict keeps ingestion idempotent.
        conn.execute(
            """
            INSERT INTO vault.account_tokens (token, account_id, first_seen_batch_date)
            VALUES (?, ?, ?) ON CONFLICT (token) DO NOTHING;
            """,
            [token, account_id, batch_date],
        )

    return mapping


def resolve_token(db_path: str, token: str) -> str | None:
    """Re-identifies a single token. The privileged operation, isolated in one place."""
    conn = duckdb.connect(db_path)
    try:
        row = conn.execute(
            "SELECT account_id FROM vault.account_tokens WHERE token = ?;", [token]
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None

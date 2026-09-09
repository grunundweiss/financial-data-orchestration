# airflow/dags/audit_ledger.py
"""Append-only, hash-chained record of what each pipeline run did.

Ingestion is DELETE-then-INSERT scoped to a partition, and the gold model is
incremental with delete+insert. That is the correct idempotency design and
shouldn't change - but it means any re-run silently replaces a day, and
nothing records that it happened, what the numbers were before, or who asked
for it. The quality gate has the same problem in reverse: it runs, it passes,
and the evidence evaporates.

Each row here carries a hash over its own content plus the previous row's
hash, so the sequence can be verified later. Editing or deleting history
breaks the chain at the point of the edit, which `verify_chain` reports.

Kept free of Airflow imports so it can be unit-tested without installing
apache-airflow, matching pipeline_tasks.py.
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import duckdb

GENESIS_HASH = "0" * 64

# Columns that make up a row's identity, in the exact order they are hashed.
# Changing this order or set invalidates every existing chain.
_HASHED_FIELDS = (
    "seq",
    "run_id",
    "dag_id",
    "task_id",
    "batch_date",
    "event_type",
    "row_count",
    "payload_hash",
    "recorded_at",
    "prev_hash",
)


def compute_payload_hash(payload: dict[str, Any]) -> str:
    """Hashes an arbitrary payload dict deterministically."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def compute_entry_hash(entry: dict[str, Any]) -> str:
    """Hashes a ledger row over its content and the previous row's hash."""
    parts = [str(entry[field]) for field in _HASHED_FIELDS]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def ensure_ledger(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit.pipeline_events (
            seq BIGINT PRIMARY KEY,
            run_id TEXT NOT NULL,
            dag_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            batch_date DATE,
            event_type TEXT NOT NULL,
            row_count BIGINT,
            payload_hash TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            prev_hash TEXT NOT NULL,
            entry_hash TEXT NOT NULL
        );
    """)


def record_event(
    db_path: str,
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    batch_date: str | None,
    event_type: str,
    row_count: int | None = None,
    payload: dict[str, Any] | None = None,
) -> str:
    """Appends one event and returns its entry hash (the new chain head)."""
    conn = duckdb.connect(db_path)
    try:
        ensure_ledger(conn)

        head = conn.execute(
            "SELECT seq, entry_hash FROM audit.pipeline_events ORDER BY seq DESC LIMIT 1;"
        ).fetchone()
        seq = (head[0] + 1) if head else 1
        prev_hash = head[1] if head else GENESIS_HASH

        entry = {
            "seq": seq,
            "run_id": run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "batch_date": batch_date,
            "event_type": event_type,
            "row_count": row_count,
            "payload_hash": compute_payload_hash(payload or {}),
            # Second resolution: the timestamp is part of the hashed identity,
            # so it must survive the round-trip through DuckDB's TIMESTAMP
            # unchanged or verification would fail on formatting alone.
            "recorded_at": datetime.now(UTC).replace(microsecond=0, tzinfo=None).isoformat(sep=" "),
            "prev_hash": prev_hash,
        }
        entry["entry_hash"] = compute_entry_hash(entry)

        conn.execute(
            """
            INSERT INTO audit.pipeline_events
                (seq, run_id, dag_id, task_id, batch_date, event_type,
                 row_count, payload_hash, recorded_at, prev_hash, entry_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                entry["seq"], entry["run_id"], entry["dag_id"], entry["task_id"],
                entry["batch_date"], entry["event_type"], entry["row_count"],
                entry["payload_hash"], entry["recorded_at"], entry["prev_hash"],
                entry["entry_hash"],
            ],
        )
        return str(entry["entry_hash"])
    finally:
        conn.close()


def record_task_failure(
    db_path: str,
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    try_number: int | None = None,
    logger: Any = None,
) -> bool:
    """Alerts on a failed task and records it durably. Returns whether it recorded.

    Lives here rather than in the DAG so it is testable without installing
    Airflow, matching how pipeline_tasks.py is kept importable. The DAG's
    on_failure_callback is a thin wrapper that unpacks Airflow's context and
    calls this.

    Never raises: a broken ledger must not mask the failure it was called to
    report.
    """
    if logger is not None:
        logger.error("ALERT: task %s in dag %s failed on run %s", task_id, dag_id, run_id)

    try:
        record_event(
            db_path,
            run_id=run_id,
            dag_id=dag_id,
            task_id=task_id,
            batch_date=None,
            event_type="task_failed",
            payload={"try_number": try_number},
        )
        return True
    except Exception:
        if logger is not None:
            logger.exception("could not write the failure to the audit ledger")
        return False


def snapshot_gold(db_path: str, batch_date: str) -> dict[str, Any]:
    """Summarises a day's gold rows so the ledger commits to their content.

    The ledger stores a hash of this, not the rows themselves: enough to prove
    a later edit happened, without duplicating the warehouse.
    """
    conn = duckdb.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT account_id, batch_date, total_transactions, net_balance,
                   high_value_count, average_transaction_size
            FROM gold.fct_account_risk_metrics
            WHERE batch_date = ?
            ORDER BY account_id;
            """,
            [batch_date],
        ).fetchall()
    finally:
        conn.close()

    return {"batch_date": batch_date, "rows": [[str(c) for c in row] for row in rows]}


class ChainBreak(Exception):
    """Raised with a human-readable account of the first inconsistency found."""


def verify_chain(db_path: str) -> int:
    """Walks the ledger and returns the number of verified entries.

    Raises ChainBreak naming the first row that fails, whether it was edited,
    deleted, reordered, or re-parented.
    """
    conn = duckdb.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT seq, run_id, dag_id, task_id, batch_date, event_type,
                   row_count, payload_hash, recorded_at, prev_hash, entry_hash
            FROM audit.pipeline_events ORDER BY seq;
            """
        ).fetchall()
    finally:
        conn.close()

    expected_prev = GENESIS_HASH
    expected_seq = 1

    for row in rows:
        entry = dict(zip(_HASHED_FIELDS + ("entry_hash",), row, strict=True))
        seq = entry["seq"]

        if seq != expected_seq:
            raise ChainBreak(
                f"ledger seq {seq}: expected seq {expected_seq} - "
                f"{'an entry was deleted' if seq > expected_seq else 'entries are out of order'}"
            )

        if entry["prev_hash"] != expected_prev:
            raise ChainBreak(
                f"ledger seq {seq} (task {entry['task_id']}, batch {entry['batch_date']}): "
                f"prev_hash does not match the previous entry - history was re-parented"
            )

        # DuckDB hands timestamps back as datetime; re-render to the stored form.
        recorded_at = entry["recorded_at"]
        if isinstance(recorded_at, datetime):
            entry["recorded_at"] = recorded_at.isoformat(sep=" ")
        entry["batch_date"] = str(entry["batch_date"]) if entry["batch_date"] else entry["batch_date"]

        recomputed = compute_entry_hash(entry)
        if recomputed != entry["entry_hash"]:
            raise ChainBreak(
                f"ledger seq {seq} (task {entry['task_id']}, batch {entry['batch_date']}): "
                f"entry_hash does not match its contents - this row was modified after it was written"
            )

        expected_prev = entry["entry_hash"]
        expected_seq += 1

    return len(rows)


def verify_gold_against_ledger(db_path: str) -> list[str]:
    """Recomputes each batch_date's most recent gold snapshot and reports days
    that no longer match.

    The chain proves the ledger wasn't edited. This proves the *warehouse*
    still holds what the ledger last committed to - the case where someone
    edits a gold row directly and leaves the ledger untouched.

    Only the latest snapshot per batch_date is checked, not every one ever
    recorded: re-running a date is a supported, idempotent operation (see
    pipeline_tasks.py) that intentionally replaces that date's gold rows via
    delete+insert. A superseded snapshot going stale is that design working,
    not tampering - checking against it would make every legitimate re-run
    indistinguishable from an attack.
    """
    conn = duckdb.connect(db_path)
    try:
        recorded = conn.execute(
            """
            SELECT batch_date, payload_hash FROM audit.pipeline_events
            WHERE event_type = 'gold_snapshot'
            QUALIFY seq = MAX(seq) OVER (PARTITION BY batch_date)
            ORDER BY seq;
            """
        ).fetchall()
    finally:
        conn.close()

    mismatches = []
    for batch_date, payload_hash in recorded:
        current = compute_payload_hash(snapshot_gold(db_path, str(batch_date)))
        if current != payload_hash:
            mismatches.append(
                f"gold rows for {batch_date} no longer match the hash recorded when the "
                f"run completed (recorded {payload_hash[:12]}..., now {current[:12]}...)"
            )
    return mismatches

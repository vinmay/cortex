import sqlite3

import pytest

from cortex.storage.database import get_connection, init_db, transaction


def test_init_db_creates_tables(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = {row[0] for row in cursor.fetchall()}
    assert "events" in tables
    assert "memory_state" in tables
    assert "embedding_lookup" in tables


def test_wal_mode_enabled(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


def test_events_table_columns(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    cursor = conn.execute("PRAGMA table_info(events)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "sequence_number",
        "event_id",
        "event_type",
        "namespace",
        "entity_type",
        "entity_id",
        "payload",
        "actor_runtime",
        "actor_agent_id",
        "metadata",
        "parent_event_id",
        "idempotency_key",
        "payload_hash",
        "schema_version",
        "timestamp",
    }
    assert expected.issubset(columns)


def test_memory_state_table_columns(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    cursor = conn.execute("PRAGMA table_info(memory_state)")
    columns = {row[1] for row in cursor.fetchall()}
    expected = {
        "namespace",
        "entity_id",
        "entity_type",
        "payload",
        "actor_runtime",
        "actor_agent_id",
        "metadata",
        "last_event_id",
        "last_sequence_number",
        "timestamp",
        "is_retracted",
    }
    assert expected.issubset(columns)


def test_idempotency_unique_index(tmp_path):
    """Inserting same idempotency_key + namespace should fail."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    conn.execute(
        """INSERT INTO events
        (event_id, event_type, namespace, entity_type, entity_id,
         payload, actor_runtime, idempotency_key, payload_hash,
         schema_version, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("e1", "memory.created", "ns", "fact", "f1",
         '{"x":1}', "test", "key1", "hash1", 1, "2026-01-01T00:00:00.000000Z"),
    )

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO events
            (event_id, event_type, namespace, entity_type, entity_id,
             payload, actor_runtime, idempotency_key, payload_hash,
             schema_version, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("e2", "memory.created", "ns", "fact", "f2",
             '{"x":2}', "test", "key1", "hash2", 1, "2026-01-01T00:00:00.000000Z"),
        )


def test_init_db_idempotent(tmp_path):
    """Calling init_db twice should not fail."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_db(conn)  # Should not raise


def test_transaction_rolls_back_on_exception(tmp_path):
    """transaction() must roll back all writes if an exception is raised."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    with pytest.raises(RuntimeError, match="forced rollback"):
        with transaction(conn):
            conn.execute(
                """INSERT INTO events
                (event_id, event_type, namespace, entity_type, entity_id,
                 payload, actor_runtime, payload_hash, schema_version, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("e-rollback", "memory.created", "ns", "fact", "fact:1",
                 '{"x":1}', "test", "hash1", 1, "2026-01-01T00:00:00.000000Z"),
            )
            raise RuntimeError("forced rollback")

    # Row must not exist after rollback
    row = conn.execute(
        "SELECT 1 FROM events WHERE event_id = ?", ("e-rollback",)
    ).fetchone()
    assert row is None


def test_transaction_commits_on_success(tmp_path):
    """transaction() must commit all writes on successful exit."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    with transaction(conn):
        conn.execute(
            """INSERT INTO events
            (event_id, event_type, namespace, entity_type, entity_id,
             payload, actor_runtime, payload_hash, schema_version, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("e-commit", "memory.created", "ns", "fact", "fact:1",
             '{"x":1}', "test", "hash1", 1, "2026-01-01T00:00:00.000000Z"),
        )

    row = conn.execute(
        "SELECT event_id FROM events WHERE event_id = ?", ("e-commit",)
    ).fetchone()
    assert row is not None
    assert row[0] == "e-commit"


def test_foreign_key_constraint_enforced(tmp_path):
    """parent_event_id FK must be enforced (foreign_keys=ON)."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO events
            (event_id, event_type, namespace, entity_type, entity_id,
             payload, actor_runtime, payload_hash, schema_version, timestamp,
             parent_event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("e-child", "memory.updated", "ns", "fact", "fact:1",
             '{"x":2}', "test", "hash2", 1, "2026-01-01T00:00:01.000000Z",
             "non-existent-parent"),
        )

import sqlite3

from cortex.storage.database import init_db, get_connection


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
         '{}', "test", "key1", "hash1", 1, "2026-01-01T00:00:00Z"),
    )
    conn.commit()

    import pytest

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO events
            (event_id, event_type, namespace, entity_type, entity_id,
             payload, actor_runtime, idempotency_key, payload_hash,
             schema_version, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("e2", "memory.created", "ns", "fact", "f2",
             '{}', "test", "key1", "hash2", 1, "2026-01-01T00:00:00Z"),
        )


def test_init_db_idempotent(tmp_path):
    """Calling init_db twice should not fail."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_db(conn)  # Should not raise

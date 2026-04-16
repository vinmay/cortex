import sqlite3
from pathlib import Path

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    namespace TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    actor_runtime TEXT NOT NULL,
    actor_agent_id TEXT,
    metadata TEXT,
    parent_event_id TEXT,
    idempotency_key TEXT,
    payload_hash TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    timestamp TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotency
    ON events(idempotency_key, namespace)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_namespace_entity
    ON events(namespace, entity_id);

CREATE INDEX IF NOT EXISTS idx_events_namespace_type
    ON events(namespace, event_type);

CREATE INDEX IF NOT EXISTS idx_events_namespace_timestamp
    ON events(namespace, timestamp);

CREATE TABLE IF NOT EXISTS memory_state (
    namespace TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_type TEXT,
    payload TEXT NOT NULL DEFAULT '{}',
    actor_runtime TEXT NOT NULL,
    actor_agent_id TEXT,
    metadata TEXT,
    last_event_id TEXT NOT NULL,
    last_sequence_number INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    is_retracted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, entity_id)
);

CREATE TABLE IF NOT EXISTS embedding_lookup (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace TEXT NOT NULL,
    entity_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_embedding_lookup_namespace
    ON embedding_lookup(namespace);
"""


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA_SQL)

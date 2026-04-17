"""SQLite connection and schema management for Cortex.

Transaction ownership
---------------------
Repositories do **not** commit.  The service layer (Task 9) owns transaction
boundaries using the `transaction()` context manager provided here.  When no
explicit transaction is open (isolation_level=None), each individual statement
auto-commits — which is fine for repository-level tests in isolation.
"""
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator


# --- Schema DDL split per table for readability / diffability ---

_SQL_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    sequence_number INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    namespace       TEXT NOT NULL,
    entity_type     TEXT,
    entity_id       TEXT NOT NULL,
    payload         TEXT NOT NULL,
    actor_runtime   TEXT NOT NULL,
    actor_agent_id  TEXT,
    metadata        TEXT,
    parent_event_id TEXT REFERENCES events(event_id),
    idempotency_key TEXT,
    payload_hash    TEXT,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    timestamp       TEXT NOT NULL
);
"""

_SQL_EVENTS_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotency
    ON events(idempotency_key, namespace)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_namespace_entity
    ON events(namespace, entity_id);

CREATE INDEX IF NOT EXISTS idx_events_namespace_type
    ON events(namespace, event_type);

CREATE INDEX IF NOT EXISTS idx_events_namespace_timestamp
    ON events(namespace, timestamp);
"""

_SQL_MEMORY_STATE = """
CREATE TABLE IF NOT EXISTS memory_state (
    namespace            TEXT NOT NULL,
    entity_id            TEXT NOT NULL,
    entity_type          TEXT,
    payload              TEXT NOT NULL,
    actor_runtime        TEXT NOT NULL,
    actor_agent_id       TEXT,
    metadata             TEXT,
    last_event_id        TEXT NOT NULL,
    last_sequence_number INTEGER NOT NULL,
    timestamp            TEXT NOT NULL,
    is_retracted         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, entity_id)
);
"""

# M9: partial index — only active (non-retracted) rows for efficient queries
_SQL_MEMORY_STATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_memory_state_ns_type_ts
    ON memory_state(namespace, entity_type, timestamp DESC)
    WHERE is_retracted = 0;
"""

_SQL_EMBEDDING_LOOKUP = """
CREATE TABLE IF NOT EXISTS embedding_lookup (
    rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace TEXT NOT NULL,
    entity_id TEXT NOT NULL
);
"""

# M8: unique index so each entity has at most one embedding entry per namespace
_SQL_EMBEDDING_LOOKUP_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_lookup_ns_entity
    ON embedding_lookup(namespace, entity_id);

CREATE INDEX IF NOT EXISTS idx_embedding_lookup_namespace
    ON embedding_lookup(namespace);
"""

_SCHEMA_SQL = "".join([
    _SQL_EVENTS,
    _SQL_EVENTS_INDEXES,
    _SQL_MEMORY_STATE,
    _SQL_MEMORY_STATE_INDEXES,
    _SQL_EMBEDDING_LOOKUP,
    _SQL_EMBEDDING_LOOKUP_INDEXES,
])


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open (and configure) a SQLite connection.

    Pragmas applied:
    - journal_mode=WAL     — concurrent readers during write
    - foreign_keys=ON      — enforce parent_event_id FK
    - synchronous=NORMAL   — safe durability / good throughput balance
    - busy_timeout=5000    — wait up to 5 s before raising OperationalError

    isolation_level=None disables Python's implicit transaction management;
    the service layer controls transactions explicitly via `transaction()`.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # C3: disable implicit transaction management; service layer owns BEGIN/COMMIT
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # C4: additional pragmas
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't already exist."""
    conn.executescript(_SCHEMA_SQL)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator[None, None, None]:
    """Explicit transaction context manager.

    Usage (service layer)::

        with transaction(conn):
            journal_repo.insert_event(...)
            projection_repo.upsert(...)
        # committed — or rolled back on any exception

    Repositories must NOT call commit/rollback themselves.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

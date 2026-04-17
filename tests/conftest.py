import sqlite3
from pathlib import Path

import pytest

from cortex.storage.database import get_connection, init_db
from cortex.storage.journal import JournalRepository
from cortex.storage.projections import ProjectionRepository


@pytest.fixture
def db_conn(tmp_path) -> sqlite3.Connection:
    """Fresh SQLite database for each test."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def make_event(db_conn):
    """Factory fixture for inserting journal events.

    Returns a callable `_make(**overrides)` that inserts an event with sensible
    defaults, which can be selectively overridden.  Returns (result_dict, defaults_dict).
    """
    repo = JournalRepository(db_conn)
    counter = {"i": 0}

    def _make(**overrides):
        counter["i"] += 1
        i = counter["i"]
        defaults = dict(
            event_id=f"evt-{i}",
            namespace="test",
            timestamp=f"2026-04-13T10:{i:02d}:00.000000Z",
            actor_runtime="test",
            actor_agent_id=None,
            event_type="memory.created",
            entity_type="fact",
            entity_id=f"fact:{i}",
            payload='{"content":"x"}',
            metadata=None,
            parent_event_id=None,
            idempotency_key=None,
            payload_hash=f"hash-{i}",
            schema_version=1,
        )
        defaults.update(overrides)
        result = repo.insert_event(**defaults)
        return result, defaults

    return _make


@pytest.fixture
def make_upsert(db_conn):
    """Factory fixture for inserting projection rows.

    Returns a callable `_make(**overrides)` that upserts a projection with
    sensible defaults.  Returns the defaults dict (with overrides applied).
    """
    repo = ProjectionRepository(db_conn)
    counter = {"i": 0}

    def _make(**overrides):
        counter["i"] += 1
        i = counter["i"]
        defaults = dict(
            namespace="test",
            entity_id=f"fact:{i}",
            entity_type="fact",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            last_event_id=f"evt-{i}",
            last_sequence_number=i,
            timestamp=f"2026-04-13T10:{i:02d}:00.000000Z",
        )
        defaults.update(overrides)
        repo.upsert(**defaults)
        return defaults

    return _make

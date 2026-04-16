import sqlite3
from pathlib import Path

import pytest

from cortex.storage.database import get_connection, init_db


@pytest.fixture
def db_conn(tmp_path) -> sqlite3.Connection:
    """Fresh SQLite database for each test."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn
    conn.close()

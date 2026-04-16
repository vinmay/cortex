import sqlite3


class JournalRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def insert_event(
        self,
        *,
        event_id: str,
        event_type: str,
        namespace: str,
        entity_type: str | None,
        entity_id: str,
        payload: str,
        actor_runtime: str,
        actor_agent_id: str | None,
        metadata: str | None,
        parent_event_id: str | None,
        idempotency_key: str | None,
        payload_hash: str,
        schema_version: int,
        timestamp: str,
    ) -> dict:
        cursor = self._conn.execute(
            """INSERT INTO events
            (event_id, event_type, namespace, entity_type, entity_id,
             payload, actor_runtime, actor_agent_id, metadata,
             parent_event_id, idempotency_key, payload_hash,
             schema_version, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_id, event_type, namespace, entity_type, entity_id,
                payload, actor_runtime, actor_agent_id, metadata,
                parent_event_id, idempotency_key, payload_hash,
                schema_version, timestamp,
            ),
        )
        return {
            "sequence_number": cursor.lastrowid,
            "event_id": event_id,
            "timestamp": timestamp,
        }

    def find_by_idempotency_key(
        self, namespace: str, idempotency_key: str
    ) -> dict | None:
        cursor = self._conn.execute(
            """SELECT event_id, payload_hash, sequence_number, timestamp
            FROM events
            WHERE namespace = ? AND idempotency_key = ?""",
            (namespace, idempotency_key),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    def get_events_by_entity(
        self, namespace: str, entity_id: str
    ) -> list[dict]:
        cursor = self._conn.execute(
            """SELECT sequence_number, event_id, event_type, entity_type,
                      entity_id, payload, actor_runtime, actor_agent_id,
                      metadata, parent_event_id, schema_version, timestamp
            FROM events
            WHERE namespace = ? AND entity_id = ?
            ORDER BY sequence_number ASC""",
            (namespace, entity_id),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_all_events_in_namespace(self, namespace: str) -> list[dict]:
        cursor = self._conn.execute(
            """SELECT sequence_number, event_id, event_type, entity_type,
                      entity_id, payload, actor_runtime, actor_agent_id,
                      metadata, parent_event_id, schema_version, timestamp
            FROM events
            WHERE namespace = ?
            ORDER BY sequence_number ASC""",
            (namespace,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_event_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()
        return row[0]

    def get_namespace_count(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(DISTINCT namespace) FROM events"
        ).fetchone()
        return row[0]

    def get_last_write_timestamp(self) -> str | None:
        row = self._conn.execute(
            "SELECT timestamp FROM events ORDER BY sequence_number DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

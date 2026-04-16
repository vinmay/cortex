import sqlite3


class ProjectionRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def upsert(
        self,
        *,
        namespace: str,
        entity_id: str,
        entity_type: str | None,
        payload: str,
        actor_runtime: str,
        actor_agent_id: str | None,
        metadata: str | None,
        last_event_id: str,
        last_sequence_number: int,
        timestamp: str,
    ) -> None:
        self._conn.execute(
            """INSERT INTO memory_state
            (namespace, entity_id, entity_type, payload, actor_runtime,
             actor_agent_id, metadata, last_event_id, last_sequence_number,
             timestamp, is_retracted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(namespace, entity_id) DO UPDATE SET
                entity_type = excluded.entity_type,
                payload = excluded.payload,
                actor_runtime = excluded.actor_runtime,
                actor_agent_id = excluded.actor_agent_id,
                metadata = excluded.metadata,
                last_event_id = excluded.last_event_id,
                last_sequence_number = excluded.last_sequence_number,
                timestamp = excluded.timestamp,
                is_retracted = 0""",
            (
                namespace, entity_id, entity_type, payload,
                actor_runtime, actor_agent_id, metadata,
                last_event_id, last_sequence_number, timestamp,
            ),
        )

    def set_retracted(
        self,
        *,
        namespace: str,
        entity_id: str,
        last_event_id: str,
        last_sequence_number: int,
        timestamp: str,
    ) -> None:
        self._conn.execute(
            """UPDATE memory_state
            SET is_retracted = 1,
                last_event_id = ?,
                last_sequence_number = ?,
                timestamp = ?
            WHERE namespace = ? AND entity_id = ?""",
            (last_event_id, last_sequence_number, timestamp,
             namespace, entity_id),
        )

    def get_entity(self, namespace: str, entity_id: str) -> dict | None:
        cursor = self._conn.execute(
            "SELECT * FROM memory_state WHERE namespace = ? AND entity_id = ?",
            (namespace, entity_id),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def entity_exists(self, namespace: str, entity_id: str) -> bool:
        cursor = self._conn.execute(
            """SELECT 1 FROM memory_state
            WHERE namespace = ? AND entity_id = ?""",
            (namespace, entity_id),
        )
        return cursor.fetchone() is not None

    def query(
        self,
        *,
        namespace: str,
        entity_type: str | None = None,
        time_after: str | None = None,
        time_before: str | None = None,
        metadata_filter: dict[str, str] | None = None,
        limit: int = 10,
        entity_ids: list[str] | None = None,
    ) -> list[dict]:
        sql = "SELECT * FROM memory_state WHERE namespace = ? AND is_retracted = 0"
        params: list = [namespace]

        if entity_type is not None:
            sql += " AND entity_type = ?"
            params.append(entity_type)

        if time_after is not None:
            sql += " AND timestamp > ?"
            params.append(time_after)

        if time_before is not None:
            sql += " AND timestamp < ?"
            params.append(time_before)

        if metadata_filter:
            for key, value in metadata_filter.items():
                sql += " AND json_extract(metadata, ?) = ?"
                params.append(f"$.{key}")
                params.append(value)

        if entity_ids is not None:
            placeholders = ",".join("?" for _ in entity_ids)
            sql += f" AND entity_id IN ({placeholders})"
            params.extend(entity_ids)

        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor = self._conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_recent(self, namespace: str, limit: int = 10) -> list[dict]:
        return self.query(namespace=namespace, limit=limit)

    def delete_namespace(self, namespace: str) -> None:
        self._conn.execute(
            "DELETE FROM memory_state WHERE namespace = ?",
            (namespace,),
        )

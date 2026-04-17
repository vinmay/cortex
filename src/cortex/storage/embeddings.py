"""Embedding repository.

Does not commit. Service layer owns transactions via
cortex.storage.database.transaction().

``EmbeddingRepository`` instances cache the vector dimension locally in
``_dimension``.  If multiple instances share a connection, call
``init_vec_table`` on only one of them; the others will populate the cache via
schema introspection on the first ``get_dimension`` call.
"""

import math
import re
import sqlite3
import struct

import sqlite_vec


def _validate_embedding(vec: list[float]) -> None:
    if not vec:
        raise ValueError("embedding must not be empty")
    if not all(math.isfinite(v) for v in vec):
        raise ValueError("embedding must contain only finite floats")


def _serialize_float_vec(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


class EmbeddingRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        conn.enable_load_extension(True)
        try:
            sqlite_vec.load(conn)
        finally:
            conn.enable_load_extension(False)
        self._dimension: int | None = None

    def init_vec_table(self, dimension: int) -> None:
        if not (1 <= dimension <= 4096):
            raise ValueError(f"dimension must be in [1, 4096], got {dimension}")
        self._dimension = dimension
        self._conn.execute(
            f"CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings "
            f"USING vec0(embedding float[{dimension}])"
        )

    def get_dimension(self) -> int | None:
        if self._dimension is not None:
            return self._dimension
        cursor = self._conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type IN ('table','view') AND name='vec_embeddings'"
        )
        row = cursor.fetchone()
        if row is None:
            return None
        match = re.search(r"float\[(\d+)\]", row["sql"])
        if match:
            self._dimension = int(match.group(1))
        return self._dimension

    def insert(self, *, namespace: str, entity_id: str, embedding: list[float]) -> None:
        """Insert or replace the embedding for (namespace, entity_id).

        Raises ValueError if the embedding is empty, contains non-finite
        values, or does not match the table dimension.
        """
        _validate_embedding(embedding)
        dim = self.get_dimension()
        if dim is None:
            raise ValueError("init_vec_table not called")
        if len(embedding) != dim:
            raise ValueError(
                f"embedding dimension {len(embedding)} does not match table dimension {dim}"
            )
        blob = _serialize_float_vec(embedding)

        self._conn.execute("SAVEPOINT emb_insert")
        try:
            cursor = self._conn.execute(
                "SELECT rowid FROM embedding_lookup "
                "WHERE namespace = ? AND entity_id = ?",
                (namespace, entity_id),
            )
            existing = cursor.fetchone()
            if existing is not None:
                old_rowid = existing["rowid"]
                self._conn.execute(
                    "DELETE FROM vec_embeddings WHERE rowid = ?", (old_rowid,)
                )
                self._conn.execute(
                    "DELETE FROM embedding_lookup WHERE rowid = ?", (old_rowid,)
                )

            cursor = self._conn.execute(
                "INSERT INTO embedding_lookup (namespace, entity_id) VALUES (?, ?)",
                (namespace, entity_id),
            )
            rowid = cursor.lastrowid

            self._conn.execute(
                "INSERT INTO vec_embeddings (rowid, embedding) VALUES (?, ?)",
                (rowid, blob),
            )
            self._conn.execute("RELEASE SAVEPOINT emb_insert")
        except Exception:
            self._conn.execute("ROLLBACK TO SAVEPOINT emb_insert")
            self._conn.execute("RELEASE SAVEPOINT emb_insert")
            raise

    def search(self, *, namespace: str, embedding: list[float], limit: int = 10) -> list[dict]:
        """ANN search scoped to namespace. Returns list of {entity_id, distance} ordered by L2 distance ASC."""
        _validate_embedding(embedding)
        dim = self.get_dimension()
        if dim is not None and len(embedding) != dim:
            raise ValueError(
                f"embedding dimension {len(embedding)} does not match table dimension {dim}"
            )

        # Step 1: collect all rowids for this namespace
        cursor = self._conn.execute(
            "SELECT rowid, entity_id FROM embedding_lookup WHERE namespace = ?",
            (namespace,),
        )
        ns_rows = cursor.fetchall()
        if not ns_rows:
            return []
        rowid_to_entity = {row["rowid"]: row["entity_id"] for row in ns_rows}
        rowids = list(rowid_to_entity.keys())

        # Step 2: ANN search scoped to those rowids via sqlite-vec's rowid IN filter.
        # This avoids namespace starvation when sibling namespaces are large.
        blob = _serialize_float_vec(embedding)
        placeholders = ",".join("?" * len(rowids))
        sql = (
            f"SELECT rowid, distance FROM vec_embeddings "
            f"WHERE embedding MATCH ? AND k = ? AND rowid IN ({placeholders}) "
            f"ORDER BY distance ASC"
        )
        params = [blob, limit] + rowids
        cursor = self._conn.execute(sql, params)
        return [
            {"entity_id": rowid_to_entity[row["rowid"]], "distance": row["distance"]}
            for row in cursor.fetchall()
        ]

    def delete_namespace(self, *, namespace: str) -> None:
        """Delete all embeddings for a namespace."""
        self._conn.execute("SAVEPOINT emb_delete_ns")
        try:
            cursor = self._conn.execute(
                "SELECT rowid FROM embedding_lookup WHERE namespace = ?",
                (namespace,),
            )
            rowids = [row["rowid"] for row in cursor.fetchall()]
            for rowid in rowids:
                self._conn.execute(
                    "DELETE FROM vec_embeddings WHERE rowid = ?", (rowid,)
                )
            self._conn.execute(
                "DELETE FROM embedding_lookup WHERE namespace = ?",
                (namespace,),
            )
            self._conn.execute("RELEASE SAVEPOINT emb_delete_ns")
        except Exception:
            self._conn.execute("ROLLBACK TO SAVEPOINT emb_delete_ns")
            self._conn.execute("RELEASE SAVEPOINT emb_delete_ns")
            raise

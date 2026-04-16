import pytest
from pydantic import ValidationError
from cortex.core.models import (
    Actor,
    WriteMemoryInput,
    QueryMemoryInput,
    GetRecentInput,
    GetEntityInput,
    InspectHistoryInput,
    RebuildNamespaceInput,
    WriteMemoryOutput,
    MemoryResult,
    QueryMemoryOutput,
    HistoryEvent,
    InspectHistoryOutput,
    RebuildOutput,
    HealthOutput,
    VALID_EVENT_TYPES,
)


class TestWriteMemoryInput:
    def test_valid_memory_created(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.created",
            entity_type="fact",
            entity_id="fact:likes-coffee",
            payload={"content": "User likes coffee"},
            actor=Actor(runtime="claude-code"),
        )
        assert inp.namespace == "test"
        assert inp.event_type == "memory.created"

    def test_invalid_event_type_rejected(self):
        with pytest.raises(ValidationError, match="event_type"):
            WriteMemoryInput(
                namespace="test",
                event_type="invalid.type",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_updated_requires_parent_event_id(self):
        with pytest.raises(ValidationError, match="parent_event_id"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.updated",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_retracted_requires_parent_event_id(self):
        with pytest.raises(ValidationError, match="parent_event_id"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.retracted",
                entity_id="fact:x",
                payload={},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_created_requires_entity_type(self):
        with pytest.raises(ValidationError, match="entity_type"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_updated_with_parent_is_valid(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.updated",
            entity_type="fact",
            entity_id="fact:x",
            payload={"content": "updated"},
            actor=Actor(runtime="claude-code"),
            parent_event_id="abc-123",
        )
        assert inp.parent_event_id == "abc-123"

    def test_embedding_optional(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.created",
            entity_type="fact",
            entity_id="fact:x",
            payload={"content": "x"},
            actor=Actor(runtime="claude-code"),
            embedding=[0.1, 0.2, 0.3],
        )
        assert inp.embedding == [0.1, 0.2, 0.3]

    def test_idempotency_key_optional(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.created",
            entity_type="fact",
            entity_id="fact:x",
            payload={"content": "x"},
            actor=Actor(runtime="claude-code"),
            idempotency_key="my-key-1",
        )
        assert inp.idempotency_key == "my-key-1"

    def test_metadata_optional(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.created",
            entity_type="fact",
            entity_id="fact:x",
            payload={"content": "x"},
            actor=Actor(runtime="claude-code"),
            metadata={"source": "chat"},
        )
        assert inp.metadata == {"source": "chat"}

    def test_memory_linked_requires_target_in_payload(self):
        with pytest.raises(ValidationError, match="target_entity_id"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.linked",
                entity_type="link",
                entity_id="link:a-to-b",
                payload={"relation": "related_to"},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_linked_with_target_is_valid(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.linked",
            entity_type="link",
            entity_id="link:a-to-b",
            payload={"target_entity_id": "fact:b", "relation": "related_to"},
            actor=Actor(runtime="claude-code"),
        )
        assert inp.payload["target_entity_id"] == "fact:b"


class TestQueryMemoryInput:
    def test_minimal_query(self):
        q = QueryMemoryInput(namespace="test")
        assert q.limit == 10
        assert q.entity_type is None
        assert q.embedding is None

    def test_limit_clamped_to_100(self):
        q = QueryMemoryInput(namespace="test", limit=200)
        assert q.limit == 100

    def test_limit_defaults_to_10(self):
        q = QueryMemoryInput(namespace="test")
        assert q.limit == 10

    def test_full_query(self):
        q = QueryMemoryInput(
            namespace="test",
            entity_type="fact",
            time_after="2026-01-01T00:00:00Z",
            time_before="2026-12-31T23:59:59Z",
            metadata_filter={"source": "chat"},
            embedding=[0.1, 0.2],
            limit=50,
        )
        assert q.entity_type == "fact"
        assert q.limit == 50


class TestGetRecentInput:
    def test_defaults(self):
        g = GetRecentInput(namespace="test")
        assert g.limit == 10

    def test_limit_clamped(self):
        g = GetRecentInput(namespace="test", limit=500)
        assert g.limit == 100


class TestOutputModels:
    def test_write_output(self):
        out = WriteMemoryOutput(
            event_id="abc-123",
            sequence_number=1,
            timestamp="2026-04-13T10:00:00Z",
        )
        assert out.event_id == "abc-123"

    def test_memory_result(self):
        r = MemoryResult(
            entity_id="fact:x",
            entity_type="fact",
            namespace="test",
            payload={"content": "hello"},
            actor=Actor(runtime="claude-code"),
            timestamp="2026-04-13T10:00:00Z",
        )
        assert r.similarity_score is None

    def test_memory_result_with_similarity(self):
        r = MemoryResult(
            entity_id="fact:x",
            entity_type="fact",
            namespace="test",
            payload={"content": "hello"},
            actor=Actor(runtime="claude-code"),
            timestamp="2026-04-13T10:00:00Z",
            similarity_score=0.87,
        )
        assert r.similarity_score == 0.87

    def test_health_output(self):
        h = HealthOutput(
            status="ok",
            event_count=100,
            namespace_count=3,
            last_write_timestamp="2026-04-13T10:00:00Z",
            embedding_dimension=768,
        )
        assert h.embedding_dimension == 768

    def test_health_output_no_embeddings(self):
        h = HealthOutput(
            status="ok",
            event_count=0,
            namespace_count=0,
            last_write_timestamp=None,
            embedding_dimension=None,
        )
        assert h.embedding_dimension is None

import math

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

    def test_memory_linked_target_must_be_non_empty(self):
        with pytest.raises(ValidationError, match="target_entity_id"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.linked",
                entity_type="link",
                entity_id="link:a-to-b",
                payload={"target_entity_id": ""},
                actor=Actor(runtime="claude-code"),
            )

    # H9: non-empty payload required for certain event types
    def test_memory_created_requires_non_empty_payload(self):
        with pytest.raises(ValidationError, match="payload must not be empty"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_updated_requires_non_empty_payload(self):
        with pytest.raises(ValidationError, match="payload must not be empty"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.updated",
                entity_type="fact",
                entity_id="fact:x",
                payload={},
                actor=Actor(runtime="claude-code"),
                parent_event_id="evt-1",
            )

    def test_observation_recorded_requires_non_empty_payload(self):
        with pytest.raises(ValidationError, match="payload must not be empty"):
            WriteMemoryInput(
                namespace="test",
                event_type="observation.recorded",
                entity_type="obs",
                entity_id="obs:1",
                payload={},
                actor=Actor(runtime="claude-code"),
            )

    def test_memory_retracted_allows_empty_payload(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.retracted",
            entity_id="fact:x",
            payload={},
            actor=Actor(runtime="claude-code"),
            parent_event_id="evt-1",
        )
        assert inp.payload == {}

    # C2: metadata key validation
    def test_metadata_invalid_key_rejected(self):
        with pytest.raises(ValidationError, match="invalid"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                metadata={"bad-key": "value"},
            )

    def test_metadata_key_with_dot_rejected(self):
        with pytest.raises(ValidationError, match="invalid"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                metadata={"key.with.dot": "value"},
            )

    def test_metadata_key_with_underscore_allowed(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.created",
            entity_type="fact",
            entity_id="fact:x",
            payload={"content": "x"},
            actor=Actor(runtime="claude-code"),
            metadata={"valid_key": "value", "_also_valid": "v2"},
        )
        assert inp.metadata["valid_key"] == "value"

    def test_metadata_key_too_long_rejected(self):
        long_key = "a" * 65
        with pytest.raises(ValidationError, match="exceeds maximum length"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                metadata={long_key: "value"},
            )

    # M13: embedding validation
    def test_embedding_empty_list_rejected(self):
        with pytest.raises(ValidationError, match="must not be empty"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                embedding=[],
            )

    def test_embedding_non_finite_rejected(self):
        with pytest.raises(ValidationError, match="not finite"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                embedding=[0.1, float("inf"), 0.3],
            )

    def test_embedding_nan_rejected(self):
        with pytest.raises(ValidationError, match="not finite"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                embedding=[float("nan"), 0.2],
            )

    def test_embedding_too_many_dims_rejected(self):
        with pytest.raises(ValidationError, match="4096"):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                embedding=[0.1] * 4097,
            )

    def test_embedding_exactly_4096_dims_allowed(self):
        inp = WriteMemoryInput(
            namespace="test",
            event_type="memory.created",
            entity_type="fact",
            entity_id="fact:x",
            payload={"content": "x"},
            actor=Actor(runtime="claude-code"),
            embedding=[0.1] * 4096,
        )
        assert len(inp.embedding) == 4096

    # M10: extra fields forbidden
    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
                unknown_extra_field="oops",
            )

    # H5: non-empty strings
    def test_empty_namespace_rejected(self):
        with pytest.raises(ValidationError):
            WriteMemoryInput(
                namespace="",
                event_type="memory.created",
                entity_type="fact",
                entity_id="fact:x",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
            )

    def test_empty_entity_id_rejected(self):
        with pytest.raises(ValidationError):
            WriteMemoryInput(
                namespace="test",
                event_type="memory.created",
                entity_type="fact",
                entity_id="",
                payload={"content": "x"},
                actor=Actor(runtime="claude-code"),
            )


class TestQueryMemoryInput:
    def test_minimal_query(self):
        q = QueryMemoryInput(namespace="test")
        assert q.limit == 10
        assert q.entity_type is None
        assert q.embedding is None

    def test_limit_above_100_rejected(self):
        """H4: limit > 100 must raise ValidationError, not silently clamp."""
        with pytest.raises(ValidationError):
            QueryMemoryInput(namespace="test", limit=200)

    def test_limit_exactly_100_allowed(self):
        q = QueryMemoryInput(namespace="test", limit=100)
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

    def test_metadata_filter_invalid_key_rejected(self):
        with pytest.raises(ValidationError, match="invalid"):
            QueryMemoryInput(
                namespace="test",
                metadata_filter={"bad-key": "value"},
            )

    def test_metadata_filter_valid_key_allowed(self):
        q = QueryMemoryInput(namespace="test", metadata_filter={"source": "chat"})
        assert q.metadata_filter == {"source": "chat"}

    def test_time_after_canonicalized(self):
        """H3: time_after with Z suffix must be stored in canonical form."""
        q = QueryMemoryInput(namespace="test", time_after="2026-01-01T00:00:00Z")
        assert q.time_after == "2026-01-01T00:00:00.000000Z"

    def test_time_after_with_offset_canonicalized(self):
        q = QueryMemoryInput(namespace="test", time_after="2026-01-01T00:00:00+00:00")
        assert q.time_after == "2026-01-01T00:00:00.000000Z"

    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            QueryMemoryInput(namespace="test", bogus_field="nope")

    def test_empty_namespace_rejected(self):
        with pytest.raises(ValidationError):
            QueryMemoryInput(namespace="")


class TestGetRecentInput:
    def test_defaults(self):
        g = GetRecentInput(namespace="test")
        assert g.limit == 10

    def test_limit_above_100_rejected(self):
        """H4: limit > 100 must raise ValidationError."""
        with pytest.raises(ValidationError):
            GetRecentInput(namespace="test", limit=500)

    def test_limit_exactly_100_allowed(self):
        g = GetRecentInput(namespace="test", limit=100)
        assert g.limit == 100

    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            GetRecentInput(namespace="test", extra="nope")


class TestGetEntityInput:
    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            GetEntityInput(namespace="test", entity_id="fact:x", extra="nope")

    def test_empty_entity_id_rejected(self):
        with pytest.raises(ValidationError):
            GetEntityInput(namespace="test", entity_id="")


class TestInspectHistoryInput:
    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            InspectHistoryInput(namespace="test", entity_id="fact:x", extra="nope")


class TestRebuildNamespaceInput:
    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            RebuildNamespaceInput(namespace="test", extra="nope")

    def test_empty_namespace_rejected(self):
        with pytest.raises(ValidationError):
            RebuildNamespaceInput(namespace="")


class TestActor:
    def test_empty_runtime_rejected(self):
        with pytest.raises(ValidationError):
            Actor(runtime="")

    def test_none_agent_id_allowed(self):
        a = Actor(runtime="claude-code", agent_id=None)
        assert a.agent_id is None

    def test_empty_agent_id_rejected(self):
        """agent_id when present must be non-empty."""
        with pytest.raises(ValidationError):
            Actor(runtime="claude-code", agent_id="")

    def test_extra_fields_rejected(self):
        with pytest.raises(ValidationError):
            Actor(runtime="claude-code", bogus="nope")


class TestOutputModels:
    def test_write_output(self):
        out = WriteMemoryOutput(
            event_id="abc-123",
            sequence_number=1,
            timestamp="2026-04-13T10:00:00Z",
        )
        assert out.event_id == "abc-123"
        # H3: timestamp should be canonicalized
        assert out.timestamp == "2026-04-13T10:00:00.000000Z"

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
        assert r.timestamp == "2026-04-13T10:00:00.000000Z"

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


class TestTimestampCanonicalization:
    """H3: timestamp canonicalization tests."""

    def test_z_suffix_canonicalized(self):
        out = WriteMemoryOutput(
            event_id="e1", sequence_number=1, timestamp="2026-04-13T10:00:00Z"
        )
        assert out.timestamp == "2026-04-13T10:00:00.000000Z"

    def test_plus_offset_canonicalized(self):
        out = WriteMemoryOutput(
            event_id="e1", sequence_number=1, timestamp="2026-04-13T10:00:00+00:00"
        )
        assert out.timestamp == "2026-04-13T10:00:00.000000Z"

    def test_microseconds_preserved(self):
        out = WriteMemoryOutput(
            event_id="e1", sequence_number=1, timestamp="2026-04-13T10:00:00.123456Z"
        )
        assert out.timestamp == "2026-04-13T10:00:00.123456Z"

    def test_canonical_round_trips(self):
        canonical = "2026-04-13T10:00:00.000000Z"
        out = WriteMemoryOutput(
            event_id="e1", sequence_number=1, timestamp=canonical
        )
        assert out.timestamp == canonical

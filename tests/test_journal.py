import json

from cortex.storage.journal import JournalRepository


def test_insert_event(db_conn):
    repo = JournalRepository(db_conn)
    result = repo.insert_event(
        event_id="evt-1",
        event_type="memory.created",
        namespace="test",
        entity_type="fact",
        entity_id="fact:coffee",
        payload=json.dumps({"content": "likes coffee"}),
        actor_runtime="claude-code",
        actor_agent_id=None,
        metadata=None,
        parent_event_id=None,
        idempotency_key=None,
        payload_hash="abc123",
        schema_version=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    assert result["sequence_number"] >= 1
    assert result["event_id"] == "evt-1"


def test_get_events_by_entity(db_conn):
    repo = JournalRepository(db_conn)
    for i in range(3):
        repo.insert_event(
            event_id=f"evt-{i}",
            event_type="memory.created",
            namespace="test",
            entity_type="fact",
            entity_id="fact:coffee",
            payload=json.dumps({"version": i}),
            actor_runtime="claude-code",
            actor_agent_id=None,
            metadata=None,
            parent_event_id=None,
            idempotency_key=None,
            payload_hash=f"hash-{i}",
            schema_version=1,
            timestamp=f"2026-04-13T10:0{i}:00Z",
        )
    db_conn.commit()

    events = repo.get_events_by_entity("test", "fact:coffee")
    assert len(events) == 3
    assert events[0]["event_id"] == "evt-0"  # ordered by sequence_number ASC


def test_get_events_by_entity_different_namespace(db_conn):
    repo = JournalRepository(db_conn)
    repo.insert_event(
        event_id="evt-1",
        event_type="memory.created",
        namespace="ns-a",
        entity_type="fact",
        entity_id="fact:x",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        parent_event_id=None,
        idempotency_key=None,
        payload_hash="h1",
        schema_version=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()

    events = repo.get_events_by_entity("ns-b", "fact:x")
    assert len(events) == 0


def test_find_by_idempotency_key(db_conn):
    repo = JournalRepository(db_conn)
    repo.insert_event(
        event_id="evt-1",
        event_type="memory.created",
        namespace="test",
        entity_type="fact",
        entity_id="fact:x",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        parent_event_id=None,
        idempotency_key="idem-1",
        payload_hash="hash-1",
        schema_version=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()

    found = repo.find_by_idempotency_key("test", "idem-1")
    assert found is not None
    assert found["event_id"] == "evt-1"
    assert found["payload_hash"] == "hash-1"

    not_found = repo.find_by_idempotency_key("test", "idem-999")
    assert not_found is None


def test_get_all_events_in_namespace(db_conn):
    repo = JournalRepository(db_conn)
    for i in range(5):
        repo.insert_event(
            event_id=f"evt-{i}",
            event_type="memory.created",
            namespace="test",
            entity_type="fact",
            entity_id=f"fact:{i}",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            parent_event_id=None,
            idempotency_key=None,
            payload_hash=f"h{i}",
            schema_version=1,
            timestamp=f"2026-04-13T10:0{i}:00Z",
        )
    db_conn.commit()

    events = repo.get_all_events_in_namespace("test")
    assert len(events) == 5
    assert events[0]["event_id"] == "evt-0"
    assert events[4]["event_id"] == "evt-4"


def test_get_event_count(db_conn):
    repo = JournalRepository(db_conn)
    assert repo.get_event_count() == 0

    repo.insert_event(
        event_id="evt-1",
        event_type="memory.created",
        namespace="test",
        entity_type="fact",
        entity_id="fact:x",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        parent_event_id=None,
        idempotency_key=None,
        payload_hash="h1",
        schema_version=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()
    assert repo.get_event_count() == 1


def test_get_namespace_count(db_conn):
    repo = JournalRepository(db_conn)
    assert repo.get_namespace_count() == 0

    for i, ns in enumerate(["ns-a", "ns-b", "ns-a"]):
        repo.insert_event(
            event_id=f"evt-{ns}-{i}",
            event_type="memory.created",
            namespace=ns,
            entity_type="fact",
            entity_id=f"fact:{ns}",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            parent_event_id=None,
            idempotency_key=None,
            payload_hash=f"h-{ns}-{i}",
            schema_version=1,
            timestamp="2026-04-13T10:00:00Z",
        )
    db_conn.commit()
    assert repo.get_namespace_count() == 2


def test_get_last_write_timestamp(db_conn):
    repo = JournalRepository(db_conn)
    assert repo.get_last_write_timestamp() is None

    repo.insert_event(
        event_id="evt-1",
        event_type="memory.created",
        namespace="test",
        entity_type="fact",
        entity_id="fact:x",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        parent_event_id=None,
        idempotency_key=None,
        payload_hash="h1",
        schema_version=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()
    assert repo.get_last_write_timestamp() == "2026-04-13T10:00:00Z"

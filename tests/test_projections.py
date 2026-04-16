import json

from cortex.storage.projections import ProjectionRepository


def test_upsert_and_get(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:coffee",
        entity_type="fact",
        payload=json.dumps({"content": "likes coffee"}),
        actor_runtime="claude-code",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()

    row = repo.get_entity("test", "fact:coffee")
    assert row is not None
    assert row["entity_type"] == "fact"
    assert json.loads(row["payload"])["content"] == "likes coffee"
    assert row["is_retracted"] == 0


def test_get_entity_not_found(db_conn):
    repo = ProjectionRepository(db_conn)
    assert repo.get_entity("test", "nope") is None


def test_upsert_updates_existing(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:x",
        entity_type="fact",
        payload=json.dumps({"v": 1}),
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    repo.upsert(
        namespace="test",
        entity_id="fact:x",
        entity_type="fact",
        payload=json.dumps({"v": 2}),
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-2",
        last_sequence_number=2,
        timestamp="2026-04-13T10:01:00Z",
    )
    db_conn.commit()

    row = repo.get_entity("test", "fact:x")
    assert json.loads(row["payload"])["v"] == 2
    assert row["last_event_id"] == "evt-2"


def test_set_retracted(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:x",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()

    repo.set_retracted(
        namespace="test",
        entity_id="fact:x",
        last_event_id="evt-2",
        last_sequence_number=2,
        timestamp="2026-04-13T10:01:00Z",
    )
    db_conn.commit()

    row = repo.get_entity("test", "fact:x")
    assert row["is_retracted"] == 1


def test_query_excludes_retracted(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:a",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    repo.upsert(
        namespace="test",
        entity_id="fact:b",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-2",
        last_sequence_number=2,
        timestamp="2026-04-13T10:01:00Z",
    )
    repo.set_retracted(
        namespace="test",
        entity_id="fact:b",
        last_event_id="evt-3",
        last_sequence_number=3,
        timestamp="2026-04-13T10:02:00Z",
    )
    db_conn.commit()

    results = repo.query(namespace="test")
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:a"


def test_query_filter_by_entity_type(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:x",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    repo.upsert(
        namespace="test",
        entity_id="obs:y",
        entity_type="observation",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-2",
        last_sequence_number=2,
        timestamp="2026-04-13T10:01:00Z",
    )
    db_conn.commit()

    results = repo.query(namespace="test", entity_type="fact")
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:x"


def test_query_filter_by_time_range(db_conn):
    repo = ProjectionRepository(db_conn)
    for i, ts in enumerate(["2026-01-01T00:00:00Z", "2026-06-01T00:00:00Z", "2026-12-01T00:00:00Z"]):
        repo.upsert(
            namespace="test",
            entity_id=f"fact:{i}",
            entity_type="fact",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            last_event_id=f"evt-{i}",
            last_sequence_number=i + 1,
            timestamp=ts,
        )
    db_conn.commit()

    results = repo.query(
        namespace="test",
        time_after="2026-03-01T00:00:00Z",
        time_before="2026-09-01T00:00:00Z",
    )
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:1"


def test_query_filter_by_metadata(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:a",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=json.dumps({"source": "chat", "team": "eng"}),
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    repo.upsert(
        namespace="test",
        entity_id="fact:b",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=json.dumps({"source": "email"}),
        last_event_id="evt-2",
        last_sequence_number=2,
        timestamp="2026-04-13T10:01:00Z",
    )
    db_conn.commit()

    results = repo.query(namespace="test", metadata_filter={"source": "chat"})
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:a"

    # AND semantics
    results = repo.query(
        namespace="test",
        metadata_filter={"source": "chat", "team": "eng"},
    )
    assert len(results) == 1

    results = repo.query(
        namespace="test",
        metadata_filter={"source": "chat", "team": "design"},
    )
    assert len(results) == 0


def test_query_with_limit(db_conn):
    repo = ProjectionRepository(db_conn)
    for i in range(20):
        repo.upsert(
            namespace="test",
            entity_id=f"fact:{i}",
            entity_type="fact",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            last_event_id=f"evt-{i}",
            last_sequence_number=i + 1,
            timestamp=f"2026-04-13T10:{i:02d}:00Z",
        )
    db_conn.commit()

    results = repo.query(namespace="test", limit=5)
    assert len(results) == 5


def test_query_ordered_by_timestamp_desc(db_conn):
    repo = ProjectionRepository(db_conn)
    for i, ts in enumerate(["2026-01-01T00:00:00Z", "2026-06-01T00:00:00Z", "2026-12-01T00:00:00Z"]):
        repo.upsert(
            namespace="test",
            entity_id=f"fact:{i}",
            entity_type="fact",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            last_event_id=f"evt-{i}",
            last_sequence_number=i + 1,
            timestamp=ts,
        )
    db_conn.commit()

    results = repo.query(namespace="test")
    assert results[0]["entity_id"] == "fact:2"  # most recent first
    assert results[2]["entity_id"] == "fact:0"


def test_get_recent(db_conn):
    repo = ProjectionRepository(db_conn)
    for i in range(5):
        repo.upsert(
            namespace="test",
            entity_id=f"fact:{i}",
            entity_type="fact",
            payload="{}",
            actor_runtime="test",
            actor_agent_id=None,
            metadata=None,
            last_event_id=f"evt-{i}",
            last_sequence_number=i + 1,
            timestamp=f"2026-04-13T10:0{i}:00Z",
        )
    db_conn.commit()

    results = repo.get_recent("test", limit=3)
    assert len(results) == 3
    assert results[0]["entity_id"] == "fact:4"


def test_delete_namespace(db_conn):
    repo = ProjectionRepository(db_conn)
    repo.upsert(
        namespace="test",
        entity_id="fact:x",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()

    repo.delete_namespace("test")
    db_conn.commit()

    assert repo.get_entity("test", "fact:x") is None


def test_entity_exists(db_conn):
    repo = ProjectionRepository(db_conn)
    assert repo.entity_exists("test", "fact:x") is False

    repo.upsert(
        namespace="test",
        entity_id="fact:x",
        entity_type="fact",
        payload="{}",
        actor_runtime="test",
        actor_agent_id=None,
        metadata=None,
        last_event_id="evt-1",
        last_sequence_number=1,
        timestamp="2026-04-13T10:00:00Z",
    )
    db_conn.commit()

    assert repo.entity_exists("test", "fact:x") is True

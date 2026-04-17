import math

import pytest

from cortex.storage.embeddings import EmbeddingRepository


@pytest.fixture
def emb_repo(db_conn) -> EmbeddingRepository:
    repo = EmbeddingRepository(db_conn)
    repo.init_vec_table(dimension=3)
    return repo


def test_init_vec_table(emb_repo):
    assert emb_repo.get_dimension() == 3


def test_insert_and_search(emb_repo):
    emb_repo.insert(namespace="test", entity_id="fact:a", embedding=[1.0, 0.0, 0.0])
    emb_repo.insert(namespace="test", entity_id="fact:b", embedding=[0.0, 1.0, 0.0])
    emb_repo.insert(namespace="test", entity_id="fact:c", embedding=[0.9, 0.1, 0.0])

    results = emb_repo.search(namespace="test", embedding=[1.0, 0.0, 0.0], limit=2)
    assert len(results) == 2
    # fact:a closest, then fact:c
    assert results[0]["entity_id"] == "fact:a"
    assert results[1]["entity_id"] == "fact:c"
    assert "distance" in results[0]


def test_search_filters_by_namespace(emb_repo):
    emb_repo.insert(namespace="ns-a", entity_id="fact:a", embedding=[1.0, 0.0, 0.0])
    emb_repo.insert(namespace="ns-b", entity_id="fact:b", embedding=[1.0, 0.0, 0.0])

    results = emb_repo.search(namespace="ns-a", embedding=[1.0, 0.0, 0.0], limit=10)
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:a"


def test_delete_namespace(emb_repo):
    emb_repo.insert(namespace="test", entity_id="fact:a", embedding=[1.0, 0.0, 0.0])

    emb_repo.delete_namespace(namespace="test")

    results = emb_repo.search(namespace="test", embedding=[1.0, 0.0, 0.0], limit=10)
    assert len(results) == 0


def test_get_dimension_returns_none_before_init(db_conn):
    repo = EmbeddingRepository(db_conn)
    assert repo.get_dimension() is None


def test_reinsert_replaces_existing(emb_repo):
    emb_repo.insert(namespace="test", entity_id="fact:a", embedding=[1.0, 0.0, 0.0])
    emb_repo.insert(namespace="test", entity_id="fact:a", embedding=[0.0, 1.0, 0.0])  # overwrite

    results = emb_repo.search(namespace="test", embedding=[0.0, 1.0, 0.0], limit=10)
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:a"


# ---------------------------------------------------------------------------
# New tests from QA review
# ---------------------------------------------------------------------------


def test_namespace_starvation(emb_repo):
    """Small target namespace must not be starved by larger siblings."""
    for i in range(30):
        emb_repo.insert(namespace="ns-B", entity_id=f"b:{i}", embedding=[1.0, 0.0, 0.0])
    emb_repo.insert(namespace="ns-A", entity_id="a:1", embedding=[1.0, 0.0, 0.0])

    results = emb_repo.search(namespace="ns-A", embedding=[1.0, 0.0, 0.0], limit=1)
    assert len(results) == 1
    assert results[0]["entity_id"] == "a:1"


def test_reinsert_old_embedding_gone(emb_repo):
    """Reinsert must remove the old vector, not just add another."""
    emb_repo.insert(namespace="test", entity_id="fact:a", embedding=[1.0, 0.0, 0.0])
    emb_repo.insert(namespace="test", entity_id="fact:a", embedding=[0.0, 1.0, 0.0])

    # Searching for the OLD vector should not find fact:a at distance ~0
    results = emb_repo.search(namespace="test", embedding=[1.0, 0.0, 0.0], limit=10)
    # fact:a exists but with the NEW embedding, so distance should be far from 0
    assert len(results) == 1
    assert results[0]["entity_id"] == "fact:a"
    assert results[0]["distance"] > 0.5  # new vec is orthogonal to queried old vec


def test_init_vec_table_idempotent_same_dim(emb_repo):
    emb_repo.init_vec_table(dimension=3)
    emb_repo.init_vec_table(dimension=3)
    assert emb_repo.get_dimension() == 3


def test_get_dimension_new_instance_existing_db(db_conn):
    first = EmbeddingRepository(db_conn)
    first.init_vec_table(dimension=7)
    # Simulate fresh instance on same connection (e.g., after restart)
    second = EmbeddingRepository(db_conn)
    assert second.get_dimension() == 7


def test_delete_namespace_nonexistent_is_noop(emb_repo):
    # Should not raise
    emb_repo.delete_namespace(namespace="ghost")
    results = emb_repo.search(namespace="ghost", embedding=[1.0, 0.0, 0.0], limit=10)
    assert results == []


def test_insert_rejects_non_finite_floats(emb_repo):
    with pytest.raises(ValueError):
        emb_repo.insert(namespace="test", entity_id="bad", embedding=[math.nan, 0.0, 0.0])
    with pytest.raises(ValueError):
        emb_repo.insert(namespace="test", entity_id="bad", embedding=[math.inf, 0.0, 0.0])


def test_insert_rejects_empty_embedding(emb_repo):
    with pytest.raises(ValueError):
        emb_repo.insert(namespace="test", entity_id="bad", embedding=[])


def test_insert_rejects_wrong_dimension(emb_repo):
    # Table is dim=3
    with pytest.raises(ValueError):
        emb_repo.insert(namespace="test", entity_id="bad", embedding=[1.0, 0.0])  # dim=2


def test_init_vec_table_rejects_invalid_dimension(db_conn):
    repo = EmbeddingRepository(db_conn)
    with pytest.raises(ValueError):
        repo.init_vec_table(dimension=0)
    with pytest.raises(ValueError):
        repo.init_vec_table(dimension=10_000)

"""
AetherSRE — Day 2 Unit Tests: Vector Store
===========================================
Tests the thread-safe VectorStore in isolation — no Redis, no ML models.
Pure NumPy arithmetic operations allow these tests to run in < 100ms.
"""

from __future__ import annotations

import threading
import time

import numpy as np
import pytest

from app.core.vector_store import (
    QueryResult,
    VectorRecord,
    VectorStore,
    get_vector_store,
    reset_vector_store,
)


# =============================================================================
# Fixtures
# =============================================================================

DIM: int = 384  # Matches all-MiniLM-L6-v2 output dimension


def _make_record(
    vec: np.ndarray | None = None,
    service: str = "auth-service",
    level: str = "INFO",
    msg: str = "Test log",
) -> VectorRecord:
    """Create a VectorRecord with a random or specified vector."""
    vector = vec if vec is not None else np.random.randn(DIM).astype(np.float32)
    return VectorRecord(
        vector=vector,
        raw_message=msg,
        normalized_msg=msg,
        timestamp="2024-01-15T12:00:00Z",
        service_name=service,
        log_level=level,
        stream_id=f"17180000{id(vector)}-0",
    )


@pytest.fixture()
def store() -> VectorStore:
    """Fresh VectorStore instance for each test."""
    return VectorStore(dim=DIM, max_capacity=100)


# =============================================================================
# Construction tests
# =============================================================================


class TestVectorStoreConstruction:
    def test_initial_size_is_zero(self, store: VectorStore) -> None:
        assert store.size == 0

    def test_dim_property(self, store: VectorStore) -> None:
        assert store.dim == DIM

    def test_invalid_dim_raises(self) -> None:
        with pytest.raises(ValueError, match="dim must be positive"):
            VectorStore(dim=0)

    def test_invalid_capacity_raises(self) -> None:
        with pytest.raises(ValueError, match="max_capacity must be positive"):
            VectorStore(dim=DIM, max_capacity=0)


# =============================================================================
# Write operations
# =============================================================================


class TestVectorStoreAdd:
    def test_single_add_increments_size(self, store: VectorStore) -> None:
        store.add(_make_record())
        assert store.size == 1

    def test_add_returns_index(self, store: VectorStore) -> None:
        idx = store.add(_make_record())
        assert idx == 0
        idx2 = store.add(_make_record())
        assert idx2 == 1

    def test_wrong_shape_raises(self, store: VectorStore) -> None:
        bad_vec = np.zeros(100, dtype=np.float32)
        with pytest.raises(ValueError, match="Expected vector shape"):
            store.add(_make_record(vec=bad_vec))

    def test_add_batch_returns_ordered_indices(self, store: VectorStore) -> None:
        records = [_make_record() for _ in range(5)]
        indices = store.add_batch(records)
        assert indices == [0, 1, 2, 3, 4]

    def test_add_batch_increments_size_correctly(self, store: VectorStore) -> None:
        store.add_batch([_make_record() for _ in range(10)])
        assert store.size == 10

    def test_empty_batch_returns_empty_list(self, store: VectorStore) -> None:
        indices = store.add_batch([])
        assert indices == []
        assert store.size == 0

    def test_capacity_growth_beyond_initial(self) -> None:
        small_store = VectorStore(dim=DIM, max_capacity=3)
        # Add 8 records — should trigger two doublings (3 → 6 → 12)
        for _ in range(8):
            small_store.add(_make_record())
        assert small_store.size == 8


# =============================================================================
# Query / similarity tests
# =============================================================================


class TestVectorStoreQuery:
    def test_query_empty_store_returns_empty(self, store: VectorStore) -> None:
        qv = np.random.randn(DIM).astype(np.float32)
        results = store.query(qv, top_k=5)
        assert results == []

    def test_query_returns_at_most_top_k(self, store: VectorStore) -> None:
        store.add_batch([_make_record() for _ in range(20)])
        qv = np.random.randn(DIM).astype(np.float32)
        results = store.query(qv, top_k=5)
        assert len(results) <= 5

    def test_query_identical_vector_has_highest_similarity(
        self, store: VectorStore
    ) -> None:
        """Querying with the exact stored vector should return similarity ≈ 1.0."""
        target_vec = np.random.randn(DIM).astype(np.float32)
        # Normalise the vector so cosine = dot product
        target_vec /= np.linalg.norm(target_vec)
        store.add(_make_record(vec=target_vec, msg="target"))

        # Add 10 random noise vectors
        for _ in range(10):
            store.add(_make_record())

        results = store.query(target_vec, top_k=1)
        assert len(results) == 1
        assert results[0].similarity == pytest.approx(1.0, abs=1e-4)
        assert results[0].record.raw_message == "target"

    def test_query_results_sorted_descending_similarity(
        self, store: VectorStore
    ) -> None:
        store.add_batch([_make_record() for _ in range(10)])
        qv = np.random.randn(DIM).astype(np.float32)
        results = store.query(qv, top_k=10)
        sims = [r.similarity for r in results]
        assert sims == sorted(sims, reverse=True)

    def test_query_result_rank_starts_at_one(self, store: VectorStore) -> None:
        store.add_batch([_make_record() for _ in range(5)])
        qv = np.random.randn(DIM).astype(np.float32)
        results = store.query(qv, top_k=3)
        ranks = [r.rank for r in results]
        assert ranks == [1, 2, 3]

    def test_query_wrong_shape_raises(self, store: VectorStore) -> None:
        store.add(_make_record())
        bad_qv = np.zeros(100, dtype=np.float32)
        with pytest.raises(ValueError, match="Query vector shape mismatch"):
            store.query(bad_qv)

    def test_min_similarity_filter(self, store: VectorStore) -> None:
        """Results below min_similarity should be excluded."""
        store.add_batch([_make_record() for _ in range(10)])
        qv = np.random.randn(DIM).astype(np.float32)
        # Setting min_similarity to 2.0 (above possible cosine range) → no results
        results = store.query(qv, top_k=5, min_similarity=2.0)
        assert results == []


# =============================================================================
# Stats and snapshot tests
# =============================================================================


class TestVectorStoreStats:
    def test_stats_size_matches_add_count(self, store: VectorStore) -> None:
        store.add_batch([_make_record(service="auth-service") for _ in range(3)])
        store.add_batch([_make_record(service="payment-gateway") for _ in range(2)])
        stats = store.stats()
        assert stats["size"] == 5
        assert stats["service_counts"]["auth-service"] == 3
        assert stats["service_counts"]["payment-gateway"] == 2

    def test_stats_level_counts(self, store: VectorStore) -> None:
        store.add(_make_record(level="INFO"))
        store.add(_make_record(level="ERROR"))
        store.add(_make_record(level="CRITICAL"))
        store.add(_make_record(level="ERROR"))
        stats = store.stats()
        assert stats["level_counts"]["INFO"] == 1
        assert stats["level_counts"]["ERROR"] == 2
        assert stats["level_counts"]["CRITICAL"] == 1

    def test_snapshot_returns_consistent_view(self, store: VectorStore) -> None:
        store.add_batch([_make_record() for _ in range(5)])
        matrix, records = store.snapshot()
        assert matrix.shape == (5, DIM)
        assert len(records) == 5


# =============================================================================
# Thread-safety tests
# =============================================================================


class TestVectorStoreThreadSafety:
    def test_concurrent_adds_produce_correct_size(self) -> None:
        """Multiple threads writing concurrently should not lose any record."""
        ts = VectorStore(dim=DIM, max_capacity=1000)
        errors: list[Exception] = []
        n_threads = 8
        n_per_thread = 50

        def _writer() -> None:
            try:
                for _ in range(n_per_thread):
                    ts.add(_make_record())
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_writer) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Thread errors: {errors}"
        assert ts.size == n_threads * n_per_thread

    def test_concurrent_add_and_query_do_not_deadlock(self) -> None:
        """Writer and reader threads should not deadlock."""
        ts = VectorStore(dim=DIM, max_capacity=1000)
        finished = threading.Event()

        def _writer() -> None:
            for _ in range(100):
                ts.add(_make_record())
                time.sleep(0.001)
            finished.set()

        def _reader() -> None:
            while not finished.is_set():
                qv = np.random.randn(DIM).astype(np.float32)
                ts.query(qv, top_k=3)
                time.sleep(0.002)

        writer = threading.Thread(target=_writer)
        reader = threading.Thread(target=_reader)
        writer.start()
        reader.start()
        writer.join(timeout=15)
        reader.join(timeout=5)
        assert not writer.is_alive(), "Writer thread deadlocked"


# =============================================================================
# Global singleton tests
# =============================================================================


class TestGlobalVectorStore:
    def setup_method(self) -> None:
        reset_vector_store()

    def teardown_method(self) -> None:
        reset_vector_store()

    def test_get_vector_store_returns_singleton(self) -> None:
        s1 = get_vector_store()
        s2 = get_vector_store()
        assert s1 is s2

    def test_reset_clears_singleton(self) -> None:
        s1 = get_vector_store()
        reset_vector_store()
        s2 = get_vector_store()
        assert s1 is not s2

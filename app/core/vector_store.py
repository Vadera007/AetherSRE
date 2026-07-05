"""
AetherSRE — In-Memory Thread-Safe NumPy Vector Store
=====================================================
Provides a lightweight, process-local vector store for dense embedding
vectors produced by the Sentence Transformer worker.

Why in-memory for Day 2?
  We are in the bootstrapping phase of the pipeline.  Introducing a
  production vector database (Weaviate, Qdrant, Milvus) before the
  pipeline is end-to-end validated adds operational complexity without
  measurable benefit.  This store:
    • Has zero network round-trips (no latency floor).
    • Is fast enough to hold ~500k 384-dim float32 vectors in ~750 MB.
    • Exposes the identical cosine-similarity interface that we will
      swap out for a real vector DB on Day 3/4 with minimal code change.

Thread-safety model:
  All public methods acquire a single `threading.Lock` for write
  operations.  Reads (``query``) acquire the same lock to snapshot the
  current matrix into a local reference, then release immediately before
  the expensive cosine computation — ensuring writers are never blocked
  by long-running similarity searches.

Performance characteristics:
  • ``add_batch()`` — O(B) where B = batch size (just array append).
  • ``query()``     — O(N·D) cosine similarity over all N stored vectors
                      of dimension D.  This is acceptable for < 100k
                      vectors on a CPU; Day 3 will introduce an HNSW index.
  • ``snapshot()``  — O(N) copy; call sparingly.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Final

import numpy as np
from numpy.typing import NDArray


# =============================================================================
# Data structures
# =============================================================================


@dataclass(slots=True)
class VectorRecord:
    """
    Single item stored in the vector store.

    Attributes:
        vector:         Dense float32 embedding, shape (D,).
        raw_message:    The original, unmodified log message string.
        normalized_msg: The template string after regex normalisation.
        timestamp:      ISO-8601 string of the event's originating timestamp.
        service_name:   Originating microservice (e.g., 'payment-gateway').
        log_level:      Severity string ('INFO', 'ERROR', …).
        stream_id:      Redis Stream entry ID (e.g., '1718000000000-0').
        stored_at:      Unix epoch (float) when this record was written.
    """

    vector: NDArray[np.float32]
    raw_message: str
    normalized_msg: str
    timestamp: str
    service_name: str
    log_level: str
    stream_id: str
    stored_at: float = field(default_factory=time.monotonic)


@dataclass(slots=True, frozen=True)
class QueryResult:
    """
    A single nearest-neighbour hit returned by ``VectorStore.query()``.

    Attributes:
        record:     The matching ``VectorRecord``.
        similarity: Cosine similarity in [-1, 1]; higher is more similar.
        rank:       1-based position in the result list (1 = closest).
    """

    record: VectorRecord
    similarity: float
    rank: int


# =============================================================================
# Vector store
# =============================================================================

_FLOAT32_AXIS0: Final[int] = 0
_EPSILON: Final[float] = 1e-10  # Guard against division-by-zero in cosine


class VectorStore:
    """
    Thread-safe, append-only in-memory vector store backed by NumPy arrays.

    Usage::

        store = VectorStore(dim=384, max_capacity=50_000)

        # Writer (vector processor worker thread)
        store.add_batch(records)

        # Reader (anomaly detector, API endpoint)
        hits = store.query(query_vector, top_k=5)

    The store is intentionally append-only — no update or delete — because
    the primary use-case (nearest-neighbour anomaly detection) does not
    require mutability of historical records.

    Args:
        dim:          Embedding dimensionality.  Must match the model output.
        max_capacity: Pre-allocate the backing matrix for this many vectors.
                      The matrix grows automatically by doubling when full,
                      similar to a C++ std::vector amortised strategy.
    """

    def __init__(self, dim: int = 384, max_capacity: int = 10_000) -> None:
        if dim <= 0:
            raise ValueError(f"dim must be positive, got {dim}")
        if max_capacity <= 0:
            raise ValueError(f"max_capacity must be positive, got {max_capacity}")

        self._dim: int = dim
        self._lock = threading.Lock()

        # Pre-allocate backing matrix — avoids per-record allocation.
        # Shape: (max_capacity, dim); grows by doubling when exhausted.
        self._matrix: NDArray[np.float32] = np.empty(
            (max_capacity, dim), dtype=np.float32
        )
        self._records: list[VectorRecord] = []
        self._size: int = 0         # Number of vectors currently stored
        self._capacity: int = max_capacity

        # Pre-computed L2 norms of stored vectors — maintained lazily.
        # None signals that the cache is dirty and must be recomputed.
        self._norms: NDArray[np.float32] | None = None

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def size(self) -> int:
        """Number of vectors currently stored (thread-safe read)."""
        with self._lock:
            return self._size

    @property
    def dim(self) -> int:
        """Embedding dimensionality (immutable after construction)."""
        return self._dim

    # ── Write operations ──────────────────────────────────────────────────────

    def add(self, record: VectorRecord) -> int:
        """
        Append a single ``VectorRecord`` to the store.

        Args:
            record: The record to append.  ``record.vector`` must have
                    shape ``(self.dim,)`` and dtype float32.

        Returns:
            The zero-based index assigned to this record.

        Raises:
            ValueError: If the vector shape or dtype is incompatible.
        """
        vec = np.asarray(record.vector, dtype=np.float32)
        if vec.shape != (self._dim,):
            raise ValueError(
                f"Expected vector shape ({self._dim},), got {vec.shape}"
            )

        with self._lock:
            self._ensure_capacity()
            self._matrix[self._size] = vec
            self._records.append(record)
            idx = self._size
            self._size += 1
            self._norms = None  # Invalidate norm cache
            return idx

    def add_batch(self, records: list[VectorRecord]) -> list[int]:
        """
        Append multiple ``VectorRecord`` objects in a single lock acquisition.

        Batching writes is significantly faster than calling ``add()`` in a
        loop because it minimises lock contention and amortises the cost of
        ``_ensure_capacity()`` across the whole batch.

        Args:
            records: List of records to append.

        Returns:
            List of zero-based indices assigned to each record, in order.

        Raises:
            ValueError: If any vector has an incompatible shape.
        """
        if not records:
            return []

        # Validate all vectors outside the lock to minimise critical section.
        vectors: list[NDArray[np.float32]] = []
        for r in records:
            vec = np.asarray(r.vector, dtype=np.float32)
            if vec.shape != (self._dim,):
                raise ValueError(
                    f"Expected vector shape ({self._dim},), got {vec.shape} "
                    f"for record service={r.service_name}"
                )
            vectors.append(vec)

        with self._lock:
            self._ensure_capacity(needed=len(records))
            start_idx = self._size
            for i, (record, vec) in enumerate(zip(records, vectors)):
                self._matrix[self._size] = vec
                self._records.append(record)
                self._size += 1
            self._norms = None
            return list(range(start_idx, self._size))

    # ── Read operations ───────────────────────────────────────────────────────

    def query(
        self,
        query_vector: NDArray[np.float32],
        top_k: int = 5,
        min_similarity: float = -1.0,
    ) -> list[QueryResult]:
        """
        Return the top-K nearest neighbours by cosine similarity.

        The implementation:
          1. Acquires the lock just long enough to snapshot the current
             size and grab a reference to the backing matrix slice.
          2. Releases the lock before the O(N·D) similarity computation.
          3. Uses vectorised NumPy operations — no Python loops.

        Args:
            query_vector:   Dense float32 vector of shape ``(self.dim,)``.
            top_k:          Maximum number of results to return.
            min_similarity: Discard results below this cosine threshold.

        Returns:
            List of ``QueryResult`` sorted by descending similarity,
            at most ``top_k`` items.  May be shorter if fewer than
            ``top_k`` records exist or pass the ``min_similarity`` filter.

        Raises:
            ValueError: If the query vector has an incompatible shape.
        """
        qvec = np.asarray(query_vector, dtype=np.float32)
        if qvec.shape != (self._dim,):
            raise ValueError(
                f"Query vector shape mismatch: expected ({self._dim},), "
                f"got {qvec.shape}"
            )

        # ── Snapshot under lock (minimal critical section) ─────────────────
        with self._lock:
            n = self._size
            if n == 0:
                return []
            # Slice is a view; copy only if the caller might mutate it.
            matrix_view: NDArray[np.float32] = self._matrix[:n]
            # Recompute norms if cache is dirty.
            if self._norms is None:
                self._norms = np.linalg.norm(matrix_view, axis=1, keepdims=False)
            norms_snapshot: NDArray[np.float32] = self._norms.copy()
            records_snapshot: list[VectorRecord] = list(self._records)
            matrix_snapshot: NDArray[np.float32] = matrix_view.copy()

        # ── Cosine similarity (lock-free) ──────────────────────────────────
        # dot(query, corpus[i]) / (||query|| * ||corpus[i]||)
        q_norm = float(np.linalg.norm(qvec)) + _EPSILON
        q_unit = qvec / q_norm

        # Matrix multiplication: shape (N,)
        dots: NDArray[np.float32] = matrix_snapshot @ q_unit

        # Safe element-wise division
        safe_norms = norms_snapshot + _EPSILON
        similarities: NDArray[np.float32] = dots / safe_norms

        # Apply minimum similarity filter
        valid_mask = similarities >= min_similarity
        valid_indices = np.where(valid_mask)[0]

        if valid_indices.size == 0:
            return []

        # Sort by descending similarity and take top_k
        k = min(top_k, valid_indices.size)
        top_idx_local = np.argpartition(
            -similarities[valid_indices], kth=min(k - 1, valid_indices.size - 1)
        )[:k]
        top_idx_global = valid_indices[top_idx_local]
        sorted_order = np.argsort(-similarities[top_idx_global])

        results: list[QueryResult] = []
        for rank, pos in enumerate(sorted_order, start=1):
            global_idx = int(top_idx_global[pos])
            results.append(
                QueryResult(
                    record=records_snapshot[global_idx],
                    similarity=float(similarities[global_idx]),
                    rank=rank,
                )
            )

        return results

    def snapshot(self) -> tuple[NDArray[np.float32], list[VectorRecord]]:
        """
        Return a consistent copy of the entire store.

        Useful for bulk exports, model training, or persistence checkpoints.

        Returns:
            Tuple of:
                - Float32 matrix of shape ``(N, dim)``.
                - List of ``VectorRecord`` of length N.
        """
        with self._lock:
            n = self._size
            matrix_copy = self._matrix[:n].copy()
            records_copy = list(self._records)
        return matrix_copy, records_copy

    def stats(self) -> dict[str, object]:
        """
        Return a dict of diagnostic statistics.

        Returns:
            Dict with keys: size, capacity, dim, memory_mb,
            service_counts, level_counts.
        """
        with self._lock:
            n = self._size
            records = list(self._records)
            cap = self._capacity

        memory_bytes = self._matrix.nbytes
        service_counts: dict[str, int] = {}
        level_counts: dict[str, int] = {}
        for rec in records:
            service_counts[rec.service_name] = (
                service_counts.get(rec.service_name, 0) + 1
            )
            level_counts[rec.log_level] = level_counts.get(rec.log_level, 0) + 1

        return {
            "size": n,
            "capacity": cap,
            "dim": self._dim,
            "memory_mb": round(memory_bytes / 1024 / 1024, 2),
            "service_counts": service_counts,
            "level_counts": level_counts,
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _ensure_capacity(self, needed: int = 1) -> None:
        """
        Grow the backing matrix by doubling if there is not enough room.

        Must be called **inside** the lock.

        Args:
            needed: Minimum number of free slots required.
        """
        free = self._capacity - self._size
        if free >= needed:
            return

        # Double until sufficient
        new_capacity = self._capacity
        while new_capacity - self._size < needed:
            new_capacity *= 2

        new_matrix: NDArray[np.float32] = np.empty(
            (new_capacity, self._dim), dtype=np.float32
        )
        if self._size > 0:
            new_matrix[: self._size] = self._matrix[: self._size]

        self._matrix = new_matrix
        self._capacity = new_capacity
        self._norms = None  # Stale after realloc


# =============================================================================
# Module-level singleton
# =============================================================================

# Shared store used by the vector processor worker and query endpoints.
# Dimension 384 matches the all-MiniLM-L6-v2 output size.
_GLOBAL_STORE: VectorStore | None = None


def get_vector_store() -> VectorStore:
    """
    Return the process-global VectorStore, creating it on first call.

    The store is intentionally initialised lazily so that importing this
    module in tests (where we don't want a 750 MB matrix) has zero cost.
    """
    global _GLOBAL_STORE  # noqa: PLW0603
    if _GLOBAL_STORE is None:
        _GLOBAL_STORE = VectorStore(dim=384, max_capacity=10_000)
    return _GLOBAL_STORE


def reset_vector_store() -> None:
    """
    Destroy and reset the global store.

    Intended for use in tests only — never call in production code.
    """
    global _GLOBAL_STORE  # noqa: PLW0603
    _GLOBAL_STORE = None

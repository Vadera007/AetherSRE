"""
AetherSRE — Stream Consumer, Vector Embedding & Anomaly Detection Worker
=========================================================================
Reads raw log events from the Redis Stream, runs them through the full
Day 2 + Day 3 pipeline, and emits Incident Context Frames to a downstream
``incident_alerts_stream`` when an anomaly is detected.

Full pipeline (Day 3)
---------------------

  Redis Stream                 ┌──────────────────────────────────────────┐
  (telemetry_log_stream)       │         VectorProcessorWorker            │
              │                │                                          │
              │ XREADGROUP     │  ┌───────────────────────────────────┐   │
              └───────────────►│  │  Async Polling Loop               │   │
                               │  │  (consumer group: aether)         │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │ raw stream entry       │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  LogContextBuffer.append()        │   │
                               │  │  per-service rotating ring buffer  │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │ packet buffered        │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  MicroBatchAccumulator             │   │
                               │  │  max_size=32 | timeout=0.5s        │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │ batch ready            │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  Regex Normalizer                 │   │
                               │  │  (app.core.normalizer)            │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │ normalised templates   │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  Sentence Transformer             │   │
                               │  │  all-MiniLM-L6-v2 (CPU)          │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │ float32[batch, 384]    │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  NumPy Vector Store               │   │
                               │  │  (app.core.vector_store)          │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │                        │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  AetherAnomalyDetector            │   │
                               │  │  Isolation Forest (sklearn)        │   │
                               │  │  score_vector() → [0.0 … 1.0]     │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │ anomaly_score          │
                               │  ┌───────────────▼───────────────────┐   │
                               │  │  is_anomaly(threshold=0.55)?       │   │
                               │  │  YES → build Incident Frame        │   │
                               │  │        + get_context_frame(5)      │   │
                               │  └───────────────┬───────────────────┘   │
                               │                  │                        │
                               └──────────────────┼────────────────────────┘
                                                  │ XADD (incident)
                                                  ▼
                               Redis Stream: incident_alerts_stream
                               + WARNING log [ANOMALY_DETECTED]

Consumer Group Design
---------------------
XREADGROUP with consumer group ``aether-vector-workers`` and ID ``">"``
ensures at-most-one delivery per worker instance.  XACK is sent only after
the full pipeline (embed + store + anomaly check + optional alert) completes
for each batch, preserving at-least-once semantics.

Training Gate
-------------
The Isolation Forest baseline is trained on the first batch of ≥ 128 normal
embeddings accumulated in the vector store.  Until then, ``score_vector``
returns 0.0 (no false alerts during cold start).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Final

import numpy as np
import redis.asyncio as aioredis

from app.core.anomaly_detector import AetherAnomalyDetector, get_anomaly_detector
from app.core.config import get_settings
from app.core.context_buffer import LogContextBuffer, get_context_buffer
from app.core.logging_config import configure_logging, get_logger
from app.core.normalizer import NormalizationResult, normalize_batch
from app.core.vector_store import VectorRecord, VectorStore, get_vector_store

logger = get_logger(__name__)


# =============================================================================
# Worker configuration constants (can be overridden via env vars)
# =============================================================================

_DEFAULT_BATCH_MAX_SIZE: Final[int] = 32
_DEFAULT_BATCH_TIMEOUT_SECONDS: Final[float] = 0.5
_DEFAULT_CONSUMER_GROUP: Final[str] = "aether-vector-workers"
_DEFAULT_CONSUMER_NAME_PREFIX: Final[str] = "vector-worker"
_DEFAULT_MODEL_NAME: Final[str] = "all-MiniLM-L6-v2"
_XREAD_BLOCK_MS: Final[int] = 200
_XREAD_COUNT: Final[int] = 10
_STATS_INTERVAL_SECONDS: Final[int] = 30

# Minimum vectors in store before we attempt training the baseline
_BASELINE_TRAIN_THRESHOLD: Final[int] = 128

# Incident alert stream configuration
_INCIDENT_STREAM_NAME: Final[str] = "incident_alerts_stream"
_INCIDENT_STREAM_MAXLEN: Final[int] = 10_000

# Anomaly detection threshold — score above this fires an alert
_DEFAULT_ANOMALY_THRESHOLD: Final[float] = 0.55

# Window of preceding log frames included in each Incident Context Frame
_CONTEXT_WINDOW_SIZE: Final[int] = 5


# =============================================================================
# Worker statistics
# =============================================================================


@dataclass
class WorkerStats:
    """Mutable counters maintained across the worker's lifetime."""

    batches_processed: int = 0
    messages_embedded: int = 0
    messages_failed: int = 0
    messages_acknowledged: int = 0
    anomalies_detected: int = 0
    incidents_published: int = 0
    total_embed_time_s: float = 0.0
    start_time: float = 0.0

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self.start_time if self.start_time else 0.0

    @property
    def throughput_msgs_per_s(self) -> float:
        e = self.elapsed_s
        return self.messages_embedded / e if e > 0 else 0.0

    @property
    def avg_embed_ms_per_msg(self) -> float:
        if self.messages_embedded == 0:
            return 0.0
        return (self.total_embed_time_s / self.messages_embedded) * 1000.0

    def summary(self) -> str:
        return (
            f"elapsed={self.elapsed_s:.0f}s "
            f"batches={self.batches_processed} "
            f"embedded={self.messages_embedded} "
            f"anomalies={self.anomalies_detected} "
            f"incidents={self.incidents_published} "
            f"failed={self.messages_failed} "
            f"acked={self.messages_acknowledged} "
            f"throughput={self.throughput_msgs_per_s:.2f} msg/s "
            f"avg_embed={self.avg_embed_ms_per_msg:.1f}ms/msg"
        )


# =============================================================================
# Sentence Transformer model helpers
# =============================================================================


def _load_sentence_transformer(model_name: str) -> object:
    """
    Load and return a SentenceTransformer model instance.

    Blocking call — must be run in a thread pool executor.

    Args:
        model_name: HuggingFace model identifier or local path.

    Returns:
        A ``SentenceTransformer`` instance ready for ``.encode()`` calls.

    Raises:
        ImportError: If ``sentence_transformers`` is not installed.
        OSError:     If the model cannot be loaded or downloaded.
    """
    try:
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "sentence-transformers is not installed. "
            "Run: pip install sentence-transformers"
        ) from exc

    logger.info("Loading Sentence Transformer model | name=%s", model_name)
    t0 = time.monotonic()
    model = SentenceTransformer(model_name, device="cpu")
    elapsed = time.monotonic() - t0
    logger.info("Model loaded | name=%s elapsed=%.2fs", model_name, elapsed)
    return model


def _encode_batch(model: object, texts: list[str]) -> np.ndarray:
    """
    Encode a list of text strings into float32 embedding vectors.

    Blocking, CPU-bound — run in a thread pool executor.

    Args:
        model: A loaded ``SentenceTransformer`` instance.
        texts: Non-empty list of text strings to encode.

    Returns:
        Float32 array of shape ``(len(texts), 384)``.
    """
    # Check for encode method to support both real SentenceTransformer and test mock objects
    assert hasattr(model, "encode")
    embeddings: np.ndarray = model.encode(
        texts,
        batch_size=len(texts),
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return embeddings.astype(np.float32)


def _train_baseline_sync(
    detector: AetherAnomalyDetector,
    matrix: np.ndarray,
) -> None:
    """
    Synchronous wrapper for ``detector.train_baseline()`` — run in executor.

    Args:
        detector: The AetherAnomalyDetector to train.
        matrix:   Float32 array of shape ``(N, 384)``.
    """
    detector.train_baseline(matrix)


# =============================================================================
# Raw stream entry parser
# =============================================================================


@dataclass(slots=True)
class ParsedEntry:
    """A single Redis Stream entry decoded into typed fields."""

    stream_id: str
    service_name: str
    timestamp: str
    level: str
    message: str
    metadata: dict[str, str]


def _parse_stream_entry(stream_id: str, fields: dict[str, str]) -> ParsedEntry | None:
    """
    Parse a raw Redis Stream entry dict into a typed ``ParsedEntry``.

    Args:
        stream_id: The Redis-assigned entry ID.
        fields:    The flat string dict from XREADGROUP.

    Returns:
        A ``ParsedEntry`` or None if required fields are missing.
    """
    service_name = fields.get("service_name", "").strip()
    message = fields.get("message", "").strip()
    timestamp = fields.get("timestamp", "")
    level = fields.get("level", "INFO").upper()

    if not service_name or not message:
        logger.warning(
            "Skipping malformed stream entry | id=%s missing_fields=%s",
            stream_id,
            [k for k in ("service_name", "message") if not fields.get(k)],
        )
        return None

    raw_metadata = fields.get("metadata", "{}")
    try:
        metadata: dict[str, str] = json.loads(raw_metadata)
    except json.JSONDecodeError:
        metadata = {}

    return ParsedEntry(
        stream_id=stream_id,
        service_name=service_name,
        timestamp=timestamp,
        level=level,
        message=message,
        metadata=metadata,
    )


# =============================================================================
# Micro-batch accumulator
# =============================================================================


class MicroBatchAccumulator:
    """
    Accumulates parsed stream entries and flushes when the batch is ready.

    A batch is ready when it has ``max_size`` entries OR more than
    ``timeout_seconds`` have elapsed since the first entry was added.

    Not thread-safe — designed for single-asyncio-task use.
    """

    def __init__(self, max_size: int, timeout_seconds: float) -> None:
        self._max_size = max_size
        self._timeout_seconds = timeout_seconds
        self._batch: list[ParsedEntry] = []
        self._batch_start: float | None = None

    def add(self, entry: ParsedEntry) -> None:
        """Append an entry to the current batch."""
        if not self._batch:
            self._batch_start = time.monotonic()
        self._batch.append(entry)

    def is_ready(self) -> bool:
        """Return True if the batch should be flushed."""
        if not self._batch:
            return False
        if len(self._batch) >= self._max_size:
            return True
        if self._batch_start is not None:
            return time.monotonic() - self._batch_start >= self._timeout_seconds
        return False

    def flush(self) -> list[ParsedEntry]:
        """Return the current batch and reset state."""
        result = self._batch
        self._batch = []
        self._batch_start = None
        return result

    @property
    def pending_count(self) -> int:
        """Number of entries in the current unflushed batch."""
        return len(self._batch)


# =============================================================================
# Core worker class
# =============================================================================


class VectorProcessorWorker:
    """
    Orchestrates the full Day 3 pipeline:
      Redis Stream → Context Buffer → Normalizer → Embedder → Vector Store
      → Isolation Forest → (anomaly?) → incident_alerts_stream

    Lifecycle:
        1. ``start()``  — Connect to Redis, create consumer group, load model.
        2. ``run()``    — Infinite polling loop; stopped by stop_event.
        3. ``stop()``   — Signal the loop to exit cleanly.
        4. ``close()``  — Close Redis connection.

    Args:
        batch_max_size:      Flush batch when this many entries are pending.
        batch_timeout_s:     Flush batch after this many seconds even if not full.
        consumer_group:      Redis consumer group name.
        consumer_name:       Unique instance name in the consumer group.
        model_name:          Sentence Transformer model identifier.
        vector_store:        Optional VectorStore override (for testing).
        anomaly_detector:    Optional AetherAnomalyDetector override.
        context_buffer:      Optional LogContextBuffer override.
        anomaly_threshold:   Score threshold above which an alert is fired.
    """

    def __init__(
        self,
        batch_max_size: int = _DEFAULT_BATCH_MAX_SIZE,
        batch_timeout_s: float = _DEFAULT_BATCH_TIMEOUT_SECONDS,
        consumer_group: str = _DEFAULT_CONSUMER_GROUP,
        consumer_name: str = f"{_DEFAULT_CONSUMER_NAME_PREFIX}-0",
        model_name: str = _DEFAULT_MODEL_NAME,
        vector_store: VectorStore | None = None,
        anomaly_detector: AetherAnomalyDetector | None = None,
        context_buffer: LogContextBuffer | None = None,
        anomaly_threshold: float = _DEFAULT_ANOMALY_THRESHOLD,
    ) -> None:
        self._settings = get_settings()
        self._batch_max_size = batch_max_size
        self._batch_timeout_s = batch_timeout_s
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._model_name = model_name
        self._anomaly_threshold = anomaly_threshold

        self._store: VectorStore = vector_store or get_vector_store()
        self._detector: AetherAnomalyDetector = anomaly_detector or get_anomaly_detector()
        self._ctx_buf: LogContextBuffer = context_buffer or get_context_buffer()

        self._client: aioredis.Redis | None = None  # type: ignore[type-arg]
        self._model: object | None = None
        self._stop_event = asyncio.Event()
        self._stats = WorkerStats()
        self._accumulator = MicroBatchAccumulator(
            max_size=batch_max_size,
            timeout_seconds=batch_timeout_s,
        )

        # Track whether we have already triggered baseline training
        self._baseline_training_triggered: bool = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Initialise the Redis client and load the embedding model.

        Must be awaited before calling ``run()``.
        """
        logger.info(
            "VectorProcessorWorker starting | group=%s consumer=%s model=%s",
            self._consumer_group,
            self._consumer_name,
            self._model_name,
        )

        # ── Redis connection ──────────────────────────────────────────────────
        self._client = aioredis.Redis(
            host=self._settings.redis_host,
            port=self._settings.redis_port,
            db=self._settings.redis_db,
            password=self._settings.redis_password,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=30,
            retry_on_timeout=True,
        )
        await self._client.ping()
        logger.info(
            "Redis connected | host=%s port=%d",
            self._settings.redis_host,
            self._settings.redis_port,
        )

        # ── Consumer group (idempotent) ───────────────────────────────────────
        stream_name = self._settings.redis_stream_name
        try:
            await self._client.xgroup_create(
                name=stream_name,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Consumer group created | stream=%s group=%s",
                stream_name,
                self._consumer_group,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.info(
                    "Consumer group already exists | stream=%s group=%s",
                    stream_name,
                    self._consumer_group,
                )
            else:
                raise

        # ── Load embedding model ──────────────────────────────────────────────
        loop = asyncio.get_running_loop()
        self._model = await loop.run_in_executor(
            None, _load_sentence_transformer, self._model_name
        )

        self._stats.start_time = time.monotonic()
        logger.info("VectorProcessorWorker fully initialised — entering run loop.")

    async def run(self) -> None:
        """
        Main polling loop.

        Reads from the stream, batches, embeds, scores for anomaly, stores,
        and publishes incidents.  Exits cleanly when ``stop()`` is called.
        """
        if self._client is None or self._model is None:
            raise RuntimeError("Worker has not been started. Call start() first.")

        stream_name = self._settings.redis_stream_name
        stats_last_reported = time.monotonic()

        logger.info(
            "Entering main consumer loop | stream=%s batch_size=%d timeout=%.2fs "
            "anomaly_threshold=%.2f",
            stream_name,
            self._batch_max_size,
            self._batch_timeout_s,
            self._anomaly_threshold,
        )

        while not self._stop_event.is_set():
            try:
                await self._poll_once(stream_name)
            except Exception as exc:
                logger.error(
                    "Unhandled error in consumer loop | error=%s",
                    exc,
                    exc_info=True,
                )
                await asyncio.sleep(1.0)

            if self._accumulator.is_ready():
                await self._flush_batch()

            # Periodic statistics
            if time.monotonic() - stats_last_reported >= _STATS_INTERVAL_SECONDS:
                logger.info("📊 WORKER STATS | %s", self._stats.summary())
                logger.info("📦 STORE  STATS | %s", self._store.stats())
                logger.info(
                    "🔬 DETECTOR STATS | %s", self._detector.diagnostics()
                )
                logger.info(
                    "📋 BUFFER  STATS | sizes=%s",
                    self._ctx_buf.buffer_sizes(),
                )
                stats_last_reported = time.monotonic()

        # Drain partial batch on clean shutdown
        if self._accumulator.pending_count > 0:
            logger.info(
                "Draining %d pending entries before shutdown.",
                self._accumulator.pending_count,
            )
            await self._flush_batch()

        logger.info("Consumer loop exited. Final stats | %s", self._stats.summary())

    async def stop(self) -> None:
        """Signal the run loop to stop after the current iteration."""
        logger.info("VectorProcessorWorker stop requested.")
        self._stop_event.set()

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._client is not None:
            await self._client.aclose()
            logger.info("Redis connection closed.")

    # ── Internal polling ───────────────────────────────────────────────────────

    async def _poll_once(self, stream_name: str) -> None:
        """
        Issue a single XREADGROUP call and feed entries to the context buffer
        and the micro-batch accumulator.
        """
        assert self._client is not None

        try:
            response = await self._client.xreadgroup(
                groupname=self._consumer_group,
                consumername=self._consumer_name,
                streams={stream_name: ">"},
                count=_XREAD_COUNT,
                block=_XREAD_BLOCK_MS,
            )
        except Exception as exc:
            logger.error("XREADGROUP failed | error=%s", exc)
            raise

        if not response:
            return

        for _stream, entries in response:
            for stream_id, fields in entries:
                entry = _parse_stream_entry(stream_id, fields)
                if entry is None:
                    continue

                # ── Context buffer: push raw frame BEFORE embedding ───────────
                log_frame: dict[str, object] = {
                    "service_name": entry.service_name,
                    "timestamp": entry.timestamp,
                    "level": entry.level,
                    "message": entry.message,
                    "stream_id": entry.stream_id,
                    "metadata": entry.metadata,
                }
                self._ctx_buf.append(entry.service_name, log_frame)

                self._accumulator.add(entry)

                if self._accumulator.is_ready():
                    await self._flush_batch()

    async def _flush_batch(self) -> None:
        """
        Process a micro-batch through the full Day 3 pipeline:
          Normalise → Embed → Store → Score → (Alert?) → Ack
        """
        assert self._client is not None
        assert self._model is not None

        entries = self._accumulator.flush()
        if not entries:
            return

        stream_name = self._settings.redis_stream_name
        batch_size = len(entries)
        logger.debug("Flushing batch | size=%d", batch_size)

        # ── Step 1: Regex Normalisation ───────────────────────────────────────
        raw_messages = [e.message for e in entries]
        try:
            norm_results: list[NormalizationResult] = normalize_batch(raw_messages)
        except Exception as exc:
            logger.error("Normalisation failed | error=%s", exc, exc_info=True)
            self._stats.messages_failed += batch_size
            return

        normalised_texts = [r.normalized for r in norm_results]

        # ── Step 2: Sentence Embedding (thread pool) ──────────────────────────
        t_embed_start = time.monotonic()
        loop = asyncio.get_running_loop()
        try:
            embeddings: np.ndarray = await loop.run_in_executor(
                None, _encode_batch, self._model, normalised_texts
            )
        except Exception as exc:
            logger.error("Embedding failed | error=%s", exc, exc_info=True)
            self._stats.messages_failed += batch_size
            return

        embed_elapsed = time.monotonic() - t_embed_start
        self._stats.total_embed_time_s += embed_elapsed

        # ── Step 3: Write to Vector Store ─────────────────────────────────────
        records: list[VectorRecord] = []
        for i, entry in enumerate(entries):
            records.append(
                VectorRecord(
                    vector=embeddings[i],
                    raw_message=entry.message,
                    normalized_msg=normalised_texts[i],
                    timestamp=entry.timestamp,
                    service_name=entry.service_name,
                    log_level=entry.level,
                    stream_id=entry.stream_id,
                )
            )

        try:
            self._store.add_batch(records)
        except Exception as exc:
            logger.error("Vector store write failed | error=%s", exc, exc_info=True)
            self._stats.messages_failed += batch_size
            return

        # ── Step 4: Train baseline if threshold is met (once) ─────────────────
        if not self._baseline_training_triggered and not self._detector.is_trained:
            await self._try_trigger_baseline_training(loop)

        # ── Step 5: Anomaly scoring per message ───────────────────────────────
        for i, entry in enumerate(entries):
            anomaly_score = self._detector.score_vector(embeddings[i])
            is_anom = self._detector.is_anomaly(
                embeddings[i], threshold=self._anomaly_threshold
            )

            if is_anom:
                self._stats.anomalies_detected += 1
                await self._publish_incident(
                    entry=entry,
                    normalised_msg=normalised_texts[i],
                    anomaly_score=anomaly_score,
                )

        # ── Step 6: Acknowledge in Redis ──────────────────────────────────────
        ids_to_ack = [e.stream_id for e in entries]
        try:
            await self._client.xack(stream_name, self._consumer_group, *ids_to_ack)
            self._stats.messages_acknowledged += len(ids_to_ack)
        except Exception as exc:
            logger.warning("XACK failed | ids=%s error=%s", ids_to_ack, exc)

        # ── Stats ─────────────────────────────────────────────────────────────
        self._stats.batches_processed += 1
        self._stats.messages_embedded += batch_size

        logger.info(
            "Batch processed | size=%d embed_time=%.3fs store_total=%d "
            "detector_trained=%s",
            batch_size,
            embed_elapsed,
            self._store.size,
            self._detector.is_trained,
        )

    async def _try_trigger_baseline_training(
        self, loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Check if enough embeddings have accumulated to train the baseline.

        If the vector store has ≥ ``_BASELINE_TRAIN_THRESHOLD`` vectors,
        snapshot the matrix and kick off training in a thread pool executor.
        This is a one-shot operation per worker instance.
        """
        current_size = self._store.size
        if current_size < _BASELINE_TRAIN_THRESHOLD:
            logger.debug(
                "Baseline training gate: store_size=%d < threshold=%d — waiting",
                current_size,
                _BASELINE_TRAIN_THRESHOLD,
            )
            return

        # Mark triggered immediately to prevent concurrent re-entry
        self._baseline_training_triggered = True

        logger.info(
            "Baseline training gate triggered | store_size=%d threshold=%d",
            current_size,
            _BASELINE_TRAIN_THRESHOLD,
        )

        # Snapshot the matrix — O(N) copy outside the lock
        matrix, _ = self._store.snapshot()

        try:
            await loop.run_in_executor(
                None, _train_baseline_sync, self._detector, matrix
            )
            logger.info(
                "✅ Isolation Forest baseline armed | samples=%d",
                matrix.shape[0],
            )
        except Exception as exc:
            # Non-fatal: the worker continues; training will not be retried
            # in this session (to avoid infinite retry storms).
            logger.error(
                "Baseline training failed | error=%s — detector remains untrained",
                exc,
                exc_info=True,
            )

    async def _publish_incident(
        self,
        entry: ParsedEntry,
        normalised_msg: str,
        anomaly_score: float,
    ) -> None:
        """
        Build an Incident Context Frame and push it to ``incident_alerts_stream``.

        The frame contains:
          - The anomalous event's full metadata and anomaly score.
          - The last ``_CONTEXT_WINDOW_SIZE`` preceding logs from the same
            service, enabling Root Cause Analysis without external querying.

        Args:
            entry:          The ParsedEntry for the anomalous event.
            normalised_msg: The regex-normalised template string.
            anomaly_score:  Float in [0, 1] from the Isolation Forest.
        """
        assert self._client is not None

        # Pull causal context window (preceding events for this service)
        context_window = self._ctx_buf.get_context_frame(
            entry.service_name, window_size=_CONTEXT_WINDOW_SIZE
        )

        incident_frame: dict[str, str] = {
            "stream_id": entry.stream_id,
            "service_name": entry.service_name,
            "timestamp": entry.timestamp,
            "level": entry.level,
            "raw_message": entry.message,
            "normalized_message": normalised_msg,
            "anomaly_score": f"{anomaly_score:.6f}",
            "anomaly_threshold": f"{self._anomaly_threshold:.4f}",
            "context_window_size": str(len(context_window)),
            "context_window": json.dumps(context_window),
            "metadata": json.dumps(entry.metadata),
            "detected_at": str(time.time()),
        }

        logger.warning(
            "[ANOMALY_DETECTED] service=%s level=%s score=%.4f "
            "stream_id=%s message=%r context_events=%d",
            entry.service_name,
            entry.level,
            anomaly_score,
            entry.stream_id,
            entry.message[:80],
            len(context_window),
        )

        try:
            incident_id = await self._client.xadd(
                name=_INCIDENT_STREAM_NAME,
                fields=incident_frame,
                maxlen=_INCIDENT_STREAM_MAXLEN,
                approximate=True,
            )
            self._stats.incidents_published += 1
            logger.info(
                "Incident published | incident_id=%s anomaly_score=%.4f service=%s",
                incident_id,
                anomaly_score,
                entry.service_name,
            )
        except Exception as exc:
            logger.error(
                "Failed to publish incident to stream | error=%s", exc, exc_info=True
            )


# =============================================================================
# Entrypoint
# =============================================================================


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="aether-vector-worker",
        description="AetherSRE — Redis Stream consumer, vector embedding & anomaly detection worker.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("WORKER_BATCH_SIZE", str(_DEFAULT_BATCH_MAX_SIZE))),
        help="Flush batch when this many messages are accumulated.",
    )
    parser.add_argument(
        "--batch-timeout",
        type=float,
        default=float(os.getenv("WORKER_BATCH_TIMEOUT", str(_DEFAULT_BATCH_TIMEOUT_SECONDS))),
        help="Flush batch after this many seconds even if not full.",
    )
    parser.add_argument(
        "--consumer-group",
        default=os.getenv("WORKER_CONSUMER_GROUP", _DEFAULT_CONSUMER_GROUP),
        help="Redis consumer group name.",
    )
    parser.add_argument(
        "--consumer-name",
        default=os.getenv("WORKER_CONSUMER_NAME", f"{_DEFAULT_CONSUMER_NAME_PREFIX}-0"),
        help="Unique consumer name within the group.",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("SENTENCE_TRANSFORMER_MODEL", _DEFAULT_MODEL_NAME),
        help="Sentence Transformer model identifier.",
    )
    parser.add_argument(
        "--anomaly-threshold",
        type=float,
        default=float(os.getenv("ANOMALY_THRESHOLD", str(_DEFAULT_ANOMALY_THRESHOLD))),
        help="Anomaly score threshold above which incidents are fired (0.0–1.0).",
    )
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> None:
    """Top-level async entrypoint."""
    settings = get_settings()
    configure_logging(level=settings.log_level)

    logger.info("=" * 64)
    logger.info("  AetherSRE Vector Processor & Anomaly Detection Worker")
    logger.info("  Redis  : %s:%d/%d", settings.redis_host, settings.redis_port, settings.redis_db)
    logger.info("  Ingest : %s", settings.redis_stream_name)
    logger.info("  Alerts : %s", _INCIDENT_STREAM_NAME)
    logger.info("  Group  : %s", args.consumer_group)
    logger.info("  Model  : %s", args.model)
    logger.info("  Batch  : size=%d timeout=%.2fs", args.batch_size, args.batch_timeout)
    logger.info("  Thresh : anomaly=%.2f", args.anomaly_threshold)
    logger.info("  Gate   : train_threshold=%d vectors", _BASELINE_TRAIN_THRESHOLD)
    logger.info("=" * 64)

    worker = VectorProcessorWorker(
        batch_max_size=args.batch_size,
        batch_timeout_s=args.batch_timeout,
        consumer_group=args.consumer_group,
        consumer_name=args.consumer_name,
        model_name=args.model,
        anomaly_threshold=args.anomaly_threshold,
    )

    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info("Signal %s received — requesting worker stop.", sig.name)
        asyncio.create_task(worker.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            pass  # Windows

    try:
        await worker.start()
        await worker.run()
    except Exception as exc:
        logger.critical("Fatal worker error | %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        await worker.close()
        logger.info("Worker shutdown complete.")


def main() -> None:
    """Module entry point — called by ``python -m app.workers.vector_processor``."""
    args = _parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()

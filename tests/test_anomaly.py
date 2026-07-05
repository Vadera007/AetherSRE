"""
AetherSRE — Day 3 Unit & Integration Tests: Anomaly Detection Pipeline
=====================================================================
Validates:
  1. AetherAnomalyDetector baseline training, threshold checking, and score scaling.
  2. LogContextBuffer thread-safe isolation, ring buffer eviction, and context retrieval.
  3. Integration pipeline tracing: raw event flow -> normalized -> anomaly scored -> incident frame.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
import pytest
import numpy as np
import redis.asyncio as aioredis

from app.core.anomaly_detector import AetherAnomalyDetector
from app.core.context_buffer import LogContextBuffer
from app.core.vector_store import VectorStore, VectorRecord
from app.workers.vector_processor import VectorProcessorWorker, ParsedEntry, _parse_stream_entry

DIM = 384


# =============================================================================
# 1. Anomaly Detector Unit Tests
# =============================================================================

class TestAnomalyDetector:
    """Validates the model initialization, baseline training, and score scaling."""

    def test_initial_state(self) -> None:
        detector = AetherAnomalyDetector(n_estimators=10, random_state=42)
        assert not detector.is_trained
        assert detector.train_sample_count == 0

        # Unfitted model should optimistically return 0.0 anomaly score
        dummy_vector = np.zeros(DIM, dtype=np.float32)
        assert detector.score_vector(dummy_vector) == 0.0
        assert not detector.is_anomaly(dummy_vector, threshold=0.5)

    def test_train_baseline_insufficient_samples_raises(self) -> None:
        detector = AetherAnomalyDetector(n_estimators=10)
        # Minimum training samples is 10. Let's give it 5.
        bad_matrix = np.random.randn(5, DIM).astype(np.float32)
        with pytest.raises(ValueError, match="Need at least 10 samples to train"):
            detector.train_baseline(bad_matrix)

    def test_train_baseline_and_scoring(self) -> None:
        detector = AetherAnomalyDetector(n_estimators=50, random_state=42)

        # Generate mock "normal" logs (clumped around origin)
        normal_data = np.random.normal(loc=0.0, scale=0.1, size=(50, DIM)).astype(np.float32)
        detector.train_baseline(normal_data)

        assert detector.is_trained
        assert detector.train_sample_count == 50

        # Test scoring a "normal" vector (close to training cluster)
        normal_query = np.random.normal(loc=0.0, scale=0.05, size=DIM).astype(np.float32)
        score_normal = detector.score_vector(normal_query)

        # Test scoring an outlier vector (far from cluster)
        outlier_query = np.random.normal(loc=10.0, scale=0.5, size=DIM).astype(np.float32)
        score_outlier = detector.score_vector(outlier_query)

        # Under the sigmoid scaling: outlier score should be strictly greater than normal score
        # and outlier should be flagged as anomaly, while normal should not.
        assert score_outlier > score_normal
        assert score_outlier >= 0.0 and score_outlier <= 1.0
        assert score_normal >= 0.0 and score_normal <= 1.0

        assert detector.is_anomaly(outlier_query, threshold=0.5)
        # Normal query should not be an anomaly at a high threshold
        assert not detector.is_anomaly(normal_query, threshold=0.8)

    def test_invalid_anomaly_threshold(self) -> None:
        detector = AetherAnomalyDetector(n_estimators=10)
        dummy_vector = np.zeros(DIM, dtype=np.float32)
        with pytest.raises(ValueError, match="threshold must be in"):
            detector.is_anomaly(dummy_vector, threshold=1.2)


# =============================================================================
# 2. Context Buffer Unit Tests
# =============================================================================

class TestLogContextBuffer:
    """Validates thread-safe service isolation and context window retrieval."""

    def test_sliding_window_basic_operations(self) -> None:
        buf = LogContextBuffer(maxlen=5)

        # Populate a single service
        for i in range(7):
            buf.append("auth-service", {"index": i, "message": f"Log {i}"})

        # Check total frames (capped by maxlen=5)
        assert buf.total_frames() == 5

        # Fetch context window of 3
        ctx = buf.get_context_frame("auth-service", window_size=3)
        assert len(ctx) == 3
        # Chronological oldest-first ordering: elements should be index 4, 5, 6
        assert ctx[0]["index"] == 4
        assert ctx[1]["index"] == 5
        assert ctx[2]["index"] == 6

    def test_service_isolation(self) -> None:
        buf = LogContextBuffer(maxlen=10)
        buf.append("auth-service", {"msg": "auth log"})
        buf.append("payment-gateway", {"msg": "payment log"})

        assert len(buf.get_context_frame("auth-service")) == 1
        assert buf.get_context_frame("auth-service")[0]["msg"] == "auth log"

        assert len(buf.get_context_frame("payment-gateway")) == 1
        assert buf.get_context_frame("payment-gateway")[0]["msg"] == "payment log"

        # Querying an unknown service should safely return empty list
        assert buf.get_context_frame("non-existent-service") == []

    def test_concurrent_writes(self) -> None:
        """Ensure concurrent calls to append do not cause state corruption."""
        buf = LogContextBuffer(maxlen=500)
        num_threads = 5
        writes_per_thread = 100

        def _worker(thread_id: int) -> None:
            for i in range(writes_per_thread):
                buf.append(f"svc-{thread_id}", {"index": i})

        threads = [threading.Thread(target=_worker, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert buf.total_frames() == num_threads * writes_per_thread
        for t in range(num_threads):
            assert len(buf.get_context_frame(f"svc-{t}", window_size=writes_per_thread)) == writes_per_thread


# =============================================================================
# 3. Integration Pipeline Tests
# =============================================================================

@dataclass
class MockRedisClient:
    """Mock async Redis client targeting the XADD / XACK methods."""
    xadds: list[dict] = field(default_factory=list)
    xacks: list[str] = field(default_factory=list)

    async def xadd(self, name: str, fields: dict, maxlen: int | None = None, approximate: bool = True) -> str:
        self.xadds.append({"stream": name, "fields": fields})
        return f"1718000000000-{len(self.xadds)}"

    async def xack(self, name: str, group: str, *ids: str) -> int:
        self.xacks.extend(ids)
        return len(ids)

    async def ping(self) -> bool:
        return True

    async def xgroup_create(self, name: str, groupname: str, id: str = "0", mkstream: bool = True) -> bool:
        return True

    async def aclose(self) -> None:
        pass


class TestPipelineIntegration:
    """Verifies that an anomalous vector processed by the worker emits alert frames."""

    @pytest.mark.asyncio
    async def test_worker_anomaly_detection_flow(self) -> None:
        # 1. Setup mock components
        mock_redis = MockRedisClient()
        store = VectorStore(dim=DIM, max_capacity=200)
        detector = AetherAnomalyDetector(n_estimators=10, random_state=42)
        ctx_buf = LogContextBuffer(maxlen=10)

        # Pre-train the detector so it scores anomalies
        normal_data = np.random.normal(loc=0.0, scale=0.05, size=(20, DIM)).astype(np.float32)
        detector.train_baseline(normal_data)

        # 2. Instantiate worker (substituting dependencies)
        worker = VectorProcessorWorker(
            batch_max_size=2,
            batch_timeout_s=0.1,
            vector_store=store,
            anomaly_detector=detector,
            context_buffer=ctx_buf,
            anomaly_threshold=0.55
        )
        # Direct client injection for verification
        worker._client = mock_redis
        # Mock transformer model wrapper
        class MockModel:
            def encode(self, texts, **kwargs):
                # Returns 0.0 coordinates for normal messages, 5.0 for anomaly keyword
                res = []
                for t in texts:
                    if "ANOMALY" in t:
                        res.append(np.ones(DIM, dtype=np.float32) * 5.0)
                    else:
                        res.append(np.zeros(DIM, dtype=np.float32))
                return np.array(res, dtype=np.float32)

        worker._model = MockModel()

        # 3. Simulate inbound logs in stream representation
        parsed_entries = [
            ParsedEntry(
                stream_id="1718000000000-1",
                service_name="payment-gateway",
                timestamp="2024-01-15T12:00:00.000Z",
                level="INFO",
                message="正常交易正常交易 Normal transaction",
                metadata={"trace_id": "tx-1"}
            ),
            ParsedEntry(
                stream_id="1718000000000-2",
                service_name="payment-gateway",
                timestamp="2024-01-15T12:00:01.000Z",
                level="CRITICAL",
                message="ANOMALY! Connection reset by foreign database peer",
                metadata={"trace_id": "tx-2"}
            )
        ]

        # Populate context buffer prior to processing
        for entry in parsed_entries:
            log_frame = {
                "service_name": entry.service_name,
                "timestamp": entry.timestamp,
                "level": entry.level,
                "message": entry.message,
                "stream_id": entry.stream_id,
                "metadata": entry.metadata
            }
            ctx_buf.append(entry.service_name, log_frame)
            worker._accumulator.add(entry)

        # 4. Trigger flush batch to run normalizer, embedder, and anomaly scorer
        await worker._flush_batch()

        # Verify stream insertions and ACKs
        assert len(mock_redis.xacks) == 2  # Both stream messages acknowledged
        assert mock_redis.xacks == ["1718000000000-1", "1718000000000-2"]

        # An anomaly should have been published to the incident alerts stream
        assert len(mock_redis.xadds) == 1
        incident = mock_redis.xadds[0]
        assert incident["stream"] == "incident_alerts_stream"

        fields = incident["fields"]
        assert fields["service_name"] == "payment-gateway"
        assert "ANOMALY!" in fields["raw_message"]
        assert float(fields["anomaly_score"]) > 0.55

        # Verify that context window captures preceding log frames
        context_window = json.loads(fields["context_window"])
        assert len(context_window) == 2
        # First context element is the oldest log in the window
        assert "Normal transaction" in context_window[0]["message"]

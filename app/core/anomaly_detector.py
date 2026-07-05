"""
AetherSRE — Unsupervised Anomaly Detection Engine
===================================================
Wraps scikit-learn's Isolation Forest in a thread-safe, lifecycle-aware
class that integrates cleanly into the streaming pipeline.

Design rationale
----------------
Isolation Forest was chosen for Day 3 because:

  1. **Unsupervised** — we have no labelled anomaly data from the log
     simulator; we must learn "normal" from unlabelled operational logs.

  2. **O(n log n) training, O(log n) inference** — trivially fast on CPU
     even for hundreds of thousands of 384-dim embeddings.

  3. **Model-free distribution assumption** — works well on high-dimensional
     embedding spaces where Gaussian assumptions break down.

  4. **Contamination="auto"** — the sklearn heuristic sets the contamination
     fraction automatically from the training data, avoiding manual tuning.

Score transformation
--------------------
``IsolationForest.decision_function()`` returns raw anomaly scores in an
unbounded range approximately centred on 0.  Negative values are more
anomalous; positive values are more normal.  We map this to [0, 1]:

    raw_score = decision_function(x)           # e.g., -0.3 … +0.3
    anomaly_score = sigmoid(-raw_score * 6)    # invert + amplify

The sigmoid with scale factor 6 converts the raw score into a smooth
probability-like value where:
  • raw ≪ 0 (very anomalous) → anomaly_score ≈ 1.0
  • raw ≈ 0 (boundary)       → anomaly_score ≈ 0.5
  • raw ≫ 0 (very normal)    → anomaly_score ≈ 0.0

This transformation is monotone, differentiable, and bounded — safe for
downstream threshold comparisons.

Thread-safety model
-------------------
A single ``threading.RLock`` serialises both ``train_baseline()`` and
``score_vector()`` calls.  This prevents a race condition where:
  • The baseline training replaces the internal model while a scoring
    call is in the middle of its ``decision_function()`` computation.

``is_anomaly()`` delegates to ``score_vector()`` and inherits its safety.
"""

from __future__ import annotations

import math
import threading
from typing import Final

import numpy as np
from numpy.typing import NDArray
from sklearn.ensemble import IsolationForest

from app.core.logging_config import get_logger

logger = get_logger(__name__)

# The sigmoid scale factor controls decision boundary sharpness.
# 6 maps the typical ±0.15 decision_function range to 0.09–0.91.
_SIGMOID_SCALE: Final[float] = 6.0

# Minimum training samples required before the model can be fitted.
# Isolation Forest with n_estimators=100 needs at least this many
# samples to build statistically meaningful trees.
_MIN_TRAIN_SAMPLES: Final[int] = 10


def _sigmoid(x: float) -> float:
    """
    Numerically stable sigmoid: σ(x) = 1 / (1 + exp(-x)).

    Args:
        x: Input scalar.

    Returns:
        Value in (0, 1).
    """
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    # Numerically stable form for large negative x
    ex = math.exp(x)
    return ex / (1.0 + ex)


class AetherAnomalyDetector:
    """
    Unsupervised anomaly detection engine backed by Isolation Forest.

    Lifecycle::

        detector = AetherAnomalyDetector()

        # Called once when enough normal logs have been collected.
        detector.train_baseline(normal_embeddings)

        # Called per log event in the streaming loop.
        score = detector.score_vector(embedding)   # float in [0, 1]
        if detector.is_anomaly(embedding):
            # ... fire alert ...

    The detector is safe to share across threads: all public methods
    acquire the internal RLock before touching the model.

    Args:
        n_estimators:  Number of isolation trees (default: 100).
        contamination: Fraction of expected outliers or "auto"
                       (default: "auto" — sklearn heuristic).
        random_state:  RNG seed for reproducibility (default: 42).
        n_jobs:        Parallelism for tree building (-1 = all CPUs).
    """

    def __init__(
        self,
        n_estimators: int = 100,
        contamination: float | str = "auto",
        random_state: int = 42,
        n_jobs: int = -1,
    ) -> None:
        self._n_estimators = n_estimators
        self._contamination = contamination
        self._random_state = random_state
        self._n_jobs = n_jobs

        self._lock = threading.RLock()
        self._is_trained: bool = False
        self._model: IsolationForest = self._build_model()

        # Track training corpus metadata for diagnostics
        self._train_sample_count: int = 0
        self._train_dim: int = 0

    # ── Private helpers ────────────────────────────────────────────────────────

    def _build_model(self) -> IsolationForest:
        """Construct a fresh, unfitted Isolation Forest instance."""
        return IsolationForest(
            n_estimators=self._n_estimators,
            contamination=self._contamination,
            random_state=self._random_state,
            n_jobs=self._n_jobs,
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    @property
    def is_trained(self) -> bool:
        """
        True if ``train_baseline()`` has been successfully called at least once.

        Thread-safe read — acquires the lock.
        """
        with self._lock:
            return self._is_trained

    @property
    def train_sample_count(self) -> int:
        """Number of samples used in the most recent baseline training run."""
        with self._lock:
            return self._train_sample_count

    def train_baseline(self, embeddings: NDArray[np.float32]) -> None:
        """
        Fit the Isolation Forest on a corpus of "normal" operational embeddings.

        This is a **blocking, CPU-bound** call.  It should be invoked from a
        thread pool executor when called from an asyncio context:

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, detector.train_baseline, matrix)

        The training is atomic with respect to concurrent ``score_vector()``
        calls: the lock is held for the entire fit duration, so in-flight
        scoring calls will block until training completes.

        Args:
            embeddings: Float32 array of shape ``(N, D)`` where N is the
                        number of normal log embeddings and D is the embedding
                        dimension (384 for all-MiniLM-L6-v2).

        Raises:
            ValueError: If ``embeddings`` has fewer than ``_MIN_TRAIN_SAMPLES``
                        rows or is not 2-dimensional.
        """
        arr = np.asarray(embeddings, dtype=np.float32)

        if arr.ndim != 2:
            raise ValueError(
                f"embeddings must be 2-dimensional, got shape {arr.shape}"
            )
        if arr.shape[0] < _MIN_TRAIN_SAMPLES:
            raise ValueError(
                f"Need at least {_MIN_TRAIN_SAMPLES} samples to train, "
                f"got {arr.shape[0]}"
            )

        n_samples, dim = arr.shape
        logger.info(
            "Training Isolation Forest baseline | samples=%d dim=%d "
            "n_estimators=%d contamination=%s",
            n_samples,
            dim,
            self._n_estimators,
            self._contamination,
        )

        # Build a fresh model — avoids warm_start complexity and ensures
        # a clean fit even if called repeatedly for rolling retraining.
        fresh_model = self._build_model()

        import time  # noqa: PLC0415
        t0 = time.monotonic()

        with self._lock:
            fresh_model.fit(arr)
            self._model = fresh_model
            self._is_trained = True
            self._train_sample_count = n_samples
            self._train_dim = dim

        elapsed = time.monotonic() - t0
        logger.info(
            "Isolation Forest baseline trained | samples=%d elapsed=%.3fs",
            n_samples,
            elapsed,
        )

    def score_vector(self, embedding: NDArray[np.float32]) -> float:
        """
        Compute the anomaly probability score for a single embedding vector.

        The raw ``decision_function`` output is mapped through an inverted,
        amplified sigmoid to produce an intuitive score in [0, 1]:

            • 0.0 — perfectly normal (core of the learned distribution)
            • 0.5 — decision boundary (ambiguous)
            • 1.0 — extreme outlier

        If the model has not been trained yet, returns ``0.0`` (optimistic
        default — assume normal until baseline is established).

        Args:
            embedding: Float32 vector of shape ``(D,)`` or ``(1, D)``.

        Returns:
            Anomaly score as a float in [0.0, 1.0].
        """
        with self._lock:
            if not self._is_trained:
                return 0.0

            vec = np.asarray(embedding, dtype=np.float32)

            # Ensure 2-D input: IsolationForest.decision_function expects (N, D)
            if vec.ndim == 1:
                vec = vec.reshape(1, -1)

            # decision_function returns shape (1,); negative = more anomalous
            raw: float = float(self._model.decision_function(vec)[0])

        # Invert and amplify: anomalous (raw < 0) → high score
        anomaly_score: float = _sigmoid(-raw * _SIGMOID_SCALE)
        return float(np.clip(anomaly_score, 0.0, 1.0))

    def is_anomaly(
        self,
        embedding: NDArray[np.float32],
        threshold: float = 0.55,
    ) -> bool:
        """
        Return True if the embedding's anomaly score exceeds the threshold.

        A threshold of 0.55 is slightly above the 0.5 decision boundary,
        reducing false positives while remaining sensitive to genuine outliers.

        Args:
            embedding:  Float32 vector of shape ``(D,)`` or ``(1, D)``.
            threshold:  Score above which the event is classified as anomalous.
                        Must be in (0.0, 1.0].

        Returns:
            True if ``score_vector(embedding) > threshold``, else False.
        """
        if not (0.0 < threshold <= 1.0):
            raise ValueError(
                f"threshold must be in (0.0, 1.0], got {threshold}"
            )
        score = self.score_vector(embedding)
        return score > threshold

    def diagnostics(self) -> dict[str, object]:
        """
        Return a snapshot of internal state for monitoring and debugging.

        Returns:
            Dict with keys: is_trained, train_sample_count, train_dim,
            n_estimators, contamination, random_state.
        """
        with self._lock:
            return {
                "is_trained": self._is_trained,
                "train_sample_count": self._train_sample_count,
                "train_dim": self._train_dim,
                "n_estimators": self._n_estimators,
                "contamination": str(self._contamination),
                "random_state": self._random_state,
            }


# =============================================================================
# Module-level singleton
# =============================================================================

_GLOBAL_DETECTOR: AetherAnomalyDetector | None = None


def get_anomaly_detector() -> AetherAnomalyDetector:
    """
    Return the process-global AetherAnomalyDetector, creating it on first call.

    The singleton is initialised lazily so that importing this module in
    tests (which don't want IsolationForest overhead) is zero-cost until
    the detector is actually needed.
    """
    global _GLOBAL_DETECTOR  # noqa: PLW0603
    if _GLOBAL_DETECTOR is None:
        _GLOBAL_DETECTOR = AetherAnomalyDetector()
    return _GLOBAL_DETECTOR


def reset_anomaly_detector() -> None:
    """
    Destroy and reset the global detector.

    Intended for use in tests only — never call in production code.
    """
    global _GLOBAL_DETECTOR  # noqa: PLW0603
    _GLOBAL_DETECTOR = None

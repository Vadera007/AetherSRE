"""
AetherSRE — Thread-Safe Sliding Window Log Context Buffer
==========================================================
Provides a per-service rotating ring buffer that retains the most recent
N telemetry packets for each microservice.  When an anomaly is detected,
the buffer delivers the causal context window — the logs that *preceded*
the anomalous event — enabling Root Cause Analysis (RCA) without any
external storage.

Design
------

Why a deque, not a list?
  ``collections.deque(maxlen=50)`` provides O(1) append-and-evict semantics.
  A plain list would require O(N) left-shift on every append once capacity
  is reached.  At our target rate of 100s msgs/s this matters.

Why per-service isolation?
  Mixing log frames from payment-gateway and auth-service into a single
  buffer would corrupt the causal chain: an auth anomaly would have
  payment logs in its context window, which is analytically useless.

Why a dict of deques rather than a class per service?
  The set of services is dynamic — we can't pre-instantiate them.
  A ``defaultdict``-like approach with lazy initialisation scales to
  any number of services discovered at runtime from the stream.

Thread-safety
  All mutations (``append``) and reads (``get_context_frame``) acquire
  a single shared ``threading.Lock``.  This is safe because the critical
  section is short: a deque append is O(1) and list slicing is O(K).

Immutability contract
  ``get_context_frame()`` returns a **shallow copy** of the deque slice
  as a plain list.  The caller owns that list and may mutate it freely
  without affecting the buffer state.  The dicts inside the list are
  references (not deep copies) — callers must not mutate them.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Final

from app.core.logging_config import get_logger

logger = get_logger(__name__)

_DEFAULT_DEQUE_MAXLEN: Final[int] = 50
_DEFAULT_WINDOW_SIZE: Final[int] = 5


class LogContextBuffer:
    """
    Thread-safe per-service sliding window buffer of raw log frame dicts.

    Each "log frame" is a plain Python dict containing the structured
    fields of one telemetry event:

        {
            "service_name": "payment-gateway",
            "timestamp":    "2024-01-15T12:34:56.789Z",
            "level":        "ERROR",
            "message":      "Transaction timed out after 5s",
            "stream_id":    "1718000000000-0",
            "normalized":   "Transaction timed out after <DURATION>",
            "metadata":     {...},
        }

    Frames are stored in arrival order; the most recent frame is at the
    right end of the deque (``deque[-1]``).

    Args:
        maxlen: Maximum number of frames retained per service before the
                oldest frame is silently evicted.

    Usage::

        buf = LogContextBuffer(maxlen=50)
        buf.append("payment-gateway", {"timestamp": "...", "level": "INFO", ...})
        ctx = buf.get_context_frame("payment-gateway", window_size=5)
        # ctx is a list of the last 5 frames, oldest first
    """

    def __init__(self, maxlen: int = _DEFAULT_DEQUE_MAXLEN) -> None:
        if maxlen <= 0:
            raise ValueError(f"maxlen must be positive, got {maxlen}")

        self._maxlen = maxlen
        self._lock = threading.Lock()
        # Dict mapping service_name → deque of log frame dicts
        self._buffers: dict[str, deque[dict[str, object]]] = {}

    # ── Write ──────────────────────────────────────────────────────────────────

    def append(self, service_name: str, log_frame: dict[str, object]) -> None:
        """
        Append a telemetry log frame to the named service's ring buffer.

        If the service has not been seen before, its deque is created
        automatically with the configured ``maxlen``.  Once the buffer is
        full, the oldest frame is silently evicted (standard deque behaviour).

        Args:
            service_name: Unique identifier for the microservice.
            log_frame:    Dict of structured log fields.  May contain any
                          keys; the buffer stores whatever it receives.
        """
        with self._lock:
            if service_name not in self._buffers:
                self._buffers[service_name] = deque(maxlen=self._maxlen)
                logger.debug(
                    "Context buffer created for new service | service=%s maxlen=%d",
                    service_name,
                    self._maxlen,
                )
            self._buffers[service_name].append(log_frame)

    # ── Read ───────────────────────────────────────────────────────────────────

    def get_context_frame(
        self,
        service_name: str,
        window_size: int = _DEFAULT_WINDOW_SIZE,
    ) -> list[dict[str, object]]:
        """
        Return the last ``window_size`` log frames for the given service.

        The frames are ordered chronologically — oldest at index 0, most
        recent at index ``len(result) - 1``.  This is the natural order for
        displaying a timeline of events leading up to an anomaly.

        If the buffer contains fewer than ``window_size`` frames (e.g. at
        startup), all available frames are returned without error.

        If the service has no buffer at all (never received a frame), an
        empty list is returned.

        Args:
            service_name: Identifier of the microservice to query.
            window_size:  Number of most-recent frames to retrieve.
                          Must be ≥ 1.

        Returns:
            List of at most ``window_size`` frame dicts, ordered oldest→newest.

        Raises:
            ValueError: If ``window_size`` is less than 1.
        """
        if window_size < 1:
            raise ValueError(f"window_size must be ≥ 1, got {window_size}")

        with self._lock:
            buf = self._buffers.get(service_name)
            if buf is None:
                return []

            # Convert to list for slicing, then take the tail.
            # list(deque) is O(N); slicing is O(K).  N ≤ maxlen = 50, K ≤ 5.
            all_frames = list(buf)

        return all_frames[-window_size:]

    # ── Diagnostics ────────────────────────────────────────────────────────────

    def buffer_sizes(self) -> dict[str, int]:
        """
        Return a snapshot mapping each known service to its current buffer size.

        Useful for monitoring dashboards and debugging.

        Returns:
            Dict of ``{service_name: current_frame_count}``.
        """
        with self._lock:
            return {svc: len(buf) for svc, buf in self._buffers.items()}

    def known_services(self) -> list[str]:
        """
        Return a sorted list of service names that have received at least one frame.

        Returns:
            Sorted list of service name strings.
        """
        with self._lock:
            return sorted(self._buffers.keys())

    def clear_service(self, service_name: str) -> bool:
        """
        Clear all frames for a specific service.

        Args:
            service_name: Service whose buffer should be emptied.

        Returns:
            True if the service existed and was cleared; False if unknown.
        """
        with self._lock:
            if service_name in self._buffers:
                self._buffers[service_name].clear()
                return True
            return False

    def total_frames(self) -> int:
        """
        Return the total number of frames across all service buffers.

        Returns:
            Integer count.
        """
        with self._lock:
            return sum(len(buf) for buf in self._buffers.values())


# =============================================================================
# Module-level singleton
# =============================================================================

_GLOBAL_CONTEXT_BUFFER: LogContextBuffer | None = None


def get_context_buffer() -> LogContextBuffer:
    """
    Return the process-global LogContextBuffer, creating it on first call.

    Lazily initialised for the same reason as the vector store singleton.
    """
    global _GLOBAL_CONTEXT_BUFFER  # noqa: PLW0603
    if _GLOBAL_CONTEXT_BUFFER is None:
        _GLOBAL_CONTEXT_BUFFER = LogContextBuffer(maxlen=_DEFAULT_DEQUE_MAXLEN)
    return _GLOBAL_CONTEXT_BUFFER


def reset_context_buffer() -> None:
    """
    Destroy and reset the global context buffer.

    Intended for use in tests only — never call in production code.
    """
    global _GLOBAL_CONTEXT_BUFFER  # noqa: PLW0603
    _GLOBAL_CONTEXT_BUFFER = None

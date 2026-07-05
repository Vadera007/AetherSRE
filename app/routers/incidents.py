"""
AetherSRE — Incident Alert Router
===================================
Exposes HTTP endpoints for inspecting the ``incident_alerts_stream``
in real-time.  This router is the read-side of the anomaly detection
pipeline — it converts Redis Stream entries into structured JSON
responses suitable for a dashboard, on-call UI, or downstream webhook.

Endpoints
---------
  GET /api/v1/incidents/recent
      Returns the 20 most recent Incident Context Frames from the
      ``incident_alerts_stream``, ordered newest-first.

  GET /api/v1/incidents/stats
      Returns aggregate statistics about the incident stream
      (total incidents, stream length, service-level breakdown).

Design notes
------------
- Uses ``XREVRANGE stream + - COUNT N`` which returns entries in
  reverse chronological order (newest first).  This is the most
  natural order for an alert feed.
- The raw Redis stream fields (all strings) are parsed into typed
  Python objects before serialisation, so the caller receives proper
  floats and nested dicts rather than raw JSON-encoded strings.
- All Redis I/O is ``async`` — no blocking calls on the event loop.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.core.redis_client import RedisStreamClient, get_redis_client
from app.core.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/incidents", tags=["incidents"])

_INCIDENT_STREAM_NAME: str = "incident_alerts_stream"
_DEFAULT_COUNT: int = 20
_MAX_COUNT: int = 100


def _parse_incident_entry(stream_id: str, fields: dict[str, str]) -> dict[str, Any]:
    """
    Convert a raw Redis Stream entry into a structured incident dict.

    All fields in a Redis Stream are stored as flat strings.  This function
    deserialises JSON-encoded subfields and coerces numeric strings.

    Args:
        stream_id: The Redis-assigned entry ID (e.g. '1718000000000-0').
        fields:    Flat string dict from XREVRANGE.

    Returns:
        Dict with typed values ready for JSON serialisation.
    """
    # Parse the context window JSON back into a list of dicts
    raw_context = fields.get("context_window", "[]")
    try:
        context_window: list[dict[str, Any]] = json.loads(raw_context)
    except json.JSONDecodeError:
        context_window = []

    # Parse attached metadata JSON
    raw_metadata = fields.get("metadata", "{}")
    try:
        metadata: dict[str, Any] = json.loads(raw_metadata)
    except json.JSONDecodeError:
        metadata = {}

    # Safely coerce numeric strings
    try:
        anomaly_score = float(fields.get("anomaly_score", "0.0"))
    except ValueError:
        anomaly_score = 0.0

    try:
        anomaly_threshold = float(fields.get("anomaly_threshold", "0.55"))
    except ValueError:
        anomaly_threshold = 0.55

    try:
        detected_at = float(fields.get("detected_at", "0.0"))
    except ValueError:
        detected_at = 0.0

    try:
        context_window_size = int(fields.get("context_window_size", "0"))
    except ValueError:
        context_window_size = len(context_window)

    return {
        "incident_id": stream_id,
        "service_name": fields.get("service_name", "unknown"),
        "timestamp": fields.get("timestamp", ""),
        "level": fields.get("level", "UNKNOWN"),
        "raw_message": fields.get("raw_message", ""),
        "normalized_message": fields.get("normalized_message", ""),
        "anomaly_score": anomaly_score,
        "anomaly_threshold": anomaly_threshold,
        "context_window_size": context_window_size,
        "context_window": context_window,
        "metadata": metadata,
        "detected_at": detected_at,
        "source_stream_id": fields.get("stream_id", ""),
    }


@router.get(
    "/recent",
    summary="Retrieve recent incident alerts",
    description=(
        "Query the ``incident_alerts_stream`` and return the most recent "
        "Incident Context Frames in reverse chronological order (newest first). "
        "Each frame includes the anomaly score, the raw and normalised log "
        "message, and a sliding-window context of the preceding operational logs."
    ),
    response_description="List of incident context frames, newest first.",
    status_code=status.HTTP_200_OK,
)
async def get_recent_incidents(
    count: int = Query(
        default=_DEFAULT_COUNT,
        ge=1,
        le=_MAX_COUNT,
        description=f"Number of incidents to return (1–{_MAX_COUNT}).",
    ),
    redis: RedisStreamClient = Depends(get_redis_client),
) -> JSONResponse:
    """
    Return the most recent ``count`` incident context frames.

    Internally executes:
        XREVRANGE incident_alerts_stream + - COUNT <count>

    Returns:
        JSON with ``total``, ``stream``, and ``incidents`` fields.
    """
    if redis._client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis client is not initialised.",
        )

    try:
        raw_entries: list[tuple[str, dict[str, str]]] = (
            await redis._client.xrevrange(  # type: ignore[union-attr]
                name=_INCIDENT_STREAM_NAME,
                max="+",
                min="-",
                count=count,
            )
        )
    except Exception as exc:
        logger.error(
            "XREVRANGE failed | stream=%s error=%s",
            _INCIDENT_STREAM_NAME,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to read from incident stream: {exc}",
        ) from exc

    incidents = [
        _parse_incident_entry(sid, fields) for sid, fields in raw_entries
    ]

    logger.info(
        "GET /incidents/recent | count=%d returned=%d",
        count,
        len(incidents),
    )

    return JSONResponse(
        content={
            "total": len(incidents),
            "stream": _INCIDENT_STREAM_NAME,
            "incidents": incidents,
        }
    )


@router.get(
    "/stats",
    summary="Get incident stream statistics",
    description=(
        "Return aggregate statistics about the incident alert stream, "
        "including total incident count, per-service breakdown, and "
        "severity distribution."
    ),
    status_code=status.HTTP_200_OK,
)
async def get_incident_stats(
    redis: RedisStreamClient = Depends(get_redis_client),
) -> JSONResponse:
    """
    Compute aggregate statistics over the entire incident stream.

    Uses XLEN for the total count and fetches the last 100 entries for
    service/level breakdown (a full scan is avoided for performance).

    Returns:
        JSON with ``stream_length``, ``service_counts``, and ``level_counts``.
    """
    if redis._client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis client is not initialised.",
        )

    try:
        stream_length: int = await redis._client.xlen(_INCIDENT_STREAM_NAME)  # type: ignore[union-attr]
    except Exception:
        stream_length = 0

    # Fetch last 100 for breakdown analysis
    try:
        sample_entries: list[tuple[str, dict[str, str]]] = (
            await redis._client.xrevrange(  # type: ignore[union-attr]
                name=_INCIDENT_STREAM_NAME,
                max="+",
                min="-",
                count=100,
            )
        )
    except Exception:
        sample_entries = []

    service_counts: dict[str, int] = {}
    level_counts: dict[str, int] = {}
    score_sum: float = 0.0
    score_count: int = 0

    for _sid, fields in sample_entries:
        svc = fields.get("service_name", "unknown")
        lvl = fields.get("level", "UNKNOWN")
        service_counts[svc] = service_counts.get(svc, 0) + 1
        level_counts[lvl] = level_counts.get(lvl, 0) + 1
        try:
            score_sum += float(fields.get("anomaly_score", "0"))
            score_count += 1
        except ValueError:
            pass

    avg_score = score_sum / score_count if score_count > 0 else 0.0

    return JSONResponse(
        content={
            "stream": _INCIDENT_STREAM_NAME,
            "stream_length": stream_length,
            "sample_size": len(sample_entries),
            "average_anomaly_score": round(avg_score, 4),
            "service_counts": service_counts,
            "level_counts": level_counts,
        }
    )

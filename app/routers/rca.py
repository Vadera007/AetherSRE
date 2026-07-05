"""
AetherSRE — RCA Router
======================
Exposes HTTP endpoints for inspecting automated root cause analysis reports
read directly from the ``rca_insights_stream``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.redis_client import RedisStreamClient, get_redis_client
from app.core.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/rca", tags=["rca"])

_DEFAULT_COUNT: int = 10
_MAX_COUNT: int = 50


def _parse_rca_entry(stream_id: str, fields: dict[str, str]) -> dict[str, Any]:
    """Parse flat string fields from Redis Stream into structured JSON."""
    return {
        "rca_id": stream_id,
        "incident_id": fields.get("incident_id", "unknown"),
        "service_name": fields.get("service_name", "unknown"),
        "timestamp": fields.get("timestamp", ""),
        "level": fields.get("level", "UNKNOWN"),
        "raw_message": fields.get("raw_message", ""),
        "normalized_message": fields.get("normalized_message", ""),
        "anomaly_score": float(fields.get("anomaly_score", "0.0")),
        "root_cause": fields.get("root_cause", ""),
        "suggested_fix": fields.get("suggested_fix", ""),
        "risk_level": fields.get("risk_level", "LOW"),
        "impact_analysis": fields.get("impact_analysis", ""),
        "generation_time_s": float(fields.get("generation_time_s", "0.0")),
        "analyzed_at": float(fields.get("analyzed_at", "0.0")),
    }


@router.get(
    "/recent",
    summary="Retrieve recent Root Cause Analysis reports",
    description="Query the ``rca_insights_stream`` and return the most recent automated RCA reports.",
    status_code=status.HTTP_200_OK,
)
async def get_recent_rca(
    count: int = Query(
        default=_DEFAULT_COUNT,
        ge=1,
        le=_MAX_COUNT,
        description=f"Number of RCA reports to return (1–{_MAX_COUNT}).",
    ),
    redis: RedisStreamClient = Depends(get_redis_client),
) -> JSONResponse:
    """Return the most recent ``count`` RCA insights."""
    if redis._client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis client is not initialised.",
        )

    settings = get_settings()
    stream_name = settings.rca_insights_stream_name

    try:
        raw_entries = await redis._client.xrevrange(
            name=stream_name,
            max="+",
            min="-",
            count=count,
        )
    except Exception as exc:
        logger.error(
            "XREVRANGE failed | stream=%s error=%s",
            stream_name,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to read from RCA stream: {exc}",
        ) from exc

    reports = [_parse_rca_entry(sid, fields) for sid, fields in raw_entries]

    return JSONResponse(
        content={
            "total": len(reports),
            "stream": stream_name,
            "reports": reports,
        }
    )

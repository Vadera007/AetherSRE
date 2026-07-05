"""
AetherSRE — Remediation History Router
======================================
Exposes endpoints for auditing self-healing execution histories.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.redis_client import RedisStreamClient, get_redis_client
from app.core.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/remediation", tags=["remediation-history"])

_DEFAULT_COUNT: int = 15
_MAX_COUNT: int = 50
_HISTORY_STREAM_NAME: str = "remediation_history_stream"


def _parse_history_entry(stream_id: str, fields: dict[str, str]) -> dict[str, Any]:
    """Parse flat string fields from Redis history stream into structured dict."""
    return {
        "execution_id": stream_id,
        "incident_id": fields.get("incident_id", "unknown"),
        "service_name": fields.get("service_name", "unknown"),
        "action_id": fields.get("action_id", ""),
        "risk_level": fields.get("risk_level", "LOW"),
        "target_command": fields.get("target_command", ""),
        "execution_type": fields.get("execution_type", ""),
        "status": fields.get("status", "UNKNOWN"),
        "stdout": fields.get("stdout", ""),
        "stderr": fields.get("stderr", ""),
        "duration_s": float(fields.get("duration_s", "0.0")),
        "executed_by": fields.get("executed_by", "System"),
        "timestamp": float(fields.get("timestamp", "0.0")),
    }


@router.get(
    "/history",
    summary="Retrieve recent remediation execution history",
    description="Query the ``remediation_history_stream`` and return the most recent mitigation runs.",
    status_code=status.HTTP_200_OK,
)
async def get_remediation_history(
    count: int = Query(
        default=_DEFAULT_COUNT,
        ge=1,
        le=_MAX_COUNT,
        description=f"Number of history records to return (1–{_MAX_COUNT}).",
    ),
    redis: RedisStreamClient = Depends(get_redis_client),
) -> JSONResponse:
    """Return the most recent remediation execution runs."""
    if redis._client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis client is not initialised.",
        )

    try:
        raw_entries = await redis._client.xrevrange(
            name=_HISTORY_STREAM_NAME,
            max="+",
            min="-",
            count=count,
        )
    except Exception as exc:
        logger.error(
            "XREVRANGE failed | stream=%s error=%s",
            _HISTORY_STREAM_NAME,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to read from remediation history stream: {exc}",
        ) from exc

    history = [_parse_history_entry(sid, fields) for sid, fields in raw_entries]

    return JSONResponse(
        content={
            "total": len(history),
            "stream": _HISTORY_STREAM_NAME,
            "history": history,
        }
    )

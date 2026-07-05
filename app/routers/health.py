"""
AetherSRE — Health Check Router
================================
Provides the GET /health endpoint which reports:
- Overall service status
- Uptime since process start
- Live Redis connectivity check
- Current stream length (useful for detecting backpressure)
"""

from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.logging_config import get_logger
from app.core.redis_client import RedisStreamClient, get_redis_client
from app.core.config import get_settings
from app.models.log_event import HealthStatus

logger = get_logger(__name__)

router = APIRouter(tags=["Observability"])

# Process start time for uptime calculation
_PROCESS_START: float = time.monotonic()


@router.get(
    "/health",
    response_model=HealthStatus,
    summary="Service health check",
    description=(
        "Returns the operational status of the AetherSRE API including "
        "Redis connectivity, stream depth, and process uptime. "
        "Returns HTTP 503 if Redis is unavailable."
    ),
    responses={
        200: {"description": "Service is healthy"},
        503: {"description": "Service is degraded (Redis unavailable)"},
    },
)
async def health_check(
    redis: Annotated[RedisStreamClient, Depends(get_redis_client)],
) -> HealthStatus:
    """
    Perform a live health check against all downstream dependencies.

    This endpoint is intentionally lightweight — it issues a single PING
    and a single XLEN command so it can be polled at high frequency by
    orchestrators (Kubernetes liveness/readiness probes, load balancers).
    """
    settings = get_settings()
    uptime = time.monotonic() - _PROCESS_START

    redis_ok = False
    stream_length = 0

    try:
        redis_ok = await redis.ping()
        stream_length = await redis.xlen()
    except Exception as exc:
        logger.warning("Health check detected Redis failure: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "degraded",
                "reason": f"Redis unreachable: {exc!s}",
                "uptime_seconds": round(uptime, 3),
            },
        ) from exc

    logger.debug(
        "Health check passed | uptime=%.2fs redis=%s stream_len=%d",
        uptime,
        redis_ok,
        stream_length,
    )

    return HealthStatus(
        status="healthy",
        environment=settings.api_env,
        uptime_seconds=round(uptime, 3),
        redis_connected=redis_ok,
        stream_name=settings.redis_stream_name,
        stream_length=stream_length,
    )

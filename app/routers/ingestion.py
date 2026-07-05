"""
AetherSRE — Log Ingestion Router
==================================
Handles POST /api/v1/logs — the primary data plane endpoint.

Responsibilities:
1. Accept a JSON payload conforming to the LogEvent schema.
2. Validate and sanitise the payload (handled by Pydantic automatically).
3. Serialise the event into Redis Stream–compatible flat string fields.
4. Write to the telemetry_log_stream via XADD.
5. Return an IngestResponse confirming the entry ID.

All Redis I/O is non-blocking (redis.asyncio) so this coroutine never
stalls the event loop, even under high concurrency.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.logging_config import get_logger
from app.core.metrics import logs_ingested_total
from app.core.redis_client import RedisStreamClient, get_redis_client
from app.models.log_event import IngestResponse, LogEvent

logger = get_logger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["Ingestion"],
)


@router.post(
    "/logs",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a log event",
    description=(
        "Accept a structured log event from any microservice and append it "
        "to the `telemetry_log_stream` Redis Stream for downstream processing. "
        "Returns HTTP 202 Accepted with the assigned stream entry ID."
    ),
    responses={
        202: {"description": "Log event accepted and written to stream"},
        422: {"description": "Validation error — malformed payload"},
        503: {"description": "Redis write failed"},
    },
)
async def ingest_log(
    event: LogEvent,
    redis: Annotated[RedisStreamClient, Depends(get_redis_client)],
    request: Request,
) -> IngestResponse:
    """
    Validate and stream a single log event to Redis.

    The FastAPI dependency injection system resolves the redis client from the
    application state set during lifespan startup, ensuring the connection pool
    is reused rather than recreated per request.
    """
    client_host = request.client.host if request.client else "unknown"

    logger.debug(
        "Ingest request | service=%s level=%s client=%s",
        event.service_name,
        event.level.value,
        client_host,
    )

    stream_fields = event.to_stream_fields()

    try:
        entry_id = await redis.xadd(fields=stream_fields)
    except Exception as exc:
        logger.error(
            "Failed to write to Redis stream | service=%s error=%s",
            event.service_name,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Stream write failed: {exc!s}",
        ) from exc

    logs_ingested_total.labels(service_name=event.service_name, level=event.level.value).inc()

    log_fn = logger.warning if event.level.value in {"ERROR", "CRITICAL"} else logger.info
    log_fn(
        "Log ingested | service=%-20s level=%-8s stream_id=%s",
        event.service_name,
        event.level.value,
        entry_id,
    )

    return IngestResponse(
        status="accepted",
        stream_id=entry_id,
        stream_name=redis._settings.redis_stream_name,
        service_name=event.service_name,
        level=event.level.value,
    )

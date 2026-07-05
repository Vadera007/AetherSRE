"""
AetherSRE — Human-in-the-Loop Webhooks Router
=============================================
Manages endpoints for operator validation, approvals, and denials.
"""

from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.core.redis_client import RedisStreamClient, get_redis_client
from app.core.logging_config import get_logger
from app.core.remediation_executor import LocalActionExecutor

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/remediation", tags=["remediation-gate"])

_PENDING_HASH_KEY = "aether:remediation:pending"


class ApprovalPayload(BaseModel):
    """Pydantic request format for approving a pending action."""
    incident_id: str = Field(description="The incident ID associated with the pending gate.")


class DenialPayload(BaseModel):
    """Pydantic request format for denying a pending action."""
    incident_id: str = Field(description="The incident ID associated with the pending gate.")


@router.post(
    "/gate",
    summary="Mock simulated external webhook channel",
    description="Target endpoint representing where rich Slack or PagerDuty cards would land.",
    status_code=status.HTTP_200_OK,
)
async def webhook_gate_notification(payload: dict) -> JSONResponse:
    """Mock webhook logging targets."""
    logger.info("📡 Outbound Webhook notification triggered | payload=%s", json.dumps(payload))
    return JSONResponse(content={"status": "dispatched", "message": "Notification broadcast complete."})


@router.post(
    "/approve",
    summary="Approve pending mitigation action",
    description="Removes context from pending state, runs local action executor, and records log.",
    status_code=status.HTTP_200_OK,
)
async def approve_remediation(
    payload: ApprovalPayload,
    redis: Annotated[RedisStreamClient, Depends(get_redis_client)],
) -> JSONResponse:
    """Operator approval override."""
    if redis._client is None:
        raise HTTPException(status_code=503, detail="Redis client not initialised.")

    incident_id = payload.incident_id

    # Retrieve context from pending hash map
    serialized_ctx = await redis._client.hget(_PENDING_HASH_KEY, incident_id)
    if not serialized_ctx:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No pending remediation action found for incident ID: {incident_id}",
        )

    ctx = json.loads(serialized_ctx)
    command = ctx.get("target_command", "")
    service_name = ctx.get("service_name", "unknown")

    logger.info("✅ Operator APPROVED remediation | incident_id=%s service=%s command=%r", incident_id, service_name, command)

    # Trigger action executor
    result = await LocalActionExecutor.execute(command)

    # Publish to history stream
    history_payload = {
        "incident_id": incident_id,
        "service_name": service_name,
        "action_id": ctx.get("action_id", ""),
        "risk_level": ctx.get("risk_level", ""),
        "target_command": command,
        "execution_type": "WEBHOOK_GATE",
        "status": "SUCCESS" if result.is_success else "FAILED",
        "stdout": result.stdout,
        "stderr": result.stderr,
        "duration_s": f"{result.duration_s:.3f}",
        "executed_by": "Operator (Approved)",
        "timestamp": str(result.duration_s),
    }

    # Write to history stream
    settings = redis._settings
    history_stream = getattr(settings, "rca_insights_stream_name", "remediation_history_stream")
    # For safety, let's use the explicit string or settings name if added
    target_history_stream = "remediation_history_stream"

    await redis._client.xadd(
        name=target_history_stream,
        fields=history_payload,
        maxlen=10_000,
        approximate=True,
    )

    # Clean up pending hash map
    await redis._client.hdel(_PENDING_HASH_KEY, incident_id)

    return JSONResponse(
        content={
            "status": "executed",
            "incident_id": incident_id,
            "success": result.is_success,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    )


@router.post(
    "/deny",
    summary="Deny pending mitigation action",
    description="Aborts the mitigation execution flow.",
    status_code=status.HTTP_200_OK,
)
async def deny_remediation(
    payload: DenialPayload,
    redis: Annotated[RedisStreamClient, Depends(get_redis_client)],
) -> JSONResponse:
    """Operator deny override."""
    if redis._client is None:
        raise HTTPException(status_code=503, detail="Redis client not initialised.")

    incident_id = payload.incident_id

    # Retrieve context from pending hash map
    serialized_ctx = await redis._client.hget(_PENDING_HASH_KEY, incident_id)
    if not serialized_ctx:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No pending remediation action found for incident ID: {incident_id}",
        )

    ctx = json.loads(serialized_ctx)
    service_name = ctx.get("service_name", "unknown")

    logger.warning("❌ Operator DENIED remediation | incident_id=%s service=%s", incident_id, service_name)

    # Publish to history stream representing the abort state
    history_payload = {
        "incident_id": incident_id,
        "service_name": service_name,
        "action_id": ctx.get("action_id", ""),
        "risk_level": ctx.get("risk_level", ""),
        "target_command": ctx.get("target_command", ""),
        "execution_type": "WEBHOOK_GATE",
        "status": "ABORTED",
        "stdout": "",
        "stderr": "Mitigation Aborted by Operator",
        "duration_s": "0.000",
        "executed_by": "Operator (Denied)",
        "timestamp": "0.0",
    }

    target_history_stream = "remediation_history_stream"
    await redis._client.xadd(
        name=target_history_stream,
        fields=history_payload,
        maxlen=10_000,
        approximate=True,
    )

    # Clean up pending hash map
    await redis._client.hdel(_PENDING_HASH_KEY, incident_id)

    return JSONResponse(
        content={
            "status": "aborted",
            "incident_id": incident_id,
            "message": "Mitigation Aborted by Operator",
        }
    )

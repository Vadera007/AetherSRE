"""
AetherSRE — Dashboard & WebSockets Telemetry Router
===================================================
Serves the HTML template and handles real-time message broadcasting by polling
Redis streams (`telemetry_log_stream`, `incident_alerts_stream`, `rca_insights_stream`, `remediation_history_stream`).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Final

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from app.core.config import get_settings
from app.core.redis_client import RedisStreamClient, get_redis_client
from app.core.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["dashboard"])

# Setup Jinja2 templates location
templates = Jinja2Templates(directory="app/templates")

_POLL_INTERVAL_S: Final[float] = 0.5
_PENDING_HASH_KEY: Final[str] = "aether:remediation:pending"


import httpx
settings = get_settings()

async def query_prometheus(query: str) -> float:
    """Helper to query Prometheus HTTP API for aggregated metric values."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(
                "http://prometheus:9090/api/v1/query",
                params={"query": query}
            )
            if r.status_code == 200:
                data = r.json()
                results = data.get("data", {}).get("result", [])
                if results:
                    return float(results[0]["value"][1])
    except Exception as exc:
        logger.warning("Failed to query Prometheus for query %s | error=%s", query, exc)
    return 0.0


@router.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard(request: Request) -> HTMLResponse:
    """Render the dashboard UI page view."""
    return templates.TemplateResponse(request, "dashboard.html")


@router.get("/api/v1/dashboard/stats")
async def get_dashboard_stats(redis: RedisStreamClient = Depends(get_redis_client)) -> dict[str, int]:
    """Fetch current aggregate metrics directly from Redis stream lengths (source of truth)."""
    total_logs = 0
    total_anomalies = 0
    total_remediations = 0
    pending_approvals = 0

    if redis._client:
        try:
            total_logs = await redis._client.xlen(settings.redis_stream_name)
        except Exception as exc:
            logger.warning("Failed to query telemetry stream length | error=%s", exc)

        try:
            total_anomalies = await redis._client.xlen("incident_alerts_stream")
        except Exception as exc:
            logger.warning("Failed to query incident stream length | error=%s", exc)

        try:
            total_remediations = await redis._client.xlen("remediation_history_stream")
        except Exception as exc:
            logger.warning("Failed to query remediation history stream length | error=%s", exc)

        try:
            pending_approvals = len(await redis._client.hkeys(_PENDING_HASH_KEY))
        except Exception as exc:
            logger.warning("Failed to query pending approvals from Redis | error=%s", exc)

    return {
        "total_logs": total_logs,
        "total_anomalies": total_anomalies,
        "total_remediations": total_remediations,
        "pending_approvals": pending_approvals,
    }



class ConnectionManager:
    """Manages active WebSockets connections and broadcasts events."""

    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("New WebSocket client connected | active_count=%d", len(self.active_connections))

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info("WebSocket client disconnected | active_count=%d", len(self.active_connections))

    async def broadcast(self, payload: dict[str, Any]) -> None:
        """Sends payload to all connected clients."""
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(payload))
            except Exception:
                # Connection might be dead, handled during sweep on close
                pass


manager = ConnectionManager()


async def _poll_redis_streams(redis: RedisStreamClient) -> None:
    """
    Indefinitely polls all pipeline Redis streams and broadcasts new events
    to connected WebSocket clients.
    """
    if redis._client is None:
        logger.error("Redis stream poll task started but client is not initialised.")
        return

    settings = redis._settings
    telemetry_stream = settings.redis_stream_name
    incident_stream = "incident_alerts_stream"
    rca_stream = settings.rca_insights_stream_name
    remediation_stream = "remediation_history_stream"

    # Maintain last read offsets to poll new messages only
    offsets: dict[str, str] = {
        telemetry_stream: "$",
        incident_stream: "$",
        rca_stream: "$",
        remediation_stream: "$",
    }

    # Eagerly capture any existing pending operator approvals at startup
    try:
        pending_hashes = await redis._client.hgetall(_PENDING_HASH_KEY)
        for key_id, raw_val in pending_hashes.items():
            parsed_val = json.loads(raw_val)
            await manager.broadcast({
                "type": "pending_remediation",
                "data": parsed_val
            })
    except Exception as exc:
        logger.warning("Failed to broadcast initial pending remediations | error=%s", exc)

    logger.info("Background Redis stream poll task running.")

    while True:
        if not manager.active_connections:
            # Throttle polling if no one is viewing the dashboard
            await asyncio.sleep(1.0)
            continue

        try:
            # Poll streams
            for stream_name, offset in offsets.items():
                entries = await redis._client.xread(
                    streams={stream_name: offset},
                    count=10,
                    block=100,
                )
                if not entries:
                    continue

                for _str_name, message_list in entries:
                    for msg_id, fields in message_list:
                        offsets[stream_name] = msg_id  # Update read cursor

                        # Format broadcast type based on source stream
                        if stream_name == telemetry_stream:
                            await manager.broadcast({
                                "type": "telemetry",
                                "data": {
                                    "service_name": fields.get("service_name", "unknown"),
                                    "level": fields.get("level", "INFO"),
                                    "message": fields.get("message", ""),
                                }
                            })
                        elif stream_name == incident_stream:
                            await manager.broadcast({
                                "type": "anomaly",
                                "data": {
                                    "incident_id": msg_id,
                                    "service_name": fields.get("service_name", ""),
                                }
                            })
                        elif stream_name == rca_stream:
                            risk_level = fields.get("risk_level", "LOW")
                            # If it requires gating approval, wait for worker to push pending map,
                            # but broadcast RCA results immediately.
                            await manager.broadcast({
                                "type": "rca",
                                "data": {
                                    "incident_id": fields.get("incident_id", msg_id),
                                    "service_name": fields.get("service_name", ""),
                                    "raw_message": fields.get("raw_message", ""),
                                    "anomaly_score": float(fields.get("anomaly_score", "0.0")),
                                    "root_cause": fields.get("root_cause", ""),
                                    "suggested_fix": fields.get("suggested_fix", ""),
                                    "risk_level": risk_level,
                                }
                            })
                            # If it requires manual gate validation, broadcast approval card details too
                            if risk_level in ("HIGH", "CRITICAL"):
                                target_command = "mock-remediation complex_mitigation"
                                if "scale" in fields.get("suggested_fix", "").lower():
                                    target_command = "mock-remediation scale_up"
                                elif "db" in fields.get("suggested_fix", "").lower():
                                    target_command = "mock-remediation modify_db_pool"
                                
                                await manager.broadcast({
                                    "type": "pending_remediation",
                                    "data": {
                                        "incident_id": fields.get("incident_id", msg_id),
                                        "service_name": fields.get("service_name", ""),
                                        "risk_level": risk_level,
                                        "target_command": target_command,
                                    }
                                })
                        elif stream_name == remediation_stream:
                            await manager.broadcast({
                                "type": "remediation_run",
                                "data": {
                                    "incident_id": fields.get("incident_id", ""),
                                    "service_name": fields.get("service_name", ""),
                                    "execution_type": fields.get("execution_type", ""),
                                    "status": fields.get("status", ""),
                                }
                            })

        except Exception as exc:
            logger.error("Error polling Redis streams in broadcast thread | error=%s", exc, exc_info=True)

        await asyncio.sleep(_POLL_INTERVAL_S)


# Reference to the stream polling task
_polling_task: asyncio.Task[None] | None = None


def start_stream_polling(redis: RedisStreamClient) -> None:
    """Initialize background polling loop task."""
    global _polling_task  # noqa: PLW0603
    if _polling_task is None:
        _polling_task = asyncio.create_task(_poll_redis_streams(redis), name="ws-redis-stream-poller")


def stop_stream_polling() -> None:
    """Shut down background polling task."""
    global _polling_task  # noqa: PLW0603
    if _polling_task is not None:
        _polling_task.cancel()
        _polling_task = None


@router.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(
    websocket: WebSocket,
    redis: RedisStreamClient = Depends(get_redis_client),
) -> None:
    """WebSocket communication portal endpoint."""
    await manager.connect(websocket)

    # Lazily start background polling when the first client connects
    start_stream_polling(redis)

    try:
        # Keep connection open by listening for any client heartbeats
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as exc:
        logger.warning("Error in WebSocket connection thread | error=%s", exc)
        manager.disconnect(websocket)

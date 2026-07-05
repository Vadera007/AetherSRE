"""
AetherSRE — API Integration Tests
==================================
Tests the health check, log ingestion, and incident retrieval endpoints
in-process using the ASGI client. By mocking the RedisStreamClient dependency,
these tests run completely hermetically without requiring a running Redis instance
or Docker daemon.
"""

from __future__ import annotations

import json
import pytest
import httpx
from typing import Any

from app.main import app
from app.core.redis_client import get_redis_client, RedisStreamClient
from app.core.config import get_settings


# =============================================================================
# Mock Redis Client
# =============================================================================

class MockInnerRedisClient:
    """Mock for the low-level redis.asyncio client."""

    async def xrevrange(self, name: str, max: str = "+", min: str = "-", count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        # Return mock incident alerts
        context_window = [
            {"service_name": "payment-gateway", "timestamp": "2024-01-15T12:00:00.000Z", "level": "INFO", "message": "Transaction normal", "stream_id": "1718000000000-1", "metadata": {}}
        ]
        mock_fields = {
            "service_name": "payment-gateway",
            "timestamp": "2024-01-15T12:00:05.000Z",
            "level": "CRITICAL",
            "raw_message": "ANOMALY! database timeout",
            "normalized_message": "ANOMALY! database timeout",
            "anomaly_score": "0.852100",
            "anomaly_threshold": "0.55",
            "context_window_size": "1",
            "context_window": json.dumps(context_window),
            "metadata": json.dumps({"trace_id": "tx-anom"}),
            "detected_at": "1718000005.0",
            "stream_id": "1718000000000-2"
        }
        return [("1718000000000-2", mock_fields)]

    async def xlen(self, name: str) -> int:
        return 1


class MockRedisStreamClient:
    """Mock for the high-level RedisStreamClient."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._client = MockInnerRedisClient()

    async def ping(self) -> bool:
        return True

    async def xlen(self, stream_name: str | None = None) -> int:
        return 42

    async def xadd(self, fields: dict[str, str], stream_name: str | None = None, maxlen: int | None = None) -> str:
        return "1718000000000-9"


# =============================================================================
# Pytest Lifespan & Dependency Override Setup
# =============================================================================

@pytest.fixture(autouse=True)
def setup_dependency_overrides() -> Any:
    # Instantiate the mock client
    mock_client = MockRedisStreamClient()

    # Register FastAPI dependency override
    app.dependency_overrides[get_redis_client] = lambda: mock_client

    # Initialize app state variables to prevent AttributeErrors on lifespan components
    app.state.redis_client = mock_client

    yield

    # Clean up overrides after test
    app.dependency_overrides.clear()


# =============================================================================
# API Endpoint Tests
# =============================================================================

@pytest.mark.asyncio
class TestHealthEndpoint:
    """Test suite for GET /health."""

    async def test_health_returns_200(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

    async def test_health_response_schema(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")
        data = response.json()

        required_keys = {
            "status", "environment", "uptime_seconds",
            "redis_connected", "stream_name", "stream_length", "version",
        }
        assert required_keys.issubset(data.keys()), (
            f"Missing keys: {required_keys - data.keys()}"
        )

    async def test_health_redis_connected(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")
        data = response.json()
        assert data["redis_connected"] is True
        assert data["status"] == "healthy"

    async def test_health_uptime_positive(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")
        data = response.json()
        assert data["uptime_seconds"] >= 0, "Uptime should be non-negative"


@pytest.mark.asyncio
class TestIngestionEndpoint:
    """Test suite for POST /api/v1/logs."""

    _VALID_PAYLOAD = {
        "service_name": "test-service",
        "level": "INFO",
        "message": "Integration test log event",
        "metadata": {"test": "true", "suite": "day3"},
    }

    async def test_ingest_returns_202(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/api/v1/logs", json=self._VALID_PAYLOAD)
        assert response.status_code == 202, (
            f"Expected 202 Accepted, got {response.status_code}: {response.text}"
        )

    async def test_ingest_rejects_empty_message(self) -> None:
        """Empty messages should be rejected with 422 Unprocessable Entity."""
        payload = {
            "service_name": "auth-service",
            "level": "INFO",
            "message": "   ",  # Only whitespace
        }
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/api/v1/logs", json=payload)
        assert response.status_code == 422

    async def test_ingest_rejects_invalid_level(self) -> None:
        """Unknown log levels should be rejected with 422."""
        payload = {
            "service_name": "auth-service",
            "level": "VERBOSE",
            "message": "Test message",
        }
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/api/v1/logs", json=payload)
        assert response.status_code == 422


@pytest.mark.asyncio
class TestIncidentsEndpoint:
    """Test suite for GET /api/v1/incidents/recent and GET /api/v1/incidents/stats."""

    async def test_get_recent_incidents(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/incidents/recent?count=5")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "incidents" in data
        assert isinstance(data["incidents"], list)
        assert len(data["incidents"]) == 1
        assert data["incidents"][0]["service_name"] == "payment-gateway"
        assert data["incidents"][0]["anomaly_score"] == 0.852100

    async def test_get_incident_stats(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/incidents/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["stream_length"] == 1
        assert data["service_counts"] == {"payment-gateway": 1}
        assert data["level_counts"] == {"CRITICAL": 1}
        assert data["average_anomaly_score"] == 0.8521

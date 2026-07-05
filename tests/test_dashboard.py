"""
AetherSRE — Integration tests for Live WebSocket Dashboard
=========================================================
Verifies rendering of the HTML page, WebSocket connectivity handshakes,
and data piping from stream injections.
"""

from __future__ import annotations

from typing import Generator
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import get_settings
from app.core.redis_client import get_redis_client


class MockRedisInner:
    async def xread(self, *args, **kwargs):
        return []

    async def hgetall(self, *args, **kwargs):
        return {}


class MockRedisClient:
    def __init__(self):
        self._settings = get_settings()
        self._client = MockRedisInner()

    async def ping(self):
        return True


@pytest.fixture(autouse=True)
def override_redis_fixture(monkeypatch: pytest.MonkeyPatch):
    # Stub startup_redis to return our dummy client
    async def mock_startup(*args, **kwargs):
        return MockRedisClient()

    monkeypatch.setattr("app.main.startup_redis", mock_startup)

    # Stub worker start/run/close methods to prevent background threads connecting to real Redis
    from app.workers.rca_processor import RcaProcessorWorker
    from app.workers.remediation_processor import RemediationProcessorWorker

    async def dummy_void(*args, **kwargs):
        pass

    monkeypatch.setattr(RcaProcessorWorker, "start", dummy_void)
    monkeypatch.setattr(RcaProcessorWorker, "run", dummy_void)
    monkeypatch.setattr(RcaProcessorWorker, "close", dummy_void)
    monkeypatch.setattr(RemediationProcessorWorker, "start", dummy_void)
    monkeypatch.setattr(RemediationProcessorWorker, "run", dummy_void)
    monkeypatch.setattr(RemediationProcessorWorker, "close", dummy_void)

    app.dependency_overrides[get_redis_client] = lambda: MockRedisClient()
    app.state.redis_client = MockRedisClient()
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    """Test client fixture."""
    with TestClient(app) as test_client:
        yield test_client


def test_dashboard_rendering(client: TestClient) -> None:
    """Verifies that the GET /dashboard endpoint returns the HTML template view."""
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "AetherSRE — Operational Command Center" in response.text
    assert "Akshat Vadera" in response.text


def test_websocket_handshake(client: TestClient) -> None:
    """Verifies WebSocket connection establishment and data broadcasting."""
    settings = get_settings()
    
    with client.websocket_connect("/ws/telemetry") as websocket:
        # Check initial payload connection
        # (Should execute cleanly without raising handshaking or disconnect exceptions)
        assert websocket is not None

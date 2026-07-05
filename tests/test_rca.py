"""
AetherSRE — Day 4 RCA Verification Suite
=========================================
Covers Pydantic parsing of Ollama outputs, HTTP mocks, and the RCA streaming worker pipeline.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncGenerator

import pytest
import httpx
from pydantic import ValidationError

from app.core.config import Settings
from app.core.llm_client import OllamaRcaClient, AetherRcaReport, RiskLevel
from app.workers.rca_processor import RcaProcessorWorker


# ── Client Unit Tests ────────────────────────────────────────────────────────


def test_rca_report_validation() -> None:
    """Validate that AetherRcaReport enforces schema correctly."""
    valid_data = {
        "root_cause": "Database connection pool leak.",
        "suggested_fix": "Restart service payment-gateway.",
        "risk_level": "CRITICAL",
        "impact_analysis": "Payments are failing globally."
    }
    report = AetherRcaReport.model_validate(valid_data)
    assert report.root_cause == "Database connection pool leak."
    assert report.risk_level == RiskLevel.CRITICAL

    # Invalid risk level
    invalid_data = valid_data.copy()
    invalid_data["risk_level"] = "VERY_HIGH"
    with pytest.raises(ValidationError):
        AetherRcaReport.model_validate(invalid_data)


@pytest.mark.asyncio
async def test_ollama_client_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock the HTTP client to check successful extraction."""
    client = OllamaRcaClient()

    class MockResponse:
        status_code = 200
        request = httpx.Request("POST", "http://localhost:11434/api/generate")
        def json(self) -> dict[str, Any]:
            return {
                "response": json.dumps({
                    "root_cause": "Disk pressure on node-a.",
                    "suggested_fix": "Prune docker system volumes.",
                    "risk_level": "HIGH",
                    "impact_analysis": "Ingestion latency increased."
                })
            }

    async def mock_post(*args: Any, **kwargs: Any) -> MockResponse:
        return MockResponse()

    monkeypatch.setattr(httpx.AsyncClient, "post", mock_post)

    report = await client.analyze_incident(
        service_name="storage-service",
        raw_message="Disk full",
        context_window=[]
    )
    assert report.risk_level == RiskLevel.HIGH
    assert "node-a" in report.root_cause
    assert "Prune" in report.suggested_fix


# ── Mocking Redis Client for Worker Pipeline Verification ───────────────────


class MockRedis:
    """Simulated Redis engine mapping XREADGROUP and XADD."""

    def __init__(self) -> None:
        self.xadds: list[dict[str, Any]] = []
        self.xacks: list[str] = []
        self.stream_data: list[tuple[str, dict[str, str]]] = [
            (
                "1700000000000-0",
                {
                    "service_name": "payment-gateway",
                    "raw_message": "Timeout contacting processor",
                    "timestamp": "2026-07-03T00:00:00Z",
                    "level": "ERROR",
                    "normalized_message": "Timeout contacting processor",
                    "anomaly_score": "0.82",
                    "context_window": "[]",
                }
            )
        ]

    async def ping(self) -> bool:
        return True

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        if not self.stream_data:
            return []
        data = self.stream_data
        self.stream_data = []  # consume
        return [("incident_alerts_stream", data)]

    async def xadd(self, name: str, fields: dict[str, str], maxlen: int | None = None, approximate: bool = True) -> str:
        self.xadds.append({"stream": name, "fields": fields})
        return f"rca-1700000000000-0"

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        self.xacks.extend(ids)
        return len(ids)

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_rca_worker_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end local test verifying worker runs, calls LLM client, writes rca, and ACKs."""
    mock_redis = MockRedis()
    worker = RcaProcessorWorker()
    worker._client = mock_redis  # inject mock Redis

    # Stub client call
    async def mock_analyze(*args: Any, **kwargs: Any) -> AetherRcaReport:
        return AetherRcaReport(
            root_cause="Processor gateway is down.",
            suggested_fix="Failover to secondary gateway.",
            risk_level=RiskLevel.HIGH,
            impact_analysis="Payment pipeline stalled."
        )
    monkeypatch.setattr(worker._llm_client, "analyze_incident", mock_analyze)

    # Process one entry
    await worker._poll_once("incident_alerts_stream")

    assert mock_redis.xacks == ["1700000000000-0"]
    assert len(mock_redis.xadds) == 1
    assert mock_redis.xadds[0]["stream"] == "rca_insights_stream"
    fields = mock_redis.xadds[0]["fields"]
    assert fields["service_name"] == "payment-gateway"
    assert fields["risk_level"] == "HIGH"
    assert fields["root_cause"] == "Processor gateway is down."

"""
AetherSRE — Day 5 Remediation Verification Suite
================================================
Covers Risk Policy Matrix mappings, Local process execution safety,
and pending approvals/denials state hooks.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from pydantic import ValidationError

from app.core.llm_client import AetherRcaReport, RiskLevel
from app.core.remediation_policy import RiskPolicyMatrix, ExecutionType
from app.core.remediation_executor import LocalActionExecutor
from app.workers.remediation_processor import RemediationProcessorWorker


# ── Risk Policy Engine Mappings ─────────────────────────────────────────────


def test_remediation_policy_evaluation() -> None:
    """Verify mapping of risk policies across various categories."""
    # Test Auto action mapping
    low_report = AetherRcaReport(
        root_cause="Redis memory limit hit.",
        suggested_fix="Clear transient cache entries.",
        risk_level=RiskLevel.LOW,
        impact_analysis="Mild degradation."
    )
    action = RiskPolicyMatrix.evaluate(low_report)
    assert action.execution_type == ExecutionType.AUTO_EXECUTE
    assert action.target_command == "mock-remediation clear_cache"

    # Test Webhook gate mapping
    critical_report = AetherRcaReport(
        root_cause="Runaway schema migration failure.",
        suggested_fix="Modify production database connection pools.",
        risk_level=RiskLevel.CRITICAL,
        impact_analysis="Full API down."
    )
    action = RiskPolicyMatrix.evaluate(critical_report)
    assert action.execution_type == ExecutionType.WEBHOOK_GATE
    assert action.target_command == "mock-remediation modify_db_pool"


# ── Subprocess Runner Safety ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_local_executor_command_splitting() -> None:
    """Ensure shlex splits targets securely to avoid shell execution vulnerabilities."""
    result = await LocalActionExecutor.execute("mock-remediation restart_daemon")
    assert result.is_success
    assert "[MOCK_HEALING]" in result.stdout
    assert "restart_daemon" in result.stdout


# ── Worker Stream State Integration ─────────────────────────────────────────


class MockRemediationRedis:
    """Simulated Redis engine mapping stream reads/writes."""

    def __init__(self) -> None:
        self.xadds: list[dict[str, Any]] = []
        self.xacks: list[str] = []
        self.hsets: dict[str, dict[str, str]] = {}
        self.stream_data: list[tuple[str, dict[str, str]]] = [
            (
                "1800000000000-0",
                {
                    "incident_id": "incident-123",
                    "service_name": "payment-gateway",
                    "root_cause": "Database connection pool saturated",
                    "suggested_fix": "Modify DB connection pool parameters",
                    "risk_level": "CRITICAL",
                    "impact_analysis": "Critical checkout degradation",
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
        self.stream_data = []
        return [("rca_insights_stream", data)]

    async def xadd(self, name: str, fields: dict[str, str], maxlen: int | None = None, approximate: bool = True) -> str:
        self.xadds.append({"stream": name, "fields": fields})
        return "rem-1800000000000-0"

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        self.xacks.extend(ids)
        return len(ids)

    async def hset(self, key: str, field: str, value: str) -> int:
        if key not in self.hsets:
            self.hsets[key] = {}
        self.hsets[key][field] = value
        return 1

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_remediation_worker_pauses_critical_risk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure worker maps critical threat levels directly to pending webhook gates."""
    mock_redis = MockRemediationRedis()
    worker = RemediationProcessorWorker()
    worker._client = mock_redis

    # Process entry
    await worker._poll_once("rca_insights_stream")

    # High/Critical risk mapping must not trigger direct LocalActionExecutor commands immediately
    assert len(mock_redis.xadds) == 0
    assert mock_redis.xacks == ["1800000000000-0"]  # Acknowledged RCA stream entry

    # Verify pending state hash mapping
    assert "incident-123" in mock_redis.hsets["aether:remediation:pending"]
    pending_ctx = json.loads(mock_redis.hsets["aether:remediation:pending"]["incident-123"])
    assert pending_ctx["service_name"] == "payment-gateway"
    assert pending_ctx["risk_level"] == "CRITICAL"

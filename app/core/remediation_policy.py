"""
AetherSRE — Risk Policy Engine Matrix
=====================================
Evaluates the risk of suggested remediation actions and maps them to either
automatic execution or operator verification gates.
"""

from __future__ import annotations

from enum import Enum
from pydantic import BaseModel, Field

from app.core.llm_client import AetherRcaReport, RiskLevel


class ExecutionType(str, Enum):
    """How the remediation action should be executed."""
    AUTO_EXECUTE = "AUTO_EXECUTE"
    WEBHOOK_GATE = "WEBHOOK_GATE"


class RemediationAction(BaseModel):
    """Pydantic model holding mapped self-healing target attributes."""
    action_id: str = Field(description="Unique identifier for the action mapping.")
    risk_level: RiskLevel = Field(description="Operational risk severity level.")
    execution_type: ExecutionType = Field(description="Throttling gate selection.")
    target_command: str = Field(description="Script command command prefix to run.")


class RiskPolicyMatrix:
    """
    Evaluates the LLM-generated RCA suggestions and maps them to concrete
    remediation commands based on risk boundaries.
    """

    @staticmethod
    def evaluate(report: AetherRcaReport) -> RemediationAction:
        """
        Evaluate the risk profile and return the appropriate action configuration.
        """
        risk = report.risk_level
        action_id = f"remediate-{risk.value.lower()}"

        # Default commands based on target suggestions or service
        suggested = report.suggested_fix.lower()

        # Map execution types and define safe mock executable scripts
        if risk in (RiskLevel.LOW, RiskLevel.MEDIUM):
            execution_type = ExecutionType.AUTO_EXECUTE
            if "cache" in suggested or "redis" in suggested:
                target_command = "mock-remediation clear_cache"
            elif "restart" in suggested:
                target_command = "mock-remediation restart_daemon"
            else:
                target_command = "mock-remediation benign_routine"
        else:
            execution_type = ExecutionType.WEBHOOK_GATE
            if "scale" in suggested:
                target_command = "mock-remediation scale_up"
            elif "infrastructure" in suggested or "db" in suggested or "pool" in suggested:
                target_command = "mock-remediation modify_db_pool"
            else:
                target_command = "mock-remediation complex_mitigation"

        return RemediationAction(
            action_id=action_id,
            risk_level=risk,
            execution_type=execution_type,
            target_command=target_command,
        )

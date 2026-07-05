"""
AetherSRE — Asynchronous Ollama Client Layer
============================================
Communicates with the localized Ollama HTTP API to generate structured
root cause analyses. Includes automatic retries, backoff, and strict Pydantic
schema enforcement.
"""

from __future__ import annotations

import asyncio
import json
import logging
from enum import Enum
from typing import Any

import httpx
from pydantic import BaseModel, Field

from app.core.config import Settings, get_settings
from app.core.logging_config import get_logger

logger = get_logger(__name__)


class RiskLevel(str, Enum):
    """Normalized operational risk severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AetherRcaReport(BaseModel):
    """
    Structured response model parsed directly from Ollama JSON output.
    Forces the LLM to commit to structured root cause diagnoses.
    """
    root_cause: str = Field(
        description="Detailed explanation of why the anomaly occurred."
    )
    suggested_fix: str = Field(
        description="Step-by-step command or procedure to remediate the service issue."
    )
    risk_level: RiskLevel = Field(
        description="Assessed risk level associated with this class of system anomaly."
    )
    impact_analysis: str = Field(
        description="Description of what downstream dependencies or user-facing features are impacted."
    )


class OllamaRcaClient:
    """
    Asynchronous client wrapper for localized Ollama inference.
    Handles structural prompt construction, JSON request formatting, and retry logic.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._url = self._settings.ollama_url.rstrip("/")
        self._model = self._settings.ollama_model

    def _build_prompt(self, service_name: str, raw_message: str, context_window: list[dict[str, Any]]) -> str:
        """
        Assemble the incident details and historical context into a strict prompt.
        """
        # Format the sliding window context logs for LLM comprehension
        formatted_context = []
        for idx, log in enumerate(context_window):
            level = log.get("level", "UNKNOWN")
            msg = log.get("message", log.get("raw_message", ""))
            ts = log.get("timestamp", "")
            formatted_context.append(f"[{ts}] {level} - {msg}")

        context_str = "\n".join(formatted_context) if formatted_context else "No preceding log history."

        # Prompt system instructions and constraint schema
        prompt = f"""You are a Principal SRE Agent analyzing a production incident.
Analyze this microservice anomaly and output a valid JSON report.

[Incident Context]
Service Name: {service_name}
Anomalous Message: {raw_message}

[Preceding Context Log Window]
{context_str}

[Instruction]
Return a JSON object conforming exactly to this structure:
{{
  "root_cause": "Detailed root-cause analysis based on context logs and anomalous line.",
  "suggested_fix": "Clear action to fix this issue (e.g. restart pod, scale deployment, clear cache, block IP, run migrations).",
  "risk_level": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
  "impact_analysis": "Summary of the business or technical blast radius."
}}

CRITICAL REQUIREMENT: Output raw parseable JSON only. Do not wrap in markdown code blocks like ```json ... ```. Do not output preamble or conversational text. Output valid JSON only.
"""
        return prompt

    async def analyze_incident(
        self,
        service_name: str,
        raw_message: str,
        context_window: list[dict[str, Any]],
        max_retries: int = 3,
        initial_backoff: float = 1.0,
    ) -> AetherRcaReport:
        """
        Sends the incident context to Ollama and returns a validated Pydantic model.
        Implements exponential backoff and retry handling.
        """
        prompt = self._build_prompt(service_name, raw_message, context_window)
        payload = {
            "model": self._model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1,  # Keep it highly deterministic
            }
        }

        url = f"{self._url}/api/generate"
        headers = {"Content-Type": "application/json"}
        current_backoff = initial_backoff

        for attempt in range(1, max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=45.0) as client:
                    logger.info(
                        "Sending RCA request to Ollama | service=%s model=%s attempt=%d/%d",
                        service_name,
                        self._model,
                        attempt,
                        max_retries,
                    )
                    t0 = asyncio.get_running_loop().time()
                    response = await client.post(url, json=payload, headers=headers)
                    elapsed = asyncio.get_running_loop().time() - t0

                    if response.status_code != 200:
                        raise httpx.HTTPStatusError(
                            f"Non-200 status code: {response.status_code}",
                            request=response.request,
                            response=response,
                        )

                    data = response.json()
                    response_text = data.get("response", "").strip()
                    logger.info(
                        "Ollama response received | service=%s status=%d elapsed=%.2fs text_len=%d",
                        service_name,
                        response.status_code,
                        elapsed,
                        len(response_text),
                    )

                    # Parse JSON content
                    parsed_json = json.loads(response_text)
                    return AetherRcaReport.model_validate(parsed_json)

            except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError, ValueError) as exc:
                logger.warning(
                    "Ollama request attempt %d failed | error=%s. Retrying in %.2fs...",
                    attempt,
                    exc,
                    current_backoff,
                )
                if attempt == max_retries:
                    logger.error(
                        "All %d attempts to call Ollama failed for service %s. Raising error.",
                        max_retries,
                        service_name,
                    )
                    raise

                await asyncio.sleep(current_backoff)
                current_backoff *= 2.0

        raise RuntimeError("Unexpected end of retry loop in OllamaRcaClient")

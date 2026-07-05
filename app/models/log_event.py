"""
AetherSRE — Pydantic Domain Models
====================================
Defines the canonical data shapes that flow through the ingestion pipeline.
All models use strict validation so malformed payloads are rejected at the
API boundary before touching Redis.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class LogLevel(str, Enum):
    """
    Standard syslog-inspired severity levels supported by AetherSRE.
    Using a string enum lets the values pass cleanly through JSON serialisation
    without a custom encoder.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ---------------------------------------------------------------------------
# Request / Ingestion Models
# ---------------------------------------------------------------------------


class LogEvent(BaseModel):
    """
    Canonical representation of a single log line arriving at the ingest API.

    All fields are strictly validated:
    - `service_name` is normalised to lowercase and must be non-empty.
    - `timestamp` defaults to *now* (UTC) if omitted by the caller.
    - `level` is validated against the LogLevel enum.
    - `message` must be non-empty and is trimmed of leading/trailing whitespace.
    - `metadata` is an optional free-form dict for structured context (trace IDs,
      pod names, etc.) that future pipeline stages can leverage.
    """

    service_name: Annotated[
        str,
        Field(
            min_length=1,
            max_length=64,
            description="Originating microservice identifier (e.g., 'payment-gateway')",
            examples=["auth-service", "payment-gateway"],
        ),
    ]

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="ISO-8601 event timestamp. Defaults to server receive time.",
        examples=["2024-01-15T12:34:56.789Z"],
    )

    level: LogLevel = Field(
        default=LogLevel.INFO,
        description="Log severity level.",
        examples=["INFO", "ERROR", "CRITICAL"],
    )

    message: Annotated[
        str,
        Field(
            min_length=1,
            max_length=4096,
            description="Human-readable log message body.",
            examples=["User login successful", "Database connection pool exhausted"],
        ),
    ]

    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Optional structured key-value pairs for additional context.",
        examples=[{"trace_id": "abc123", "pod": "payment-gateway-7d9f8b"}],
    )

    @field_validator("service_name", mode="before")
    @classmethod
    def _normalise_service_name(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError("service_name must be a string")
        stripped = v.strip().lower()
        if not stripped:
            raise ValueError("service_name must not be blank after stripping whitespace")
        return stripped

    @field_validator("message", mode="before")
    @classmethod
    def _strip_message(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError("message must be a string")
        stripped = v.strip()
        if not stripped:
            raise ValueError("message must not be blank after stripping whitespace")
        return stripped

    @field_validator("timestamp", mode="before")
    @classmethod
    def _ensure_utc(cls, v: object) -> datetime:
        """Accept naive datetimes and localise them to UTC."""
        if isinstance(v, str):
            # Let Pydantic's default ISO parser handle string → datetime
            return v  # type: ignore[return-value]
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v
        return v  # type: ignore[return-value]

    def to_stream_fields(self) -> dict[str, str]:
        """
        Serialise this event into a flat string dict suitable for XADD.

        Redis Streams store values as strings; nested structures (metadata)
        are JSON-serialised so they can be deserialised by downstream consumers.
        """
        import json  # noqa: PLC0415 — import inside method to avoid circular deps

        return {
            "service_name": self.service_name,
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "metadata": json.dumps(self.metadata),
        }


# ---------------------------------------------------------------------------
# Response Models
# ---------------------------------------------------------------------------


class IngestResponse(BaseModel):
    """Response returned after successfully appending a log event to the stream."""

    status: str = Field(default="accepted", description="Ingestion outcome.")
    stream_id: str = Field(
        description="The Redis Stream entry ID assigned to this event.",
        examples=["1718000000000-0"],
    )
    stream_name: str = Field(description="The Redis Stream the event was written to.")
    service_name: str = Field(description="Echo of the ingested service name.")
    level: str = Field(description="Echo of the ingested log level.")


class HealthStatus(BaseModel):
    """Response returned by the /health endpoint."""

    status: str = Field(description="'healthy' or 'degraded'.")
    environment: str = Field(description="Runtime environment (development/production).")
    uptime_seconds: float = Field(description="Seconds since the API process started.")
    redis_connected: bool = Field(description="Whether Redis PING succeeded.")
    stream_name: str = Field(description="Monitored Redis Stream name.")
    stream_length: int = Field(
        default=0, description="Current number of entries in the stream."
    )
    version: str = Field(default="1.0.0", description="AetherSRE API version.")

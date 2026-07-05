"""
AetherSRE — Application Configuration
======================================
Centralised settings loaded from environment variables with full type safety.
Pydantic-Settings ensures early failure if required variables are absent.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application-wide configuration sourced from environment variables.

    Priority: env-file (.env) → OS environment → defaults.
    All values are validated and type-coerced at import time.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Application ──────────────────────────────────────────────────────────
    api_env: str = Field(default="development", description="Runtime environment label")
    log_level: str = Field(default="INFO", description="Python logging level")
    project_name: str = Field(default="AetherSRE", description="Human-readable project name")
    api_version: str = Field(default="v1", description="API version prefix")

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_host: str = Field(default="localhost", description="Redis hostname")
    redis_port: int = Field(default=6379, ge=1, le=65535, description="Redis TCP port")
    redis_db: int = Field(default=0, ge=0, le=15, description="Redis logical database index")
    redis_password: str | None = Field(default=None, description="Redis AUTH password (optional)")
    redis_max_connections: int = Field(
        default=20, ge=1, description="Max connections in the async pool"
    )

    # ── Stream ────────────────────────────────────────────────────────────────
    redis_stream_name: str = Field(
        default="telemetry_log_stream", description="Name of the Redis Stream"
    )
    redis_stream_max_len: int = Field(
        default=100_000,
        ge=1_000,
        description="MAXLEN cap for the stream (approximate trimming)",
    )

    # ── Day 2: Vector Processor Worker ───────────────────────────────────────
    worker_batch_size: int = Field(
        default=32,
        ge=1,
        le=256,
        description="Number of messages per micro-batch before flushing to the embedder.",
    )
    worker_batch_timeout: float = Field(
        default=0.5,
        gt=0.0,
        description="Maximum seconds to hold an incomplete batch before flushing.",
    )
    worker_consumer_group: str = Field(
        default="aether-vector-workers",
        description="Redis consumer group name for the vector processor.",
    )
    worker_consumer_name: str = Field(
        default="vector-worker-0",
        description="Unique consumer instance name within the consumer group.",
    )
    sentence_transformer_model: str = Field(
        default="all-MiniLM-L6-v2",
        description="HuggingFace SentenceTransformer model identifier.",
    )

    # ── Day 4: Localised LLM RCA Settings ──────────────────────────────────────
    ollama_url: str = Field(default="http://localhost:11434", description="Ollama API base URL")
    ollama_model: str = Field(default="llama3", description="Ollama model name to use for RCA")
    rca_consumer_group: str = Field(
        default="rca-processor-group",
        description="Redis consumer group name for the RCA processor.",
    )
    rca_consumer_name: str = Field(
        default="rca-worker-0",
        description="Unique consumer instance name within the RCA consumer group.",
    )
    rca_insights_stream_name: str = Field(
        default="rca_insights_stream",
        description="Redis Stream to write enriched RCA diagnostic summaries to.",
    )
    rca_insights_stream_max_len: int = Field(
        default=10_000,
        ge=1_000,
        description="MAXLEN cap for the RCA insights stream.",
    )

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"log_level must be one of {allowed}, got '{v}'")
        return upper

    @property
    def redis_url(self) -> str:
        """Construct a redis:// URL from individual components."""
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Using lru_cache ensures the .env file is parsed exactly once per process,
    keeping startup cost minimal and settings immutable at runtime.
    """
    return Settings()


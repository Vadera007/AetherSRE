"""
AetherSRE — Async Redis Client & Connection Pool
=================================================
Manages the lifecycle of the Redis connection pool and exposes a thin,
typed interface over redis.asyncio for stream operations.

Design decisions:
- A single ConnectionPool is created once at startup and shared across
  all request coroutines, avoiding the overhead of reconnecting per-request.
- All operations are async-native (redis.asyncio) so they never block
  the Uvicorn event loop.
- Errors are raised immediately rather than swallowed, letting the
  FastAPI exception handlers return meaningful HTTP 5xx responses.
"""

from __future__ import annotations

import logging
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from app.core.config import Settings, get_settings
from app.core.logging_config import get_logger

logger = get_logger(__name__)


class RedisStreamClient:
    """
    Thin async wrapper around the Redis client scoped to stream operations.

    Lifecycle:
        1. Call `initialise()` once during FastAPI lifespan startup.
        2. Use `xadd()` on every ingest request.
        3. Call `close()` during FastAPI lifespan shutdown.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: ConnectionPool | None = None
        self._client: aioredis.Redis | None = None  # type: ignore[type-arg]

    async def initialise(self) -> None:
        """
        Create the async connection pool and validate the Redis connection.

        Raises:
            RedisConnectionError: If Redis is unreachable at startup.
        """
        logger.info(
            "Initialising Redis connection pool | host=%s port=%d db=%d pool_size=%d",
            self._settings.redis_host,
            self._settings.redis_port,
            self._settings.redis_db,
            self._settings.redis_max_connections,
        )
        self._pool = aioredis.ConnectionPool.from_url(
            self._settings.redis_url,
            max_connections=self._settings.redis_max_connections,
            decode_responses=True,  # Always work with str, not bytes
        )
        self._client = aioredis.Redis(connection_pool=self._pool)

        # Eagerly validate the connection during startup so a misconfiguration
        # surfaces immediately rather than on the first ingest request.
        await self.ping()
        logger.info("Redis connection pool initialised and verified.")

    async def ping(self) -> bool:
        """
        Send a PING and return True if Redis responds with PONG.

        Raises:
            RuntimeError: If the client has not been initialised.
            RedisConnectionError: If Redis is unreachable.
        """
        if self._client is None:
            raise RuntimeError("RedisStreamClient has not been initialised. Call initialise() first.")
        response = await self._client.ping()
        return bool(response)

    async def xadd(
        self,
        fields: dict[str, str],
        stream_name: str | None = None,
        maxlen: int | None = None,
    ) -> str:
        """
        Append an entry to the Redis Stream using XADD.

        Uses approximate trimming (MAXLEN ~ N) for O(1) amortised cost.

        Args:
            fields:      Key-value pairs to store in the stream entry.
            stream_name: Override the default stream name from config.
            maxlen:      Override the default max-length cap from config.

        Returns:
            The auto-generated stream entry ID (e.g., '1718000000000-0').

        Raises:
            RuntimeError: If the client has not been initialised.
            RedisError:   On any Redis-level failure.
        """
        if self._client is None:
            raise RuntimeError("RedisStreamClient has not been initialised. Call initialise() first.")

        target_stream = stream_name or self._settings.redis_stream_name
        cap = maxlen or self._settings.redis_stream_max_len

        entry_id: str = await self._client.xadd(  # type: ignore[assignment]
            name=target_stream,
            fields=fields,
            maxlen=cap,
            approximate=True,  # MAXLEN ~ N — efficient trimming
        )
        logger.debug("XADD stream=%s id=%s", target_stream, entry_id)
        return entry_id

    async def xlen(self, stream_name: str | None = None) -> int:
        """
        Return the current number of entries in the stream via XLEN.

        Args:
            stream_name: Override the default stream name from config.

        Returns:
            Entry count as an integer.
        """
        if self._client is None:
            raise RuntimeError("RedisStreamClient has not been initialised.")
        target_stream = stream_name or self._settings.redis_stream_name
        count: int = await self._client.xlen(target_stream)  # type: ignore[assignment]
        return count

    async def close(self) -> None:
        """
        Gracefully close the connection pool.

        Called during FastAPI's lifespan shutdown event to ensure all
        in-flight commands complete before the pool is torn down.
        """
        if self._client is not None:
            await self._client.aclose()
            logger.info("Redis connection pool closed gracefully.")
        if self._pool is not None:
            await self._pool.aclose()


# ---------------------------------------------------------------------------
# Module-level singleton — accessed via dependency injection in FastAPI
# ---------------------------------------------------------------------------

_redis_client: RedisStreamClient | None = None


def get_redis_client() -> RedisStreamClient:
    """
    FastAPI dependency that returns the module-level RedisStreamClient.

    Raises:
        RuntimeError: If called before the lifespan startup event completes.
    """
    if _redis_client is None:
        raise RuntimeError(
            "Redis client is not initialised. "
            "Ensure the FastAPI lifespan startup event has completed."
        )
    return _redis_client


async def startup_redis(settings: Settings | None = None) -> RedisStreamClient:
    """
    Initialise the global Redis client. Called once from the lifespan context.

    Args:
        settings: Optional Settings override (useful for testing).

    Returns:
        The initialised RedisStreamClient singleton.
    """
    global _redis_client  # noqa: PLW0603
    cfg = settings or get_settings()
    client = RedisStreamClient(settings=cfg)
    await client.initialise()
    _redis_client = client
    return _redis_client


async def shutdown_redis() -> None:
    """
    Close the global Redis client. Called once from the lifespan context.
    """
    global _redis_client  # noqa: PLW0603
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None

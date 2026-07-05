"""
AetherSRE — RCA Processor Background Stream Worker
==================================================
Subscribes to the `incident_alerts_stream`, pulls anomalies, triggers the
Ollama-based LLM root-cause analysis, and writes the enriched diagnostic summary
to `rca_insights_stream`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Final

import redis.asyncio as aioredis

from app.core.config import get_settings
from app.core.llm_client import OllamaRcaClient
from app.core.logging_config import configure_logging, get_logger
from app.core.metrics import rca_duration_seconds, rca_requests_total

logger = get_logger(__name__)

_XREAD_BLOCK_MS: Final[int] = 200
_XREAD_COUNT: Final[int] = 5
_STATS_INTERVAL_SECONDS: Final[int] = 30


@dataclass
class RcaWorkerStats:
    """Statistics tracked during the lifecycle of the RCA worker."""
    incidents_consumed: int = 0
    rca_generated: int = 0
    failures: int = 0
    acks: int = 0
    total_latency_s: float = 0.0
    start_time: float = 0.0

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self.start_time if self.start_time else 0.0

    @property
    def avg_latency_ms(self) -> float:
        if self.rca_generated == 0:
            return 0.0
        return (self.total_latency_s / self.rca_generated) * 1000.0

    def summary(self) -> str:
        return (
            f"elapsed={self.elapsed_s:.0f}s "
            f"consumed={self.incidents_consumed} "
            f"rca_generated={self.rca_generated} "
            f"failures={self.failures} "
            f"acks={self.acks} "
            f"avg_latency={self.avg_latency_ms:.1f}ms"
        )


class RcaProcessorWorker:
    """
    Subscribes to Redis Stream `incident_alerts_stream`, processes anomalies via Ollama LLM,
    and publishes structured RCA reports to `rca_insights_stream`.
    """

    def __init__(
        self,
        consumer_group: str | None = None,
        consumer_name: str | None = None,
        llm_client: OllamaRcaClient | None = None,
    ) -> None:
        self._settings = get_settings()
        self._consumer_group = consumer_group or self._settings.rca_consumer_group
        self._consumer_name = consumer_name or self._settings.rca_consumer_name
        self._llm_client = llm_client or OllamaRcaClient(settings=self._settings)

        self._client: aioredis.Redis | None = None  # type: ignore[type-arg]
        self._stop_event = asyncio.Event()
        self._stats = RcaWorkerStats()

    async def start(self) -> None:
        """Connects to Redis and creates the consumer group if needed."""
        logger.info(
            "RcaProcessorWorker starting | group=%s consumer=%s",
            self._consumer_group,
            self._consumer_name,
        )

        self._client = aioredis.Redis(
            host=self._settings.redis_host,
            port=self._settings.redis_port,
            db=self._settings.redis_db,
            password=self._settings.redis_password,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=30,
            retry_on_timeout=True,
        )
        await self._client.ping()

        # Create consumer group idempotently on the incident_alerts_stream
        stream_name = "incident_alerts_stream"
        try:
            await self._client.xgroup_create(
                name=stream_name,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "RCA Consumer group created | stream=%s group=%s",
                stream_name,
                self._consumer_group,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.info(
                    "RCA Consumer group already exists | stream=%s group=%s",
                    stream_name,
                    self._consumer_group,
                )
            else:
                raise

        self._stats.start_time = time.monotonic()

    async def run(self) -> None:
        """Main polling loop consuming from the incident alerts stream."""
        if self._client is None:
            raise RuntimeError("RcaProcessorWorker has not been started.")

        stream_name = "incident_alerts_stream"
        stats_last_reported = time.monotonic()

        logger.info("Entering main RCA consumer loop | stream=%s", stream_name)

        while not self._stop_event.is_set():
            try:
                await self._poll_once(stream_name)
            except Exception as exc:
                logger.error("Unhandled error in RCA consumer loop | error=%s", exc, exc_info=True)
                await asyncio.sleep(1.0)

            if time.monotonic() - stats_last_reported >= _STATS_INTERVAL_SECONDS:
                logger.info("🤖 RCA WORKER STATS | %s", self._stats.summary())
                stats_last_reported = time.monotonic()

    async def _poll_once(self, stream_name: str) -> None:
        assert self._client is not None

        try:
            response = await self._client.xreadgroup(
                groupname=self._consumer_group,
                consumername=self._consumer_name,
                streams={stream_name: ">"},
                count=_XREAD_COUNT,
                block=_XREAD_BLOCK_MS,
            )
        except Exception as exc:
            logger.error("RCA XREADGROUP failed | error=%s", exc)
            raise

        if not response:
            return

        for _stream, entries in response:
            for stream_id, fields in entries:
                self._stats.incidents_consumed += 1
                try:
                    await self._process_entry(stream_id, fields)
                except Exception as exc:
                    logger.error(
                        "Failed to process stream entry for RCA | id=%s error=%s",
                        stream_id,
                        exc,
                        exc_info=True,
                    )
                    self._stats.failures += 1

    async def _process_entry(self, stream_id: str, fields: dict[str, str]) -> None:
        assert self._client is not None

        service_name = fields.get("service_name", "unknown")
        raw_message = fields.get("raw_message", "")
        context_raw = fields.get("context_window", "[]")

        try:
            context_window = json.loads(context_raw)
        except json.JSONDecodeError:
            context_window = []

        # Run LLM analysis
        t0 = time.monotonic()
        try:
            report = await self._llm_client.analyze_incident(
                service_name=service_name,
                raw_message=raw_message,
                context_window=context_window,
            )
            rca_requests_total.labels(service_name=service_name, status="success").inc()
        except Exception as exc:
            rca_requests_total.labels(service_name=service_name, status="error").inc()
            logger.error("RCA generation failed | id=%s error=%s", stream_id, exc)
            raise
        finally:
            rca_duration_seconds.observe(time.monotonic() - t0)

        latency = time.monotonic() - t0
        self._stats.total_latency_s += latency
        self._stats.rca_generated += 1

        # Build downstream enriched payload
        rca_payload = {
            "incident_id": stream_id,
            "service_name": service_name,
            "timestamp": fields.get("timestamp", ""),
            "level": fields.get("level", "UNKNOWN"),
            "raw_message": raw_message,
            "normalized_message": fields.get("normalized_message", ""),
            "anomaly_score": fields.get("anomaly_score", "0.0"),
            "root_cause": report.root_cause,
            "suggested_fix": report.suggested_fix,
            "risk_level": report.risk_level.value,
            "impact_analysis": report.impact_analysis,
            "generation_time_s": f"{latency:.3f}",
            "analyzed_at": str(time.time()),
        }

        # Write to rca_insights_stream
        insights_stream = self._settings.rca_insights_stream_name
        cap = self._settings.rca_insights_stream_max_len

        rca_id = await self._client.xadd(
            name=insights_stream,
            fields=rca_payload,
            maxlen=cap,
            approximate=True,
        )

        logger.info(
            "✅ RCA Insights generated and published | incident_id=%s rca_id=%s service=%s latency=%.2fs",
            stream_id,
            rca_id,
            service_name,
            latency,
        )

        # ACK the incident stream message
        try:
            await self._client.xack("incident_alerts_stream", self._consumer_group, stream_id)
            self._stats.acks += 1
        except Exception as exc:
            logger.warning("RCA XACK failed | id=%s error=%s", stream_id, exc)

    async def stop(self) -> None:
        """Signal the run loop to stop."""
        logger.info("RcaProcessorWorker stop requested.")
        self._stop_event.set()

    async def close(self) -> None:
        """Close the Redis client connection."""
        if self._client is not None:
            await self._client.aclose()
            logger.info("RCA Redis client closed.")


async def _async_main(args: argparse.Namespace) -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level)

    logger.info("=" * 64)
    logger.info("  AetherSRE RCA Processor Worker starting...")
    logger.info("  Redis    : %s:%d/%d", settings.redis_host, settings.redis_port, settings.redis_db)
    logger.info("  Source   : incident_alerts_stream")
    logger.info("  Sink     : %s", settings.rca_insights_stream_name)
    logger.info("  Ollama   : %s (model: %s)", settings.ollama_url, settings.ollama_model)
    logger.info("=" * 64)

    worker = RcaProcessorWorker(
        consumer_group=args.consumer_group,
        consumer_name=args.consumer_name,
    )

    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info("Signal %s received — requesting worker stop.", sig.name)
        asyncio.create_task(worker.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            pass  # Windows

    try:
        await worker.start()
        await worker.run()
    except Exception as exc:
        logger.critical("Fatal worker error | %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        await worker.close()
        logger.info("RCA Worker shutdown complete.")


def main() -> None:
    """Module entry point."""
    parser = argparse.ArgumentParser(
        prog="aether-rca-worker",
        description="AetherSRE — Redis Stream consumer, LLM root-cause analysis worker.",
    )
    parser.add_argument(
        "--consumer-group",
        default=None,
        help="Redis consumer group name override.",
    )
    parser.add_argument(
        "--consumer-name",
        default=None,
        help="Unique consumer name override.",
    )
    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()

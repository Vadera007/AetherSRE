"""
AetherSRE — Remediation Processor Stream Worker
==============================================
Reads from `rca_insights_stream`, processes the risk matrix policy mapping,
executes auto-healing command scripts asynchronously, and fires webhook gates.
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

import httpx
import redis.asyncio as aioredis

from app.core.config import get_settings
from app.core.llm_client import AetherRcaReport, RiskLevel
from app.core.logging_config import configure_logging, get_logger
from app.core.metrics import remediations_total
from app.core.remediation_policy import RiskPolicyMatrix, ExecutionType
from app.core.remediation_executor import LocalActionExecutor

logger = get_logger(__name__)

_XREAD_BLOCK_MS: Final[int] = 200
_XREAD_COUNT: Final[int] = 5
_STATS_INTERVAL_SECONDS: Final[int] = 30
_PENDING_HASH_KEY: Final[str] = "aether:remediation:pending"


@dataclass
class RemediationWorkerStats:
    """Statistics tracked during the lifecycle of the Remediation worker."""
    consumed_insights: int = 0
    auto_executed: int = 0
    webhook_gates: int = 0
    failures: int = 0
    acks: int = 0
    start_time: float = 0.0

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self.start_time if self.start_time else 0.0

    def summary(self) -> str:
        return (
            f"elapsed={self.elapsed_s:.0f}s "
            f"consumed={self.consumed_insights} "
            f"auto_executed={self.auto_executed} "
            f"webhook_gates={self.webhook_gates} "
            f"failures={self.failures} "
            f"acks={self.acks}"
        )


class RemediationProcessorWorker:
    """
    Subscribes to Redis Stream `rca_insights_stream`, evaluates risk policy matrices,
    runs automated healing actions, or maps contexts to webhook gates.
    """

    def __init__(
        self,
        consumer_group: str = "remediation-processor-group",
        consumer_name: str = "remediation-worker-0",
    ) -> None:
        self._settings = get_settings()
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name

        self._client: aioredis.Redis | None = None  # type: ignore[type-arg]
        self._stop_event = asyncio.Event()
        self._stats = RemediationWorkerStats()

    async def start(self) -> None:
        """Connects to Redis and creates the consumer group if needed."""
        logger.info(
            "RemediationProcessorWorker starting | group=%s consumer=%s",
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

        # Create consumer group idempotently on the rca_insights_stream
        stream_name = self._settings.rca_insights_stream_name
        try:
            await self._client.xgroup_create(
                name=stream_name,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Remediation Consumer group created | stream=%s group=%s",
                stream_name,
                self._consumer_group,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.info(
                    "Remediation Consumer group already exists | stream=%s group=%s",
                    stream_name,
                    self._consumer_group,
                )
            else:
                raise

        self._stats.start_time = time.monotonic()

    async def run(self) -> None:
        """Main polling loop consuming from the rca stream."""
        if self._client is None:
            raise RuntimeError("RemediationProcessorWorker has not been started.")

        stream_name = self._settings.rca_insights_stream_name
        stats_last_reported = time.monotonic()

        logger.info("Entering main Remediation consumer loop | stream=%s", stream_name)

        while not self._stop_event.is_set():
            try:
                await self._poll_once(stream_name)
            except Exception as exc:
                logger.error("Unhandled error in Remediation consumer loop | error=%s", exc, exc_info=True)
                await asyncio.sleep(1.0)

            if time.monotonic() - stats_last_reported >= _STATS_INTERVAL_SECONDS:
                logger.info("🛠️ REMEDIATION WORKER STATS | %s", self._stats.summary())
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
            logger.error("Remediation XREADGROUP failed | error=%s", exc)
            raise

        if not response:
            return

        for _stream, entries in response:
            for stream_id, fields in entries:
                self._stats.consumed_insights += 1
                try:
                    await self._process_entry(stream_id, fields)
                except Exception as exc:
                    logger.error(
                        "Failed to process rca insight entry for remediation | id=%s error=%s",
                        stream_id,
                        exc,
                        exc_info=True,
                    )
                    self._stats.failures += 1

    async def _process_entry(self, stream_id: str, fields: dict[str, str]) -> None:
        assert self._client is not None

        service_name = fields.get("service_name", "unknown")
        root_cause = fields.get("root_cause", "")
        suggested_fix = fields.get("suggested_fix", "")
        risk_level_str = fields.get("risk_level", "LOW")
        incident_id = fields.get("incident_id", stream_id)

        try:
            risk_level = RiskLevel(risk_level_str)
        except ValueError:
            risk_level = RiskLevel.LOW

        # Re-construct Pydantic report context for evaluation
        report = AetherRcaReport(
            root_cause=root_cause,
            suggested_fix=suggested_fix,
            risk_level=risk_level,
            impact_analysis=fields.get("impact_analysis", ""),
        )

        action = RiskPolicyMatrix.evaluate(report)

        if action.execution_type == ExecutionType.AUTO_EXECUTE:
            # ── Auto execution flow ──────────────────────────────────────────
            logger.info(
                "⚡ [AUTO_EXECUTE] Initiating self-healing | service=%s risk=%s command=%r",
                service_name,
                risk_level.value,
                action.target_command,
            )
            self._stats.auto_executed += 1

            result = await LocalActionExecutor.execute(action.target_command)

            history_payload = {
                "incident_id": incident_id,
                "service_name": service_name,
                "action_id": action.action_id,
                "risk_level": risk_level.value,
                "target_command": action.target_command,
                "execution_type": "AUTO_EXECUTE",
                "status": "SUCCESS" if result.is_success else "FAILED",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration_s": f"{result.duration_s:.3f}",
                "executed_by": "System (Auto)",
                "timestamp": str(result.duration_s),
            }

            await self._client.xadd(
                name="remediation_history_stream",
                fields=history_payload,
                maxlen=10_000,
                approximate=True,
            )

            remediations_total.labels(
                execution_type="AUTO_EXECUTE",
                status="SUCCESS" if result.is_success else "FAILED",
            ).inc()

        else:
            # ── Human-in-the-loop gate flow ──────────────────────────────────
            logger.warning(
                "⏳ [PENDING_APPROVAL] High-risk remediation paused | service=%s risk=%s target_command=%r",
                service_name,
                risk_level.value,
                action.target_command,
            )
            self._stats.webhook_gates += 1

            pending_ctx = {
                "incident_id": incident_id,
                "service_name": service_name,
                "action_id": action.action_id,
                "risk_level": risk_level.value,
                "target_command": action.target_command,
            }

            remediations_total.labels(
                execution_type="MANUAL_APPROVE",
                status="PENDING",
            ).inc()

            # Store in Redis pending hash map
            await self._client.hset(
                _PENDING_HASH_KEY,
                incident_id,
                json.dumps(pending_ctx),
            )

            # Fire simulated external webhook gate call
            webhook_payload = {
                "event": "pending_remediation_approval",
                "incident_id": incident_id,
                "service_name": service_name,
                "risk_level": risk_level.value,
                "target_command": action.target_command,
                "root_cause": root_cause,
                "suggested_fix": suggested_fix,
            }

            # Use local webhook endpoint to broadcast
            try:
                async with httpx.AsyncClient(timeout=5.0) as http_client:
                    # In local dev environment, target URL. Fallback to logging.
                    url = f"http://localhost:8000/api/v1/remediation/gate"
                    await http_client.post(url, json=webhook_payload)
            except Exception as exc:
                logger.warning("Simulated webhook broadcast failed | error=%s", exc)

        # ACK the RCA insight stream message
        try:
            await self._client.xack(self._settings.rca_insights_stream_name, self._consumer_group, stream_id)
            self._stats.acks += 1
        except Exception as exc:
            logger.warning("Remediation XACK failed | id=%s error=%s", stream_id, exc)

    async def stop(self) -> None:
        """Signal the run loop to stop."""
        logger.info("RemediationProcessorWorker stop requested.")
        self._stop_event.set()

    async def close(self) -> None:
        """Close the Redis client connection."""
        if self._client is not None:
            await self._client.aclose()
            logger.info("Remediation Redis client closed.")


async def _async_main() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level)

    logger.info("=" * 64)
    logger.info("  AetherSRE Remediation Processor Worker starting...")
    logger.info("  Redis    : %s:%d/%d", settings.redis_host, settings.redis_port, settings.redis_db)
    logger.info("  Source   : %s", settings.rca_insights_stream_name)
    logger.info("  Sink     : remediation_history_stream")
    logger.info("  Pending  : %s", _PENDING_HASH_KEY)
    logger.info("=" * 64)

    worker = RemediationProcessorWorker()

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
        logger.info("Remediation Worker shutdown complete.")


def main() -> None:
    """Module entry point."""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()

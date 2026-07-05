"""
AetherSRE — Multi-Service Fake Log Simulator
=============================================
Standalone async script that impersonates four realistic corporate
microservices and continuously streams structured log events to the
AetherSRE ingestion API.

Services simulated:
  • auth-service        — Authentication & JWT management
  • payment-gateway     — Payment processing & fraud detection
  • api-gateway         — Reverse proxy, rate limiting, routing
  • user-db             — User profile data store

Traffic characteristics:
  • Normal log volume: Poisson-distributed inter-arrival with jitter
  • Anomaly injection: Configurable burst rate (~5% of events)
  • Concurrent workers: One asyncio task per service running independently
  • Graceful shutdown: Handles SIGINT / SIGTERM cleanly

Usage:
    python -m simulator.log_generator [--url URL] [--rate RATE] [--workers N]

Dependencies:
    pip install httpx rich
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, NamedTuple

import httpx

# ---------------------------------------------------------------------------
# Rich console output (graceful fallback if rich is not installed)
# ---------------------------------------------------------------------------

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.table import Table
    from rich.text import Text

    _RICH_AVAILABLE = True
    _console = Console(stderr=True)
except ImportError:
    _RICH_AVAILABLE = False
    _console = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def _configure_simulator_logging() -> None:
    """Configure logging for the simulator process."""
    if _RICH_AVAILABLE:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler(console=_console, rich_tracebacks=True)],  # type: ignore[arg-type]
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
            stream=sys.stdout,
        )


_configure_simulator_logging()
logger = logging.getLogger("aether.simulator")


# ---------------------------------------------------------------------------
# Simulator configuration
# ---------------------------------------------------------------------------


@dataclass
class SimulatorConfig:
    """Runtime configuration for the log simulator."""

    api_url: str = "http://localhost:8000/api/v1/logs"
    """Target FastAPI ingest endpoint."""

    events_per_second: float = 5.0
    """Average events across all workers per second."""

    anomaly_probability: float = 0.05
    """Fraction of events that will be ERROR or CRITICAL level."""

    max_retries: int = 3
    """HTTP retry attempts before dropping an event."""

    request_timeout_seconds: float = 5.0
    """Per-request HTTP timeout."""

    jitter_factor: float = 0.4
    """Multiplier for random delay jitter (0 = no jitter, 1 = up to 100% variance)."""

    http_concurrency: int = 20
    """Maximum number of concurrent HTTP connections."""

    stats_report_interval: int = 30
    """Print cumulative statistics every N seconds."""


# ---------------------------------------------------------------------------
# Log templates per service
# ---------------------------------------------------------------------------


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogTemplate(NamedTuple):
    level: LogLevel
    message: str
    weight: float  # Relative sampling probability


# auth-service templates
AUTH_SERVICE_NORMAL: list[LogTemplate] = [
    LogTemplate(LogLevel.INFO, "User login successful | user_id={uid} method=password", 30),
    LogTemplate(LogLevel.INFO, "JWT token issued | user_id={uid} ttl=3600s", 25),
    LogTemplate(LogLevel.INFO, "User logout | user_id={uid} session_duration={dur}s", 15),
    LogTemplate(LogLevel.INFO, "OAuth2 token refresh | client_id={cid}", 12),
    LogTemplate(LogLevel.DEBUG, "Session validation passed | session_id={sid}", 8),
    LogTemplate(LogLevel.INFO, "Password reset email dispatched | user_id={uid}", 5),
    LogTemplate(LogLevel.WARNING, "Failed login attempt | user_id={uid} attempt=3 ip={ip}", 4),
    LogTemplate(LogLevel.WARNING, "Rate limit approaching | user_id={uid} remaining=5", 1),
]

AUTH_SERVICE_ANOMALIES: list[LogTemplate] = [
    LogTemplate(LogLevel.CRITICAL, "BRUTE FORCE DETECTED | ip={ip} attempts=50 user_id={uid}", 3),
    LogTemplate(LogLevel.ERROR, "JWT signing key rotation failed | error=KeyVaultConnectionTimeout", 2),
    LogTemplate(LogLevel.CRITICAL, "Authentication service UNRESPONSIVE | downstream=identity-provider", 1),
    LogTemplate(LogLevel.ERROR, "Session store write failure | backend=redis error=ECONNREFUSED", 2),
    LogTemplate(LogLevel.CRITICAL, "Heap out of memory | used=3.9GB limit=4GB OOM_KILLER_INVOKED", 1),
    LogTemplate(LogLevel.ERROR, "Token validation deadlock detected | goroutines=4200 blocked", 1),
]

# payment-gateway templates
PAYMENT_SERVICE_NORMAL: list[LogTemplate] = [
    LogTemplate(LogLevel.INFO, "Payment authorised | txn_id={txn} amount={amt} currency=USD", 30),
    LogTemplate(LogLevel.INFO, "Refund processed | txn_id={txn} amount={amt} status=completed", 15),
    LogTemplate(LogLevel.INFO, "Payment captured | order_id={oid} processor=Stripe", 20),
    LogTemplate(LogLevel.INFO, "Fraud check passed | txn_id={txn} score=0.{score}", 18),
    LogTemplate(LogLevel.WARNING, "Payment retry queued | txn_id={txn} attempt=2 reason=network_timeout", 8),
    LogTemplate(LogLevel.DEBUG, "Idempotency key checked | key={txn} status=new", 5),
    LogTemplate(LogLevel.INFO, "Webhook dispatched | event=payment.succeeded endpoint=https://merchant.io", 4),
]

PAYMENT_SERVICE_ANOMALIES: list[LogTemplate] = [
    LogTemplate(LogLevel.CRITICAL, "Payment processor UNREACHABLE | provider=Stripe latency=30000ms", 2),
    LogTemplate(LogLevel.ERROR, "Database connection pool EXHAUSTED | pool_size=50 waiters=120", 3),
    LogTemplate(LogLevel.CRITICAL, "Fraud detection model TIMEOUT | txn_id={txn} fallback=block_all", 1),
    LogTemplate(LogLevel.ERROR, "Double-spend detected | txn_id={txn} idempotency_violation=true", 2),
    LogTemplate(LogLevel.CRITICAL, "PCI DSS COMPLIANCE ALERT | unencrypted PAN detected in logs", 1),
    LogTemplate(LogLevel.ERROR, "Settlement batch failed | batch_id={txn} records=5000 error=DB_DEADLOCK", 1),
]

# api-gateway templates
API_GATEWAY_NORMAL: list[LogTemplate] = [
    LogTemplate(LogLevel.INFO, "Request routed | method=GET path=/api/users/{uid} upstream=user-service status=200 latency=12ms", 30),
    LogTemplate(LogLevel.INFO, "Request routed | method=POST path=/api/payments upstream=payment-gateway status=201 latency=45ms", 20),
    LogTemplate(LogLevel.INFO, "Cache hit | key=user:{uid}:profile ttl_remaining=280s", 15),
    LogTemplate(LogLevel.WARNING, "Rate limit applied | client_ip={ip} limit=100 window=60s", 8),
    LogTemplate(LogLevel.INFO, "SSL certificate auto-renewed | domain=api.aethersre.io expires_in=90d", 3),
    LogTemplate(LogLevel.DEBUG, "Health check probe | target=auth-service status=healthy latency=2ms", 10),
    LogTemplate(LogLevel.INFO, "Request routed | method=DELETE path=/api/users/{uid} upstream=user-service status=204 latency=8ms", 10),
    LogTemplate(LogLevel.WARNING, "Upstream slow response | upstream=payment-gateway latency=2100ms sla=500ms", 4),
]

API_GATEWAY_ANOMALIES: list[LogTemplate] = [
    LogTemplate(LogLevel.CRITICAL, "Upstream CIRCUIT BREAKER OPEN | target=payment-gateway failure_rate=92%", 2),
    LogTemplate(LogLevel.ERROR, "Connection pool SATURATED | upstream=user-db pool=100 queue=450", 3),
    LogTemplate(LogLevel.CRITICAL, "DDoS pattern detected | rps=50000 source_asn=AS{score} blocking=true", 1),
    LogTemplate(LogLevel.ERROR, "TLS handshake failure spike | errors=500 in last 60s certificate=expired", 2),
    LogTemplate(LogLevel.CRITICAL, "Memory pressure critical | rss=7.8GB limit=8GB swap_used=100%", 1),
    LogTemplate(LogLevel.ERROR, "Config reload FAILED | invalid nginx.conf syntax error at line 247", 1),
]

# user-db templates
USER_DB_NORMAL: list[LogTemplate] = [
    LogTemplate(LogLevel.INFO, "Query executed | op=SELECT table=users rows=1 latency=3ms", 30),
    LogTemplate(LogLevel.INFO, "Query executed | op=UPDATE table=users rows=1 latency=5ms", 20),
    LogTemplate(LogLevel.INFO, "Index scan | table=users index=email_idx rows_scanned=1", 18),
    LogTemplate(LogLevel.DEBUG, "Connection checkout | pool_available=45 pool_max=50", 10),
    LogTemplate(LogLevel.INFO, "Vacuum completed | table=sessions pages_removed=1240 duration=340ms", 5),
    LogTemplate(LogLevel.INFO, "Checkpoint completed | wal_files=3 sync_time=12ms", 8),
    LogTemplate(LogLevel.WARNING, "Slow query detected | op=SELECT table=audit_log latency=820ms threshold=500ms", 5),
    LogTemplate(LogLevel.INFO, "Replication lag within bounds | replica=user-db-replica-1 lag=12ms", 4),
]

USER_DB_ANOMALIES: list[LogTemplate] = [
    LogTemplate(LogLevel.CRITICAL, "Database connection pool EXHAUSTED | used=50 max=50 waiting=200", 3),
    LogTemplate(LogLevel.ERROR, "Replication LAG CRITICAL | replica=user-db-replica-1 lag=45000ms", 2),
    LogTemplate(LogLevel.CRITICAL, "DISK SPACE CRITICAL | mount=/data used=98% free=2.1GB", 1),
    LogTemplate(LogLevel.ERROR, "Deadlock detected | txn_A=2041 txn_B=2039 victim=2041 table=orders", 2),
    LogTemplate(LogLevel.CRITICAL, "WAL archiving FAILED | error=S3AccessDenied wal_files_pending=8420", 1),
    LogTemplate(LogLevel.ERROR, "OOM Killer invoked | process=postgres pid={uid} killed=true rss=15GB", 1),
]


# ---------------------------------------------------------------------------
# Service descriptor
# ---------------------------------------------------------------------------


@dataclass
class ServiceDescriptor:
    """Describes a simulated microservice and its log template catalogue."""

    name: str
    normal_templates: list[LogTemplate]
    anomaly_templates: list[LogTemplate]
    metadata_extra: dict[str, str] = field(default_factory=dict)

    def _weighted_choice(self, templates: list[LogTemplate]) -> LogTemplate:
        """Pick a template using weight-proportional random sampling."""
        total = sum(t.weight for t in templates)
        r = random.uniform(0, total)
        cumulative = 0.0
        for template in templates:
            cumulative += template.weight
            if r <= cumulative:
                return template
        return templates[-1]

    def generate_event(self, anomaly_probability: float) -> dict[str, Any]:
        """
        Produce a single log event dictionary ready for JSON serialisation.

        The message template is filled with realistic random values using
        Python's str.format() substitution.

        Args:
            anomaly_probability: 0–1 probability of emitting an anomaly event.

        Returns:
            Dict matching the LogEvent Pydantic schema.
        """
        is_anomaly = random.random() < anomaly_probability

        if is_anomaly and self.anomaly_templates:
            template = self._weighted_choice(self.anomaly_templates)
        else:
            template = self._weighted_choice(self.normal_templates)

        # Fill in template placeholders with random realistic values
        message = template.message.format(
            uid=_rand_user_id(),
            cid=_rand_client_id(),
            sid=_rand_hex(16),
            txn=_rand_txn_id(),
            oid=_rand_order_id(),
            amt=f"{random.uniform(1.0, 9999.99):.2f}",
            dur=random.randint(10, 3600),
            ip=_rand_ip(),
            score=random.randint(10, 89),
        )

        metadata: dict[str, str] = {
            "trace_id": _rand_hex(32),
            "span_id": _rand_hex(16),
            "is_anomaly": str(is_anomaly).lower(),
            **self.metadata_extra,
        }

        return {
            "service_name": self.name,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": template.level.value,
            "message": message,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Random value helpers
# ---------------------------------------------------------------------------


def _rand_hex(length: int) -> str:
    return "".join(random.choices("0123456789abcdef", k=length))


def _rand_user_id() -> str:
    return f"usr_{_rand_hex(8)}"


def _rand_client_id() -> str:
    return f"cli_{_rand_hex(6)}"


def _rand_txn_id() -> str:
    return f"txn_{_rand_hex(12)}"


def _rand_order_id() -> str:
    return f"ord_{_rand_hex(10)}"


def _rand_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


# ---------------------------------------------------------------------------
# Statistics tracker
# ---------------------------------------------------------------------------


@dataclass
class SimulatorStats:
    """Thread-safe (within asyncio) counters for the simulator run."""

    sent: int = 0
    failed: int = 0
    retried: int = 0
    anomalies: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def record_sent(self, is_anomaly: bool) -> None:
        self.sent += 1
        if is_anomaly:
            self.anomalies += 1

    def record_failed(self) -> None:
        self.failed += 1

    def record_retry(self) -> None:
        self.retried += 1

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    @property
    def throughput(self) -> float:
        e = self.elapsed
        return self.sent / e if e > 0 else 0.0

    def summary(self) -> str:
        return (
            f"elapsed={self.elapsed:.0f}s "
            f"sent={self.sent} "
            f"failed={self.failed} "
            f"retried={self.retried} "
            f"anomalies={self.anomalies} "
            f"throughput={self.throughput:.2f} ev/s"
        )


# ---------------------------------------------------------------------------
# Core async worker
# ---------------------------------------------------------------------------


async def _worker(
    service: ServiceDescriptor,
    config: SimulatorConfig,
    stats: SimulatorStats,
    client: httpx.AsyncClient,
    stop_event: asyncio.Event,
) -> None:
    """
    Async coroutine that continuously generates and POSTs log events for
    a single microservice until `stop_event` is set.

    Inter-event delay is drawn from an exponential distribution (Poisson
    arrivals) with an additional uniform jitter component to prevent
    artificial synchronisation between workers.

    Args:
        service:    The ServiceDescriptor for this worker.
        config:     Simulator runtime configuration.
        stats:      Shared statistics accumulator.
        client:     Shared async HTTP client.
        stop_event: Signals all workers to stop gracefully.
    """
    # Number of services sharing the target rate
    num_services = 4
    base_interval = num_services / config.events_per_second

    logger.info("[%s] Worker started | target_rate=%.2f ev/s", service.name, config.events_per_second / num_services)

    while not stop_event.is_set():
        event_data = service.generate_event(anomaly_probability=config.anomaly_probability)
        is_anomaly = event_data["metadata"].get("is_anomaly") == "true"

        success = False
        for attempt in range(1, config.max_retries + 1):
            try:
                response = await client.post(
                    config.api_url,
                    json=event_data,
                    timeout=config.request_timeout_seconds,
                )
                response.raise_for_status()
                success = True

                level = event_data["level"]
                stream_id = response.json().get("stream_id", "?")

                if level in ("ERROR", "CRITICAL"):
                    logger.warning(
                        "🔴 [%-20s] %-8s %s | id=%s",
                        service.name,
                        level,
                        event_data["message"][:80],
                        stream_id,
                    )
                else:
                    logger.debug(
                        "🟢 [%-20s] %-8s %s | id=%s",
                        service.name,
                        level,
                        event_data["message"][:80],
                        stream_id,
                    )

                stats.record_sent(is_anomaly=is_anomaly)
                break

            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "[%s] HTTP %d on attempt %d/%d | %s",
                    service.name, exc.response.status_code, attempt, config.max_retries, exc,
                )
                if attempt < config.max_retries:
                    stats.record_retry()
                    await asyncio.sleep(2 ** attempt * 0.1)  # Exponential back-off
                    
            except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError) as exc:
                logger.warning(
                    "[%s] Request failed on attempt %d/%d | %s: %s",
                    service.name, attempt, config.max_retries, type(exc).__name__, exc,
                )
                if attempt < config.max_retries:
                    stats.record_retry()
                    await asyncio.sleep(2 ** attempt * 0.2)

        if not success:
            stats.record_failed()
            logger.error("[%s] Dropping event after %d failed attempts.", service.name, config.max_retries)

        # ── Delay until next event ────────────────────────────────────────────
        # Exponential inter-arrival time with uniform jitter
        raw_delay = random.expovariate(1.0 / base_interval)
        jitter = raw_delay * config.jitter_factor * random.uniform(-1.0, 1.0)
        delay = max(0.01, raw_delay + jitter)  # Floor at 10ms
        await asyncio.sleep(delay)

    logger.info("[%s] Worker stopped gracefully.", service.name)


async def _stats_reporter(
    stats: SimulatorStats,
    config: SimulatorConfig,
    stop_event: asyncio.Event,
) -> None:
    """
    Periodically logs a summary statistics line to the console.
    """
    while not stop_event.is_set():
        await asyncio.sleep(config.stats_report_interval)
        if not stop_event.is_set():
            logger.info("📊 STATS | %s", stats.summary())


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the simulator."""
    parser = argparse.ArgumentParser(
        prog="aether-simulator",
        description="AetherSRE fake log generator — streams events to the ingest API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000/api/v1/logs",
        help="Target FastAPI ingest endpoint URL.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="Total target events per second across all service workers.",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Fraction of events (0–1) that will be ERROR or CRITICAL.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-request HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=30,
        help="Print cumulative statistics every N seconds.",
    )
    return parser.parse_args()


async def main() -> None:
    """
    Orchestrate all service workers as concurrent async tasks.

    Lifecycle:
        1. Parse CLI args into a SimulatorConfig.
        2. Create a shared httpx.AsyncClient with connection limits.
        3. Launch one worker task per service + one stats reporter task.
        4. Wait for SIGINT/SIGTERM → set stop_event → await all tasks.
    """
    args = _parse_args()

    config = SimulatorConfig(
        api_url=args.url,
        events_per_second=args.rate,
        anomaly_probability=args.anomaly_rate,
        request_timeout_seconds=args.timeout,
        stats_report_interval=args.stats_interval,
    )

    services: list[ServiceDescriptor] = [
        ServiceDescriptor(
            name="auth-service",
            normal_templates=AUTH_SERVICE_NORMAL,
            anomaly_templates=AUTH_SERVICE_ANOMALIES,
            metadata_extra={"team": "identity", "tier": "critical"},
        ),
        ServiceDescriptor(
            name="payment-gateway",
            normal_templates=PAYMENT_SERVICE_NORMAL,
            anomaly_templates=PAYMENT_SERVICE_ANOMALIES,
            metadata_extra={"team": "fintech", "tier": "critical", "pci_scope": "true"},
        ),
        ServiceDescriptor(
            name="api-gateway",
            normal_templates=API_GATEWAY_NORMAL,
            anomaly_templates=API_GATEWAY_ANOMALIES,
            metadata_extra={"team": "platform", "tier": "critical"},
        ),
        ServiceDescriptor(
            name="user-db",
            normal_templates=USER_DB_NORMAL,
            anomaly_templates=USER_DB_ANOMALIES,
            metadata_extra={"team": "data", "tier": "high", "engine": "postgresql-16"},
        ),
    ]

    stats = SimulatorStats()
    stop_event = asyncio.Event()

    # ── Graceful shutdown signal handling ────────────────────────────────────
    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info("Signal %s received — initiating graceful shutdown...", sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            # Windows: signal handlers not supported in asyncio event loop
            pass

    logger.info("=" * 64)
    logger.info("  AetherSRE Log Simulator starting")
    logger.info("  Target API : %s", config.api_url)
    logger.info("  Rate       : %.2f ev/s total across %d services", config.events_per_second, len(services))
    logger.info("  Anomaly %%  : %.1f%%", config.anomaly_probability * 100)
    logger.info("=" * 64)

    # ── Verify API is reachable before starting workers ──────────────────────
    logger.info("Checking API connectivity...")
    async with httpx.AsyncClient(timeout=10.0) as probe_client:
        health_url = config.api_url.replace("/api/v1/logs", "/health")
        for attempt in range(1, 6):
            try:
                resp = await probe_client.get(health_url)
                resp.raise_for_status()
                data = resp.json()
                logger.info(
                    "✅ API reachable | status=%s redis=%s stream=%s",
                    data.get("status"),
                    data.get("redis_connected"),
                    data.get("stream_name"),
                )
                break
            except Exception as exc:
                logger.warning("Attempt %d/5: API not ready (%s). Retrying in 3s...", attempt, exc)
                if attempt == 5:
                    logger.error("API unreachable after 5 attempts. Aborting.")
                    sys.exit(1)
                await asyncio.sleep(3)

    # ── Create shared HTTP client ─────────────────────────────────────────────
    limits = httpx.Limits(
        max_connections=config.http_concurrency,
        max_keepalive_connections=config.http_concurrency // 2,
        keepalive_expiry=30,
    )

    async with httpx.AsyncClient(limits=limits, timeout=config.request_timeout_seconds) as client:
        # Launch all worker tasks + stats reporter
        worker_tasks = [
            asyncio.create_task(
                _worker(service, config, stats, client, stop_event),
                name=f"worker-{service.name}",
            )
            for service in services
        ]

        stats_task = asyncio.create_task(
            _stats_reporter(stats, config, stop_event),
            name="stats-reporter",
        )

        all_tasks = worker_tasks + [stats_task]

        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            # Ensure any remaining tasks are cancelled cleanly
            for task in all_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)

    logger.info("=" * 64)
    logger.info("  Simulator finished | %s", stats.summary())
    logger.info("=" * 64)


if __name__ == "__main__":
    asyncio.run(main())

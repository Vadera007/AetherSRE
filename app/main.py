"""
AetherSRE — FastAPI Application Entry Point (Day 3)
====================================================
Wires together all routers, middleware, exception handlers, and the
lifespan context manager into a single deployable ASGI application.

Architecture overview (Day 3):
  ┌───────────────────────────────────────────────────────────┐
  │                    FastAPI ASGI App                       │
  │                                                           │
  │  ┌─────────────────────────────────────────────────────┐  │
  │  │  Lifespan Context Manager                           │  │
  │  │  ├── Redis pool init / teardown                     │  │
  │  │  ├── AetherAnomalyDetector singleton init           │  │
  │  │  ├── LogContextBuffer singleton init                 │  │
  │  │  └── Background baseline-training monitor task      │  │
  │  └─────────────────────────────────────────────────────┘  │
  │                                                           │
  │  ┌──────────────┐ ┌───────────────┐ ┌─────────────────┐  │
  │  │ GET /health  │ │POST /api/v1/  │ │GET /api/v1/     │  │
  │  │              │ │     logs      │ │incidents/recent  │  │
  │  └──────────────┘ └───────────────┘ └─────────────────┘  │
  └───────────────────────────────────────────────────────────┘
              │                              │
              ▼                              ▼
    Redis Streams                  Redis Streams
    (telemetry_log_stream)         (incident_alerts_stream)

Baseline Training Monitor
-------------------------
A long-running asyncio background task polls the global vector store
every 10 seconds.  The first time it finds ≥ 128 vectors, it trains the
Isolation Forest baseline in a thread pool executor and then exits.
This decouples the API server from the ML training path — the API stays
responsive throughout the training window.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import numpy as np
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.anomaly_detector import AetherAnomalyDetector, get_anomaly_detector
from app.core.config import get_settings
from app.core.context_buffer import LogContextBuffer, get_context_buffer
from app.core.logging_config import configure_logging, get_logger
from app.core.redis_client import shutdown_redis, startup_redis
from app.core.vector_store import VectorStore, get_vector_store
from app.routers import health as health_router
from app.routers import ingestion as ingestion_router
from app.routers import incidents as incidents_router
from app.routers import rca as rca_router
from app.routers import webhooks as webhooks_router
from app.routers import remediation as remediation_router
from app.routers import dashboard as dashboard_router
from app.workers.rca_processor import RcaProcessorWorker
from app.workers.remediation_processor import RemediationProcessorWorker
from prometheus_client import make_asgi_app

# ---------------------------------------------------------------------------
# Bootstrap logging before anything else
# ---------------------------------------------------------------------------

settings = get_settings()
configure_logging(level=settings.log_level)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Baseline training constants
# ---------------------------------------------------------------------------

_BASELINE_MIN_VECTORS: int = 128
_BASELINE_POLL_INTERVAL_S: float = 10.0


# ---------------------------------------------------------------------------
# Background task: baseline training monitor
# ---------------------------------------------------------------------------


async def _baseline_training_monitor(
    store: VectorStore,
    detector: AetherAnomalyDetector,
) -> None:
    """
    Background asyncio task that waits until the vector store has accumulated
    enough embeddings and then trains the Isolation Forest baseline.

    Polls every ``_BASELINE_POLL_INTERVAL_S`` seconds.  Exits immediately
    after training completes (or if the detector is already trained from a
    previous session).

    Args:
        store:    The global VectorStore instance to monitor.
        detector: The global AetherAnomalyDetector to train.
    """
    logger.info(
        "Baseline training monitor started | "
        "waiting for %d vectors in store (poll=%.0fs)",
        _BASELINE_MIN_VECTORS,
        _BASELINE_POLL_INTERVAL_S,
    )

    while True:
        if detector.is_trained:
            logger.info("Baseline training monitor: detector already trained — exiting.")
            return

        current_size = store.size
        logger.debug(
            "Baseline monitor poll | store_size=%d / threshold=%d",
            current_size,
            _BASELINE_MIN_VECTORS,
        )

        if current_size >= _BASELINE_MIN_VECTORS:
            logger.info(
                "Baseline threshold met | store_size=%d — triggering training.",
                current_size,
            )
            matrix: np.ndarray
            matrix, _ = store.snapshot()

            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, detector.train_baseline, matrix)
                logger.info(
                    "✅ API-side Isolation Forest baseline trained | samples=%d",
                    matrix.shape[0],
                )
            except Exception as exc:
                logger.error(
                    "API-side baseline training failed | error=%s", exc, exc_info=True
                )
            return  # Exit regardless of success/failure — do not retry

        await asyncio.sleep(_BASELINE_POLL_INTERVAL_S)


async def _metrics_sync_loop(redis_client: RedisStreamClient) -> None:
    """Background task to sync persistent Gauges directly with Redis stream sizes."""
    from app.core.metrics import (
        aether_logs_total_persistent,
        aether_anomalies_total_persistent,
        aether_remediations_total_persistent
    )
    settings = redis_client._settings
    
    while True:
        try:
            if redis_client._client:
                # Query sizes from Redis
                log_len = await redis_client._client.xlen(settings.redis_stream_name)
                anomaly_len = await redis_client._client.xlen("incident_alerts_stream")
                rem_len = await redis_client._client.xlen("remediation_history_stream")
                
                # Set Prometheus gauge values
                aether_logs_total_persistent.set(log_len)
                aether_anomalies_total_persistent.set(anomaly_len)
                aether_remediations_total_persistent.set(rem_len)
        except Exception:
            pass
        await asyncio.sleep(5.0)


# ---------------------------------------------------------------------------
# Lifespan — manage resources that span the entire application lifetime
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    FastAPI lifespan context manager (Day 6 version).

    Startup:
        1. Initialise the async Redis connection pool.
        2. Resolve global singletons (VectorStore, AnomalyDetector,
           LogContextBuffer) so API endpoints can access them via state.
        3. Launch the background baseline training monitor task.
        4. Launch the background RCA processor worker task.
        5. Launch the background Remediation processor worker task.

    Shutdown:
        1. Cancel the training monitor task (if still running).
        2. Cancel and close the RCA processor worker task.
        3. Cancel and close the Remediation processor worker task.
        4. Stop the WebSocket background stream polling task.
        5. Gracefully drain and close the Redis pool.
    """
    logger.info("=" * 60)
    logger.info("AetherSRE API starting up (Day 6) | env=%s", settings.api_env)
    logger.info("=" * 60)

    # ── Startup ───────────────────────────────────────────────────────────────
    redis_client = await startup_redis(settings=settings)
    app.state.redis_client = redis_client

    # Resolve/create global ML singletons
    vector_store: VectorStore = get_vector_store()
    anomaly_detector: AetherAnomalyDetector = get_anomaly_detector()
    context_buffer: LogContextBuffer = get_context_buffer()

    # Expose via app.state for optional use by routers/health checks
    app.state.vector_store = vector_store
    app.state.anomaly_detector = anomaly_detector
    app.state.context_buffer = context_buffer

    # Launch the background training monitor task
    training_monitor_task: asyncio.Task[None] = asyncio.create_task(
        _baseline_training_monitor(vector_store, anomaly_detector),
        name="baseline-training-monitor",
    )
    app.state.training_monitor_task = training_monitor_task

    # Launch the RCA processor worker inside the API process as a background task
    rca_worker = RcaProcessorWorker()
    await rca_worker.start()
    rca_worker_task: asyncio.Task[None] = asyncio.create_task(
        rca_worker.run(),
        name="rca-processor-worker",
    )
    app.state.rca_worker = rca_worker
    app.state.rca_worker_task = rca_worker_task

    # Launch the Remediation processor worker inside the API process as a background task
    remediation_worker = RemediationProcessorWorker()
    await remediation_worker.start()
    remediation_worker_task: asyncio.Task[None] = asyncio.create_task(
        remediation_worker.run(),
        name="remediation-processor-worker",
    )
    app.state.remediation_worker = remediation_worker
    app.state.remediation_worker_task = remediation_worker_task

    # Launch the background metrics sync loop
    metrics_sync_task: asyncio.Task[None] = asyncio.create_task(
        _metrics_sync_loop(redis_client),
        name="metrics-sync-loop",
    )
    app.state.metrics_sync_task = metrics_sync_task

    logger.info(
        "All startup tasks complete. "
        "API is ready | anomaly_detector_trained=%s store_size=%d",
        anomaly_detector.is_trained,
        vector_store.size,
    )

    yield  # Application is live here

    # ── Shutdown ──────────────────────────────────────────────────────────────
    logger.info("AetherSRE API shutting down | draining connections...")

    # Cancel the monitor task if it's still waiting
    if not training_monitor_task.done():
        training_monitor_task.cancel()
        try:
            await training_monitor_task
        except asyncio.CancelledError:
            pass

    # Cancel the metrics sync loop task
    logger.info("Stopping metrics sync loop background task...")
    if not metrics_sync_task.done():
        metrics_sync_task.cancel()
        try:
            await metrics_sync_task
        except asyncio.CancelledError:
            pass

    # Cancel and close the RCA worker
    logger.info("Stopping RCA worker background task...")
    await rca_worker.stop()
    if not rca_worker_task.done():
        try:
            await asyncio.wait_for(rca_worker_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            rca_worker_task.cancel()
    await rca_worker.close()

    # Cancel and close the Remediation worker
    logger.info("Stopping Remediation worker background task...")
    await remediation_worker.stop()
    if not remediation_worker_task.done():
        try:
            await asyncio.wait_for(remediation_worker_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            remediation_worker_task.cancel()
    await remediation_worker.close()

    # Cancel WebSocket stream poller
    logger.info("Stopping WebSocket stream poller...")
    from app.routers.dashboard import stop_stream_polling
    stop_stream_polling()

    await shutdown_redis()
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_application() -> FastAPI:
    """
    Construct and configure the FastAPI application (Day 6).

    Returns a fully wired ASGI app ready to be served by Uvicorn.
    """
    cfg = get_settings()

    application = FastAPI(
        title="AetherSRE: Autonomous Log Anomaly & Self-Healing Engine",
        description=(
            "Production-grade AIOps backend for real-time log ingestion, "
            "unsupervised anomaly detection, and autonomous remediation.\n\n"
            "**Day 1** — Redis Streams async ingestion pipeline.\n\n"
            "**Day 2** — Regex normalisation + Sentence Transformer embeddings.\n\n"
            "**Day 3** — Isolation Forest anomaly detection + "
            "sliding-window incident context framing.\n\n"
            "**Day 4** — AI-driven localized LLM root-cause analysis (Ollama).\n\n"
            "**Day 5** — Closed-loop autonomous risk-gated self-healing remediation.\n\n"
            "**Day 6** — Live WebSocket Operations Command Center Dashboard."
        ),
        version="6.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # ── CORS ──────────────────────────────────────────────────────────────────
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if cfg.api_env == "development" else [],
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-Request-ID"],
    )

    # ── Routers ───────────────────────────────────────────────────────────────
    application.include_router(health_router.router)
    application.include_router(ingestion_router.router)
    application.include_router(incidents_router.router)
    application.include_router(rca_router.router)
    application.include_router(webhooks_router.router)
    application.include_router(remediation_router.router)
    application.include_router(dashboard_router.router)

    # ── Prometheus metrics endpoint ───────────────────────────────────────────
    metrics_app = make_asgi_app()
    application.mount("/metrics", metrics_app)

    # ── Global exception handler ──────────────────────────────────────────────
    @application.exception_handler(Exception)
    async def _unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        """
        Catch-all handler for unhandled exceptions.

        Returns a sanitised 500 response without leaking internal stack traces.
        The full exception is logged server-side.
        """
        logger.error(
            "Unhandled exception | method=%s path=%s error=%s",
            request.method,
            request.url.path,
            exc,
            exc_info=True,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An internal server error occurred."},
        )

    logger.info(
        "FastAPI application constructed | routes=%d",
        len(application.routes),
    )
    return application


# ---------------------------------------------------------------------------
# Module-level app instance — referenced by Uvicorn's `app.main:app`
# ---------------------------------------------------------------------------

app: FastAPI = create_application()




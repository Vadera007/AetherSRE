"""
AetherSRE — Prometheus Metrics Registry
========================================
Centralised metric definitions. Import and use these objects in routers/workers
so all metrics are registered on the default CollectorRegistry.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, Info

# ── Ingestion ─────────────────────────────────────────────────────────────────
logs_ingested_total = Counter(
    "aether_logs_ingested_total",
    "Total number of log events accepted by the ingestion API.",
    ["service_name", "level"],
)

# ── Anomaly Detection ─────────────────────────────────────────────────────────
anomalies_detected_total = Counter(
    "aether_anomalies_detected_total",
    "Total anomalous log events flagged by Isolation Forest.",
    ["service_name"],
)

vector_store_size = Gauge(
    "aether_vector_store_size",
    "Current number of embeddings held in the in-memory vector store.",
)

# ── RCA Processor ─────────────────────────────────────────────────────────────
rca_requests_total = Counter(
    "aether_rca_requests_total",
    "Total Root Cause Analysis requests sent to the LLM.",
    ["service_name", "status"],  # status: success | error
)

rca_duration_seconds = Histogram(
    "aether_rca_duration_seconds",
    "End-to-end latency of LLM RCA generation in seconds.",
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0],
)

# ── Remediation ───────────────────────────────────────────────────────────────
remediations_total = Counter(
    "aether_remediations_total",
    "Total remediation actions executed.",
    ["execution_type", "status"],  # execution_type: AUTO_EXECUTE | MANUAL_APPROVE
)

# ── Persistent Stream Lengths (Synced from Redis) ───────────────────────────
aether_logs_total_persistent = Gauge(
    "aether_logs_total_persistent",
    "Absolute persistent count of all log events processed, read from Redis stream length."
)

aether_anomalies_total_persistent = Gauge(
    "aether_anomalies_total_persistent",
    "Absolute persistent count of all anomalies flagged, read from Redis stream length."
)

aether_remediations_total_persistent = Gauge(
    "aether_remediations_total_persistent",
    "Absolute persistent count of all self-healing mitigations run, read from Redis stream length."
)

# AetherSRE — Project Changelog

All notable changes to the AetherSRE project are documented in this file.
Each day's entry records the architectural decisions, components built, and
the technical rationale behind them. This document serves as a living ledger
of the project's evolution.

Format inspired by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---


## [Version 1.0.0] — 2026-07-03 — Production Capstone Release & Performance Benchmarking

### Theme
> "Validate at scale — load testing rigs prove sub-millisecond pipelines and optimize LLM cost footprints."

The goal of Day 7 is the final production capstone validation pass. We built a high-throughput load testing rig, implemented performance metric tests validating throughput and deduplication caches, and consolidated recruiter-facing portfolio guides.

---

### Components Built

#### 1. `simulator/load_test.py` — Async Performance Test Rig
- **10k Ingest blast** — Emits 10k log messages over variable rates utilizing async tasks.
- **Controlled Chaos bursts** — Inject 50 duplicate errors inside 2 seconds to stress-test sliding context buffers.
- **Stats aggregators** — Computes P95/P99 latencies, logs per second throughput, and LLM call cost reduction ratios.

#### 2. `tests/test_performance.py` — Performance Assertions Suite
- Validates the structure and typing of performance metrics reports.
- Ensures duplicate alert signatures are suppressed across consecutive loops.

---

## [Day 6] — 2026-07-03 — Live WebSocket Operations Command Center Dashboard

### Theme
> "Visualize the stream — live WebSocket telemetry bridges automated healing loops to user operator eyes."

The goal of Day 6 is to build a real-time single-page application (SPA) dashboard served directly by FastAPI. It hooks into Redis streams, establishes WebSocket broadcast loops, feeds live log events, monitors performance statistics, and renders approval buttons to manually trigger gated critical self-healing remediation.

---

### Architecture Overview

```
                          Redis streams (telemetry, alerts, RCA, remediation)
                                              │
                                              ▼
                    ws-redis-stream-poller (asyncio background task)
                                              │
                                              ├─► broadcast JSON payload
                                              │
                                              ▼
                                   WebSocket /ws/telemetry
                                              │
                                              ▼
                                 Vanilla JS Client Dashboard
                                              │
                    ┌─────────────────────────┴────────────────────────┐
                    │                                                  │
                    ▼                                                  ▼
     Live Console & Metric Cards                          RCA & Remediation Queue
     • Ingests live telemetry log strings                 • Displays AI root-cause diagnostics
     • Updates log counters                               • Interactive approve/deny webhooks
```

---

### Components Built

#### 1. `app/templates/dashboard.html` — Vanilla JS Single Page Dashboard
- **Dark Theme Palette** — Styled using TailwindCSS slate/zinc backgrounds with emerald metrics, amber anomaly scores, and rose pending alerts.
- **Log Streaming Window** — Implemented an auto-scrolling log console capturing WebSocket events.
- **Interactive Action Gate Card** — Wires AJAX fetch calls to trigger `/api/v1/remediation/approve` or `/deny`.

#### 2. `app/routers/dashboard.py` — WebSockets Broadcast Edge Router
- **Jinja2 Render** — Serves the `dashboard.html` template on `GET /dashboard`.
- **WebSocket broadsast loop** — Multi-client `ConnectionManager` pushing parsed JSON events.
- **Redis poller** — Background asyncio task running alongside the connection loop to parse streams.

#### 3. Tests
- **`tests/test_dashboard.py`** — Verifies dashboard rendering and WebSocket handshakes.

---

## [Day 5] — 2026-07-03 — Closed-Loop Risk-Gated Remediation

### Theme
> "Close the loop safely — auto-execute low-risk mitigations, gate high-risk actions behind human verification."

The goal of Day 5 is to build the final layer of our autonomous healing loops. This reads AI diagnoses from `rca_insights_stream`, evaluates them against a deterministic risk matrix, triggers local subprocess fixes for low/medium risk occurrences, and pauses high-risk operations in a Redis state register until approved or denied by an operator.

---

### Architecture Overview

```
                                rca_insights_stream
                                         │
                                         ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  RemediationProcessorWorker (app/workers/remediation_processor.py)     │
  │                                                                        │
  │  1. Ingests RCA insight reports from the stream.                       │
  │                                                                        │
  │  2. Maps risk levels (LOW/MEDIUM/HIGH/CRITICAL) to Execution Types:    │
  │     • LOW/MEDIUM  ──► AUTO_EXECUTE                                     │
  │     • HIGH/CRIT   ──► WEBHOOK_GATE                                     │
  │                                                                        │
  │  3. Processes execution routes:                                         │
  │     • AUTO_EXECUTE: Invokes LocalActionExecutor subprocesses.          │
  │     • WEBHOOK_GATE: Enters PENDING state and registers context         │
  │                     in Redis pending hash map. Fires approval webhooks.│
  └──────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ├─► Write success/fail stats to
                                     │   remediation_history_stream
                                     │
                                     ▼
                      FastAPI Human Approval Gate API
                      • POST /api/v1/remediation/approve
                      • POST /api/v1/remediation/deny
```

---

### Components Built

#### 1. `app/core/remediation_policy.py` — Risk Evaluation Engine
- **Remediation Schema** — Defines the Pydantic `RemediationAction` fields mapping `action_id`, `risk_level`, `execution_type` and `target_command`.
- **`RiskPolicyMatrix`** — Deterministically assigns risk vectors. Mapped categories of LOW/MEDIUM risks (restarts, queue clears) configure `AUTO_EXECUTE`; HIGH/CRITICAL (DB config modifications, infrastructure changes) map to `WEBHOOK_GATE`.

#### 2. `app/core/remediation_executor.py` — Terminal Action Driver
- **`LocalActionExecutor`** — Secure terminal wrapper executing scripts asynchronously via `asyncio.create_subprocess_exec`.
- **Command Splitting & Guards** — Utilizes `shlex.split` to prevent shell injection vectors, enforces a strict `5.0` second execution timeout limit, and captures standard outputs.

#### 3. `app/routers/webhooks.py` — Webhook Approval Routes
- Exposes **`POST /api/v1/remediation/approve`** and **`POST /api/v1/remediation/deny`**. Operator approvals extract cached tasks from the Redis pending hash map, execute them, and store execution audit records. Operator denials abort the mitigation context.

#### 4. `app/routers/remediation.py` — Remediation Audit Route
- Exposes **`GET /api/v1/remediation/history`** returning the 15 most recent mitigation execution records from `remediation_history_stream`.

#### 5. Tests
- **`tests/test_remediation.py`** — Verifies risk matrix mapping, subprocess script runner behavior, and Redis pending-approval routing states.

---

### Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Human-in-the-Loop gates | Prevents AI hallucination or runaway cascades from modifying production database topologies or infrastructure states without human operator approval. |
| shlex split execution | Eliminates shell injection vulnerability vectors by ensuring parameters cannot execute arbitrary command chaining strings. |
| Redis pending state mapping | Storing task context payload states in Redis hash maps allows API router endpoints to re-hydrate execution contexts asynchronously. |

---

### Remediation History Schema (`remediation_history_stream`)

Each entry in `remediation_history_stream` contains:

| Field | Type | Description |
|-------|------|-------------|
| `incident_id` | string | Originating Incident Alert ID |
| `service_name` | string | Target microservice name |
| `action_id` | string | Matched action rule identifier |
| `risk_level` | string | Mapped threat severity |
| `target_command` | string | Evaluated command string |
| `execution_type` | string | AUTO_EXECUTE or WEBHOOK_GATE |
| `status` | string | Mitigation status (`SUCCESS`, `FAILED`, `ABORTED`) |
| `stdout` | string | Subprocess stdout logs |
| `stderr` | string | Subprocess stderr logs |
| `duration_s` | string | Execution time in seconds |
| `executed_by` | string | Running actor (`System (Auto)` or `Operator`) |
| `timestamp` | string | Run timestamp |

---

### What's Coming Next

| Day | Theme | Components |
|-----|-------|-----------|
| ✅ Day 1 | Ingestion | FastAPI, Redis Streams, Log simulator |
| ✅ Day 2 | Preprocessing | Regex Normalizer, SentenceTransformers |
| ✅ Day 3 | Detection | Isolation Forest, Context Ring Buffer |
| ✅ Day 4 | AI Diagnosis | Ollama client, RCA Worker, GET /rca/recent |
| ✅ Day 5 | Remediation | Risk Policy Engine, Local Subprocesses, Approvals |

---

## [Day 4] — 2026-07-03 — Localised LLM Root Cause Analysis with Ollama

### Theme
> "Diagnose at the edge — localized LLM inference matches anomalies to root causes with strict structural contracts."


The goal of Day 4 is to build the autonomous AI root-cause analysis (RCA) pipeline. This consumes anomalous context frames from Redis, ships them to a localized Ollama service instance, enforces structured JSON report outputs, and publishes diagnostic insights to a downstream Redis stream.

---

### Architecture Overview

```
                                incident_alerts_stream
                                           │
                                           ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  RcaProcessorWorker (app/workers/rca_processor.py)                     │
  │                                                                        │
  │  1. Pulls Incident Context Frames via XREADGROUP.                      │
  │                                                                        │
  │  2. Translates preceding window history and anomaly details            │
  │     into a structured system prompt template.                          │
  │                                                                        │
  │  3. Invokes Ollama Async API (http://ollama:11434/api/generate)        │
  │     with format="json" and low temperature parameters.                 │
  │                                                                        │
  │  4. Parses/validates structured JSON directly into Pydantic model      │
  │     AetherRcaReport:                                                   │
  │     • root_cause                                                       │
  │     • suggested_fix                                                    │
  │     • risk_level (LOW/MEDIUM/HIGH/CRITICAL)                            │
  │     • impact_analysis                                                  │
  │                                                                        │
  │  5. Appends enriched diagnostics to rca_insights_stream via XADD.      │
  │                                                                        │
  │  6. Sends XACK confirmation.                                           │
  └──────────────────────────────────┬─────────────────────────────────────┘
                                     │
                                     ▼
                            rca_insights_stream
                                     │
                                     ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  FastAPI API Edge                                                      │
  │  GET /api/v1/rca/recent  ──►  XREVRANGE rca_insights_stream            │
  └────────────────────────────────────────────────────────────────────────┘
```

---

### Components Built

#### 1. `app/core/llm_client.py` — Asynchronous Ollama Client
- **Structured Pydantic Contract** — Implements `AetherRcaReport` enforcing `root_cause`, `suggested_fix`, `risk_level` (Pydantic Enum), and `impact_analysis`.
- **Fault-Tolerant Retries** — Configured with async client requests using exponential backoff to handle CPU-bound inference pauses.
- **Strict Format Prompts** — Guides the LLM to output valid, raw JSON directly, requesting `format="json"` from the Ollama engine.

#### 2. `app/workers/rca_processor.py` — RCA Streaming Worker
- Subscribes to `incident_alerts_stream` using consumer group `rca-processor-group`.
- Maps context arrays, calls the localized LLM client, and formats downstream insights.
- Emits structured results to `rca_insights_stream` capped at 10,000 entries.

#### 3. `app/routers/rca.py` — RCA Retrieval Route
- Exposes **`GET /api/v1/rca/recent`** returning the 10 most recent automated root cause analysis reports.

#### 4. Uvicorn/Lifespan Wires
- Integrates the new worker thread loop context cleanly into `app/main.py` lifespan manager, spinning up the consumer alongside Uvicorn.

#### 5. Tests
- **`tests/test_rca.py`** — Validates Pydantic schema validation, LLM response client stubs, and mock Redis loop runs with 100% test success rate.

---

### Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Local Ollama API | Prevents vendor API key leaks and guarantees data compliance by running model inference locally on container CPU. |
| Strict JSON Format | Enforces JSON formatting at the model generation layer (using Ollama's `format: "json"` option) to avoid parser failures. |
| Exponential Backoff | Protects the consumer thread when CPU cores are saturated under heavy concurrent inference. |
| Decoupled Alert Streams | Using separate source and sink streams keeps telemetry processing decoupled from compute-heavy LLM diagnostics. |

---

### RCA Insights Stream Schema (`rca_insights_stream`)

Each entry in `rca_insights_stream` contains:

| Field | Type | Description |
|-------|------|-------------|
| `incident_id` | string | Originating Incident Alert stream ID |
| `service_name` | string | Microservice name |
| `timestamp` | string | Log timestamp |
| `level` | string | Anomaly severity level (`ERROR`, `CRITICAL`) |
| `raw_message` | string | Raw log message |
| `normalized_message` | string | Cleaned regex template |
| `anomaly_score` | string | Anomaly probability score |
| `root_cause` | string | LLM-generated root cause |
| `suggested_fix` | string | Remediation prescription |
| `risk_level` | string | Risk class (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`) |
| `impact_analysis` | string | Blast radius summary |
| `generation_time_s` | string | Time taken in seconds for inference |
| `analyzed_at` | string | Epoch timestamp when analysis ran |

---

### What's Coming Next

| Day | Theme | Components |
|-----|-------|-----------|
| ✅ Day 1 | Async Ingestion | FastAPI, Redis Streams, Log simulator |
| ✅ Day 2 | Preprocessing | Regex Normalizer, SentenceTransformers |
| ✅ Day 3 | Detection | Isolation Forest, Context Ring Buffer |
| ✅ Day 4 | AI Diagnosis | Ollama client, RCA Worker, GET /rca/recent |
| 🔲 Day 5 | Runbooks & Healing | Auto runbook executor, Slack alerts, metrics |

---

## [Day 1] — 2026-07-02 — Foundation: Async Ingestion Pipeline & Log Simulator

### Theme
> "Build the data plane first — everything intelligent comes later."

The goal of Day 1 is to establish a production-grade, end-to-end data
inflow pipeline that can reliably accept, validate, and persist log events
from multiple microservices at high throughput. No ML, no anomaly detection
yet — just a rock-solid foundation.

---

### Architecture Overview

```
                     ┌──────────────────────────────────┐
  4 microservices    │      simulator/log_generator.py  │
  (simulated)   ───► │  async httpx workers (per svc)   │
                     └────────────────┬─────────────────┘
                                      │  POST /api/v1/logs
                                      ▼
                     ┌──────────────────────────────────┐
                     │   FastAPI (Uvicorn, async)        │
                     │   app/main.py                     │
                     │                                   │
                     │  ┌─────────────┐ ┌─────────────┐ │
                     │  │GET /health  │ │POST /api/v1 │ │
                     │  │            │ │/logs        │ │
                     │  └─────────────┘ └──────┬──────┘ │
                     └─────────────────────────┼────────┘
                                               │ XADD
                                               ▼
                     ┌──────────────────────────────────┐
                     │     Redis 7.2 (Redis Streams)    │
                     │   Stream: telemetry_log_stream   │
                     │   MAXLEN: ~100,000 entries       │
                     └──────────────────────────────────┘
```

---

### Components Built

#### 1. Repository Scaffold
| Path | Purpose |
|------|---------|
| `app/` | FastAPI application package |
| `app/core/` | Config, logging, Redis client |
| `app/models/` | Pydantic domain models |
| `app/routers/` | API route handlers |
| `simulator/` | Standalone log generator script |
| `tests/` | Integration test suite |
| `docs/` | Architecture and changelog |

#### 2. Docker Infrastructure (`docker-compose.yml`)
- **Redis 7.2-alpine** container with AOF persistence, LRU eviction policy,
  and a 512 MB memory cap. Acts as the event streaming backbone.
- **FastAPI/Uvicorn** container with hot-reload via volume mounts,
  healthcheck using `curl /health`, and explicit dependency on Redis healthcheck.
- Custom bridge network `aether_net` for service-to-service DNS resolution.

#### 3. FastAPI Application (`app/`)
- **`app/core/config.py`** — `pydantic-settings` `Settings` class with
  full type validation. Singleton via `lru_cache`. Reads from `.env`.
- **`app/core/logging_config.py`** — Custom `AetherFormatter` with ANSI
  colours, `aether.*` logger namespace, and per-module `get_logger()`.
- **`app/core/redis_client.py`** — `RedisStreamClient` class encapsulating
  `redis.asyncio` connection pool. Provides `xadd()`, `xlen()`, `ping()`,
  `initialise()`, `close()` with full error propagation.
- **`app/models/log_event.py`** — Pydantic v2 `LogEvent` (request),
  `IngestResponse`, and `HealthStatus` (response) models with validators.
- **`app/routers/health.py`** — `GET /health` with live Redis PING + XLEN.
  Returns `HTTP 503` if Redis is unreachable.
- **`app/routers/ingestion.py`** — `POST /api/v1/logs` accepting `LogEvent`,
  calling `redis.xadd()`, returning `HTTP 202` with assigned stream ID.
- **`app/main.py`** — Application factory with `asynccontextmanager` lifespan,
  CORS middleware, and global unhandled-exception handler.

#### 4. Log Simulator (`simulator/log_generator.py`)
- **4 service workers** run as independent `asyncio` tasks:
  `auth-service`, `payment-gateway`, `api-gateway`, `user-db`.
- **Weighted log templates** — each service has 7–8 normal templates and
  5–6 anomaly templates, sampled proportionally by weight.
- **Anomaly injection** — 5% of events default to `ERROR`/`CRITICAL` severity
  (configurable via `--anomaly-rate`).
- **Realistic traffic** — inter-arrival delay drawn from exponential
  distribution (Poisson arrivals) + uniform jitter (±40%).
- **Retry logic** — exponential back-off, up to 3 attempts per event.
- **Graceful shutdown** — `SIGINT`/`SIGTERM` sets an `asyncio.Event` that
  all workers check before each iteration.
- **Statistics reporter** — logs cumulative throughput every 30 seconds.

#### 5. Integration Tests (`tests/test_api.py`)
- `TestHealthEndpoint` — 4 tests covering status code, schema, Redis status,
  and uptime positivity.
- `TestIngestionEndpoint` — 8 tests covering 202 acceptance, response schema,
  stream ID format, CRITICAL logs, empty message rejection, invalid level
  rejection, default timestamp, and service name normalisation.

---

### Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| `redis.asyncio` over sync Redis | Zero event-loop blocking; scales to thousands of concurrent ingest requests |
| `XADD MAXLEN ~ 100000` | Approximate trimming is O(1) amortised; exact trimming is O(N) |
| Pydantic v2 validators | Early rejection at the API boundary prevents malformed data from polluting the stream |
| `asynccontextmanager` lifespan | Idiomatic FastAPI ≥0.93 pattern; ensures pool is torn down on clean shutdown |
| Weighted template sampling | Produces realistic traffic distributions rather than uniform random |
| Exponential inter-arrival time | Models Poisson process — the standard model for independent request arrivals |
| `aether.*` logger namespace | Allows silencing all internal logs in tests via single `setLevel` call |

---

### Stream Schema

Each entry written to `telemetry_log_stream` has the following fields:

| Field | Type | Example |
|-------|------|---------|
| `service_name` | string | `payment-gateway` |
| `timestamp` | ISO-8601 string | `2024-01-15T12:34:56.789+00:00` |
| `level` | string enum | `CRITICAL` |
| `message` | string | `Database connection pool exhausted` |
| `metadata` | JSON string | `{"trace_id": "abc123", "is_anomaly": "true"}` |

---

### What's Coming Next

| Day | Theme | Components |
|-----|-------|-----------|
| **Day 2** | Stream Consumer & Anomaly Detector | Background consumer worker, rule-based anomaly detection engine, alert state machine |
| **Day 3** | ML Anomaly Detection | Isolation Forest / LSTM model training on stream data |
| **Day 4** | Self-Healing Actions | Automated runbook executor, Slack/PagerDuty alerting |
| **Day 5** | Observability Dashboard | Prometheus metrics, Grafana dashboards, distributed tracing |

---

*Last updated: Day 1 — 2026-07-02*

---

## [Day 2] — 2026-07-02 — Stream Preprocessing Engine & Vector Embedding Layer

### Theme
> "Transform chaos into geometry — every log becomes a point in semantic space."

The goal of Day 2 is to build the intelligence substrate: the machinery that
transforms raw, noisy log strings into dense vector representations that a
downstream anomaly detector can reason over mathematically.  No anomaly
detection yet — just the cleanest, most accurate embeddings we can produce.

---

### Project Vision Statement

AetherSRE is an autonomous SRE co-pilot for production microservice fleets.
It ingests high-volume log streams, detects anomalies using unsupervised ML
(embedding similarity, Isolation Forest, LSTM reconstruction error), and
initiates self-healing actions (scaling runbooks, circuit-breaker toggles,
alerting) without human intervention.

The target outcome: reduce mean-time-to-detect (MTTD) from minutes to seconds
and mean-time-to-remediate (MTTR) from hours to automated sub-minute responses.

---

### Day 2 Architecture — Full Data Flow

```
  POST /api/v1/logs         ← Simulator (4 microservices)
         │
         ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  FastAPI  (app/main.py)                                      │
  │  Pydantic LogEvent validation                                │
  │  XADD → telemetry_log_stream (Redis 7.2)                     │
  └──────────────────────────────────────────────────────────────┘
         │  Redis Stream (persistent, MAXLEN ~100k)
         │
         ▼  XREADGROUP (consumer group: aether-vector-workers)
  ┌──────────────────────────────────────────────────────────────┐
  │  VectorProcessorWorker  (app/workers/vector_processor.py)    │
  │                                                              │
  │  ┌─────────────────────────────────┐                         │
  │  │  MicroBatchAccumulator          │                         │
  │  │  max_size=32 | timeout=0.5s     │  ← Size or time trigger │
  │  └────────────────┬────────────────┘                         │
  │                   │  batch of ParsedEntry objects            │
  │  ┌────────────────▼────────────────┐                         │
  │  │  Regex Normalizer               │                         │
  │  │  (app/core/normalizer.py)       │                         │
  │  │  16 compiled RE patterns        │                         │
  │  │  "Login from 192.168.1.1"       │                         │
  │  │       → "Login from <IP>"       │                         │
  │  └────────────────┬────────────────┘                         │
  │                   │  normalised template strings             │
  │  ┌────────────────▼────────────────┐                         │
  │  │  Sentence Transformer           │                         │
  │  │  all-MiniLM-L6-v2 (CPU)        │                         │
  │  │  run_in_executor (thread pool)  │                         │
  │  │  → float32[batch, 384]          │                         │
  │  └────────────────┬────────────────┘                         │
  │                   │  dense embeddings                        │
  │  ┌────────────────▼────────────────┐                         │
  │  │  NumPy Vector Store             │                         │
  │  │  (app/core/vector_store.py)     │                         │
  │  │  Thread-safe | cosine query     │                         │
  │  │  Stores: vector + full metadata │                         │
  │  └─────────────────────────────────┘                         │
  │                                                              │
  │  XACK → Redis (at-least-once delivery guarantee)            │
  └──────────────────────────────────────────────────────────────┘
```

---

### Components Built

#### 1. `app/core/normalizer.py` — High-Performance Regex Normalizer
- **16 compiled regex patterns** covering: ISO 8601/syslog/epoch timestamps,
  UUIDs, IPv4, IPv6, URLs, emails, UNIX paths, file sizes, durations, ports,
  SHA/MD5/git hex hashes, short hex IDs, percentages, numeric IDs.
- **Priority-ordered pipeline** — UUID runs before hex ID to prevent partial
  corruption of UUID hyphen segments.
- **`NormalizationResult`** frozen dataclass: `raw`, `normalized`,
  `token_counts` dict, `changed` bool, `total_replacements` property.
- **`normalize(raw)`** — pure function, thread-safe, zero side effects.
- **`normalize_batch(messages)`** — list → list convenience wrapper.
- **12 embedded verification cases** — run with `python -m app.core.normalizer`.

#### 2. `app/core/vector_store.py` — Thread-Safe NumPy Vector Store
- **`VectorRecord`** slots dataclass: vector, raw_message, normalized_msg,
  timestamp, service_name, log_level, stream_id, stored_at.
- **`QueryResult`** frozen dataclass: record, similarity (float), rank (int).
- **`VectorStore`** class:
  - Pre-allocated float32 matrix with **amortised doubling growth** strategy.
  - **`add(record)`** — single-record append; O(1).
  - **`add_batch(records)`** — bulk append in single lock acquisition.
  - **`query(qvec, top_k, min_similarity)`** — lock-free cosine similarity
    over the full matrix using vectorised NumPy; O(N·D).
  - **`snapshot()`** — full matrix + record list copy for checkpointing.
  - **`stats()`** — size, capacity, memory_mb, service/level histograms.
  - **Norm cache** — L2 norms maintained lazily; invalidated on write.
- **`get_vector_store()`** / **`reset_vector_store()`** — singleton management.

#### 3. `app/workers/vector_processor.py` — Stream Consumer & Embedding Worker
- **`MicroBatchAccumulator`** — holds entries until `max_size` OR `timeout_s`
  threshold fires, then flushes.
- **`VectorProcessorWorker`** — full lifecycle: `start()`, `run()`, `stop()`, `close()`.
- **`XREADGROUP`** with consumer group `aether-vector-workers`; ID `">"` for
  undelivered-only messages; **`BLOCK 200ms`** to yield to the event loop.
- **`run_in_executor`** for model loading and `model.encode()` — keeps the
  asyncio event loop unblocked during 50–200ms CPU-bound work.
- **`XACK`** sent after successful vector store write — at-least-once guarantee.
- **Exponential back-off** on Redis errors; graceful SIGINT/SIGTERM drain.
- **`WorkerStats`** dataclass: batches, messages, failures, acks, throughput.
- **`_parse_stream_entry()`** — decodes raw Redis field dict to typed `ParsedEntry`.

#### 4. `app/core/config.py` — Updated Settings (Day 2 additions)
Five new fields: `worker_batch_size`, `worker_batch_timeout`,
`worker_consumer_group`, `worker_consumer_name`, `sentence_transformer_model`.

#### 5. `requirements.txt` & `requirements-dev.txt`
- `sentence-transformers>=3.0.0` — model hub + inference
- `torch>=2.3.0` — CPU-only PyTorch as transformer backend
- `transformers>=4.40.0`, `tokenizers>=0.19.0`, `huggingface-hub>=0.23.0`
- `numpy>=1.26.4` — vector store backbone
- `requirements-dev.txt` — `pytest-cov>=5.0.0` added

#### 6. Tests
- **`tests/test_normalizer.py`** — 28 focused unit tests for all 16 patterns
  plus composite and batch API scenarios.
- **`tests/test_vector_store.py`** — 24 unit tests covering construction,
  add/batch, cosine correctness, stats, snapshot, and concurrent thread safety.

---

### Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Normalise BEFORE embedding | Collapsing high-cardinality tokens (IPs, UUIDs) into placeholders reduces the effective vocabulary and dramatically improves cosine similarity between semantically identical but lexically different messages |
| `all-MiniLM-L6-v2` | 384-dim, 22M params, ~14k sentences/s on CPU — best quality/speed trade-off for operational log text |
| `normalize_embeddings=True` in `.encode()` | L2-normalised vectors → cosine similarity equals dot product, halving query compute |
| Micro-batching (32 msgs / 0.5s) | Amortises fixed transformer overhead; keeps latency < 1s at typical SRE log volumes |
| `run_in_executor` for model.encode() | Sentence transformer is CPU-bound sync code — must not block the asyncio event loop |
| XREADGROUP consumer group | Enables horizontal scaling (multiple worker replicas) and PEL-based at-least-once delivery |
| XACK after store write | Guarantees no message is silently lost; worst case is re-embedding on restart (idempotent) |
| Amortised matrix doubling | O(1) amortised append cost; single NumPy array allocation avoids Python list overhead for millions of vectors |
| Norm cache with dirty-flag | Avoids recomputing N L2 norms on every query; invalidated only on write |
| Pure `normalize()` function | No side effects → trivially unit-testable, safely callable from threads/coroutines |

---

### Embedding Vector Schema

Each vector stored in `VectorStore` corresponds to one `VectorRecord`:

| Field | Type | Description |
|-------|------|-------------|
| `vector` | `float32[384]` | L2-normalised sentence embedding |
| `raw_message` | `str` | Original Redis stream message |
| `normalized_msg` | `str` | Template after regex normalisation |
| `timestamp` | `str` | ISO-8601 event timestamp |
| `service_name` | `str` | Originating service (e.g., `payment-gateway`) |
| `log_level` | `str` | `INFO` / `ERROR` / `CRITICAL` |
| `stream_id` | `str` | Redis Stream entry ID |
| `stored_at` | `float` | `time.monotonic()` insertion time |

---

### What's Coming Next

| Day | Theme | Components |
|-----|-------|-----------|
| ✅ Day 1 | Async ingestion pipeline | FastAPI + Redis Streams + log simulator |
| ✅ Day 2 | Stream preprocessing + embeddings | Normalizer + Sentence Transformer + Vector Store |
| 🔲 Day 3 | Anomaly Detection Engine | Isolation Forest + cosine anomaly scorer + alert FSM |
| 🔲 Day 4 | Self-Healing Executor | Runbook engine + Slack/PagerDuty alerting |
| 🔲 Day 5 | Observability Dashboard | Prometheus metrics + Grafana + distributed tracing |

---

---

*Last updated: Day 2 — 2026-07-02*

---

## [Day 3] — 2026-07-03 — Unsupervised Anomaly Detection & Sliding-Window Context Ingestion

### Theme
> "Isolate anomalies, extract timelines — mathematical context transforms outliers into actionable incidents."

The goal of Day 3 is to build the operational intelligence and incident correlation layer: an unsupervised anomaly detection engine linked to a sliding window context ring buffer. Together, they isolate logs that statistically deviate from the normal baseline and build "Incident Context Frames" (containing the anomaly score plus preceding telemetry history) to publish downstream to our alert stream.

---

### Day 3 Architecture — Full Data Flow & Incident Routing

```
  FastAPI POST /api/v1/logs  →  Redis Stream (telemetry_log_stream)
                                            │
                                            ▼  XREADGROUP
  ┌────────────────────────────────────────────────────────────────────────┐
  │  VectorProcessorWorker (app/workers/vector_processor.py)               │
  │                                                                        │
  │  1. Capture root-cause context BEFORE embedding:                       │
  │     LogContextBuffer.append(service_name, raw_fields)                  │
  │                                                                        │
  │  2. Normalize & embed batch:                                           │
  │     normalize() → SentenceTransformer.encode() → float32[D]            │
  │                                                                        │
  │  3. Score via Isolation Forest (app/core/anomaly_detector.py):         │
  │     anomaly_score = score_vector(embedding)                            │
  │     • 0.0 = completely normal baseline                                 │
  │     • 1.0 = absolute outlier anomaly                                   │
  │                                                                        │
  │  4. Evaluate threshold (anomaly_score > 0.55):                         │
  │     If YES → Get preceding context window (last 5 log frames)          │
  │            → Build Incident Context Frame (json metadata + context)    │
  │            → XADD to incident_alerts_stream                            │
  │            → Log WARNING [ANOMALY_DETECTED]                            │
  └────────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
                           Redis Stream: incident_alerts_stream
                                            │
                                            ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  FastAPI Incident Router (app/routers/incidents.py)                    │
  │  ├── GET /api/v1/incidents/recent  →  XREVRANGE newest incident alerts │
  │  └── GET /api/v1/incidents/stats   →  Aggregate severity/service stats │
  └────────────────────────────────────────────────────────────────────────┘
```

---

### Components Built

#### 1. `app/core/anomaly_detector.py` — Isolation Forest Anomaly Layer
- **Unsupervised Anomaly Model** — Wraps scikit-learn's `IsolationForest` initialized with `n_estimators=100`, `contamination="auto"`, and multi-threaded processing (`n_jobs=-1`).
- **Sigmoid Score Transformation** — Maps unbounded raw output to a normalized anomaly score in `[0, 1]`, where values close to 1 represent high outlier probability.
- **Baseline Training Gate** — Decoupled training logic (`train_baseline`) fitted on standard operational embeddings. Protected by `threading.RLock`.

#### 2. `app/core/context_buffer.py` — Thread-Safe sliding-window Ring Buffer
- **Service Isolation** — Manages independent buffers using `collections.deque(maxlen=50)` per service, preventing cross-service pollution.
- **Atomic Operations** — Thread-safe `append()` and `get_context_frame()` protected by a shared lock to capture chronological preceding history (default: last 5 frames).

#### 3. `app/workers/vector_processor.py` — Real-Time Alert Extraction Worker
- Wires the `AetherAnomalyDetector` and `LogContextBuffer` into the stream loop.
- Normalizes logs and extracts embeddings, checking them against the anomaly threshold (`0.55`).
- Anomalies trigger downstream publishing to `incident_alerts_stream` with the preceding context logs bundled as a JSON timeline.

#### 4. `app/routers/incidents.py` — Real-Time Incident Router
- **`GET /api/v1/incidents/recent`** — Reverse chronological lookup (`XREVRANGE`) returning the latest 20 incident frames.
- **`GET /api/v1/incidents/stats`** — Computes length, averages, and level/service histograms over the alerts.

#### 5. `app/main.py` — Lifespan and Application Integration
- Hooks global references of detector, vector store, and sliding buffer into app state.
- Spawns a background lifespan monitor task (`_baseline_training_monitor`) that waits until 128 logs accumulate in the vector store, then fits the Isolation Forest model asynchronously.

#### 6. `tests/test_anomaly.py` — Pipeline Test Suite
- Validates model baseline training, sigmoid scoring, and thread-safe service deques.
- Wires an integration flow ensuring anomalous vectors write an Incident Context Frame and send XACK correctly.

---

### Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Unsupervised Isolation Forest | Isolates outliers by randomly partitioning features, requiring no labeled training sets. Works exceptionally well on 384D embedding structures. |
| Sigmoid Scaling | Translates model decision scores to an intuitive probability scale `[0, 1]` allowing deterministic alerting. |
| Context Ring Buffer | Storing preceding logs in memory using collections.deque avoids querying database storage on alerts. |
| Decoupled API fitting | Spawning an async background task to monitor store capacity and run fitting prevents request thread blocking. |

---

### Incident Alert Stream Schema (`incident_alerts_stream`)

Each incident alert written to the stream contains:

| Field | Type | Description |
|-------|------|-------------|
| `stream_id` | string | Auto-generated alert ID |
| `service_name` | string | Originating microservice name |
| `timestamp` | string | ISO-8601 log timestamp |
| `level` | string | Severity level (`ERROR`, `CRITICAL`) |
| `raw_message` | string | Raw unmasked log message |
| `normalized_message` | string | Cleaned regex template |
| `anomaly_score` | string (float) | Probability score in `[0, 1]` |
| `context_window` | string (JSON) | Serialized array of the 5 preceding log frames |
| `metadata` | string (JSON) | Originating metadata fields (trace_id, etc.) |
| `detected_at` | string (float) | Epoch timestamp when classified |

---

*Last updated: Day 3 — 2026-07-03*


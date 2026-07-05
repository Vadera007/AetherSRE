<div align="center">

# ⚡ AetherSRE

### Autonomous Log Anomaly & Self-Healing Engine

![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Ollama](https://img.shields.io/badge/Ollama-LLaMA3.2-black?style=for-the-badge)
![Prometheus](https://img.shields.io/badge/Prometheus-2.51-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**AetherSRE** is an enterprise-grade, event-driven SRE engine that autonomously ingests microservice log telemetry, detects anomalies using ML embeddings, generates AI root-cause analysis via a local LLM, and executes risk-gated self-healing actions — all in real time.

*Built to replace the 3 AM production war room.*

</div>

---

## 🏗️ Architecture

```
 ┌────────────────────────────────────────────────────────────────────────────┐
 │                        AetherSRE Pipeline                                 │
 │                                                                            │
 │  ┌──────────────┐   HTTP POST    ┌─────────────────────────────────────┐  │
 │  │  Real Flask   │──/api/v1/logs─▶│      FastAPI Ingestion API           │  │
 │  │  Microservice │               │  • Pydantic validation (<1ms)        │  │
 │  │  (target-app) │               │  • Prometheus counter increment      │  │
 │  └──────────────┘               └──────────────┬──────────────────────┘  │
 │  ┌──────────────┐                              │ XADD                     │
 │  │  Load Driver  │                              ▼                          │
 │  │  (3 RPS +     │               ┌─────────────────────────────────────┐  │
 │  │   burst spikes│               │    Redis Stream: telemetry_log_stream│  │
 │  └──────────────┘               │    (max 100k events, LRU trimmed)    │  │
 │                                 └──────────────┬──────────────────────┘  │
 │                                                │ XREADGROUP               │
 │                                                ▼                          │
 │                                 ┌─────────────────────────────────────┐  │
 │                                 │     Vector Processor Worker          │  │
 │                                 │  • MiniLM-L6-v2 → 384-dim embedding  │  │
 │                                 │  • Isolation Forest anomaly score    │  │
 │                                 │  • score > 0.55 → emit to alerts     │  │
 │                                 └──────────────┬──────────────────────┘  │
 │                                                │ XADD incident_alerts     │
 │                                                ▼                          │
 │                                 ┌─────────────────────────────────────┐  │
 │                                 │      RCA Processor Worker            │  │
 │                                 │  • Ollama llama3.2:1b (local LLM)   │  │
 │                                 │  • Structured root cause + fix       │  │
 │                                 │  • Risk classification (LOW→CRITICAL)│  │
 │                                 └──────────────┬──────────────────────┘  │
 │                                                │ XADD rca_insights        │
 │                                                ▼                          │
 │                                 ┌─────────────────────────────────────┐  │
 │                                 │   Remediation Policy Engine          │  │
 │                                 │  • LOW/MEDIUM → AUTO_EXECUTE         │  │
 │                                 │  • HIGH/CRITICAL → Risk Gate (human) │  │
 │                                 └──────────────┬──────────────────────┘  │
 │                                                │ WebSocket broadcast      │
 │                                                ▼                          │
 │                                 ┌─────────────────────────────────────┐  │
 │                                 │   Real-time Dashboard (WebSocket)    │  │
 │                                 │  • Live log console                  │  │
 │                                 │  • Anomaly diagnostics table         │  │
 │                                 │  • One-click approve / deny gates    │  │
 │                                 └─────────────────────────────────────┘  │
 │                                                │                          │
 │                                 ┌──────────────▼──────────────────────┐  │
 │                                 │   GET /metrics → Prometheus scrape   │  │
 │                                 │   Grafana: 8-panel dashboard         │  │
 │                                 └─────────────────────────────────────┘  │
 └────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

- **🔍 Real-Time Anomaly Detection** — Sentence Transformers (`all-MiniLM-L6-v2`) embed every log line into a 384-dimensional vector; Isolation Forest scores each against the learned baseline. Sub-5ms classification latency.
- **🧠 AI Root Cause Analysis** — Anomalous events are sent to a locally-running Ollama LLM (`llama3.2:1b`) with structured context. Returns root cause + suggested fix in JSON.
- **🛡️ Risk-Gated Remediation** — Policy engine classifies severity and either auto-executes LOW/MEDIUM fixes or holds HIGH/CRITICAL for one-click human approval.
- **📡 Event-Driven Architecture** — Fully decoupled via Redis Streams. Each worker scales independently. No coupling between ingestion latency and ML processing.
- **🖥️ Live WebSocket Dashboard** — Real-time log terminal, anomaly diagnostics table, and approval queue — all pushed via WebSocket, no polling.
- **📊 Prometheus + Grafana** — Six custom metrics exposed at `/metrics`, scraped by Prometheus, visualised in a pre-built Grafana dashboard with 8 panels.
- **🏭 Real Microservice Traffic** — A real Flask e-commerce backend (`target-app`) + load driver generates genuine production-like log traffic with realistic error rates.
- **🌓 Dark / Light Mode** — Dashboard theme toggle with `localStorage` persistence.

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **API** | FastAPI + Uvicorn | Async log ingestion, WebSocket broadcaster, REST endpoints |
| **Event Bus** | Redis 7.2 Streams | Decoupled pipeline: telemetry → alerts → insights → remediation |
| **Embeddings** | `all-MiniLM-L6-v2` | 384-dim semantic log vectors (sentence-transformers) |
| **Anomaly Detection** | Isolation Forest (sklearn) | Unsupervised, trains on first 128 vectors |
| **LLM / RCA** | Ollama + LLaMA 3.2:1b | Local, private, no API keys required |
| **Metrics** | prometheus-client | `/metrics` endpoint with 6 custom counters/histograms |
| **Dashboards** | Grafana 10.4 | Pre-built 8-panel dashboard auto-provisioned |
| **Observability** | Prometheus 2.51 | 10s scrape interval, 7-day retention |
| **Target App** | Flask 3.x | Real e-commerce microservice with structured JSON logging |
| **Containerisation** | Docker Compose | Full 8-service stack, single command startup |
| **Validation** | Pydantic v2 | Strict log event schema validation |

---

## 📁 Project Structure

```
AetherSRE/
├── app/
│   ├── core/
│   │   ├── anomaly_detector.py    # Isolation Forest wrapper + singleton
│   │   ├── config.py              # Pydantic-settings config
│   │   ├── llm_client.py          # Ollama async HTTP client
│   │   ├── metrics.py             # Prometheus metric registry
│   │   ├── redis_client.py        # Async Redis connection pool
│   │   ├── remediation_executor.py
│   │   ├── remediation_policy.py  # Risk classification engine
│   │   └── vector_store.py        # In-memory NumPy vector store
│   ├── routers/
│   │   ├── ingestion.py           # POST /api/v1/logs
│   │   ├── health.py              # GET /health
│   │   ├── dashboard.py           # GET /dashboard + WS /ws/telemetry
│   │   ├── incidents.py
│   │   ├── rca.py
│   │   └── remediation.py
│   ├── workers/
│   │   ├── vector_processor.py    # Embedding + anomaly detection worker
│   │   ├── rca_processor.py       # LLM RCA worker
│   │   └── remediation_processor.py
│   ├── models/
│   │   └── log_event.py           # Pydantic log event schema
│   ├── templates/
│   │   └── dashboard.html         # WebSocket dashboard UI
│   └── main.py                    # FastAPI app + lifespan
├── services/
│   ├── target-app/                # Real Flask e-commerce microservice
│   ├── log-shipper/               # Docker SDK → AetherSRE forwarder
│   └── load-driver/               # Async load generator (3 RPS + bursts)
├── simulator/
│   └── log_generator.py           # Standalone fake log simulator
├── monitoring/
│   ├── prometheus.yml             # Prometheus scrape config
│   └── grafana/
│       ├── provisioning/          # Auto-provisioned datasource + dashboard
│       └── dashboards/
│           └── aethersre.json     # Pre-built 8-panel Grafana dashboard
├── tests/                         # pytest test suite
├── docs/
│   └── architecture.md            # Deep-dive architecture docs
├── docker-compose.yml             # Full 8-service stack
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## 🚀 Quick Start

### Option A: Full Docker Stack (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/Vadera007/AetherSRE.git
cd AetherSRE

# 2. Copy environment file
cp .env.example .env

# 3. Start all 8 services
docker compose up -d --build

# 4. Pull the LLM model (one-time, ~600MB)
docker exec aether_ollama ollama pull llama3.2:1b

# 5. Access the dashboard
open http://localhost:8000/dashboard
```

**Services started:**
| Service | URL | Credentials |
|---------|-----|-------------|
| AetherSRE Dashboard | http://localhost:8000/dashboard | — |
| AetherSRE API | http://localhost:8000 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / aethersre |
| Ollama | http://localhost:11434 | — |
| Target App | http://localhost:5000 | — |

### Option B: Local Development

```bash
# 1. Clone and set up Python environment
git clone https://github.com/Vadera007/AetherSRE.git
cd AetherSRE
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Start infrastructure (Redis + Ollama)
docker compose up redis ollama -d

# 3. Pull the LLM
docker exec aether_ollama ollama pull llama3.2:1b

# 4. Start the API server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 5. In a separate terminal — start vector processor worker
python -m app.workers.vector_processor

# 6. In a separate terminal — start log simulator
python -m simulator.log_generator --rate 4
```

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Service health check with Redis status |
| `POST` | `/api/v1/logs` | Ingest a structured log event |
| `GET` | `/api/v1/incidents/recent` | Recent anomaly incidents |
| `POST` | `/api/v1/remediation/approve` | Approve a pending risk-gated remediation |
| `POST` | `/api/v1/remediation/deny` | Deny a pending risk-gated remediation |
| `GET` | `/dashboard` | Live WebSocket dashboard UI |
| `WS` | `/ws/telemetry` | Real-time WebSocket event stream |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint |

---

## 📊 Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `aether_logs_ingested_total` | Counter | Logs accepted, labelled by `service_name` + `level` |
| `aether_anomalies_detected_total` | Counter | Anomalies flagged, labelled by `service_name` |
| `aether_vector_store_size` | Gauge | Current embeddings in memory |
| `aether_rca_requests_total` | Counter | LLM RCA calls, labelled by `status` |
| `aether_rca_duration_seconds` | Histogram | End-to-end LLM latency (p50/p95/p99) |
| `aether_remediations_total` | Counter | Remediations by `execution_type` + `status` |

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_STREAM_NAME` | `telemetry_log_stream` | Ingestion stream name |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama LLM endpoint |
| `OLLAMA_MODEL` | `llama3.2:1b` | LLM model name |
| `API_ENV` | `development` | Environment label |
| `LOG_LEVEL` | `INFO` | Application log level |
| `ANOMALY_THRESHOLD` | `0.55` | Isolation Forest score threshold |

---

## 📈 Performance Characteristics

| Metric | Value |
|--------|-------|
| Log ingestion latency | < 5ms (p99) |
| Vector embedding throughput | ~32 logs/batch |
| Anomaly detection latency | < 2ms post-training |
| LLM RCA latency (llama3.2:1b) | 5–30s (CPU inference) |
| Redis stream backlog | Up to 100,000 events |
| WebSocket broadcast delay | < 500ms (poll interval) |
| Baseline training threshold | 128 vectors |

---

## 🔭 Roadmap

- [ ] GPU-accelerated Ollama inference (CUDA support)
- [ ] Multi-tenant support with per-service anomaly baselines
- [ ] PagerDuty / Slack webhook integration
- [ ] Kubernetes Helm chart
- [ ] Historical anomaly trend analysis
- [ ] Auto-scaling remediation via Kubernetes API

---

## 👤 Author

**Akshat Vadera** — [GitHub](https://github.com/Vadera007) · [LinkedIn](https://linkedin.com/in/akshatvadera)

---

## 📄 License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for details.

---

<div align="center">
<i>Built with ⚡ FastAPI · 🔴 Redis · 🧠 Ollama · 📊 Prometheus · 🐳 Docker</i>
</div>

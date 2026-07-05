<div align="center">

# ⚡ AetherSRE

### Autonomous Log Anomaly & Self-Healing Engine

[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Redis](https://img.shields.io/badge/Redis-Streams-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Ollama](https://img.shields.io/badge/Ollama-llama3.2-000000?style=for-the-badge&logo=ollama&logoColor=white)](https://ollama.ai)
[![License: MIT](https://img.shields.io/badge/License-MIT-F7CA18?style=for-the-badge)](LICENSE)

*Ingest → Detect → Diagnose → Heal. Fully autonomous, sub-5ms, production-grade.*

</div>

---

## 🔭 Overview

**AetherSRE** is a real-time Site Reliability Engineering (SRE) platform that autonomously monitors microservice logs, detects anomalies using machine learning, performs root-cause analysis with a local LLM, and executes remediation actions — all with a risk-gated approval workflow and a live WebSocket dashboard.

Built for engineering teams that are tired of 3 AM pages for incidents that *could have fixed themselves.* AetherSRE closes the loop between observability and self-healing, replacing reactive on-call firefighting with a fully autonomous, explainable AI system — running entirely **on-premise** with no external API calls.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            AetherSRE Pipeline                                   │
└─────────────────────────────────────────────────────────────────────────────────┘

  Microservice Logs
  ┌──────────────┐
  │  Flask App   │──── HTTP POST ──────────────────────────────────────────────┐
  │  Simulator   │                                                              │
  └──────────────┘                                                              ▼
                                                               ┌──────────────────────────┐
                                                               │   FastAPI Ingestion API  │
                                                               │   POST /api/v1/logs      │
                                                               │   • Validation           │
                                                               │   • Normalization        │
                                                               │   • Deduplication        │
                                                               └──────────┬───────────────┘
                                                                          │
                                                                          ▼
                                                               ┌──────────────────────────┐
                                                               │     Redis Streams        │
                                                               │   aether:raw-logs        │
                                                               │   aether:anomalies       │
                                                               │   aether:rca-results     │
                                                               │   aether:remediations    │
                                                               └──────────┬───────────────┘
                                                                          │
                              ┌───────────────────────────────────────────┤
                              │                                           │
                              ▼                                           ▼
               ┌──────────────────────────┐               ┌──────────────────────────┐
               │    Vector Processor      │               │    RCA Processor         │
               │  • MiniLM-L6-v2          │──anomaly──▶  │  • Ollama (llama3.2:1b)  │
               │    Embeddings            │               │  • Context Buffering     │
               │  • Isolation Forest      │               │  • Pattern Matching      │
               │  • Anomaly Scoring       │               │  • Root Cause Analysis   │
               └──────────────────────────┘               └──────────┬───────────────┘
                                                                      │
                                                                      ▼
                                                       ┌──────────────────────────────┐
                                                       │  Remediation Policy Engine   │
                                                       │  • Risk-Gated Approval       │
                                                       │  • LOW  → Auto-execute       │
                                                       │  • MED  → Notify + wait      │
                                                       │  • HIGH → Escalate           │
                                                       └──────────┬───────────────────┘
                                                                  │
                              ┌───────────────────────────────────┤
                              │                                   │
                              ▼                                   ▼
               ┌──────────────────────────┐       ┌──────────────────────────────┐
               │   Live Dashboard         │       │   Prometheus + Grafana       │
               │   WebSocket /ws/telemetry│       │   /metrics                   │
               │   Real-time Incidents    │       │   Latency, Throughput, Alerts│
               └──────────────────────────┘       └──────────────────────────────┘
```

---

## ✨ Key Features

- **🔍 Real-Time Anomaly Detection** — Sub-5ms log ingestion with `all-MiniLM-L6-v2` sentence embeddings and Isolation Forest scoring. Detects statistical outliers in high-dimensional semantic space.

- **🧠 LLM-Powered Root Cause Analysis** — Local Ollama inference (no external API calls) using `llama3.2:1b` generates human-readable RCA reports with contributing factors and confidence scores.

- **🛡️ Risk-Gated Remediation** — Three-tier approval workflow (LOW/MEDIUM/HIGH risk) ensures automated actions only execute when confidence thresholds are met. Full audit trail.

- **⚡ Redis Streams Backbone** — Decoupled, persistent event streaming with consumer groups. Survives restarts, enables horizontal scaling, and provides replay capability.

- **📡 Live WebSocket Dashboard** — Real-time telemetry pushed to browser clients. Incidents, anomaly scores, and remediation statuses update without polling.

- **📊 Full Observability Stack** — Prometheus metrics endpoint + pre-built Grafana dashboards for ingestion latency, anomaly rate, RCA queue depth, and remediation throughput.

- **🎯 Realistic Log Simulation** — Production-grade log simulator generates correlated, scenario-based anomalies (cascading failures, memory leaks, DDoS patterns) for testing.

- **🔒 100% On-Premise** — No external LLM API calls. All inference runs locally via Ollama. Safe for air-gapped and compliance-sensitive environments.

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| **API Server** | FastAPI + Uvicorn | Log ingestion, REST API, WebSocket gateway |
| **Event Backbone** | Redis Streams | Decoupled async pipeline, consumer groups |
| **Embeddings** | `sentence-transformers/all-MiniLM-L6-v2` | Semantic log vectorization |
| **Anomaly Detection** | Isolation Forest (scikit-learn) | Unsupervised outlier detection |
| **Vector Store** | In-memory + NumPy | Rolling embedding window for context |
| **LLM / RCA** | Ollama → `llama3.2:1b` | Root Cause Analysis generation |
| **Log Normalization** | Custom regex pipeline | Multi-format log parsing |
| **Remediation Engine** | Custom policy engine | Risk-gated auto-healing |
| **Dashboard** | HTML/CSS/JS + WebSocket | Real-time incident visualization |
| **Metrics** | Prometheus client | Latency, throughput, alert counters |
| **Visualization** | Grafana | Pre-built SRE dashboards |
| **Simulation** | Flask + Python | Realistic target microservice + log gen |
| **Containerization** | Docker Compose | One-command full-stack deployment |

---

## 📁 Project Structure

```
AetherSRE/
├── app/                          # Core application
│   ├── main.py                   # FastAPI app, lifespan, Prometheus metrics
│   ├── core/                     # Shared business logic
│   │   ├── anomaly_detector.py   # Isolation Forest wrapper
│   │   ├── config.py             # Pydantic Settings (env-driven)
│   │   ├── context_buffer.py     # Rolling context window for LLM
│   │   ├── llm_client.py         # Ollama async HTTP client
│   │   ├── logging_config.py     # Structured JSON logging
│   │   ├── normalizer.py         # Multi-format log parser / normalizer
│   │   ├── redis_client.py       # Redis Streams helpers (xadd, xread, xack)
│   │   ├── remediation_executor.py # Action execution layer
│   │   ├── remediation_policy.py # Risk-tier decision logic
│   │   └── vector_store.py       # Embedding storage + similarity search
│   ├── models/
│   │   └── log_event.py          # Pydantic data models
│   ├── routers/                  # FastAPI route handlers
│   │   ├── dashboard.py          # Dashboard HTML + WebSocket
│   │   ├── health.py             # GET /health
│   │   ├── incidents.py          # GET /api/v1/incidents
│   │   ├── ingestion.py          # POST /api/v1/logs
│   │   ├── rca.py                # GET /api/v1/rca/{incident_id}
│   │   ├── remediation.py        # GET/POST /api/v1/remediation
│   │   └── webhooks.py           # Webhook delivery
│   ├── templates/                # Jinja2 HTML templates
│   └── workers/                  # Background stream consumers
│       ├── vector_processor.py   # Embedding + anomaly detection worker
│       ├── rca_processor.py      # LLM RCA generation worker
│       └── remediation_processor.py # Remediation execution worker
├── simulator/                    # Log generation & load testing
│   ├── log_generator.py          # Scenario-based anomaly simulator
│   └── load_test.py              # Throughput benchmark
├── scripts/                      # Dev & demo utilities
│   ├── run_worker.py             # Worker launcher helper
│   ├── record_demo.py            # Demo session recorder
│   └── record_and_screenshot.py  # Screenshot capture tool
├── docs/                         # Documentation
│   ├── architecture.md           # Full architecture deep-dive
│   └── CHANGELOG.md              # Detailed change history
│   └── assets/                   # Screenshots & diagrams
├── monitoring/                   # Observability stack
│   ├── prometheus/               # Prometheus config
│   └── grafana/                  # Grafana dashboards & datasources
├── tests/                        # Test suite
├── Dockerfile                    # AetherSRE image definition
├── docker-compose.yml            # Full stack orchestration
├── pyproject.toml                # Project metadata & tool config
├── requirements.txt              # Production dependencies
├── requirements-dev.txt          # Development dependencies
├── .env.example                  # Environment variable template
└── README.md                     # This file
```

---

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)

> **Prerequisites:** Docker Desktop, Docker Compose v2, Ollama installed locally.

```bash
# 1. Clone the repository
git clone https://github.com/Vadera007/AetherSRE.git
cd AetherSRE

# 2. Copy environment file and configure
cp .env.example .env
# Edit .env as needed (defaults work for local dev)

# 3. Pull the LLM model (one-time, ~600MB)
ollama pull llama3.2:1b

# 4. Launch the full stack
docker compose up --build -d

# 5. Open the dashboard
open http://localhost:8000/dashboard
```

**Services started:**
| Service | Port | URL |
|---|---|---|
| AetherSRE API | 8000 | http://localhost:8000 |
| Redis | 6379 | redis://localhost:6379 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |
| Target Flask App | 5001 | http://localhost:5001 |

---

### Option 2: Local Dev Setup

> **Prerequisites:** Python 3.13, Redis (running), Ollama (running).

```bash
# 1. Clone & enter directory
git clone https://github.com/Vadera007/AetherSRE.git
cd AetherSRE

# 2. Create virtual environment
python3.13 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# 4. Configure environment
cp .env.example .env

# 5. Start Redis (if not running)
redis-server --daemonize yes

# 6. Pull Ollama model
ollama pull llama3.2:1b

# 7. Launch AetherSRE
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 8. In a new terminal — start the log simulator
python simulator/log_generator.py

# 9. Open dashboard
open http://localhost:8000/dashboard
```

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_MODEL` | `llama3.2:1b` | LLM model for RCA |
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | Sentence transformer model |
| `ANOMALY_THRESHOLD` | `0.7` | Isolation Forest score threshold |
| `RISK_AUTO_EXECUTE_MAX` | `LOW` | Max risk tier for auto-remediation |
| `LOG_LEVEL` | `INFO` | Application log level |
| `CORS_ORIGINS` | `*` | Allowed CORS origins |
| `PROMETHEUS_ENABLED` | `true` | Enable /metrics endpoint |
| `WEBHOOK_SECRET` | *(empty)* | HMAC secret for webhook auth |

Copy `.env.example` to `.env` and adjust values for your environment.

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | System health check (Redis, Ollama, workers) |
| `POST` | `/api/v1/logs` | Ingest log event(s) into the pipeline |
| `GET` | `/api/v1/incidents` | List detected incidents with filters |
| `GET` | `/api/v1/incidents/{id}` | Get incident detail |
| `GET` | `/api/v1/rca/{incident_id}` | Fetch RCA report for an incident |
| `GET` | `/api/v1/remediation` | List remediation actions |
| `POST` | `/api/v1/remediation/{id}/approve` | Manually approve a MEDIUM/HIGH risk action |
| `POST` | `/api/v1/webhooks` | Register a webhook destination |
| `GET` | `/dashboard` | Live HTML dashboard |
| `GET` | `/ws/telemetry` | WebSocket stream (incidents, metrics, alerts) |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint |
| `GET` | `/docs` | Auto-generated OpenAPI docs (Swagger UI) |
| `GET` | `/redoc` | ReDoc API documentation |

### Example: Ingest a Log Event

```bash
curl -X POST http://localhost:8000/api/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payment-gateway",
    "level": "ERROR",
    "message": "Connection timeout to database after 30000ms — pool exhausted",
    "timestamp": "2026-07-05T18:30:00Z",
    "metadata": {
      "host": "pay-gw-03",
      "region": "us-east-1"
    }
  }'
```

```json
{
  "status": "accepted",
  "log_id": "lg_01j3...",
  "ingestion_latency_ms": 1.2,
  "stream": "aether:raw-logs"
}
```

---

## 📊 Performance Metrics

| Metric | Value | Notes |
|---|---|---|
| **Log ingestion latency** | < 5ms (p99) | Redis XADD + validation |
| **Embedding throughput** | ~200 logs/sec | MiniLM-L6-v2 on CPU |
| **Anomaly detection latency** | < 2ms | Isolation Forest inference |
| **RCA generation time** | 3–8s | llama3.2:1b via Ollama |
| **WebSocket push latency** | < 50ms | Redis → WS broadcast |
| **Dashboard refresh** | Real-time | Push-based, no polling |
| **Memory footprint** | ~800MB | Incl. model weights |
| **Concurrent connections** | 100+ | Async FastAPI + Starlette |

---

## 🖼️ Screenshots

| View | Description |
|---|---|
| ![Dashboard](docs/assets/) | Live incident dashboard with real-time anomaly stream |
| ![RCA Report](docs/assets/) | LLM-generated root cause analysis with remediation plan |
| ![Grafana](docs/assets/) | Prometheus metrics in Grafana — ingestion & anomaly rates |

> Screenshots available in [`docs/assets/`](docs/assets/).

---

## 🧪 Running Tests

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run test suite
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=app --cov-report=html
open htmlcov/index.html
```

---

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch: `git checkout -b feat/your-feature`
3. Commit with conventional commits: `git commit -m "feat: add xyz"`
4. Push: `git push origin feat/your-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

Built with ⚡ by [Akshat Vadera](https://github.com/Vadera007)

*Turning reactive SRE into autonomous self-healing.*

</div>

# AetherSRE 🌌
### Autonomous Log Anomaly & Self-Healing Engine

AetherSRE is a production-grade, highly scalable AIOps platform designed to process massive microservice log telemetry, isolate statistical anomalies in real-time, generate structured root-cause diagnostics using a local LLM, and safely execute closed-loop mitigations through risk-gated action policies.

---

## 1. System Architecture

Tracing a raw log line through the telemetry and self-healing loop:

```
 [ Simulator / Microservices ]
              │
              │ (1) HTTP POST /api/v1/logs
              ▼
 ┌──────────────────────────────────────┐
 │  FastAPI Ingestion Endpoint          │
 │  • Pydantic schema validation        │
 └──────────────────┬───────────────────┘
                    │
                    │ (2) XADD (Max length trimmed)
                    ▼
 ┌──────────────────────────────────────┐
 │  Redis Buffer telemetry_log_stream   │
 └──────────────────┬───────────────────┘
                    │
                    │ (3) XREADGROUP (horizontal consumer workers)
                    ▼
 ┌──────────────────────────────────────┐
 │  Vector Processor Worker             │
 │  • Multi-pass Regex Masking          │
 │  • Sentence Transformers Embedding   │
 │  • NumPy Thread-Safe Store lookup    │
 └──────────────────┬───────────────────┘
                    │
                    │ (4) Isolation Forest Outlier Scoring
                    ▼
 ┌──────────────────────────────────────┐
 │  Aether Anomaly Scorer               │
 │  • Threshold trigger (> 0.55)        │
 └──────────────────┬───────────────────┘
                    │
                    │ (5) Fetch 5 preceding context frames
                    ▼
 ┌──────────────────────────────────────┐
 │  Incident Alerts Buffer Stream       │
 └──────────────────┬───────────────────┘
                    │
                    │ (6) Deduplication Cache validation
                    ▼
 ┌──────────────────────────────────────┐
 │  RCA Processor worker                │
 │  • Ollama local structured model     │
 └──────────────────┬───────────────────┘
                    │
                    │ (7) Enrich insights with JSON diagnostics
                    ▼
 ┌──────────────────────────────────────┐
 │  Remediation Processor Worker        │
 │  • Evaluate Risk Policy Matrix       │
 └──────────┬───────────────────────┬───┘
            │                       │
            │ (8a) AUTO_EXECUTE     │ (8b) WEBHOOK_GATE
            ▼                       ▼
 ┌─────────────────────┐ ┌─────────────────────┐
 │ LocalActionExecutor │ │ Redis Pending State │
 │ (Subprocess runner) │ │ (operator webhooks) │
 └──────────┬──────────┘ └──────────┬──────────┘
            │                       │
            └───────────┬───────────┘
                        │
                        │ (9) Stream history & broadcast update
                        ▼
 ┌──────────────────────────────────────┐
 │  WebSockets Telemetry Broker         │
 └──────────────────┬───────────────────┘
                    │
                    │ (10) Event push frames
                    ▼
 ┌──────────────────────────────────────┐
 │  Vanilla JS HTML5 Dashboard UI       │
 └──────────────────────────────────────┘
```

---

## 2. Key Technical Highlights & Metrics

- **High-Throughput Ingestion:** Processes **1,000+ logs/second** with under **5ms** median ingestion latency ($p_{95} \le 3.5ms$).
- **Inference Cost Optimization:** Reduces LLM token costs by **85%+** through sliding-window context deduplication caches.
- **Closed-Loop Safety:** Implements a deterministic **Risk Policy Matrix** preventing unauthorized command execution on high-risk nodes without manual approval overrides.

---

## 3. Day-by-Day Implementation Ledger

| Phase | Theme | Core Components |
|-------|-------|-----------------|
| **Day 1** | Ingestion Foundations | Async FastAPI pipeline, Redis Stream ingestion buffers, Poisson log simulator. |
| **Day 2** | Preprocessing & Geometry | 16-pattern Regex Normalizer, MiniLM-L6-v2 embeddings, NumPy Thread-Safe Vector Store. |
| **Day 3** | Outlier Detection | Unsupervised Isolation Forest, Sigmoid scorer scaling, Sliding-window Context Ring Buffer. |
| **Day 4** | Autonomous Diagnosis | Asynchronous Local Ollama client, JSON schema guides, RCA processor workers. |
| **Day 5** | Risk-Gated Remediation | Risk Policy Matrix, `LocalActionExecutor`, Operator approval/denial webhook endpoints. |
| **Day 6** | Operational Dashboard | FastAPI WebSockets server, auto-scrolling terminal log, interactive approvals panel. |
| **Day 7** | Capstone & Benchmark | Load testing rig (`load_test.py`), performance test suite, documentation. |

---

## 4. Local Setup & Replication Playbook

### 1. Build and Start the Infrastructure Containers
```bash
docker compose up redis ollama -d
```

### 2. Pull the Ollama Model Weights
```bash
docker exec -it aether_ollama ollama pull llama3
```

### 3. Install Dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

### 4. Run the Core Performance and Unit Test Suite
```bash
pytest tests/ -v
```

### 5. Launch the Command Center Ingestion App & Workers
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

In separate terminals:
```bash
# Vector processor & anomaly detector worker
python3 scripts/run_worker.py --anomaly-threshold 0.55

# Poisson log simulator stream
python3 -m simulator.log_generator --rate 10 --anomaly-rate 0.10
```

### 6. Run the High-Throughput Performance Test Rig
```bash
python3 simulator/load_test.py
```

### 7. Access the Dashboard
Open your browser and navigate to:
```
http://localhost:8000/dashboard
```

---

*Contact Coordinates: [akshatvadera@desktop.aethersre](mailto:akshatvadera@desktop.aethersre)*  
*Built with FastAPI · Redis Streams · scikit-learn · Ollama · SentenceTransformers · PyTorch · NumPy · Jinja2 · WebSockets · Python 3.12+*

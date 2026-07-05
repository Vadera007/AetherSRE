# =============================================================================
# AetherSRE — FastAPI Application Container
# Multi-stage build for clean production-grade image
# =============================================================================

FROM python:3.12-slim AS base

# System-level hygiene
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies (curl needed for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifest first for layer-caching efficiency
COPY pyproject.toml ./

# Install Python dependencies via pip (reads [project] table)
RUN pip install --upgrade pip \
    && pip install "fastapi[standard]>=0.111.0" \
                   "uvicorn[standard]>=0.29.0" \
                   "redis>=5.0.4" \
                   "pydantic>=2.7.0" \
                   "pydantic-settings>=2.2.1" \
                   "httpx>=0.27.0"

# Copy application source
COPY app/ ./app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

"""
AetherSRE Log Shipper
======================
Reads structured JSON logs from the target-app container via Docker SDK
and forwards them to the AetherSRE ingestion API.
"""
from __future__ import annotations
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import httpx

AETHER_URL = os.environ.get("AETHER_URL", "http://aether-api:8000")
TARGET_CONTAINER = os.environ.get("TARGET_CONTAINER", "aether_target_app")
SHIP_INTERVAL = float(os.environ.get("SHIP_INTERVAL", "0.1"))  # seconds

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s [shipper] %(levelname)s %(message)s")
log = logging.getLogger("log-shipper")


def parse_log_line(line: str) -> dict | None:
    """Parse a JSON log line from target-app."""
    try:
        raw = json.loads(line.strip())
        return {
            "service_name": raw.get("service", "target-app"),
            "level": raw.get("level", "INFO"),
            "message": raw.get("message", line.strip()),
            "timestamp": raw.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "host": "docker-target-app",
            "environment": "production",
        }
    except (json.JSONDecodeError, AttributeError):
        if line.strip():
            return {
                "service_name": "target-app",
                "level": "INFO",
                "message": line.strip(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "host": "docker-target-app",
                "environment": "production",
            }
        return None


def ship_log(client: httpx.Client, log_entry: dict) -> bool:
    """POST a single log entry to AetherSRE."""
    try:
        r = client.post(f"{AETHER_URL}/api/v1/logs", json=log_entry, timeout=3.0)
        return r.status_code == 202
    except Exception as exc:
        log.warning("Failed to ship log: %s", exc)
        return False


def wait_for_aether(client: httpx.Client, retries: int = 30) -> bool:
    """Block until AetherSRE API is reachable."""
    for i in range(retries):
        try:
            r = client.get(f"{AETHER_URL}/health", timeout=3.0)
            if r.status_code == 200:
                log.info("AetherSRE API is reachable.")
                return True
        except Exception:
            pass
        log.info("Waiting for AetherSRE API... (%d/%d)", i + 1, retries)
        time.sleep(2)
    return False


def main() -> None:
    log.info("Log Shipper starting | target=%s aether=%s", TARGET_CONTAINER, AETHER_URL)

    try:
        import docker
        docker_client = docker.from_env()
    except Exception as exc:
        log.error("Docker SDK not available: %s — falling back to stdin mode", exc)
        docker_client = None

    with httpx.Client() as http:
        if not wait_for_aether(http):
            log.error("AetherSRE not reachable. Exiting.")
            sys.exit(1)

        shipped = 0
        if docker_client:
            # Stream logs from Docker container
            try:
                container = docker_client.containers.get(TARGET_CONTAINER)
                log.info("Streaming logs from container: %s", TARGET_CONTAINER)
                for raw_line in container.logs(stream=True, follow=True, since=0):
                    line = raw_line.decode("utf-8", errors="replace").strip()
                    entry = parse_log_line(line)
                    if entry and ship_log(http, entry):
                        shipped += 1
                        if shipped % 100 == 0:
                            log.info("Shipped %d log entries", shipped)
                    time.sleep(SHIP_INTERVAL)
            except Exception as exc:
                log.error("Docker stream error: %s", exc)
        else:
            # Stdin fallback
            log.info("Reading from stdin")
            for line in sys.stdin:
                entry = parse_log_line(line)
                if entry and ship_log(http, entry):
                    shipped += 1


if __name__ == "__main__":
    main()

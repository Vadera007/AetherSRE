"""
AetherSRE Load Driver
======================
Continuously drives HTTP traffic to the target microservice,
creating realistic traffic patterns including burst anomalies.
"""
from __future__ import annotations
import asyncio
import logging
import os
import random
import sys
import time

import httpx

TARGET_URL = os.environ.get("TARGET_URL", "http://target-app:5000")
BASE_RPS    = float(os.environ.get("BASE_RPS", "3"))  # requests/sec

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s [load-driver] %(message)s")
log = logging.getLogger("load-driver")

ENDPOINTS = [
    ("POST", "/api/auth/login",        {"username": "user", "password": "pass"}, 0.25),
    ("POST", "/api/orders/create",     {"items": ["sku-123"], "user_id": 42},    0.30),
    ("POST", "/api/payments/process",  {"amount": 99.99, "card": "****4242"},    0.25),
    ("GET",  "/api/inventory/check",   {},                                       0.20),
]


def pick_endpoint():
    endpoints, weights = zip(*[(e[:3], e[3]) for e in ENDPOINTS])
    return random.choices(endpoints, weights=weights, k=1)[0]


async def send_request(client: httpx.AsyncClient, method: str, path: str, body: dict) -> None:
    try:
        if method == "POST":
            await client.post(f"{TARGET_URL}{path}", json=body, timeout=5.0)
        else:
            await client.get(f"{TARGET_URL}{path}", timeout=5.0)
    except Exception:
        pass  # target-app errors are intentional signals


async def wait_for_target(client: httpx.AsyncClient, retries: int = 30) -> bool:
    for i in range(retries):
        try:
            r = await client.get(f"{TARGET_URL}/health", timeout=3.0)
            if r.status_code == 200:
                log.info("Target app is reachable.")
                return True
        except Exception:
            pass
        log.info("Waiting for target app... (%d/%d)", i + 1, retries)
        await asyncio.sleep(2)
    return False


async def main() -> None:
    log.info("Load driver starting | target=%s base_rps=%.1f", TARGET_URL, BASE_RPS)
    async with httpx.AsyncClient() as client:
        if not await wait_for_target(client):
            log.error("Target app not reachable. Exiting.")
            sys.exit(1)

        total = 0
        burst_countdown = random.randint(30, 60)  # seconds until next burst
        last_burst = time.time()

        while True:
            # Burst anomaly every 30-60s
            if time.time() - last_burst > burst_countdown:
                log.info("Injecting traffic burst anomaly!")
                tasks = [send_request(client, *pick_endpoint()) for _ in range(20)]
                await asyncio.gather(*tasks)
                total += 20
                last_burst = time.time()
                burst_countdown = random.randint(30, 60)

            method, path, body = pick_endpoint()
            await send_request(client, method, path, body)
            total += 1

            if total % 50 == 0:
                log.info("Sent %d requests", total)

            await asyncio.sleep(1.0 / BASE_RPS)


if __name__ == "__main__":
    asyncio.run(main())

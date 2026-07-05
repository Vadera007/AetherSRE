"""
AetherSRE — Target Flask Microservice
======================================
A realistic e-commerce backend that generates production-like log traffic
for AetherSRE to monitor and analyze.
"""

from __future__ import annotations
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, request

app = Flask(__name__)

# --- Structured JSON logger -------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "service":   getattr(record, "service", "target-app"),
            "message":   record.getMessage(),
            "request_id": getattr(record, "request_id", None),
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)
log = logging.getLogger("target-app")

SERVICES = ["auth-service", "payment-gateway", "order-service", "inventory-service"]

def _log(service: str, level: str, message: str, req_id: str = "") -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    log.log(lvl, message, extra={"service": service, "request_id": req_id})

def _req_id() -> str:
    return f"req-{random.randint(100000, 999999)}"

def _jitter(base_ms: float = 10.0, spread: float = 50.0) -> None:
    time.sleep((base_ms + random.uniform(0, spread)) / 1000)

# -----------------------------------------------------------------------
@app.route("/health")
def health():
    return jsonify({"status": "healthy", "service": "target-app"})

@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    req_id = _req_id()
    _jitter(5, 30)
    roll = random.random()
    if roll < 0.03:
        _log("auth-service", "ERROR",
             f"Authentication failure: invalid credentials for user_id={random.randint(1000,9999)}",
             req_id)
        return jsonify({"error": "AUTH_FAILURE"}), 401
    elif roll < 0.05:
        _log("auth-service", "CRITICAL",
             f"JWT signing service unavailable — token generation failed for user_id={random.randint(1000,9999)}",
             req_id)
        return jsonify({"error": "JWT_SERVICE_DOWN"}), 503
    _log("auth-service", "INFO",
         f"User authenticated successfully | user_id={random.randint(1000,9999)} method=JWT",
         req_id)
    return jsonify({"token": f"jwt-{req_id}", "expires_in": 3600})

@app.route("/api/orders/create", methods=["POST"])
def orders_create():
    req_id = _req_id()
    roll = random.random()
    if roll < 0.02:
        _jitter(500, 200)  # simulate slow order
        _log("order-service", "WARNING",
             f"Order processing latency spike: {random.randint(500,900)}ms | order_id={req_id}",
             req_id)
    elif roll < 0.05:
        _log("order-service", "ERROR",
             f"Inventory reservation failed: item out of stock | sku={random.randint(10000,99999)}",
             req_id)
        return jsonify({"error": "INVENTORY_ERROR"}), 422
    elif roll < 0.07:
        _log("order-service", "CRITICAL",
             f"Database write timeout after 30s | order_id={req_id} table=orders",
             req_id)
        return jsonify({"error": "DB_TIMEOUT"}), 503
    else:
        _jitter(20, 80)
        _log("order-service", "INFO",
             f"Order created successfully | order_id={req_id} items={random.randint(1,5)} total=${random.uniform(10,500):.2f}",
             req_id)
    return jsonify({"order_id": req_id, "status": "confirmed"})

@app.route("/api/payments/process", methods=["POST"])
def payments_process():
    req_id = _req_id()
    roll = random.random()
    if roll < 0.03:
        _log("payment-gateway", "ERROR",
             f"Payment gateway timeout after 30s | txn_id={req_id} provider=stripe",
             req_id)
        return jsonify({"error": "PAYMENT_TIMEOUT"}), 504
    elif roll < 0.05:
        _log("payment-gateway", "CRITICAL",
             f"FRAUD DETECTED: suspicious transaction pattern | txn_id={req_id} amount=${random.uniform(1000,9999):.2f}",
             req_id)
        return jsonify({"error": "FRAUD_DETECTED"}), 403
    elif roll < 0.06:
        _log("payment-gateway", "ERROR",
             f"Stripe API rate limit exceeded | txn_id={req_id} retry_after=60s",
             req_id)
        return jsonify({"error": "RATE_LIMIT"}), 429
    _jitter(30, 100)
    amount = random.uniform(10, 2000)
    _log("payment-gateway", "INFO",
         f"Payment processed successfully | txn_id={req_id} amount=${amount:.2f} provider=stripe",
         req_id)
    return jsonify({"txn_id": req_id, "amount": amount, "status": "settled"})

@app.route("/api/inventory/check", methods=["GET"])
def inventory_check():
    req_id = _req_id()
    roll = random.random()
    if roll < 0.02:
        _log("inventory-service", "CRITICAL",
             f"PostgreSQL connection pool exhausted | host=db-primary pool_size=20 waiting=47",
             req_id)
        return jsonify({"error": "DATABASE_CONNECTION_ERROR"}), 503
    elif roll < 0.04:
        _log("inventory-service", "WARNING",
             f"Cache miss rate high: {random.uniform(60,95):.1f}% | redis_latency={random.randint(100,400)}ms",
             req_id)
    _jitter(5, 20)
    sku_count = random.randint(1, 50)
    _log("inventory-service", "INFO",
         f"Inventory check complete | skus_checked={sku_count} cache_hit_rate={random.uniform(70,99):.1f}%",
         req_id)
    return jsonify({"available": sku_count, "cache_hit": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Target app starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)

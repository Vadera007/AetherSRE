#!/usr/bin/env python3
"""
AetherSRE — Day 2 Worker Launch Script
=======================================
Convenience wrapper that starts the Vector Processor Worker with settings
sourced from the environment / .env file.  Equivalent to running:

    python -m app.workers.vector_processor [OPTIONS]

This script exists so that Docker Compose, supervisord, or any process
manager can launch the worker without needing to know the module path.

Usage:
    python scripts/run_worker.py
    python scripts/run_worker.py --batch-size 64 --batch-timeout 1.0
    python scripts/run_worker.py --help
"""

from __future__ import annotations

import sys
import os

# Ensure the project root is on the import path when the script is run
# directly (e.g., `python scripts/run_worker.py` from any directory).
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from app.workers.vector_processor import main  # noqa: E402

if __name__ == "__main__":
    main()

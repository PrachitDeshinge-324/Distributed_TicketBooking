#!/usr/bin/env bash
# Regenerate the Python gRPC stubs (thin wrapper around generate_grpc.py,
# which also runs on Windows: `python scripts/generate_grpc.py`).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
"${PYTHON:-python3}" scripts/generate_grpc.py

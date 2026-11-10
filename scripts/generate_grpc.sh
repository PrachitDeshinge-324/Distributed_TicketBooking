#!/usr/bin/env bash
set -e

# Generate Python gRPC stubs from the proto definitions

# Output directory
OUT_DIR="src/dist_ticket_booking/grpc"

PYTHON_BIN="${PYTHON}"
if [ -z "$PYTHON_BIN" ]; then
    if [ -f "/opt/homebrew/Caskroom/miniforge/base/envs/AOS/bin/python" ]; then
        PYTHON_BIN="/opt/homebrew/Caskroom/miniforge/base/envs/AOS/bin/python"
    elif [ -f "./.venv/bin/python" ]; then
        PYTHON_BIN="./.venv/bin/python"
    else
        PYTHON_BIN="python3"
    fi
fi

echo "Using Python: $PYTHON_BIN"

$PYTHON_BIN -m grpc_tools.protoc \
    -I=proto \
    --python_out=${OUT_DIR} \
    --pyi_out=${OUT_DIR} \
    --grpc_python_out=${OUT_DIR} \
    proto/ticket_service.proto

# Fix the import path in the generated grpc file (workaround for python relative imports in grpc)
sed -i '' 's/import ticket_service_pb2 as ticket__service__pb2/from . import ticket_service_pb2 as ticket__service__pb2/g' ${OUT_DIR}/ticket_service_pb2_grpc.py

echo "gRPC stubs generated successfully in ${OUT_DIR}."

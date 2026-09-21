#!/usr/bin/env bash
set -e

# Generate Python gRPC stubs from the proto definitions

# Output directory
OUT_DIR="src/dist_ticket_booking/grpc"

PYTHON_BIN="${PYTHON:-./.venv/bin/python}"
if [ ! -f "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

$PYTHON_BIN -m grpc_tools.protoc \
    -I=proto \
    --python_out=${OUT_DIR} \
    --pyi_out=${OUT_DIR} \
    --grpc_python_out=${OUT_DIR} \
    proto/ticket_service.proto

# Fix the import path in the generated grpc file (workaround for python relative imports in grpc)
sed -i '' 's/import ticket_service_pb2 as ticket__service__pb2/from . import ticket_service_pb2 as ticket__service__pb2/g' ${OUT_DIR}/ticket_service_pb2_grpc.py

echo "gRPC stubs generated successfully in ${OUT_DIR}."

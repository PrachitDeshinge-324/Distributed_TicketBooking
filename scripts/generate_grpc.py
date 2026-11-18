"""Regenerate the Python gRPC stubs from proto/ticket_service.proto.

Works the same on Windows, macOS and Linux (needs grpcio-tools, which is in
requirements.txt):

    python scripts/generate_grpc.py
"""
import os
import re
import sys

from grpc_tools import protoc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTO_DIR = os.path.join(ROOT, "proto")
OUT_DIR = os.path.join(ROOT, "src", "dist_ticket_booking", "grpc")


def main() -> int:
    rc = protoc.main([
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--pyi_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        os.path.join(PROTO_DIR, "ticket_service.proto"),
    ])
    if rc != 0:
        print("protoc failed", file=sys.stderr)
        return rc
    # The generated module imports its sibling as a top-level module; make it
    # a package-relative import so it works inside dist_ticket_booking.grpc.
    path = os.path.join(OUT_DIR, "ticket_service_pb2_grpc.py")
    with open(path, encoding="utf-8") as f:
        code = f.read()
    code = re.sub(r"^import ticket_service_pb2 as ticket__service__pb2",
                  "from . import ticket_service_pb2 as ticket__service__pb2", code, flags=re.M)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(code)
    print(f"gRPC stubs generated in {OUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Standalone LLM FAQ server."""
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "src"))
import argparse
import logging
import os
from concurrent import futures

import grpc

from dist_ticket_booking.monitoring.console import setup_console  # noqa: E402

setup_console()
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.grpc.service import LLMServicer
from dist_ticket_booking.llm.domain_llm import DEFAULT_MODEL, DomainLLM

logging.basicConfig(level=getattr(logging, os.environ.get("RAFT_LOG_LEVEL", "INFO").upper(), logging.INFO),
                    format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def serve(port=50060, model_name=DEFAULT_MODEL):
    llm_service = DomainLLM(model_name=model_name)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=[("grpc.so_reuseport", 0)])

    ticket_service_pb2_grpc.add_LLMServiceServicer_to_server(
        LLMServicer(llm_service), server
    )

    try:
        bound = server.add_insecure_port(f"[::]:{port}")
    except RuntimeError:
        bound = 0
    if not bound:
        raise SystemExit(f"ERROR: port {port} is already in use. Is the LLM server already running?")
    server.start()
    print("=" * 78)
    mode = "one-page context" if getattr(llm_service, "page_mode", False) else "retrieved passages"
    print(f"  AI / LLM SERVER   model={llm_service.model_name} on {llm_service.device}  ({mode})"
          f"{'' if llm_service._pipeline else '  (model not loaded: keyword fallback)'}")
    print(f"  listening on :{port}  (GetLLMAnswer for FAQs, ExtractIntent for plain-English bookings)")
    print("=" * 78, flush=True)

    try:
        while server.wait_for_termination(timeout=1):  # True = timed out; polling keeps Ctrl+C working on Windows
            pass
    except KeyboardInterrupt:
        logger.info("Shutting down LLM server")
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50060)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL,
                        help="'auto' (default): Qwen3.5-2B on CUDA / Apple Silicon, Qwen3.5-0.8B on CPU; "
                             "or a HuggingFace model id. The LLM_MODEL environment variable overrides it.")
    args = parser.parse_args()

    serve(port=args.port, model_name=args.model)

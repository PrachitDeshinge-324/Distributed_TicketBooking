"""Standalone LLM FAQ server."""
import argparse
import logging
import os
from concurrent import futures

import grpc

from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.grpc.service import LLMServicer
from dist_ticket_booking.llm.domain_llm import DEFAULT_MODEL, DomainLLM

logging.basicConfig(level=getattr(logging, os.environ.get("RAFT_LOG_LEVEL", "INFO").upper(), logging.INFO), format="%(asctime)s [%(levelname)s] [LLM Server] %(message)s")
logger = logging.getLogger(__name__)


def serve(port=50052, model_name=DEFAULT_MODEL):
    llm_service = DomainLLM(model_name=model_name)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    ticket_service_pb2_grpc.add_LLMServiceServicer_to_server(
        LLMServicer(llm_service), server
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"LLM server running on port {port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down LLM server")
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50052)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    args = parser.parse_args()

    serve(port=args.port, model_name=args.model)

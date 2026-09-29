"""Application server handling ticket reservations and business requests."""
import argparse
import logging
from concurrent import futures

import grpc

from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.grpc.service import TicketClientServicer, TicketAppServicer
from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.llm.domain_llm import DomainLLM

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [App Server] %(message)s")
logger = logging.getLogger(__name__)


def serve(port=50051, llm_host="localhost", llm_port=50052, local_llm=False):
    booking_service = BookingService()
    local_llm_service = DomainLLM() if local_llm else None
    sessions = {}

    llm_channel = grpc.insecure_channel(f"{llm_host}:{llm_port}")
    llm_stub = ticket_service_pb2_grpc.LLMServiceStub(llm_channel)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    ticket_service_pb2_grpc.add_TicketClientServiceServicer_to_server(
        TicketClientServicer(
            booking_service=booking_service,
            sessions=sessions,
            llm_stub=llm_stub,
            llm_service=local_llm_service,
        ),
        server,
    )
    ticket_service_pb2_grpc.add_TicketAppServiceServicer_to_server(
        TicketAppServicer(
            booking_service=booking_service,
            llm_stub=llm_stub,
            llm_service=local_llm_service,
        ),
        server,
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Server started on port {port} (LLM upstream: {llm_host}:{llm_port})")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down server")
        llm_channel.close()
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--llm-host", type=str, default="localhost")
    parser.add_argument("--llm-port", type=int, default=50052)
    parser.add_argument("--local-llm", action="store_true", help="Load local model fallback in App Server")
    args = parser.parse_args()

    serve(port=args.port, llm_host=args.llm_host, llm_port=args.llm_port, local_llm=args.local_llm)

"""Launch a single Raft node for the distributed ticket booking service."""
import argparse
import logging
import os
import sys
from concurrent import futures

import grpc

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.grpc.raft_service import RaftServiceServicer
from dist_ticket_booking.grpc.service import TicketClientServicer, TicketAppServicer
from dist_ticket_booking.llm.domain_llm import DomainLLM
from dist_ticket_booking.raft import ClusterConfig, RaftNode

logging.basicConfig(
    level=getattr(logging, os.environ.get("RAFT_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("RaftNodeRunner")


def serve(
    node_id: str,
    client_port: int,
    raft_port: int,
    peers_str: str,
    data_dir: str = "./raft_data",
    llm_host: str = "localhost",
    llm_port: int = 50060,
    local_llm: bool = False,
    snapshot_threshold: int = 200,
):
    node_data_dir = os.path.join(data_dir, node_id)
    config = ClusterConfig.from_args(
        node_id=node_id,
        peers_str=peers_str,
        data_dir=node_data_dir,
        snapshot_threshold=snapshot_threshold,
    )

    booking_service = BookingService()
    raft_node = RaftNode(config, booking_service)

    local_llm_service = DomainLLM() if local_llm else None
    llm_channel = None
    llm_stub = None
    if not local_llm and llm_host and llm_port:
        try:
            llm_channel = grpc.insecure_channel(f"{llm_host}:{llm_port}")
            llm_stub = ticket_service_pb2_grpc.LLMServiceStub(llm_channel)
        except Exception as e:
            logger.warning(f"Could not connect to LLM server: {e}")

    # 1. Start Client-facing gRPC server
    client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
    ticket_service_pb2_grpc.add_TicketClientServiceServicer_to_server(
        TicketClientServicer(
            booking_service=booking_service,
            llm_stub=llm_stub,
            llm_service=local_llm_service,
            raft_node=raft_node,
        ),
        client_server,
    )
    ticket_service_pb2_grpc.add_TicketAppServiceServicer_to_server(
        TicketAppServicer(
            booking_service=booking_service,
            llm_stub=llm_stub,
            llm_service=local_llm_service,
            raft_node=raft_node,
        ),
        client_server,
    )
    client_server.add_insecure_port(f"[::]:{client_port}")
    client_server.start()

    # 2. Start Raft inter-node gRPC server
    raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    ticket_service_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftServiceServicer(raft_node),
        raft_server,
    )
    raft_server.add_insecure_port(f"[::]:{raft_port}")
    raft_server.start()

    # 3. Start Raft consensus engine
    raft_node.start()

    logger.info(
        f"Node '{node_id}' running: client_port={client_port}, "
        f"raft_port={raft_port}, peers={list(config.peers.keys())}"
    )

    try:
        client_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"Stopping node {node_id}...")
        raft_node.stop()
        client_server.stop(0)
        raft_server.stop(0)
        if llm_channel:
            llm_channel.close()
        logger.info(f"Node {node_id} stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Raft App Node")
    parser.add_argument("--node-id", type=str, required=True, help="Unique node identifier (e.g. node-1)")
    parser.add_argument("--client-port", type=int, required=True, help="Port for client gRPC requests")
    parser.add_argument("--raft-port", type=int, required=True, help="Port for inter-node Raft consensus")
    parser.add_argument(
        "--peers",
        type=str,
        default="",
        help="Comma-separated peers: 'node-2:localhost:50054:50053,node-3:localhost:50056:50055'",
    )
    parser.add_argument("--data-dir", type=str, default="./raft_data", help="Data directory for persistence")
    parser.add_argument("--llm-host", type=str, default="localhost", help="LLM server host")
    parser.add_argument("--llm-port", type=int, default=50060, help="LLM server port")
    parser.add_argument("--local-llm", action="store_true", help="Run local LLM fallback inside node")
    parser.add_argument("--snapshot-threshold", type=int, default=200,
                        help="Compact the log into a snapshot after this many entries (0 = off)")

    args = parser.parse_args()
    serve(
        node_id=args.node_id,
        client_port=args.client_port,
        raft_port=args.raft_port,
        peers_str=args.peers,
        data_dir=args.data_dir,
        llm_host=args.llm_host,
        llm_port=args.llm_port,
        local_llm=args.local_llm,
        snapshot_threshold=args.snapshot_threshold,
    )

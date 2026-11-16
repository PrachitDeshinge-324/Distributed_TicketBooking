"""Launch a single Raft node for the distributed ticket booking service."""
import argparse
import logging
import os
import shutil
import sys
import threading
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

_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None
_STYLES = [  # (text in the message, ANSI style) - first match wins
    ("BECAME LEADER", "1;32"), ("STATUS", "1;37"), ("Stepped down", "33"), ("Starting election", "33"),
    ("Pre-vote", "2"), ("Voted for", "33"), ("MEMBERSHIP", "1;35"), ("COMMITTED", "36"),
    ("snapshot", "35"), ("Snapshot", "35"),
]


class ConsoleFormatter(logging.Formatter):
    """Short, readable lines for a node's terminal: time, level only when it
    matters, and colour for elections (yellow/green) and commits (cyan)."""

    def format(self, record):
        msg = record.getMessage()
        level = "" if record.levelno == logging.INFO else f"{record.levelname}: "
        line = f"{self.formatTime(record, '%H:%M:%S')}  {level}{msg}"
        if _COLOR:
            style = "31" if record.levelno >= logging.ERROR else next(
                (st for key, st in _STYLES if key in msg), None)
            if style:
                line = f"\033[{style}m{line}\033[0m"
        return line


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(ConsoleFormatter())
logging.basicConfig(
    level=getattr(logging, os.environ.get("RAFT_LOG_LEVEL", "INFO").upper(), logging.INFO),
    handlers=[_handler],
)
logger = logging.getLogger("RaftNodeRunner")


# Without this, gRPC on macOS/Linux lets a second process bind the same port
# (SO_REUSEPORT) and two copies of a node would silently share it.
SERVER_OPTIONS = [("grpc.so_reuseport", 0)]


def _bind(server, port: int, what: str):
    try:
        bound = server.add_insecure_port(f"[::]:{port}")
    except RuntimeError:
        bound = 0
    if not bound:
        sys.exit(f"ERROR: {what} port {port} is already in use. Is this node (or another program) "
                 f"already running in another terminal? Stop it first.")


def _status_line(raft_node) -> str:
    st = raft_node.get_status()
    role = {"leader": "★ LEADER", "follower": "follower", "candidate": "candidate"}[st["state"]]
    if not st["voting"]:
        role += " (not a member)"
    leader = "self" if st["state"] == "leader" else (st["leader_id"] or "unknown")
    return (f"[{st['node_id']}] STATUS  {role:<10} term={st['current_term']}  leader={leader}  "
            f"commit={st['commit_index']}  log={st['log_length']}  state={st['state_digest']}  "
            f"members={','.join(st['members'])}")


def _status_ticker(raft_node, interval: float, stop: threading.Event):
    """Print the node's role and progress every few seconds, so every terminal
    always shows who is leader and that all replicas hold the same state."""
    last = None
    while not stop.wait(interval):
        line = _status_line(raft_node)
        if line != last:  # stay quiet while nothing changes
            logger.info(line)
            last = line


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
    host: str = "localhost",
    join: bool = False,
    fresh: bool = False,
    status_interval: float = 5.0,
):
    node_data_dir = os.path.join(data_dir, node_id)
    if fresh and os.path.isdir(node_data_dir):
        shutil.rmtree(node_data_dir)
        print(f"--fresh: deleted old data in {node_data_dir}")
    config = ClusterConfig.from_args(
        node_id=node_id,
        peers_str=peers_str,
        data_dir=node_data_dir,
        snapshot_threshold=snapshot_threshold,
        host=host,
        raft_port=raft_port,
        client_port=client_port,
        voting=not join,
    )

    booking_service = BookingService()
    raft_node = RaftNode(config, booking_service)

    local_llm_service = DomainLLM() if local_llm else None
    llm_channel = None
    llm_stub = None
    if not local_llm and llm_host and llm_port:
        try:
            llm_channel = grpc.insecure_channel(f"{llm_host}:{llm_port}", options=[("grpc.max_reconnect_backoff_ms", 1000)])
            llm_stub = ticket_service_pb2_grpc.LLMServiceStub(llm_channel)
        except Exception as e:
            logger.warning(f"Could not connect to LLM server: {e}")

    # 1. Start Client-facing gRPC server
    client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=64), options=SERVER_OPTIONS)
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
    _bind(client_server, client_port, "client")
    client_server.start()

    # 2. Start Raft inter-node gRPC server
    raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=SERVER_OPTIONS)
    ticket_service_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftServiceServicer(raft_node),
        raft_server,
    )
    _bind(raft_server, raft_port, "raft")
    raft_server.start()

    # 3. Start Raft consensus engine
    raft_node.start()

    restored = raft_node.get_status()
    print("=" * 78)
    print(f"  APP SERVER + RAFT NODE  {node_id}")
    print(f"  clients -> {host}:{client_port}    raft peers -> {host}:{raft_port}    LLM -> "
          f"{'local' if local_llm else f'{llm_host}:{llm_port}'}")
    print(f"  peers: {', '.join(f'{p.node_id}@{p.raft_address}' for p in config.peers.values()) or '(none)'}")
    if restored["commit_index"] >= 0:
        print(f"  recovered from disk: term {restored['current_term']}, "
              f"{restored['commit_index'] + 1} committed entries replayed")
    elif join:
        print("  joining: waiting to be added with scripts/cluster_admin.py add")
    print("  Ctrl+C stops this node (simulates a crash); run the same command again to restart it.")
    print("=" * 78, flush=True)

    stop_ticker = threading.Event()
    if status_interval > 0:
        threading.Thread(target=_status_ticker, args=(raft_node, status_interval, stop_ticker),
                         daemon=True).start()

    try:
        client_server.wait_for_termination()
    except KeyboardInterrupt:
        stop_ticker.set()
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
    parser.add_argument("--host", default="localhost",
                        help="Address other nodes and clients use to reach this node")
    parser.add_argument("--fresh", action="store_true",
                        help="Delete this node's saved Raft data first (start a demo from a clean state)")
    parser.add_argument("--status-interval", type=float, default=5.0,
                        help="Print this node's role/term/commit every N seconds when it changes (0 = off)")
    parser.add_argument("--join", action="store_true",
                        help="Start as a new, non-voting node that waits to be added with "
                             "'scripts/cluster_admin.py add' (--peers = current members)")

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
        host=args.host,
        join=args.join,
        fresh=args.fresh,
        status_interval=args.status_interval,
    )

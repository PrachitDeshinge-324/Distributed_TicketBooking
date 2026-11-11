"""gRPC Servicer for Raft inter-node consensus communication."""
import logging
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.raft.node import RaftNode

logger = logging.getLogger(__name__)


class RaftServiceServicer(ticket_service_pb2_grpc.RaftServiceServicer):
    """gRPC servicer handling RequestVote, AppendEntries and InstallSnapshot RPCs between nodes."""

    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node

    def RequestVote(self, request, context):
        try:
            return self.raft_node.handle_request_vote(request)
        except Exception as e:
            logger.error(f"Error handling RequestVote from {request.from_node}: {e}")
            raise

    def AppendEntries(self, request, context):
        try:
            return self.raft_node.handle_append_entries(request)
        except Exception as e:
            logger.error(f"Error handling AppendEntries from {request.from_node}: {e}")
            raise

    def InstallSnapshot(self, request, context):
        try:
            return self.raft_node.handle_install_snapshot(request)
        except Exception as e:
            logger.error(f"Error handling InstallSnapshot from {request.from_node}: {e}")
            raise

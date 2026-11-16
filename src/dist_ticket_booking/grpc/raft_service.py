"""gRPC Servicer for Raft inter-node consensus communication."""
import logging

import grpc

from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.raft.node import RaftNode

logger = logging.getLogger(__name__)


class RaftServiceServicer(ticket_service_pb2_grpc.RaftServiceServicer):
    """gRPC servicer handling RequestVote, AppendEntries, InstallSnapshot and TimeoutNow RPCs between nodes."""

    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node

    def _dispatch(self, rpc_name, handler, request, context):
        # Fault injection: behave as if the network dropped the message.
        if self.raft_node.is_partitioned_from(request.from_node):
            context.abort(grpc.StatusCode.UNAVAILABLE, "simulated network partition")
        try:
            return handler(request)
        except Exception as e:
            logger.error(f"Error handling {rpc_name} from {request.from_node}: {e}")
            raise

    def RequestVote(self, request, context):
        return self._dispatch("RequestVote", self.raft_node.handle_request_vote, request, context)

    def AppendEntries(self, request, context):
        return self._dispatch("AppendEntries", self.raft_node.handle_append_entries, request, context)

    def InstallSnapshot(self, request, context):
        return self._dispatch("InstallSnapshot", self.raft_node.handle_install_snapshot, request, context)

    def TimeoutNow(self, request, context):
        return self._dispatch("TimeoutNow", self.raft_node.handle_timeout_now, request, context)

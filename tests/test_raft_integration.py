"""Integration tests for 3-node Raft consensus cluster."""
import json
import os
import shutil
import tempfile
import time
from concurrent import futures
import pytest
import grpc

from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.grpc.raft_service import RaftServiceServicer
from dist_ticket_booking.grpc.service import TicketClientServicer, TicketAppServicer, TicketClient
from dist_ticket_booking.raft import ClusterConfig, PeerInfo, RaftNode


class InProcessNode:
    """Helper to run an in-process Raft node with real gRPC servers."""

    def __init__(self, node_id: str, data_dir: str):
        self.node_id = node_id
        self.data_dir = data_dir
        self.client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        self.client_port = self.client_server.add_insecure_port("[::]:0")

        self.raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        self.raft_port = self.raft_server.add_insecure_port("[::]:0")

        self.service = BookingService()
        self.raft_node = None

    def setup_raft(self, all_nodes):
        peers = {}
        for n in all_nodes:
            if n.node_id != self.node_id:
                peers[n.node_id] = PeerInfo(
                    node_id=n.node_id,
                    raft_host="localhost",
                    raft_port=n.raft_port,
                    client_host="localhost",
                    client_port=n.client_port,
                )
        config = ClusterConfig(
            node_id=self.node_id,
            peers=peers,
            data_dir=os.path.join(self.data_dir, self.node_id),
            heartbeat_interval_ms=50,
            election_timeout_min_ms=150,
            election_timeout_max_ms=300,
        )
        self.raft_node = RaftNode(config, self.service)

        ticket_service_pb2_grpc.add_TicketClientServiceServicer_to_server(
            TicketClientServicer(booking_service=self.service, raft_node=self.raft_node),
            self.client_server,
        )
        ticket_service_pb2_grpc.add_TicketAppServiceServicer_to_server(
            TicketAppServicer(booking_service=self.service, raft_node=self.raft_node),
            self.client_server,
        )
        ticket_service_pb2_grpc.add_RaftServiceServicer_to_server(
            RaftServiceServicer(self.raft_node),
            self.raft_server,
        )

    def start(self):
        self.client_server.start()
        self.raft_server.start()
        self.raft_node.start()

    def stop(self):
        if self.raft_node:
            self.raft_node.stop()
        self.client_server.stop(0)
        self.raft_server.stop(0)


@pytest.fixture
def cluster():
    temp_dir = tempfile.mkdtemp()
    nodes = [
        InProcessNode("node-1", temp_dir),
        InProcessNode("node-2", temp_dir),
        InProcessNode("node-3", temp_dir),
    ]

    for n in nodes:
        n.setup_raft(nodes)

    for n in nodes:
        n.start()

    # Wait for leader election
    time.sleep(1.0)

    yield nodes

    for n in nodes:
        n.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_cluster_elects_leader(cluster):
    """Verify that exactly one leader is elected in the 3-node cluster."""
    leaders = [n for n in cluster if n.raft_node.is_leader]
    assert len(leaders) == 1, f"Expected 1 leader, got {len(leaders)}"


def test_booking_replicated_across_all_nodes(cluster):
    """Verify that a booking proposed through the cluster replicates to all nodes."""
    leader = next(n for n in cluster if n.raft_node.is_leader)
    client = TicketClient(host="localhost", port=leader.client_port)

    # 1. Login
    login_res = client.login("alice", "password")
    assert login_res.status == "success"
    token = login_res.token

    # 2. Check initial availability
    target_slot = "SLOT-CVS-D1-110001"
    avail_before = leader.service.get_availability(target_slot)["available_count"]

    # 3. Post booking
    book_res = client.post(token, "booking", {"ticket_id": target_slot, "quantity": 1})
    assert book_res.status == "accepted"

    # Give followers a moment to receive commit index
    time.sleep(0.3)

    # 4. Verify that ALL 3 nodes in the cluster now have the decremented count
    for n in cluster:
        count = n.service.get_availability(target_slot)["available_count"]
        assert count == avail_before - 1, f"Node {n.node_id} state out of sync!"

    client.close()


def test_follower_redirects_to_leader(cluster):
    """Verify that a request directed to a follower is automatically redirected to the leader."""
    leader = next(n for n in cluster if n.raft_node.is_leader)
    follower = next(n for n in cluster if not n.raft_node.is_leader)

    # Directly create a client pointing to the follower's port
    client = TicketClient(host="localhost", port=follower.client_port)

    # Client login with auto-redirect
    login_res = client.login("bob", "password")
    assert login_res.status == "success"
    # Verify that the client was redirected to the leader's port
    assert client.port == leader.client_port

    client.close()

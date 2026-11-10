"""Unit tests for Raft consensus engine components."""
import os
import shutil
import tempfile
import pytest

from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.grpc import ticket_service_pb2
from dist_ticket_booking.raft.config import ClusterConfig, PeerInfo
from dist_ticket_booking.raft.log import RaftLog, LogEntry
from dist_ticket_booking.raft.node import RaftNode, NodeState
from dist_ticket_booking.raft.state_machine import StateMachine


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_log_entry_serialization():
    entry = LogEntry(
        term=2,
        index=5,
        command_type="create_booking",
        command={"user_id": "u1", "ticket_id": "t1", "quantity": 1},
    )
    # dict roundtrip
    d = entry.to_dict()
    restored_d = LogEntry.from_dict(d)
    assert restored_d == entry

    # proto roundtrip
    proto = entry.to_proto()
    assert proto.term == 2
    assert proto.index == 5
    assert proto.command_type == "create_booking"
    restored_p = LogEntry.from_proto(proto)
    assert restored_p == entry


def test_raft_log_operations_and_persistence(temp_dir):
    log = RaftLog(data_dir=temp_dir, node_id="test-node")
    assert len(log) == 0
    assert log.last_index() == -1
    assert log.last_term() == 0

    e0 = LogEntry(term=1, index=0, command_type="login", command={"username": "alice"})
    e1 = LogEntry(term=1, index=1, command_type="create_booking", command={"user_id": "u1", "ticket_id": "t1"})
    e2 = LogEntry(term=2, index=2, command_type="create_booking", command={"user_id": "u2", "ticket_id": "t2"})

    log.append(e0)
    log.append(e1)
    log.append(e2)

    assert len(log) == 3
    assert log.last_index() == 2
    assert log.last_term() == 2
    assert log.get(1) == e1
    assert log.slice_from(1) == [e1, e2]

    # Conflict truncation
    log.truncate_from(1)
    assert len(log) == 1
    assert log.last_index() == 0

    # Persistence verification
    reloaded_log = RaftLog(data_dir=temp_dir, node_id="test-node")
    assert len(reloaded_log) == 1
    assert reloaded_log.get(0) == e0


def test_state_machine_apply(temp_dir):
    service = BookingService()
    sm = StateMachine(service)

    # 1. Login
    login_entry = LogEntry(term=1, index=0, command_type="login", command={"username": "alice", "token": "tok-123"})
    res_login = sm.apply(login_entry)
    assert res_login["status"] == "success"
    assert sm.get_session_user("tok-123") == "alice"

    # Idempotent apply returns cached result
    assert sm.apply(login_entry) == res_login

    # 2. Create Booking
    target_slot = "SLOT-CVS-D1-110001"
    initial_avail = service.get_availability(target_slot)["available_count"]
    book_entry = LogEntry(
        term=1,
        index=1,
        command_type="create_booking",
        command={"user_id": "alice", "ticket_id": target_slot, "quantity": 2},
    )
    res_book = sm.apply(book_entry)
    assert res_book["status"] == "accepted"
    new_avail = service.get_availability(target_slot)["available_count"]
    assert new_avail == initial_avail - 2

    # 3. Cancel Booking
    booking_id = res_book["booking_id"]
    cancel_entry = LogEntry(
        term=1,
        index=2,
        command_type="cancel_booking",
        command={"user_id": "alice", "booking_id": booking_id},
    )
    res_cancel = sm.apply(cancel_entry)
    assert res_cancel["status"] == "success"
    assert service.get_availability(target_slot)["available_count"] == initial_avail

    # 4. Logout
    logout_entry = LogEntry(term=1, index=3, command_type="logout", command={"token": "tok-123"})
    res_logout = sm.apply(logout_entry)
    assert res_logout["status"] == "success"
    assert sm.get_session_user("tok-123") is None


def test_raft_node_request_vote_logic(temp_dir):
    config = ClusterConfig(node_id="node-1", data_dir=temp_dir)
    service = BookingService()
    node = RaftNode(config, service)
    node.current_term = 2
    node.log.append(LogEntry(term=2, index=0, command_type="noop", command={}))

    # Stale term rejected
    req_stale = ticket_service_pb2.RequestVoteRequest(
        from_node="node-2", to_node="node-1", term=1, last_log_index=0, last_log_term=1
    )
    res_stale = node.handle_request_vote(req_stale)
    assert not res_stale.vote_granted
    assert res_stale.term == 2

    # Higher term with up-to-date log granted
    req_valid = ticket_service_pb2.RequestVoteRequest(
        from_node="node-2", to_node="node-1", term=3, last_log_index=0, last_log_term=2
    )
    res_valid = node.handle_request_vote(req_valid)
    assert res_valid.vote_granted
    assert node.current_term == 3
    assert node.voted_for == "node-2"

    # Same term already voted for another candidate rejected
    req_dup = ticket_service_pb2.RequestVoteRequest(
        from_node="node-3", to_node="node-1", term=3, last_log_index=0, last_log_term=2
    )
    res_dup = node.handle_request_vote(req_dup)
    assert not res_dup.vote_granted


def test_raft_node_append_entries_logic(temp_dir):
    config = ClusterConfig(node_id="node-1", data_dir=temp_dir)
    service = BookingService()
    node = RaftNode(config, service)
    node.current_term = 1

    # Heartbeat from valid leader
    req_hb = ticket_service_pb2.AppendEntriesRequest(
        from_node="node-leader",
        to_node="node-1",
        term=1,
        prev_log_index=-1,
        prev_log_term=0,
        commit_index=-1,
        entries=[],
    )
    res_hb = node.handle_append_entries(req_hb)
    assert res_hb.entry_appended
    assert node.leader_id == "node-leader"

    # Log entry append
    e0 = LogEntry(term=1, index=0, command_type="login", command={"username": "alice", "token": "t1"})
    req_append = ticket_service_pb2.AppendEntriesRequest(
        from_node="node-leader",
        to_node="node-1",
        term=1,
        prev_log_index=-1,
        prev_log_term=0,
        commit_index=0,
        entries=[e0.to_proto()],
    )
    res_append = node.handle_append_entries(req_append)
    assert res_append.entry_appended
    assert len(node.log) == 1
    assert node.commit_index == 0
    assert node.state_machine.last_applied == 0
    assert node.state_machine.get_session_user("t1") == "alice"


def test_single_node_cluster_propose(temp_dir):
    import time
    config = ClusterConfig(
        node_id="standalone-node",
        peers={},
        data_dir=temp_dir,
        election_timeout_min_ms=50,
        election_timeout_max_ms=100,
    )
    service = BookingService()
    node = RaftNode(config, service)
    node.start()

    # Wait briefly for election timeout to trigger
    for _ in range(20):
        if node.is_leader:
            break
        time.sleep(0.05)

    assert node.is_leader
    assert node.state == NodeState.LEADER

    res = node.propose("login", {"username": "charlie", "token": "tok-c"})
    assert res["status"] == "success"
    assert node.state_machine.get_session_user("tok-c") == "charlie"
    node.stop()


def test_snapshot_and_log_compaction(temp_dir):
    service = BookingService()
    sm = StateMachine(service)
    log = RaftLog(temp_dir, "node-snap")

    # Add 5 entries and apply them
    for i in range(5):
        entry = LogEntry(term=1, index=i, command_type="login", command={"username": f"user{i}", "token": f"tok{i}"})
        log.append(entry)
        sm.apply(entry)

    assert len(log) == 5
    assert sm.last_applied == 4

    # Snapshot at index 3
    data = sm.get_snapshot_data()
    log.create_snapshot(last_included_index=3, last_included_term=1, state_data=data)

    # Log entries up to index 3 are compacted!
    assert len(log) == 1
    assert log.last_included_index == 3
    assert log.get(3) is None
    assert log.get(4) is not None  # only entry 4 remains in log

    # Reload log and verify snapshot metadata
    reloaded_log = RaftLog(temp_dir, "node-snap")
    assert reloaded_log.last_included_index == 3
    assert len(reloaded_log) == 1
    assert reloaded_log.last_index() == 4


def test_node_recovery_from_snapshot(temp_dir):
    import time
    config = ClusterConfig(
        node_id="snap-node",
        peers={},
        data_dir=temp_dir,
        election_timeout_min_ms=50,
        election_timeout_max_ms=100,
    )
    service = BookingService()
    node = RaftNode(config, service)
    node.start()

    for _ in range(20):
        if node.is_leader:
            break
        time.sleep(0.05)

    # Make 2 bookings
    target_slot = "SLOT-CVS-D1-110001"
    node.propose("login", {"username": "alice", "token": "tok-a"})
    node.propose("create_booking", {"user_id": "alice", "ticket_id": target_slot, "quantity": 2})

    # Take snapshot
    assert node.take_snapshot()
    node.stop()

    # Create new node from same data directory
    service2 = BookingService()
    node2 = RaftNode(config, service2)
    node2.start()

    # Verify state restored from snapshot
    assert node2.state_machine.get_session_user("tok-a") == "alice"
    assert node2.state_machine.last_applied >= 1
    avail = node2.state_machine.booking_service.get_availability(target_slot)["available_count"]
    assert avail == 48  # 50 - 2 = 48
    node2.stop()

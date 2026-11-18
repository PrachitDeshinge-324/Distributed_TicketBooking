"""Safety tests for Milestone 2: network partitions, stale reads,
crash-safe persistence and authentication.

The partition tests use RaftNode.set_partition(), which drops Raft RPCs
between chosen nodes while every process keeps running; this is the
failure that killing a process cannot show (a leader that is alive but
cut off still believes it is the leader).
"""
import json
import os
import shutil
import tempfile
import time

import pytest

from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.grpc import ticket_service_pb2
from dist_ticket_booking.grpc.service import TicketClient
from dist_ticket_booking.raft.config import ClusterConfig
from dist_ticket_booking.raft.log import LogEntry, RaftLog
from dist_ticket_booking.raft.node import RaftNode
from dist_ticket_booking.raft.storage import CorruptStateError, atomic_write_json, read_json
from test_raft_stress import SLOT, _wait_for_leader, cluster  # noqa: F401  (pytest fixture)


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _addr(node):
    return f"localhost:{node.client_port}"


def _available(node):
    return node.service.get_availability(SLOT)["available_count"]


def _wait_converged_digest(nodes, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = [n.raft_node.get_status() for n in nodes]
        if len({(s["last_applied"], s["state_digest"]) for s in st}) == 1:
            return st
        time.sleep(0.05)
    raise AssertionError(f"replicas did not converge: {[n.raft_node.get_status() for n in nodes]}")


# ── Network partition ──────────────────────────────────────────────────────

def test_partitioned_leader_never_serves_stale_data(cluster):  # noqa: F811
    """Isolate the leader. The majority elects a new leader and keeps
    booking; the old leader must refuse reads and writes instead of
    returning stale seat counts. After healing, all replicas agree."""
    old = _wait_for_leader(cluster)
    majority = [n for n in cluster if n is not old]

    stale_client = TicketClient(host="localhost", port=old.client_port)
    tok = stale_client.login("pat").token
    assert tok
    before = _available(old)

    # Cut the leader off from both followers (in both directions).
    old.raft_node.set_partition([n.node_id for n in majority])
    for n in majority:
        n.raft_node.set_partition([old.node_id])

    new = _wait_for_leader(majority, timeout=10)
    assert new.raft_node.get_status()["current_term"] > old.raft_node.get_status()["current_term"]

    # The majority side keeps working.
    good = TicketClient.for_cluster([_addr(n) for n in majority])
    good_tok = good.login("quinn").token
    assert good.post(good_tok, "booking", {"ticket_id": SLOT, "quantity": 1}).status == "accepted"
    assert _available(new) == before - 1

    # The old leader still thinks it leads, but its data is now stale: it
    # must refuse to answer rather than report the old seat count.
    time.sleep(0.5)  # > election_timeout_max (300 ms) in the test config
    assert _available(old) == before, "precondition: old leader's copy is stale"
    read = stale_client.get(tok, "availability", {"ticket_id": SLOT})
    assert read.status == "unavailable"
    write = stale_client.post(tok, "booking", {"ticket_id": SLOT, "quantity": 1})
    assert write.status == "unavailable" and "quorum" in write.message

    # A client that knows every node moves past the cut-off node and gets
    # the fresh value from the real leader.
    roaming = TicketClient.for_cluster([_addr(old)] + [_addr(n) for n in majority])
    roaming_tok = good_tok  # sessions are replicated: valid on the majority side
    res = roaming.get(roaming_tok, "availability", {"ticket_id": SLOT})
    assert res.status == "success"
    assert json.loads(res.items[0].data)["available_count"] == before - 1

    # Heal: the old leader learns the newer term, steps down and catches up.
    for n in cluster:
        n.raft_node.set_partition([])
    deadline = time.time() + 10
    while time.time() < deadline and old.raft_node.is_leader:
        time.sleep(0.05)
    assert not old.raft_node.is_leader
    _wait_converged_digest(cluster)
    assert _available(old) == before - 1
    # The write refused during the partition was never applied anywhere.
    assert all(_available(n) == before - 1 for n in cluster)

    for c in (stale_client, good, roaming):
        c.close()


def test_minority_follower_cannot_disrupt_cluster(cluster):  # noqa: F811
    """A follower cut off from everyone keeps timing out and raising its
    term, but cannot win an election, and the leader keeps committing."""
    leader = _wait_for_leader(cluster)
    loner = next(n for n in cluster if n is not leader)
    others = [n for n in cluster if n is not loner]
    loner.raft_node.set_partition([n.node_id for n in others])
    for n in others:
        n.raft_node.set_partition([loner.node_id])

    time.sleep(1.0)
    assert leader.raft_node.is_leader
    assert not loner.raft_node.is_leader
    c = TicketClient(host="localhost", port=leader.client_port)
    tok = c.login("rory").token
    assert c.post(tok, "booking", {"ticket_id": SLOT, "quantity": 1}).status == "accepted"
    c.close()

    for n in cluster:
        n.raft_node.set_partition([])
    _wait_for_leader(cluster, timeout=10)
    _wait_converged_digest(cluster)


# ── Vote handling ──────────────────────────────────────────────────────────

def test_follower_ignores_votes_while_leader_is_alive(temp_dir):
    """Raft thesis 4.2.3: a follower that heard from a leader within the
    minimum election timeout neither grants a vote nor adopts the term."""
    node = RaftNode(ClusterConfig(node_id="n1", data_dir=temp_dir), BookingService())
    node.handle_append_entries(ticket_service_pb2.AppendEntriesRequest(
        from_node="leader", to_node="n1", term=1, prev_log_index=-1, prev_log_term=0, commit_index=-1,
    ))
    vote = ticket_service_pb2.RequestVoteRequest(
        from_node="rogue", to_node="n1", term=7, last_log_index=10, last_log_term=7,
    )
    res = node.handle_request_vote(vote)
    assert not res.vote_granted
    assert node.current_term == 1

    # Once the leader has been silent for the election timeout, votes count again.
    node._last_leader_contact -= 10
    assert node.handle_request_vote(vote).vote_granted
    assert node.current_term == 7


def test_follower_truncates_conflicting_suffix(temp_dir):
    node = RaftNode(ClusterConfig(node_id="n1", data_dir=temp_dir), BookingService())
    for i, term in enumerate([1, 1, 2, 2]):  # entries 2-3 from a deposed leader of term 2
        node.log.append(LogEntry(term=term, index=i, command_type="noop"))
    node.current_term = 3
    new = [LogEntry(term=3, index=i, command_type="noop").to_proto() for i in (2, 3, 4)]
    res = node.handle_append_entries(ticket_service_pb2.AppendEntriesRequest(
        from_node="leader", to_node="n1", term=3, prev_log_index=1, prev_log_term=1,
        commit_index=-1, entries=new,
    ))
    assert res.entry_appended and res.match_index == 4
    assert [node.log.get(i).term for i in range(5)] == [1, 1, 3, 3, 3]


# ── Crash-safe persistence ─────────────────────────────────────────────────

def test_atomic_write_leaves_no_partial_file(temp_dir):
    path = os.path.join(temp_dir, "state.json")
    atomic_write_json(path, {"current_term": 4})
    # A crash mid-write would only ever leave the temporary file behind.
    with open(path + ".tmp", "w") as f:
        f.write('{"current_ter')
    assert read_json(path) == {"current_term": 4}


def test_corrupt_stable_storage_refuses_to_start(temp_dir):
    """Starting from an empty log after forgetting acknowledged entries
    would break Raft's safety, so a corrupt file is a hard error."""
    with open(os.path.join(temp_dir, "n1_log.jsonl"), "w") as f:
        # A damaged line that is NOT the last one: real corruption, not a torn append.
        f.write('{"term": 1, "ind\n{"term": 1, "index": 1, "command_type": "noop"}\n')
    with pytest.raises(CorruptStateError):
        RaftLog(temp_dir, "n1")
    with open(os.path.join(temp_dir, "n1_state.json"), "w") as f:
        f.write("{")
    os.remove(os.path.join(temp_dir, "n1_log.jsonl"))
    with pytest.raises(CorruptStateError):
        RaftNode(ClusterConfig(node_id="n1", data_dir=temp_dir), BookingService())


def test_torn_last_line_from_a_crashed_append_is_dropped(temp_dir):
    """A crash in the middle of an append can only tear the final line, which
    holds an entry that was never acknowledged: drop it and keep the rest."""
    log = RaftLog(temp_dir, "n1")
    for i in range(3):
        log.append(LogEntry(term=1, index=i, command_type="noop"))
    with open(os.path.join(temp_dir, "n1_log.jsonl"), "a") as f:
        f.write('{"term": 1, "index": 3, "comm')
    reloaded = RaftLog(temp_dir, "n1")
    assert reloaded.last_index() == 2
    reloaded.append(LogEntry(term=1, index=3, command_type="noop"))
    assert RaftLog(temp_dir, "n1").last_index() == 3


def test_append_writes_only_new_entries(temp_dir):
    """Appends cost O(new entries): earlier lines are never rewritten."""
    log = RaftLog(temp_dir, "n1")
    path = os.path.join(temp_dir, "n1_log.jsonl")
    log.append(LogEntry(term=1, index=0, command_type="noop"))
    with open(path) as f:
        first = f.read()
    log.extend([LogEntry(term=1, index=i, command_type="noop") for i in (1, 2)])
    with open(path) as f:
        after = f.read()
    assert after.startswith(first) and after.count("\n") == 3


def test_legacy_json_log_is_migrated(temp_dir):
    atomic_write_json(os.path.join(temp_dir, "n1_log.json"),
                      [{"term": 1, "index": 0, "command_type": "noop", "command": {}}])
    assert RaftLog(temp_dir, "n1").last_index() == 0
    assert not os.path.exists(os.path.join(temp_dir, "n1_log.json"))


def test_crash_between_snapshot_and_log_rewrite(temp_dir):
    """The snapshot is written before the shortened log; if the node dies in
    between, entries already covered by the snapshot are dropped on load."""
    log = RaftLog(temp_dir, "n1")
    for i in range(5):
        log.append(LogEntry(term=1, index=i, command_type="noop"))
    atomic_write_json(os.path.join(temp_dir, "n1_snapshot.json"),
                      {"last_included_index": 2, "last_included_term": 1, "state": {}})
    reloaded = RaftLog(temp_dir, "n1")
    assert len(reloaded) == 2
    assert reloaded.get(2) is None and reloaded.get(3).index == 3
    assert reloaded.term_at(2) == 1 and reloaded.last_index() == 4


# ── Authentication ─────────────────────────────────────────────────────────

def test_login_checks_password_on_every_replica(cluster):  # noqa: F811
    leader = _wait_for_leader(cluster)
    c = TicketClient(host="localhost", port=leader.client_port)
    assert c.login("sam", "s3cret-pass").status == "success"
    assert c.login("sam", "wrong").status == "unauthorized"
    assert c.login("sam", "s3cret-pass").status == "success"
    assert c.login("sam", "").status == "unauthorized"
    c.close()

    _wait_converged_digest(cluster)
    for n in cluster:
        assert "sam" in n.raft_node.state_machine.users
        with open(os.path.join(n.data_dir, n.node_id, f"{n.node_id}_log.jsonl")) as f:
            assert "s3cret-pass" not in f.read(), "plain-text password reached the Raft log"


def test_log_with_missing_prefix_refuses_to_start(temp_dir):
    """A compacted log whose snapshot is gone must not load: the node could
    never apply the missing entries yet would keep reporting progress."""
    log = RaftLog(temp_dir, "n1")
    for i in range(6):
        log.append(LogEntry(term=1, index=i, command_type="noop"))
    log.create_snapshot(3, 1, {})
    os.remove(os.path.join(temp_dir, "n1_snapshot.json"))
    with pytest.raises(CorruptStateError, match="--fresh"):
        RaftLog(temp_dir, "n1")

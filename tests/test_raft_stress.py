"""Stress tests for the Raft-backed booking cluster.

Covers the Milestone 2 "stress testing" objective:
  * many concurrent clients contending for the same slot (no overbooking),
  * leader killed while clients are booking (no lost / duplicated bookings),
  * cancel-after-failover (booking ids are identical on every replica).
"""
import shutil
import tempfile
import threading
import time

import pytest

from dist_ticket_booking.grpc.service import TicketClient
from test_raft_integration import InProcessNode

SLOT = "SLOT-CBX-BOS-560001"  # small capacity slot


def _wait_for_leader(nodes, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        leaders = [n for n in nodes if n.raft_node.is_leader]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.05)
    raise AssertionError("no single leader elected")


def _wait_converged(nodes, timeout=10.0):
    """Wait until every live node has applied the same log prefix."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        applied = {n.raft_node.get_status()["last_applied"] for n in nodes}
        if len(applied) == 1:
            return
        time.sleep(0.05)
    raise AssertionError(f"nodes did not converge: {applied}")


def _booked(node, slot):
    return {
        bid: b for bid, b in node.service.bookings.items()
        if b["ticket_id"] == slot and b["status"] == "confirmed"
    }


@pytest.fixture
def cluster():
    tmp = tempfile.mkdtemp()
    nodes = [InProcessNode(f"node-{i}", tmp) for i in (1, 2, 3)]
    for n in nodes:
        n.setup_raft(nodes)
    for n in nodes:
        n.start()
    _wait_for_leader(nodes)
    yield nodes
    for n in nodes:
        try:
            n.stop()
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)


def _client_worker(entry_port, name, results, stop_evt=None, rounds=1):
    c = TicketClient(host="localhost", port=entry_port)
    try:
        tok = c.login(name).token
        for _ in range(rounds):
            if stop_evt is not None and stop_evt.is_set():
                break
            try:
                res = c.post(tok, "booking", {"ticket_id": SLOT, "quantity": 1})
                results.append((name, res.status, res.message))
            except Exception as exc:  # leader died mid-call
                results.append((name, "exception", str(exc)))
    except Exception as exc:
        results.append((name, "exception", str(exc)))
    finally:
        c.close()


def test_concurrent_clients_never_overbook(cluster):
    """40 clients race for a slot with limited capacity."""
    leader = next(n for n in cluster if n.raft_node.is_leader)
    capacity = leader.service.get_availability(SLOT)["available_count"]
    assert capacity > 0

    results = []
    threads = [
        threading.Thread(target=_client_worker, args=(cluster[i % 3].client_port, f"user{i}", results))
        for i in range(40)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    accepted = [r for r in results if r[1] == "accepted"]
    assert len(accepted) == capacity, f"accepted={len(accepted)} capacity={capacity}"

    _wait_converged(cluster)
    for n in cluster:
        assert n.service.get_availability(SLOT)["available_count"] == 0
        assert len(_booked(n, SLOT)) == capacity
    # identical booking ids on every replica
    ids = [set(_booked(n, SLOT)) for n in cluster]
    assert ids[0] == ids[1] == ids[2]


def test_leader_crash_under_load(cluster):
    """Kill the leader while clients are booking; no overbooking, replicas agree."""
    leader = next(n for n in cluster if n.raft_node.is_leader)
    survivors = [n for n in cluster if n is not leader]
    capacity = leader.service.get_availability(SLOT)["available_count"]

    results = []
    stop_evt = threading.Event()
    threads = [
        threading.Thread(
            target=_client_worker,
            args=(survivors[i % 2].client_port, f"load{i}", results, stop_evt, 3),
        )
        for i in range(20)
    ]
    for t in threads:
        t.start()
    time.sleep(0.15)
    leader.stop()  # crash the leader mid-flight
    for t in threads:
        t.join(timeout=90)
    stop_evt.set()

    new_leader = _wait_for_leader(survivors, timeout=15)
    _wait_converged(survivors)

    # Final state is consistent and never oversold
    counts = {n.node_id: n.service.get_availability(SLOT)["available_count"] for n in survivors}
    assert len(set(counts.values())) == 1, counts
    remaining = counts[new_leader.node_id]
    assert 0 <= remaining <= capacity

    booked = [set(_booked(n, SLOT)) for n in survivors]
    assert booked[0] == booked[1]
    assert len(booked[0]) == capacity - remaining

    # Every booking the client was told "accepted" is durably present
    acked = [r for r in results if r[1] == "accepted"]
    assert len(acked) <= len(booked[0])


def test_cancel_works_after_failover(cluster):
    """A booking made via the old leader can be cancelled via the new leader."""
    leader = next(n for n in cluster if n.raft_node.is_leader)
    survivors = [n for n in cluster if n is not leader]

    c = TicketClient(host="localhost", port=leader.client_port)
    tok = c.login("carol").token
    res = c.post(tok, "booking", {"ticket_id": SLOT, "quantity": 1})
    assert res.status == "accepted"
    c.close()
    _wait_converged(cluster)

    booking_ids = set(_booked(leader, SLOT))
    assert len(booking_ids) == 1
    booking_id = next(iter(booking_ids))
    for n in survivors:
        assert booking_id in n.service.bookings, "replica assigned a different booking id"

    leader.stop()
    new_leader = _wait_for_leader(survivors, timeout=15)

    c2 = TicketClient(host="localhost", port=new_leader.client_port)
    tok2 = c2.login("carol").token
    out = c2.post(tok2, "cancel", {"booking_id": booking_id})
    assert out.status in ("accepted", "success"), out.message
    c2.close()
    _wait_converged(survivors)
    for n in survivors:
        assert n.service.bookings[booking_id]["status"] == "cancelled"


def test_no_quorum_fails_fast_with_clear_error(cluster):
    """Leader cut off from the majority rejects writes quickly and clearly."""
    leader = next(n for n in cluster if n.raft_node.is_leader)
    c = TicketClient(host="localhost", port=leader.client_port)
    tok = c.login("dave").token
    assert tok

    for n in cluster:
        if n is not leader:
            n.stop()
    time.sleep(1.2)  # > 2x election timeout max (300 ms) in the test config

    t0 = time.time()
    res = c.post(tok, "booking", {"ticket_id": SLOT, "quantity": 1})
    assert res.status == "unavailable"
    assert "quorum" in res.message
    login = c.login("erin")
    assert login.status == "unavailable" and "quorum" in login.message
    assert time.time() - t0 < 2.0, "should fail fast, not wait for commit timeout"
    c.close()

"""Raft section 6 (membership changes), Pre-Vote and leadership transfer."""
import time

from dist_ticket_booking.grpc.service import TicketClient
from test_raft_integration import InProcessNode
from test_raft_safety import _wait_converged_digest
from test_raft_stress import SLOT, _wait_for_leader, cluster  # noqa: F401  (pytest fixture)


def _addr(n):
    return f"localhost:{n.client_port}"


def _members(node):
    return node.raft_node.get_status()["members"]


def _wait(cond, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return False


def _book(targets, user):
    c = TicketClient.for_cluster(targets)
    try:
        tok = c.login(user).token
        return c.post(tok, "booking", {"ticket_id": SLOT, "quantity": 1}).status
    finally:
        c.close()


def test_add_and_remove_servers(cluster):  # noqa: F811
    """Grow 3 -> 4 nodes, then shrink by removing the leader itself."""
    leader = _wait_for_leader(cluster)
    assert _book([_addr(n) for n in cluster], "u1") == "accepted"

    newbie = InProcessNode("node-4", cluster[0].data_dir)
    newbie.setup_raft(cluster, join=True)
    newbie.start()
    try:
        time.sleep(0.6)
        assert not newbie.raft_node.config.voting  # waits; never campaigns

        admin = TicketClient.for_cluster([_addr(n) for n in cluster])
        res = admin.change_membership("add", "node-4", "localhost", newbie.raft_port, newbie.client_port)
        assert res.status == "success", res.message
        everyone = cluster + [newbie]
        assert _wait(lambda: all(_members(n) == ["node-1", "node-2", "node-3", "node-4"] for n in everyone))
        assert newbie.raft_node.config.voting
        assert leader.raft_node.config.majority == 3  # 4 voters -> majority of 3
        _wait_converged_digest(everyone)  # the newcomer received all earlier bookings

        # Only one change at a time; duplicates are rejected.
        assert admin.change_membership("add", "node-4", "localhost", 1, 2).status == "error"

        # Remove the current leader: it commits the change, then steps down.
        res = admin.change_membership("remove", leader.node_id)
        assert res.status == "success", res.message
        rest = [n for n in everyone if n is not leader]
        new_leader = _wait_for_leader(rest, timeout=10)
        assert new_leader is not leader
        assert _wait(lambda: not leader.raft_node.is_leader)
        assert _wait(lambda: all(leader.node_id not in _members(n) for n in rest))
        admin.close()

        # The removed node is idle; the three remaining members keep working.
        leader.stop()
        assert _book([_addr(n) for n in rest], "u2") == "accepted"
        _wait_converged_digest(rest)
    finally:
        newbie.stop()


def test_membership_survives_restart_and_snapshot(cluster):  # noqa: F811
    """The configuration lives in the log and in snapshots, so a restarted node
    remembers it even after the configuration entry was compacted away."""
    leader = _wait_for_leader(cluster)
    victim = next(n for n in cluster if n is not leader)
    admin = TicketClient.for_cluster([_addr(n) for n in cluster])
    assert admin.change_membership("remove", victim.node_id).status == "success"
    admin.close()
    victim.stop()
    rest = [n for n in cluster if n is not victim]
    assert all(victim.node_id not in _members(n) for n in rest)
    _wait_converged_digest(rest)  # the follower has applied the config entry

    survivor = next(n for n in rest if n is not leader)
    assert survivor.raft_node.take_snapshot()
    assert survivor.raft_node.log.snapshot["config"] is not None
    survivor.stop()

    # Restart it from disk with the ORIGINAL 3-node command line.
    reborn = InProcessNode(survivor.node_id, survivor.data_dir)
    reborn.setup_raft(cluster)
    try:
        assert sorted(reborn.raft_node.get_status()["members"]) == sorted(n.node_id for n in rest)
        assert reborn.raft_node.config.majority == 2
    finally:
        reborn.stop()


def test_leadership_transfer(cluster):  # noqa: F811
    leader = _wait_for_leader(cluster)
    target = next(n for n in cluster if n is not leader)
    term = leader.raft_node.current_term
    admin = TicketClient(host="localhost", port=leader.client_port)
    res = admin.transfer_leadership(target.node_id)
    admin.close()
    assert res.status == "success", res.message
    assert target.raft_node.is_leader
    assert not leader.raft_node.is_leader
    assert target.raft_node.current_term == term + 1  # one clean election, no split vote
    assert _book([_addr(n) for n in cluster], "u3") == "accepted"


def test_prevote_keeps_isolated_node_from_raising_its_term(cluster):  # noqa: F811
    """Without Pre-Vote, a node cut off by a partition bumps its term on every
    timeout and forces a needless election when it reconnects."""
    leader = _wait_for_leader(cluster)
    loner = next(n for n in cluster if n is not leader)
    others = [n for n in cluster if n is not loner]
    start_term = loner.raft_node.current_term
    loner.raft_node.set_partition([n.node_id for n in others])
    for n in others:
        n.raft_node.set_partition([loner.node_id])
    time.sleep(1.5)  # ~5+ election timeouts in the test configuration
    assert loner.raft_node.current_term == start_term

    for n in cluster:
        n.raft_node.set_partition([])
    time.sleep(0.5)
    assert leader.raft_node.is_leader  # reconnecting did not depose the leader
    _wait_converged_digest(cluster)

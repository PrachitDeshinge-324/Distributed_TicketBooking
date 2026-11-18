"""Accounts, roles, booking rules and payments on a live 3-node Raft cluster."""
import json

from dist_ticket_booking.business.booking_service import appointment_start
from dist_ticket_booking.grpc.service import TicketClient
from dist_ticket_booking.raft.log import LogEntry
from dist_ticket_booking.raft.state_machine import StateMachine
from dist_ticket_booking.business.booking_service import BookingService
from test_raft_safety import _wait_converged_digest
from test_raft_stress import SLOT, _wait_for_leader, cluster  # noqa: F401  (pytest fixture)


def _client(cluster):
    return TicketClient(host="localhost", port=_wait_for_leader(cluster).client_port)


def test_login_requires_an_explicit_account(cluster):  # noqa: F811
    c = _client(cluster)
    res = c.login("nobody", "pw", auto_register=False)
    assert res.status == "unknown_user" and "register" in res.message
    assert c.register("nobody", "pw").status == "success"
    assert c.register("nobody", "other").status == "error"        # name taken
    assert c.register("bad name!", "pw").status == "error"        # invalid username
    assert c.login("nobody", "wrong", auto_register=False).status == "unauthorized"
    ok = c.login("nobody", "pw", auto_register=False)
    assert ok.status == "success" and ok.role == "citizen"
    c.close()


def test_admin_role_needs_the_signup_code(cluster):  # noqa: F811
    c = _client(cluster)
    assert c.register("mallory", "pw", role="admin", admin_code="guess").status == "unauthorized"
    assert c.register("mallory", "pw", role="superuser").status == "error"
    res = c.login_admin("ops", "pw")
    assert res.status == "success" and res.role == "admin"
    c.close()
    _wait_converged_digest(cluster)
    for n in cluster:  # the account and its role are replicated
        assert n.raft_node.state_machine.get_role("ops") == "admin"


def test_cluster_operations_are_admin_only(cluster):  # noqa: F811
    leader = _wait_for_leader(cluster)
    target = next(n for n in cluster if n is not leader)
    c = TicketClient(host="localhost", port=leader.client_port)
    assert c.transfer_leadership(target.node_id).status == "unauthorized"   # no session
    c.login("citizen1", "pw")
    assert c.transfer_leadership(target.node_id).status == "forbidden"
    assert c.change_membership("remove", target.node_id).status == "forbidden"
    assert leader.raft_node.is_leader
    c.close()


def test_helpdesk_admin_sees_and_cancels_any_booking(cluster):  # noqa: F811
    c = _client(cluster)
    tok = c.login("citizen2", "pw").token
    res = c.post(tok, "booking", {"ticket_id": "SLOT-CVS-D1-110001"})
    assert res.status == "accepted"
    assert c.get(tok, "all_bookings").status == "forbidden"

    admin = _client(cluster)
    atok = admin.login_admin("helpdesk", "pw").token
    items = admin.get(atok, "all_bookings").items
    bid = next(i.id for i in items if json.loads(i.data)["user_id"] == "citizen2")
    assert admin.post(atok, "cancel", {"booking_id": bid}).status == "success"
    _wait_converged_digest(cluster)
    for n in cluster:
        assert n.service.bookings[bid]["status"] == "cancelled"
    c.close()
    admin.close()


def test_one_appointment_per_slot_and_payment_replicated(cluster):  # noqa: F811
    c = _client(cluster)
    tok = c.login("payer", "pw").token
    declined = c.post(tok, "booking", {"ticket_id": SLOT, "payment_method": "card_declined"})
    assert declined.status == "rejected" and "payment declined" in declined.message
    ok = c.post(tok, "booking", {"ticket_id": SLOT, "payment_method": "card"})
    assert ok.status == "accepted" and "250" in ok.message
    again = c.post(tok, "booking", {"ticket_id": SLOT})
    assert again.status == "rejected" and "already hold" in again.message

    bid = json.loads(c.get(tok, "my_bookings").items[0].data)["booking_id"]
    out = c.post(tok, "cancel", {"booking_id": bid})
    assert out.status == "success" and "refunded" in out.message
    c.close()

    states = _wait_converged_digest(cluster)
    assert len({s["state_digest"] for s in states}) == 1   # ledger is part of the digest
    for n in cluster:
        ledger = n.service.payments.ledger
        assert [p["status"] for p in ledger.values()] == ["refunded"]


def test_time_rules_use_the_replicated_timestamp_not_the_local_clock():
    """Two replicas applying the same entries must agree even if their clocks
    differ, because they evaluate the leader's `requested_at`."""
    slot = "SLOT-CVS-D1-110001"
    start = appointment_start(BookingService().ticket_store[slot])
    book = LogEntry(1, 0, "create_booking", {"user_id": "a", "ticket_id": slot, "requested_at": start - 5 * 86400})
    booking_id = StateMachine(BookingService()).apply(book)["booking_id"]
    entries = [
        book,
        # 2 hours before the appointment: inside the 24-hour window
        LogEntry(1, 1, "cancel_booking", {"user_id": "a", "booking_id": booking_id,
                                          "requested_at": start - 2 * 3600}),
        LogEntry(1, 2, "create_booking", {"user_id": "b", "ticket_id": slot, "requested_at": start + 60}),
    ]
    results = []
    for _ in range(2):
        sm = StateMachine(BookingService())
        results.append([sm.apply(e)["status"] for e in entries] + [sm.digest()])
    assert results[0] == results[1]
    assert results[0][:3] == ["accepted", "rejected", "rejected"]

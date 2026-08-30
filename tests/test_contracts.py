"""Basic placeholder tests for Milestone 1 shared contracts.

Owner: Person B (Simulation and Validation)
Primary responsibility: verify the shared Python data contract used by both members.

Functions to implement in this file:
- test_ticket_model_contract
- test_booking_request_contract

TODO for Milestone 1:
- add tests for booking lifecycle states
- add tests for queue order and lock conflict scenarios
- expand to ensure all shared model fields are consistent across modules
"""

from dist_ticket_booking.core.models import BookingRequest, Ticket


def test_ticket_model_contract():
    ticket = Ticket(
        ticket_id="T-001",
        name="Movie Night",
        category="concert",
        price=250.0,
        available_count=10,
    )

    assert ticket.ticket_id == "T-001"
    assert ticket.available_count == 10


def test_booking_request_contract():
    request = BookingRequest(
        request_id="R-001",
        user_id="U-001",
        ticket_id="T-001",
        quantity=2,
        priority=1,
    )

    assert request.quantity == 2
    assert request.priority == 1

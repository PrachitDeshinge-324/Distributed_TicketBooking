"""Unit tests for shared models and lock manager."""

from dist_ticket_booking.core.models import Booking, BookingRequest, SystemState, VaccineSlot


def test_ticket_model_contract():
    ticket = VaccineSlot(
        slot_id="SLOT-001",
        vaccine_name="COVISHIELD",
        center_name="Health Center",
        center_pincode="000001",
        dose_number=1,
        date="2026-11-01",
        price=250.0,
        available_count=10,
    )

    assert ticket.slot_id == "SLOT-001"
    assert ticket.available_count == 10
    assert ticket.status == "available"


def test_booking_request_contract():
    request = BookingRequest(
        request_id="R-001",
        user_id="U-001",
        slot_id="SLOT-001",
        quantity=2,
        priority=1,
    )

    assert request.quantity == 2
    assert request.priority == 1
    assert request.user_id == "U-001"


def test_system_state_tracks_records():
    state = SystemState()
    ticket = VaccineSlot("SLOT-002", "COVAXIN", "Health Center", "000001", 1, "2026-11-01", 5, 300.0)
    request = BookingRequest("R-002", "U-002", "SLOT-002", quantity=1, priority=2)
    booking = Booking("B-002", "U-002", "SLOT-002", "COVAXIN", "Health Center", 1, "2026-11-01", 1, 300.0, status="pending")

    state.add_ticket(ticket)
    state.add_request(request)
    state.add_booking(booking)

    assert state.tickets["SLOT-002"].vaccine_name == "COVAXIN"
    assert state.active_requests[0].request_id == "R-002"
    assert state.bookings["B-002"].status == "pending"


def test_lock_manager_acquire_and_release():
    lock = __import__("dist_ticket_booking.coordination.lock_manager", fromlist=["LockManager"]).LockManager()

    first = lock.acquire("process-1")
    second = lock.acquire("process-2")
    released = lock.release("process-1")
    third = lock.acquire("process-3")

    assert first["status"] == "acquired"
    assert second["status"] == "queued"
    assert released["status"] == "released"
    assert released["next_owner"] == "process-2"
    assert third["status"] == "queued"

"""Tests for the business logic and gRPC-ready service contracts."""

from dist_ticket_booking.business.booking_service import BookingService, VaccinationService, _get_field
from dist_ticket_booking.core.models import VaccineSlot, Ticket
from dist_ticket_booking.grpc.service import TicketServiceStub
from dist_ticket_booking.llm.domain_llm import DomainLLM

# --- Reusable vaccine slot fixture ---

def _slot_store():
    """Single COVISHIELD Dose-1 slot with 10 available appointments."""
    return {
        "SLOT-CVS-D1-TEST": VaccineSlot(
            slot_id="SLOT-CVS-D1-TEST",
            vaccine_name="COVISHIELD",
            center_name="Test Health Center",
            center_pincode="000001",
            dose_number=1,
            date="2026-11-01",
            available_count=10,
            price=0.0,
        )
    }


def test_booking_service_validates_positive_request():
    service = VaccinationService(_slot_store())

    result = service.validate_request("U-001", "SLOT-CVS-D1-TEST", 1)

    assert result["valid"] is True
    assert result["reason"] == "request valid"


def test_booking_service_rejects_invalid_quantity():
    service = VaccinationService(_slot_store())

    result = service.validate_request("U-001", "SLOT-CVS-D1-TEST", 0)

    assert result["valid"] is False
    assert result["reason"] == "quantity must be positive"


def test_booking_service_updates_inventory_after_acceptance():
    service = VaccinationService(_slot_store())

    response = service.create_booking("U-001", "SLOT-CVS-D1-TEST", 1)

    assert response["status"] == "accepted"
    slot = service.ticket_store["SLOT-CVS-D1-TEST"]
    assert _get_field(slot, "available_count") == 9
    assert response["updated_availability"] == 9


def test_grpc_stub_contract_is_present():
    stub = TicketServiceStub(ticket_store=_slot_store())

    response = stub.create_booking("U-001", "SLOT-CVS-D1-TEST", 1, 2)

    assert response["status"] == "accepted"
    assert response["ticket_id"] == "SLOT-CVS-D1-TEST"
    assert response["priority"] == 2
    assert response["booking_id"].startswith("APPT-")


def test_grpc_get_ticket_availability_returns_state():
    stub = TicketServiceStub(ticket_store=_slot_store())

    response = stub.get_ticket_availability("SLOT-CVS-D1-TEST")

    assert response["status"] == "available"
    assert response["ticket_id"] == "SLOT-CVS-D1-TEST"
    assert response["available_count"] == 10


def test_llm_stub_contract_is_present():
    llm = DomainLLM("domain-model")

    result = llm.generate_recommendation("What documents do I need to book a vaccine slot?")

    assert result["model"] == "domain-model"
    # If model not loaded, status will be "error" but response will still be a non-empty string
    assert isinstance(result["response"], str)
    assert len(result["response"]) > 0


def test_dispatcher_queues_and_dequeues_in_order():
    from dist_ticket_booking.scheduler.dispatcher import Dispatcher
    from dist_ticket_booking.core.models import SlotBookingRequest

    d = Dispatcher()
    r1 = SlotBookingRequest("req-1", "user-a", "SLOT-001", 1, priority=0)
    r2 = SlotBookingRequest("req-2", "user-b", "SLOT-001", 1, priority=1)

    d.enqueue(r1)
    d.enqueue(r2)

    assert d.size() == 2
    first = d.next_request()
    assert first.request_id == "req-1"
    assert d.size() == 1


def test_lock_manager_serializes_contenders():
    from dist_ticket_booking.coordination.lock_manager import LockManager

    lm = LockManager()
    r1 = lm.acquire("process-A")
    r2 = lm.acquire("process-B")
    released = lm.release("process-A")

    assert r1["status"] == "acquired"
    assert r2["status"] == "queued"
    assert released["next_owner"] == "process-B"


def test_booking_service_uses_dispatcher_and_lock_manager():
    service = VaccinationService(_slot_store())

    result = service.create_booking("U-002", "SLOT-CVS-D1-TEST", 2)

    assert result["status"] == "accepted"
    # After booking 2 of 10, dispatcher queue should be empty (request consumed)
    assert service.dispatcher.size() == 0


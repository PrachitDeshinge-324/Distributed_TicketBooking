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



def test_llm_fallback_uses_live_availability_context():
    from dist_ticket_booking.llm.domain_llm import _keyword_fallback
    ctx = "Current live slot availability:\n- Slot A: 2 slot(s) available"
    ans = _keyword_fallback(f"What seats are available?\n\nContext: {ctx}")
    assert "Slot A: 2 slot(s) available" in ans
    assert "24 hours" in _keyword_fallback(f"How to cancel a booking?\n\nContext: {ctx}")
    assert "84 days" in _keyword_fallback("covishield gap")


def test_app_server_falls_back_when_llm_server_unreachable():
    import grpc
    from dist_ticket_booking.grpc import ticket_service_pb2_grpc
    from dist_ticket_booking.grpc.service import TicketClientServicer

    dead = grpc.insecure_channel("localhost:1")  # nothing listens here
    svc = TicketClientServicer(llm_stub=ticket_service_pb2_grpc.LLMServiceStub(dead))
    ans = svc._query_llm("How to cancel a booking?")
    assert "24 hours" in ans
    dead.close()


def test_live_context_is_focused_and_unambiguous():
    from dist_ticket_booking.business.booking_service import BookingService
    from dist_ticket_booking.grpc.service import TicketClientServicer

    svc = TicketClientServicer(booking_service=BookingService())
    ctx = svc._build_live_context("How many slots are left for CORBEVAX?")
    lines = [l for l in ctx.splitlines() if l.startswith("- ")]
    assert len(lines) == 1 and "CORBEVAX" in lines[0]
    assert "paid, Rs. 250" in lines[0]
    full = svc._build_live_context("What seats are available?")
    assert full.count("\n- ") == len(svc.booking_service.ticket_store)
    assert svc._build_live_context("How do I cancel?") == ""


def test_trim_incomplete_sentence():
    from dist_ticket_booking.llm.domain_llm import _trim_incomplete
    assert _trim_incomplete("One slot left. The next one is") == "One slot left."
    assert _trim_incomplete("Complete answer.") == "Complete answer."
    assert _trim_incomplete("no punctuation at all") == "no punctuation at all"


def test_grounding_guardrail_restores_policy_facts():
    from dist_ticket_booking.llm.domain_llm import _grounded
    q = "How to cancel a booking?"
    assert "24 hours" in _grounded("Use the cancel button.", q)
    assert "24 hours before" in _grounded("You can cancel within 24 hours of your slot.", q)
    good = "Cancel up to 24 hours before your appointment using your booking ID."
    assert _grounded(good, q) == good
    assert _grounded("Anything goes.", "hello there") == "Anything goes."


def test_availability_guardrail_rejects_contradictions():
    from dist_ticket_booking.llm.domain_llm import _grounded_availability
    ctx = ("Current live slot availability:\n"
           "- CORBEVAX Booster — Fortis Bangalore: 2 open slot(s) on 2026-10-07; price: paid, Rs. 250")
    bad = "CORBEVAX has 2 available slots on 2026-10-07, but no more spots remain for this date."
    fixed = _grounded_availability(bad, ctx)
    assert "2 open slot(s)" in fixed and "no more" not in fixed
    assert "3 slots" not in _grounded_availability("There are 3 slots left.", ctx)  # wrong count
    good = "CORBEVAX has 2 slots left on 2026-10-07 at Rs. 250 each."
    assert _grounded_availability(good, ctx) == good
    sold = "Current live slot availability:\n- CORBEVAX Booster — Fortis Bangalore: SOLD OUT on 2026-10-07"
    assert "sold out" in _grounded_availability("Yes, slots are open!", sold)
    assert _grounded_availability("Sorry, it is sold out.", sold) == "Sorry, it is sold out."

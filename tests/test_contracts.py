"""Unit tests for the booking rules and the mock payment gateway.

The time rules are evaluated against an explicit `now` (in the cluster this
is the leader's timestamp carried in the command), so these tests are
deterministic and do not depend on today's date.
"""
from dist_ticket_booking.business.booking_service import (
    CANCELLATION_CUTOFF_S, VaccinationService, appointment_start,
)
from dist_ticket_booking.business.payment import MockPaymentGateway
from dist_ticket_booking.core.models import VaccineSlot

PAID = "SLOT-CBX-BOS-560001"   # CORBEVAX, Rs. 250
FREE = "SLOT-CVS-D1-110001"    # COVISHIELD, free
HOUR = 3600


def _start(service, slot_id):
    return appointment_start(service.ticket_store[slot_id])


def test_slot_model_contract():
    slot = VaccineSlot("SLOT-001", "COVISHIELD", "Health Center", "000001", 1, "2027-03-01",
                       available_count=10, price=250.0)
    assert slot.status == "available" and slot.time == "09:00"
    assert appointment_start(slot) is not None


def test_appointment_time_is_indian_standard_time():
    slot = {"date": "2027-01-11", "time": "09:00"}
    # 09:00 IST is 03:30 UTC
    assert appointment_start(slot) == 1799638200.0


def test_one_appointment_per_citizen_per_slot():
    s = VaccinationService()
    assert s.create_booking("asha", FREE)["status"] == "accepted"
    second = s.create_booking("asha", FREE)
    assert second["status"] == "rejected" and "already hold" in second["message"]
    assert s.create_booking("ravi", FREE)["status"] == "accepted"  # other citizens are fine


def test_a_booking_is_for_one_person():
    s = VaccinationService()
    res = s.create_booking("asha", FREE, quantity=2)
    assert res["status"] == "rejected" and "quantity must be 1" in res["message"]


def test_rebooking_after_cancel_is_allowed():
    s = VaccinationService()
    bid = s.create_booking("asha", FREE)["booking_id"]
    assert s.cancel_booking("asha", bid)["status"] == "success"
    assert s.create_booking("asha", FREE)["status"] == "accepted"


def test_cannot_book_a_slot_that_has_started():
    s = VaccinationService()
    start = _start(s, FREE)
    assert s.create_booking("asha", FREE, now=start + 1)["status"] == "rejected"
    assert s.create_booking("asha", FREE, now=start - HOUR)["status"] == "accepted"


def test_cancellation_closes_24_hours_before_the_appointment():
    s = VaccinationService()
    start = _start(s, FREE)
    early = s.create_booking("asha", FREE, now=start - 10 * 24 * HOUR)["booking_id"]
    late = s.create_booking("ravi", FREE, now=start - 10 * 24 * HOUR)["booking_id"]
    before = s.get_availability(FREE)["available_count"]

    ok = s.cancel_booking("asha", early, now=start - CANCELLATION_CUTOFF_S - 60)
    assert ok["status"] == "success"
    too_late = s.cancel_booking("ravi", late, now=start - CANCELLATION_CUTOFF_S + 60)
    assert too_late["status"] == "rejected" and "24 hours" in too_late["message"]
    assert s.bookings[late]["status"] == "confirmed"
    assert s.get_availability(FREE)["available_count"] == before + 1  # only the first seat returned


def test_only_the_owner_or_an_admin_may_cancel():
    s = VaccinationService()
    bid = s.create_booking("asha", FREE)["booking_id"]
    assert s.cancel_booking("ravi", bid)["status"] == "unauthorized"
    assert s.cancel_booking("helpdesk", bid, is_admin=True)["status"] == "success"


def test_paid_slot_is_charged_and_refunded_on_cancel():
    s = VaccinationService()
    res = s.create_booking("asha", PAID, payment_method="card")
    assert res["status"] == "accepted" and "250" in res["message"]
    booking = s.bookings[res["booking_id"]]
    payment = s.payments.ledger[booking["payment_id"]]
    assert payment == {"payment_id": booking["payment_id"], "booking_id": res["booking_id"],
                       "user_id": "asha", "amount": 250.0, "method": "card", "status": "captured"}

    out = s.cancel_booking("asha", res["booking_id"])
    assert out["status"] == "success" and "250 refunded" in out["message"]
    assert s.payments.ledger[booking["payment_id"]]["status"] == "refunded"


def test_declined_payment_rejects_booking_and_keeps_the_seat():
    s = VaccinationService()
    before = s.get_availability(PAID)["available_count"]
    res = s.create_booking("asha", PAID, payment_method="card_declined")
    assert res["status"] == "rejected" and "payment declined" in res["message"]
    assert s.get_availability(PAID)["available_count"] == before
    assert not s.bookings and not s.payments.ledger


def test_free_slot_needs_no_payment():
    s = VaccinationService()
    res = s.create_booking("asha", FREE, payment_method="card_declined")
    assert res["status"] == "accepted"  # nothing to charge, so the method is irrelevant
    assert s.bookings[res["booking_id"]]["payment_id"] is None


def test_gateway_refund_is_idempotent_and_rejects_unknown_methods():
    g = MockPaymentGateway()
    assert g.charge("B1", "u", 100, "bitcoin")["status"] == "declined"
    pid = g.charge("B1", "u", 100, "upi")["payment_id"]
    assert g.refund(pid)["status"] == "refunded"
    assert g.refund(pid)["status"] == "already_refunded"
    assert g.refund("PAY-unknown")["status"] == "error"

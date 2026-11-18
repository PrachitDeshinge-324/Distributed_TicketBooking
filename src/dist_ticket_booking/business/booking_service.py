"""Core business logic for vaccine slot booking and appointment management.

Booking rules enforced here (the same rules the FAQ assistant quotes):

1. One appointment per citizen per slot: a booking is for exactly one
   person (quantity 1), and a citizen cannot hold two active appointments
   in the same slot.
2. A slot cannot be booked once its appointment time has passed.
3. An appointment can be cancelled only up to 24 hours before it starts.
   Any fee paid is refunded in full on cancellation.
4. Paid slots must be paid for; a declined payment rejects the booking and
   leaves the inventory untouched.

In the Raft cluster these methods run inside the replicated state machine,
so they must be deterministic: the current time is never read here. The
leader stamps each command with ``requested_at`` (Unix seconds) before it
is replicated, and every replica evaluates the time rules against that
same value. ``now=None`` skips the time rules (internal/unit-test use).
"""
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from dist_ticket_booking.business.payment import DEFAULT_METHOD, MockPaymentGateway

# Appointment times are local Indian time.
IST = timezone(timedelta(hours=5, minutes=30))
CANCELLATION_CUTOFF_S = 24 * 3600
DEFAULT_START_TIME = "09:00"
MAX_PER_BOOKING = 1


def _get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _set_field(obj: Any, name: str, val: Any) -> None:
    if isinstance(obj, dict):
        obj[name] = val
    else:
        setattr(obj, name, val)


def appointment_start(slot: Any) -> Optional[float]:
    """Unix time at which a slot's appointments start (None if undated)."""
    date = _get_field(slot, "date", "")
    if not date:
        return None
    hhmm = _get_field(slot, "time", None) or DEFAULT_START_TIME
    try:
        start = datetime.strptime(f"{date} {hhmm}", "%Y-%m-%d %H:%M").replace(tzinfo=IST)
    except ValueError:
        return None
    return start.timestamp()


def _slot(name, vaccine, center, pincode, dose, date, count, price=0.0, time=DEFAULT_START_TIME):
    return {
        "name": name, "vaccine_name": vaccine, "center_name": center, "center_pincode": pincode,
        "dose_number": dose, "date": date, "time": time, "available_count": count,
        "price": price, "status": "available",
    }


DEFAULT_SLOT_STORE: Dict[str, Dict[str, Any]] = {
    "SLOT-CVS-D1-110001": _slot("COVISHIELD Dose 1 — AIIMS Delhi", "COVISHIELD", "AIIMS Delhi",
                                "110001", 1, "2027-01-11", 50),
    "SLOT-CVS-D2-110001": _slot("COVISHIELD Dose 2 — AIIMS Delhi", "COVISHIELD", "AIIMS Delhi",
                                "110001", 2, "2027-01-11", 40, time="14:00"),
    "SLOT-CVX-D1-400001": _slot("COVAXIN Dose 1 — KEM Hospital Mumbai", "COVAXIN", "KEM Hospital Mumbai",
                                "400001", 1, "2027-01-12", 30),
    "SLOT-CVX-D2-400001": _slot("COVAXIN Dose 2 — KEM Hospital Mumbai", "COVAXIN", "KEM Hospital Mumbai",
                                "400001", 2, "2027-01-12", 25, time="14:00"),
    "SLOT-CBX-BOS-560001": _slot("CORBEVAX Booster — Fortis Bangalore", "CORBEVAX",
                                 "Fortis Hospital Bangalore", "560001", 3, "2027-01-13", 3, price=250.0),
    "SLOT-CVS-D1-500001": _slot("COVISHIELD Dose 1 — NIMS Hyderabad", "COVISHIELD", "NIMS Hyderabad",
                                "500001", 1, "2027-01-14", 60),
}


class VaccinationService:
    """Manages vaccine slot inventory, appointments and payments.

    Concurrency control: every mutation of a slot runs under that slot's
    lock, so the check-then-decrement of `available_count` is atomic and
    two concurrent bookings can never both take the last seat. Bookings on
    different slots do not block each other. In the Raft cluster the state
    machine additionally applies committed entries one at a time, in log
    order, on every replica.
    """

    def __init__(self, ticket_store=None):
        if ticket_store is not None:
            self.ticket_store = ticket_store
        else:
            self.ticket_store = {k: dict(v) for k, v in DEFAULT_SLOT_STORE.items()}
        self._slot_locks: Dict[str, threading.Lock] = {sid: threading.Lock() for sid in self.ticket_store}
        self._store_meta_lock = threading.Lock()  # guards the _slot_locks dict itself
        self.bookings: Dict[str, Dict[str, Any]] = {}
        self.payments = MockPaymentGateway()

    def _get_slot_lock(self, slot_id: str) -> threading.Lock:
        """Return (creating if necessary) the lock for a slot."""
        with self._store_meta_lock:
            if slot_id not in self._slot_locks:
                self._slot_locks[slot_id] = threading.Lock()
            return self._slot_locks[slot_id]

    @staticmethod
    def _describe(sid: str, slot: Any) -> Dict[str, Any]:
        avail = _get_field(slot, "available_count", 0)
        return {
            "ticket_id": sid,
            "slot_id": sid,
            "status": "available" if avail > 0 else "sold_out",
            "available_count": avail,
            "name": _get_field(slot, "name", "Unknown"),
            "vaccine_name": _get_field(slot, "vaccine_name", ""),
            "center_name": _get_field(slot, "center_name", ""),
            "center_pincode": _get_field(slot, "center_pincode", ""),
            "dose_number": _get_field(slot, "dose_number", 0),
            "date": _get_field(slot, "date", ""),
            "time": _get_field(slot, "time", None) or DEFAULT_START_TIME,
            "price": _get_field(slot, "price", 0.0),
        }

    def get_availability(self, ticket_id: Optional[str] = None) -> Dict[str, Any]:
        """Current slot availability (one slot, or all of them)."""
        if ticket_id:
            slot = self.ticket_store.get(ticket_id)
            if not slot:
                return {"status": "not_found", "available_count": 0}
            return self._describe(ticket_id, slot)
        return {"status": "success",
                "tickets": {sid: self._describe(sid, slot) for sid, slot in self.ticket_store.items()}}

    def active_booking(self, user_id: str, ticket_id: str) -> Optional[str]:
        """Id of the user's confirmed appointment in this slot, if any."""
        for bid, b in self.bookings.items():
            if b["user_id"] == user_id and b["ticket_id"] == ticket_id and b["status"] == "confirmed":
                return bid
        return None

    def validate_request(self, user_id: str, ticket_id: str, quantity: int, now: Optional[float] = None):
        if not user_id or not ticket_id:
            return {"valid": False, "reason": "missing user or slot identifier"}
        if quantity <= 0:
            return {"valid": False, "reason": "quantity must be positive"}
        if quantity > MAX_PER_BOOKING:
            return {"valid": False,
                    "reason": "each booking is for one citizen only (quantity must be 1)"}
        slot = self.ticket_store.get(ticket_id)
        if slot is None:
            return {"valid": False, "reason": "vaccine slot not found"}
        start = appointment_start(slot)
        if now is not None and start is not None and now >= start:
            return {"valid": False, "reason": "this slot's appointment time has already passed"}
        existing = self.active_booking(user_id, ticket_id)
        if existing:
            return {"valid": False,
                    "reason": f"you already hold appointment {existing} in this slot "
                              f"(one appointment per citizen per slot)"}
        if quantity > _get_field(slot, "available_count", 0):
            return {"valid": False, "reason": "not enough slot availability"}
        return {"valid": True, "reason": "request valid"}

    def create_booking(self, user_id: str, ticket_id: str, quantity: int = 1, priority: int = 0,
                       booking_id: Optional[str] = None, now: Optional[float] = None,
                       payment_method: str = DEFAULT_METHOD):
        """Book one appointment in a slot: validate, take payment, decrement inventory."""
        def rejected(reason):
            return {"status": "rejected", "message": reason, "ticket_id": ticket_id, "priority": priority}

        with self._get_slot_lock(ticket_id):
            validation = self.validate_request(user_id, ticket_id, quantity, now)
            if not validation["valid"]:
                return rejected(validation["reason"])

            slot = self.ticket_store[ticket_id]
            if booking_id is None:  # standalone mode; the cluster passes a log-derived id
                booking_id = f"APPT-{ticket_id[:12]}-{user_id[:6]}-{uuid.uuid4().hex[:4]}"
            total_price = _get_field(slot, "price", 0.0) * quantity
            payment = self.payments.charge(booking_id, user_id, total_price, payment_method)
            if payment["status"] == "declined":
                return rejected(f"payment declined: {payment['reason']}")

            new_count = _get_field(slot, "available_count", 0) - quantity
            _set_field(slot, "available_count", new_count)
            if new_count == 0:
                _set_field(slot, "status", "sold_out")

            vaccine_name = _get_field(slot, "vaccine_name", "")
            center_name = _get_field(slot, "center_name", "")
            dose_number = _get_field(slot, "dose_number", 0)
            scheduled_date = _get_field(slot, "date", "")
            scheduled_time = _get_field(slot, "time", None) or DEFAULT_START_TIME
            self.bookings[booking_id] = {
                "booking_id": booking_id,
                "user_id": user_id,
                "ticket_id": ticket_id,
                "slot_id": ticket_id,
                "vaccine_name": vaccine_name,
                "center_name": center_name,
                "dose_number": dose_number,
                "scheduled_date": scheduled_date,
                "scheduled_time": scheduled_time,
                "quantity": quantity,
                "total_price": total_price,
                "payment_id": payment.get("payment_id"),
                "status": "confirmed",
            }

        payment_msg = (f"Payment of ₹{total_price:.0f} received ({payment['payment_id']}). "
                       if payment["status"] == "captured" else "Free slot — no payment required. ")
        return {
            "status": "accepted",
            "booking_id": booking_id,
            "ticket_id": ticket_id,
            "priority": priority,
            "message": (f"{payment_msg}Appointment confirmed at {center_name} for {vaccine_name} "
                        f"Dose {dose_number} on {scheduled_date} at {scheduled_time}."),
            "updated_availability": new_count,
        }

    def cancel_booking(self, user_id: str, booking_id: str, now: Optional[float] = None,
                       is_admin: bool = False) -> Dict[str, Any]:
        """Cancel an appointment, release its seat and refund any payment.

        Only the owner (or an administrator) may cancel, and only up to 24
        hours before the appointment starts.
        """
        booking = self.bookings.get(booking_id)
        if not booking:
            return {"status": "error", "message": "appointment not found"}
        if booking["user_id"] != user_id and not is_admin:
            return {"status": "unauthorized", "message": "appointment belongs to another user"}
        if booking["status"] == "cancelled":
            return {"status": "error", "message": "appointment already cancelled"}

        sid = booking["ticket_id"]
        with self._get_slot_lock(sid):
            slot = self.ticket_store.get(sid)
            start = appointment_start(slot) if slot is not None else None
            if now is not None and start is not None and start - now < CANCELLATION_CUTOFF_S:
                return {"status": "rejected",
                        "message": "cancellation closed: appointments can be cancelled only up to "
                                   "24 hours before they start"}
            if slot is not None:
                _set_field(slot, "available_count", _get_field(slot, "available_count", 0) + booking["quantity"])
                _set_field(slot, "status", "available")
            booking["status"] = "cancelled"
            refund = self.payments.refund(booking.get("payment_id"))

        refund_msg = f" ₹{refund['amount']:.0f} refunded." if refund["status"] == "refunded" else ""
        return {
            "status": "success",
            "booking_id": booking_id,
            "message": (
                f"Cancelled the {booking.get('vaccine_name', '')} Dose {booking.get('dose_number', '')} "
                f"appointment at {booking.get('center_name', '')} on {booking.get('scheduled_date', '')} "
                f"({booking_id}). The slot was released.{refund_msg}"
            ),
        }

    cancel_appointment = cancel_booking


BookingService = VaccinationService

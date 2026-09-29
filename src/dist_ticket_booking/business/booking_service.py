"""Core business logic for vaccine slot booking and appointment management."""
import threading
import uuid
from typing import Dict, Any, Optional

from dist_ticket_booking.coordination.lock_manager import LockManager
from dist_ticket_booking.scheduler.dispatcher import Dispatcher


def _get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _set_field(obj: Any, name: str, val: Any) -> None:
    if isinstance(obj, dict):
        obj[name] = val
    else:
        setattr(obj, name, val)


DEFAULT_SLOT_STORE: Dict[str, Dict[str, Any]] = {
    "SLOT-CVS-D1-110001": {
        "name": "COVISHIELD Dose 1 — AIIMS Delhi",
        "vaccine_name": "COVISHIELD",
        "center_name": "AIIMS Delhi",
        "center_pincode": "110001",
        "dose_number": 1,
        "date": "2026-10-05",
        "available_count": 50,
        "price": 0.0,
        "status": "available",
    },
    "SLOT-CVS-D2-110001": {
        "name": "COVISHIELD Dose 2 — AIIMS Delhi",
        "vaccine_name": "COVISHIELD",
        "center_name": "AIIMS Delhi",
        "center_pincode": "110001",
        "dose_number": 2,
        "date": "2026-10-05",
        "available_count": 40,
        "price": 0.0,
        "status": "available",
    },
    "SLOT-CVX-D1-400001": {
        "name": "COVAXIN Dose 1 — KEM Hospital Mumbai",
        "vaccine_name": "COVAXIN",
        "center_name": "KEM Hospital Mumbai",
        "center_pincode": "400001",
        "dose_number": 1,
        "date": "2026-10-06",
        "available_count": 30,
        "price": 0.0,
        "status": "available",
    },
    "SLOT-CVX-D2-400001": {
        "name": "COVAXIN Dose 2 — KEM Hospital Mumbai",
        "vaccine_name": "COVAXIN",
        "center_name": "KEM Hospital Mumbai",
        "center_pincode": "400001",
        "dose_number": 2,
        "date": "2026-10-06",
        "available_count": 25,
        "price": 0.0,
        "status": "available",
    },
    "SLOT-CBX-BOS-560001": {
        "name": "CORBEVAX Booster — Fortis Bangalore",
        "vaccine_name": "CORBEVAX",
        "center_name": "Fortis Hospital Bangalore",
        "center_pincode": "560001",
        "dose_number": 3,
        "date": "2026-10-07",
        "available_count": 3,
        "price": 250.0,
        "status": "available",
    },
    "SLOT-CVS-D1-500001": {
        "name": "COVISHIELD Dose 1 — NIMS Hyderabad",
        "vaccine_name": "COVISHIELD",
        "center_name": "NIMS Hyderabad",
        "center_pincode": "500001",
        "dose_number": 1,
        "date": "2026-10-08",
        "available_count": 60,
        "price": 0.0,
        "status": "available",
    },
}


class VaccinationService:
    """Manages vaccine slot inventory and appointment booking.

    Uses per-slot locks so concurrent bookings on different slots
    proceed in parallel — only bookings targeting the same slot
    are serialized against each other.

    A LockManager tracks which process owns each slot lock and
    queues contending requests. A Dispatcher holds the FIFO
    priority queue of pending booking requests.
    """

    def __init__(self, ticket_store=None):
        if ticket_store is not None:
            self.ticket_store = ticket_store
        else:
            self.ticket_store = {k: dict(v) for k, v in DEFAULT_SLOT_STORE.items()}

        # Per-slot threading locks — only the targeted slot is blocked,
        # so two users booking different slots run concurrently.
        self._slot_locks: Dict[str, threading.Lock] = {
            sid: threading.Lock() for sid in self.ticket_store
        }
        self._store_meta_lock = threading.Lock()  # guards slot_locks dict itself

        # LockManager tracks logical lock ownership for observability/auditing.
        self.lock_manager = LockManager()

        # Dispatcher holds the ordered queue of pending booking requests.
        self.dispatcher = Dispatcher()

        self.bookings: Dict[str, Dict[str, Any]] = {}

    def _get_slot_lock(self, slot_id: str) -> threading.Lock:
        """Return (creating if necessary) the threading.Lock for a slot."""
        with self._store_meta_lock:
            if slot_id not in self._slot_locks:
                self._slot_locks[slot_id] = threading.Lock()
            return self._slot_locks[slot_id]

    def get_availability(self, ticket_id: Optional[str] = None) -> Dict[str, Any]:
        """Return current slot availability without holding any lock.

        Reads are not protected by a slot lock — Python's GIL and the
        atomic nature of dict reads make this safe for an int counter.
        Only mutations (create_booking / cancel_booking) hold a lock.
        """
        if ticket_id:
            slot = self.ticket_store.get(ticket_id)
            if not slot:
                return {"status": "not_found", "available_count": 0}
            avail = _get_field(slot, "available_count", 0)
            return {
                "ticket_id": ticket_id,
                "slot_id": ticket_id,
                "status": "available" if avail > 0 else "sold_out",
                "available_count": avail,
                "name": _get_field(slot, "name", "Unknown"),
                "vaccine_name": _get_field(slot, "vaccine_name", ""),
                "center_name": _get_field(slot, "center_name", ""),
                "center_pincode": _get_field(slot, "center_pincode", ""),
                "dose_number": _get_field(slot, "dose_number", 0),
                "date": _get_field(slot, "date", ""),
                "price": _get_field(slot, "price", 0.0),
            }

        results = {}
        for sid, slot in self.ticket_store.items():
            avail = _get_field(slot, "available_count", 0)
            results[sid] = {
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
                "price": _get_field(slot, "price", 0.0),
            }
        return {"status": "success", "tickets": results}

    def validate_request(self, user_id: str, ticket_id: str, quantity: int):
        if not user_id or not ticket_id:
            return {"valid": False, "reason": "missing user or slot identifier"}
        if quantity <= 0:
            return {"valid": False, "reason": "quantity must be positive"}
        slot = self.ticket_store.get(ticket_id)
        if slot is None:
            return {"valid": False, "reason": "vaccine slot not found"}
        avail = _get_field(slot, "available_count", 0)
        if quantity > avail:
            return {"valid": False, "reason": "not enough slot availability"}
        return {"valid": True, "reason": "request valid"}

    def _process_payment(self, user_id: str, amount: float) -> bool:
        return amount >= 0.0

    def create_booking(self, user_id: str, ticket_id: str, quantity: int = 1, priority: int = 0):
        """Book vaccine appointment slot(s).

        Acquires only the per-slot lock so bookings on other slots
        are unaffected and can proceed concurrently.
        """
        request_id = f"req-{user_id[:8]}-{ticket_id[:12]}-{uuid.uuid4().hex[:4]}"

        # Register in dispatcher queue (priority-aware FIFO)
        from dist_ticket_booking.core.models import SlotBookingRequest
        booking_req = SlotBookingRequest(
            request_id=request_id,
            user_id=user_id,
            slot_id=ticket_id,
            quantity=quantity,
            priority=priority,
        )
        self.dispatcher.enqueue(booking_req)

        # Acquire the per-slot lock — only contenders for the same slot wait
        slot_lock = self._get_slot_lock(ticket_id)
        lock_result = self.lock_manager.acquire(request_id)

        with slot_lock:
            # Dequeue this request (it's now being processed)
            self.dispatcher.next_request()

            validation = self.validate_request(user_id, ticket_id, quantity)
            if not validation["valid"]:
                self.lock_manager.release(request_id)
                return {
                    "status": "rejected",
                    "message": validation["reason"],
                    "ticket_id": ticket_id,
                    "priority": priority,
                }

            slot = self.ticket_store[ticket_id]
            price = _get_field(slot, "price", 0.0)
            total_price = price * quantity

            if not self._process_payment(user_id, total_price):
                self.lock_manager.release(request_id)
                return {
                    "status": "rejected",
                    "message": "payment failed",
                    "ticket_id": ticket_id,
                    "priority": priority,
                }

            current_count = _get_field(slot, "available_count", 0)
            new_count = current_count - quantity
            _set_field(slot, "available_count", new_count)
            if new_count == 0:
                _set_field(slot, "status", "sold_out")

            booking_id = f"APPT-{ticket_id[:12]}-{user_id[:6]}-{uuid.uuid4().hex[:4]}"
            vaccine_name = _get_field(slot, "vaccine_name", "")
            center_name = _get_field(slot, "center_name", "")
            dose_number = _get_field(slot, "dose_number", 0)
            scheduled_date = _get_field(slot, "date", "")

            booking_record = {
                "booking_id": booking_id,
                "user_id": user_id,
                "ticket_id": ticket_id,
                "slot_id": ticket_id,
                "vaccine_name": vaccine_name,
                "center_name": center_name,
                "dose_number": dose_number,
                "scheduled_date": scheduled_date,
                "quantity": quantity,
                "total_price": total_price,
                "status": "confirmed",
            }
            self.bookings[booking_id] = booking_record
            self.lock_manager.release(request_id)

            payment_msg = (
                f"Payment of \u20b9{total_price:.0f} successful. " if total_price > 0
                else "Free slot — no payment required. "
            )
            return {
                "status": "accepted",
                "booking_id": booking_id,
                "ticket_id": ticket_id,
                "priority": priority,
                "message": (
                    f"{payment_msg}Appointment confirmed at {center_name} "
                    f"for {vaccine_name} Dose {dose_number} on {scheduled_date}."
                ),
                "updated_availability": new_count,
            }

    def cancel_booking(self, user_id: str, booking_id: str) -> Dict[str, Any]:
        """Cancel an appointment and restore slot inventory."""
        booking = self.bookings.get(booking_id)
        if not booking:
            return {"status": "error", "message": "appointment not found"}
        if booking["user_id"] != user_id:
            return {"status": "unauthorized", "message": "appointment belongs to another user"}
        if booking["status"] == "cancelled":
            return {"status": "error", "message": "appointment already cancelled"}

        sid = booking["ticket_id"]
        slot_lock = self._get_slot_lock(sid)
        with slot_lock:
            if sid in self.ticket_store:
                slot = self.ticket_store[sid]
                curr = _get_field(slot, "available_count", 0)
                _set_field(slot, "available_count", curr + booking["quantity"])
                _set_field(slot, "status", "available")
            booking["status"] = "cancelled"

        return {
            "status": "success",
            "booking_id": booking_id,
            "message": f"Appointment {booking_id} cancelled. Slot released.",
        }

    cancel_appointment = cancel_booking


BookingService = VaccinationService

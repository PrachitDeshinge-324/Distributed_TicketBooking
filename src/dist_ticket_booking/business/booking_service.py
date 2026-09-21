"""Vaccination slot booking service — core business logic.

Manages vaccine slot inventory and appointment creation/cancellation.
Used by the gRPC service layer and Raft-coordinated distributed nodes.
"""
import threading
import uuid
from typing import Dict, Any, Optional


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
        "available_count": 20,
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
    """Core service managing vaccine slot inventory and appointment booking."""

    def __init__(self, ticket_store=None):
        if ticket_store is not None:
            self.ticket_store = ticket_store
        else:
            self.ticket_store = {k: dict(v) for k, v in DEFAULT_SLOT_STORE.items()}

        self.lock = threading.Lock()
        self.bookings: Dict[str, Dict[str, Any]] = {}

    def get_availability(self, ticket_id: Optional[str] = None) -> Dict[str, Any]:
        """Returns slot availability for a specific slot or all slots."""
        with self.lock:
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
        """Book vaccine appointment slot(s) for a citizen."""
        with self.lock:
            validation = self.validate_request(user_id, ticket_id, quantity)
            if not validation["valid"]:
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

            payment_msg = (
                f"Payment of ₹{total_price:.0f} successful. " if total_price > 0
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
        """Cancel a vaccine appointment and restore slot inventory."""
        with self.lock:
            booking = self.bookings.get(booking_id)
            if not booking:
                return {"status": "error", "message": "appointment not found"}

            if booking["user_id"] != user_id:
                return {"status": "unauthorized", "message": "appointment belongs to another user"}

            if booking["status"] == "cancelled":
                return {"status": "error", "message": "appointment already cancelled"}

            sid = booking["ticket_id"]
            if sid in self.ticket_store:
                slot = self.ticket_store[sid]
                curr = _get_field(slot, "available_count", 0)
                _set_field(slot, "available_count", curr + booking["quantity"])
                _set_field(slot, "status", "available")

            booking["status"] = "cancelled"
            return {
                "status": "success",
                "booking_id": booking_id,
                "message": f"Appointment {booking_id} cancelled. Slot released and refund (if any) issued.",
            }

    cancel_appointment = cancel_booking

BookingService = VaccinationService

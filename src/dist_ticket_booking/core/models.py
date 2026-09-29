"""Data models for vaccine slots, appointments, and system state."""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class VaccineSlot:
    """Represents a bookable vaccine slot at a vaccination center."""

    slot_id: str
    vaccine_name: str          # e.g. "COVISHIELD", "COVAXIN", "CORBEVAX"
    center_name: str           # e.g. "AIIMS Delhi"
    center_pincode: str        # e.g. "110001"
    dose_number: int           # 1 = Dose 1, 2 = Dose 2, 3 = Booster
    date: str                  # ISO date string e.g. "2026-10-01"
    available_count: int = 0
    price: float = 0.0         # 0 for govt centers, ~250 for private
    status: str = "available"


Ticket = VaccineSlot


@dataclass
class SlotBookingRequest:
    """Represents a citizen's request to book a vaccine appointment slot."""

    request_id: str
    user_id: str               # Citizen's username / Aadhaar-linked ID
    slot_id: str               # Which VaccineSlot to book
    quantity: int = 1          # Usually 1 (one person per appointment)
    priority: int = 0          # Higher = higher priority (e.g. healthcare workers)


BookingRequest = SlotBookingRequest


@dataclass
class Appointment:
    """Represents a confirmed vaccine appointment."""

    booking_id: str            # e.g. "APPT-CVS-D1-abc123"
    user_id: str
    slot_id: str
    vaccine_name: str
    center_name: str
    dose_number: int
    scheduled_date: str
    quantity: int
    total_amount: float
    status: str = "confirmed"  # confirmed | cancelled
    created_by: Optional[str] = None


Booking = Appointment


@dataclass
class SystemState:
    """Shared state container for the distributed simulation."""

    tickets: dict[str, VaccineSlot] = field(default_factory=dict)
    bookings: dict[str, Appointment] = field(default_factory=dict)
    active_requests: list[SlotBookingRequest] = field(default_factory=list)

    def add_ticket(self, slot: VaccineSlot) -> None:
        self.tickets[slot.slot_id] = slot

    def add_request(self, request: SlotBookingRequest) -> None:
        self.active_requests.append(request)

    def add_booking(self, appointment: Appointment) -> None:
        self.bookings[appointment.booking_id] = appointment

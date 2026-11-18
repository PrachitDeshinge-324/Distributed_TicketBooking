"""Data model for vaccine slots.

Slots can be given to the booking service either as plain dicts (the form
used inside the replicated state and snapshots) or as VaccineSlot objects.
Bookings and payments are plain dicts; their fields are documented in
business/booking_service.py and business/payment.py.
"""
from dataclasses import dataclass


@dataclass
class VaccineSlot:
    """A bookable vaccination session at a centre."""

    slot_id: str
    vaccine_name: str          # e.g. "COVISHIELD", "COVAXIN", "CORBEVAX"
    center_name: str           # e.g. "AIIMS Delhi"
    center_pincode: str        # e.g. "110001"
    dose_number: int           # 1 = Dose 1, 2 = Dose 2, 3 = Booster
    date: str                  # ISO date, e.g. "2027-01-11"
    available_count: int = 0
    price: float = 0.0         # 0 for government centres, up to 250 for private ones
    status: str = "available"
    time: str = "09:00"        # session start, Indian Standard Time


Ticket = VaccineSlot

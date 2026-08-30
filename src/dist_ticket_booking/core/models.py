"""Core domain model placeholders.

Owner: Person A (Core Coordination)
Primary responsibility: define the shared state contract used by all modules.

Functions / classes to implement in this file:
- Ticket
- BookingRequest
- Booking
- SystemState

TODO for Milestone 1:
- finalize the shared ticket and booking state fields
- decide how booking lifecycle states are represented
- align process IDs and inventory state with the simulation layer
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Ticket:
    """Represents a ticket or booking inventory item."""

    ticket_id: str
    name: str
    category: str
    price: float
    available_count: int = 0
    status: str = "available"


@dataclass
class BookingRequest:
    """Represents a request from a user or process to book a ticket."""

    request_id: str
    user_id: str
    ticket_id: str
    quantity: int = 1
    priority: int = 0


@dataclass
class Booking:
    """Represents a committed or pending booking record."""

    booking_id: str
    user_id: str
    ticket_id: str
    quantity: int
    total_amount: float
    status: str = "pending"
    created_by: Optional[str] = None


@dataclass
class SystemState:
    """Shared state container for the distributed simulation."""

    tickets: dict[str, Ticket] = field(default_factory=dict)
    bookings: dict[str, Booking] = field(default_factory=dict)
    active_requests: list[BookingRequest] = field(default_factory=list)

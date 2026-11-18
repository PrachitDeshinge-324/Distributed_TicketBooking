"""Mock payment gateway with a replicated ledger (capture and refund).

No money moves: this stands in for a real gateway as the assignment allows.
It is still a proper part of the replicated state:

* Every decision is deterministic (it depends only on the command), so all
  replicas applying the same log entry reach the same outcome.
* Payments and refunds are recorded in a ledger that is part of the Raft
  snapshot and of the replica state digest.
* There is a failure path: unknown payment methods and the test method
  ``card_declined`` are declined, and the booking is then rejected without
  touching the inventory.
"""
from typing import Any, Dict, Optional

SUPPORTED_METHODS = ("upi", "card", "netbanking")
# Deterministic "card is declined" method, for demos and tests of the failure path.
DECLINED_TEST_METHOD = "card_declined"
DEFAULT_METHOD = "upi"


class MockPaymentGateway:
    """Captures payments for paid slots and refunds them on cancellation."""

    def __init__(self, ledger: Optional[Dict[str, Dict[str, Any]]] = None):
        # payment_id -> {payment_id, booking_id, user_id, amount, method, status}
        self.ledger: Dict[str, Dict[str, Any]] = ledger if ledger is not None else {}

    def charge(self, booking_id: str, user_id: str, amount: float, method: str = DEFAULT_METHOD) -> dict:
        """Capture `amount` for a booking.

        Returns {"status": "not_required"} for free slots, {"status":
        "captured", "payment_id": ...} on success, or {"status": "declined",
        "reason": ...} on failure (nothing is recorded then).
        """
        if amount <= 0:
            return {"status": "not_required", "payment_id": None}
        method = (method or DEFAULT_METHOD).lower()
        if method == DECLINED_TEST_METHOD:
            return {"status": "declined", "reason": "card declined by issuer"}
        if method not in SUPPORTED_METHODS:
            return {"status": "declined",
                    "reason": f"unsupported payment method {method!r} (use {', '.join(SUPPORTED_METHODS)})"}
        payment_id = f"PAY-{booking_id}"
        self.ledger[payment_id] = {
            "payment_id": payment_id,
            "booking_id": booking_id,
            "user_id": user_id,
            "amount": float(amount),
            "method": method,
            "status": "captured",
        }
        return {"status": "captured", "payment_id": payment_id}

    def refund(self, payment_id: Optional[str]) -> dict:
        """Refund a captured payment in full. Idempotent."""
        if not payment_id:
            return {"status": "not_required", "amount": 0.0}
        record = self.ledger.get(payment_id)
        if record is None:
            return {"status": "error", "reason": f"unknown payment {payment_id}", "amount": 0.0}
        if record["status"] == "refunded":
            return {"status": "already_refunded", "amount": record["amount"]}
        record["status"] = "refunded"
        return {"status": "refunded", "amount": record["amount"]}

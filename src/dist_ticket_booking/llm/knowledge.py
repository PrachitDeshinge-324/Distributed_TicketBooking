"""Booking-policy knowledge base for the FAQ assistant (lightweight retrieval).

This is what makes a general small model act as a *booking* assistant
without fine-tuning: for every question the most relevant policy passages
are retrieved here and placed in the prompt, the model is told to answer
only from them, and the answer is checked against them afterwards
(`domain_llm._grounded`). The same passages are the answer when the model
is unavailable.

Retrieval is plain keyword scoring: no extra model, no extra dependency, and
it takes microseconds, so it adds nothing to the answer time. Each passage
states a rule the booking service actually enforces (see
business/booking_service.py), so the assistant and the system agree.
"""
import re
from typing import List, NamedTuple, Tuple


class Passage(NamedTuple):
    topic: str
    keywords: Tuple[str, ...]  # single words (matched as whole words) or phrases
    text: str


KNOWLEDGE_BASE: List[Passage] = [
    Passage("cancel", ("cancel", "cancellation", "cancelling", "reschedule", "change my appointment"),
            "Cancellation is allowed only up to 24 hours before your scheduled appointment. "
            "To cancel, use the cancel option with your booking ID; any fee paid is refunded."),
    Passage("refund", ("refund", "refunded", "money back", "get my money"),
            "When you cancel (up to 24 hours before the appointment) any fee you paid is refunded "
            "in full to the original payment method. Free government slots have nothing to refund."),
    Passage("price", ("price", "cost", "fee", "fees", "charge", "charged", "free", "pay", "paid", "payment",
                      "upi", "card", "netbanking", "money"),
            "Government center slots are free of charge. Private center slots cost between Rs. 150 and "
            "Rs. 250, paid at booking by UPI, card or net banking. If the payment is declined the "
            "booking is not made and the seat stays open."),
    Passage("documents", ("document", "documents", "id", "aadhaar", "passport", "voter", "license",
                          "licence", "carry", "bring", "proof"),
            "You need any valid government-issued photo ID: Aadhaar card, Voter ID, Passport, or "
            "Driving License."),
    Passage("covishield", ("covishield",),
            "The minimum gap between COVISHIELD Dose 1 and Dose 2 is 84 days (12 weeks)."),
    Passage("covaxin", ("covaxin",),
            "The minimum gap between COVAXIN Dose 1 and Dose 2 is 28 days (4 weeks)."),
    Passage("booster", ("booster", "precaution", "third dose", "dose 3"),
            "The booster (precaution) dose is allowed only once at least 9 months have passed since "
            "completing Dose 2."),
    Passage("age", ("age", "old", "years", "child", "children", "kid", "kids", "teen", "teenager", "eligible",
                    "eligibility"),
            "COVISHIELD and COVAXIN are for ages 18+. CORBEVAX is for citizens aged 15 to 17 years."),
    Passage("limit", ("twice", "two slots", "more than one", "multiple", "family", "quantity", "same slot",
                      "another slot", "how many bookings", "two people", "for my"),
            "Each citizen can hold at most one active appointment per vaccination session (slot), and "
            "each booking is for one person; family members book with their own accounts."),
    Passage("how_to_book", ("how to book", "how do i book", "how can i book", "booking process", "register",
                            "sign up", "account", "login", "log in", "slot id"),
            "To book: register or log in, check availability, then book a slot by its slot ID, or simply "
            "tell the assistant what you need, for example 'book a COVAXIN second dose in Mumbai'."),
    Passage("late", ("late", "missed", "past", "started", "expired", "already over"),
            "A vaccination session cannot be booked once it has started, and cancellation closes 24 "
            "hours before the appointment."),
]

_WORD_RE = re.compile(r"[a-z0-9]+")


def _score(passage: Passage, q: str, words: set) -> int:
    score = 0
    for kw in passage.keywords:
        if " " in kw:
            score += 2 if kw in q else 0  # phrases are more specific than single words
        elif kw in words:
            score += 1
    return score


def retrieve(query: str, k: int = 2) -> List[Passage]:
    """The `k` best-matching passages (highest score first); [] if none match."""
    q = " ".join(_WORD_RE.findall(query.lower()))
    words = set(q.split())
    scored = [(_score(p, q, words), i, p) for i, p in enumerate(KNOWLEDGE_BASE)]
    scored = [s for s in scored if s[0] > 0]
    scored.sort(key=lambda s: (-s[0], s[1]))
    return [p for _, _, p in scored[:k]]


def best_passage(query: str) -> str:
    """Text of the single most relevant passage ('' if nothing matches)."""
    found = retrieve(query, k=1)
    return found[0].text if found else ""

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
import difflib
import re
from typing import List, NamedTuple, Tuple


class Passage(NamedTuple):
    topic: str
    keywords: Tuple[str, ...]  # single words (matched as whole words) or phrases
    text: str
    # strict: a rule whose numbers (deadlines, gaps, ages) every answer must keep.
    # Background passages (what a vaccine is, comparisons) only forbid invented numbers.
    strict: bool = True
    # exact: answer with this text as written. Used where the small model was seen
    # mixing up facts between vaccines (comparisons), which no number check catches.
    exact: bool = False


_GAP_WORDS = ("gap", "interval", "between", "wait", "long", "days", "weeks", "second", "doses",
              "dose 2", "second dose", "next dose")
_ABOUT_WORDS = ("about", "made", "maker", "manufacturer", "company", "type", "kind", "explain",
                "information", "info", "details", "describe")


KNOWLEDGE_BASE: List[Passage] = [
    # First so it wins ties ("is covaxin safe"). Medical "why / is it safe / side effects" questions: the assistant must not
    # explain medicine, and a small model will happily invent an explanation.
    Passage("medical", ("why booster", "why vaccine", "why dose", "why second dose", "why is", "why do i", "why should",
                        "why take", "why get", "booster needed", "need a booster", "necessary", "side effect", "side effects", "safe",
                        "safety", "immune", "immunity", "efficacy", "effective", "protection", "protect",
                        "allergy", "allergic", "pregnant", "pregnancy", "fever", "symptoms", "risk", "risks",
                        "health", "medical", "disease", "infection", "variant", "strain", "work", "works"),
            "I can't give medical advice: why a dose is needed, how well a vaccine protects, side effects "
            "and whether it is safe for you are questions for a doctor or health worker.",
            strict=False, exact=True),
    Passage("help", ("help", "assist", "assistant", "what can you do", "how can you help", "what do you do",
                     "capabilities", "features", "who are you", "hello", "hi", "hey"),
            "I can answer questions about vaccines (COVISHIELD, COVAXIN, CORBEVAX), dose gaps, age rules, "
            "prices, documents, cancellation and refunds, and check live slot availability. I can also book "
            "or cancel for you: for example 'book a COVAXIN second dose in Mumbai' or 'cancel booking <ID>'.",
            strict=False),
    Passage("cancel", ("cancel", "cancellation", "cancelling", "reschedule", "change my appointment"),
            "You can cancel any time until 24 hours before your appointment starts; in the last 24 hours "
            "before it, cancelling is no longer possible. "
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
    # What each vaccine is. Listed before the dose-gap rules so a bare
    # "what is covaxin" gets the overview; gap questions score higher on the rule.
    Passage("about_covishield", ("covishield", "what is covishield") + _ABOUT_WORDS,
            "COVISHIELD is the Oxford-AstraZeneca COVID-19 vaccine, made in India by the Serum "
            "Institute of India. It is a viral-vector vaccine given as Dose 1 and Dose 2 at least "
            "84 days apart, for ages 18+.", strict=False),
    Passage("about_covaxin", ("covaxin", "what is covaxin") + _ABOUT_WORDS,
            "COVAXIN is an inactivated (killed-virus) COVID-19 vaccine developed in India by Bharat "
            "Biotech with ICMR. It is given as Dose 1 and Dose 2 at least 28 days apart, for ages 18+.",
            strict=False),
    Passage("about_corbevax", ("corbevax", "what is corbevax") + _ABOUT_WORDS,
            "CORBEVAX is a protein-subunit COVID-19 vaccine made in India by Biological E. In this "
            "system CORBEVAX slots are for citizens aged 15 to 17 years.", strict=False),
    Passage("compare", ("difference", "differences", "different", "differ", "compare", "comparison",
                        "vs", "versus", "better", "best", "which vaccine", "which one", "choose",
                        "covishield", "covaxin", "corbevax"),
            "COVISHIELD (Serum Institute of India, viral-vector vaccine) needs at least 84 days between "
            "Dose 1 and Dose 2; COVAXIN (Bharat Biotech, inactivated-virus vaccine) needs at least 28 "
            "days. Both are for ages 18+. CORBEVAX (Biological E, "
            "protein-subunit vaccine) is for ages 15 to 17. Which vaccine suits you medically is a "
            "question for your doctor.", strict=False, exact=True),
    Passage("covishield", ("covishield",) + _GAP_WORDS,
            "The minimum gap between COVISHIELD Dose 1 and Dose 2 is 84 days (12 weeks)."),
    Passage("covaxin", ("covaxin",) + _GAP_WORDS,
            "The minimum gap between COVAXIN Dose 1 and Dose 2 is 28 days (4 weeks)."),
    Passage("booster", ("booster", "precaution", "third dose", "dose 3"),
            "The booster (precaution) dose is allowed only once at least 9 months have passed since "
            "completing Dose 2."),
    Passage("age", ("age", "old", "years", "child", "children", "kid", "kids", "teen", "teenager", "eligible",
                    "eligibility"),
            "COVISHIELD and COVAXIN are for ages 18+. CORBEVAX is for citizens aged 15 to 17 years."),
    Passage("limit", ("twice", "two slots", "more than one", "multiple", "family", "quantity", "same slot",
                      "same dose", "another centre", "another center", "different centre", "different center",
                      "another slot", "how many bookings", "two people", "for my"),
            "Each citizen can hold at most one active appointment per dose (Dose 1, Dose 2 or booster), at "
            "any centre; cancel it first to book that dose elsewhere. Each booking is for one person; "
            "family members book with their own accounts."),
    Passage("how_to_book", ("how to book", "how do i book", "how can i book", "booking process", "register",
                            "sign up", "account", "login", "log in", "slot id"),
            "To book: register or log in, check availability, then book a slot by its slot ID, or simply "
            "tell the assistant what you need, for example 'book a COVAXIN second dose in Mumbai'."),
    Passage("rejected", ("rejected", "rejection", "refused", "failed", "fail", "declined", "not booked",
                         "could not book", "couldn't book", "can't book", "can't i book", "cannot book", "unable", "error"),
            "A booking is rejected when you already hold an appointment for that dose, the slot is sold out "
            "or has already started, or the payment was declined. The rejection message says which one; "
            "'my bookings' shows the appointments you hold."),
    Passage("late", ("late", "missed", "past", "started", "expired", "already over"),
            "A vaccination session cannot be booked once it has started, and cancellation closes 24 "
            "hours before the appointment."),
]

_WORD_RE = re.compile(r"[a-z0-9]+")


def ranked(query: str) -> List[Tuple[int, Passage]]:
    """Every matching passage with its score, best first."""
    q = " ".join(_correct(w) for w in _WORD_RE.findall(query.lower()))
    words = set(q.split())
    scored = [(_score(p, q, words), i, p) for i, p in enumerate(KNOWLEDGE_BASE)]
    scored = [s for s in scored if s[0] > 0]
    scored.sort(key=lambda s: (-s[0], s[1]))
    return [(sc, p) for sc, _, p in scored]


def _score(passage: Passage, q: str, words: set) -> int:
    score = 0
    for kw in passage.keywords:
        kw = " ".join(_WORD_RE.findall(kw))  # tokenised like the query ("can't" -> "can t")
        if " " in kw:
            score += 2 if kw in q else 0  # phrases are more specific than single words
        elif kw in words:
            score += 1
    return score


# Single-word keywords long enough to correct a misspelling of ("covishiel").
_VOCAB = sorted({kw for p in KNOWLEDGE_BASE for kw in p.keywords if " " not in kw and len(kw) >= 5})


def _correct(word: str) -> str:
    if len(word) < 5 or word in _VOCAB:
        return word
    close = difflib.get_close_matches(word, _VOCAB, n=1, cutoff=0.85)
    return close[0] if close else word


def retrieve(query: str, k: int = 2) -> List[Passage]:
    """The `k` best-matching passages (highest score first); [] if none match.

    A runner-up that matched only one generic word ("about", "free") would just
    distract the model, so it is kept only with a score of 2 or more.
    """
    found = ranked(query)[:k]
    return [p for i, (sc, p) in enumerate(found) if i == 0 or sc >= 2]


def numbers(text: str) -> List[str]:
    return re.findall(r"\d+", text)


def best_passage(query: str) -> str:
    """Text of the single most relevant passage ('' if nothing matches)."""
    found = retrieve(query, k=1)
    return found[0].text if found else ""

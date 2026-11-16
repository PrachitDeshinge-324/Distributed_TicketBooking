"""Natural-language booking and cancellation: intent extraction and resolution.

Pipeline for a sentence such as "book me a second covaxin dose in Mumbai":

  1. The LLM server turns the sentence into a JSON intent
     {"action": "book", "vaccine": "COVAXIN", "city": "Mumbai", "dose": 2, "booking_id": null}
     (DomainLLM.extract_intent). A deterministic rule-based extractor runs as
     well; it fills fields the model missed and replaces the model entirely
     when the LLM server is down.
  2. The app server resolves the intent against live, Raft-replicated state:
     the open slots, or the user's own confirmed bookings (`resolve`).
  3. Exactly one match -> the normal replicated create_booking /
     cancel_booking command; several -> the assistant asks which one; none ->
     it explains why. The model never invents a slot or booking id: anything
     it returns is checked against real data before it is used.
"""
import json
import re
from typing import Dict, Iterable, List, Optional

ACTIONS = ("book", "cancel", "question")
VACCINES = ("COVISHIELD", "COVAXIN", "CORBEVAX")

_CITY_ALIASES = {
    "bengaluru": "bangalore", "bombay": "mumbai", "new delhi": "delhi", "hyd": "hyderabad",
}
_DOSE_WORDS = [
    (3, r"\b(booster|precaution|third|3rd|dose\s*3|dose\s*three)\b"),
    (2, r"\b(second|2nd|dose\s*2|dose\s*two)\b"),
    (1, r"\b(first|1st|dose\s*1|dose\s*one)\b"),
]
_CANCEL_RE = re.compile(r"\b(cancel|cancell?ation|drop|call off|don'?t need|do not need|revoke)\b", re.I)
_BOOK_RE = re.compile(
    r"\b(book|reserve|schedule|get me|need|want|would like|i'?d like|sign me up|appointment for|slot for)\b", re.I)
# Information requests. "Can/could you book ..." is a polite command, not a question.
_QUESTION_RE = re.compile(r"^\s*(how|what|which|when|where|why|is|are|do|does|will|should)\b", re.I)
_DOSE_HINT_RE = re.compile(
    r"\b([123]|one|two|three|first|second|third|1st|2nd|3rd|booster|precaution)\b", re.I)
_BOOKING_ID_RE = re.compile(r"\bAPPT-[A-Za-z0-9-]+\b", re.I)
_JSON_RE = re.compile(r"\{.*\}", re.S)


def slot_city(slot: dict) -> str:
    """City of a slot, taken from its centre name ("KEM Hospital Mumbai" -> "Mumbai")."""
    name = str(slot.get("center_name", "")).strip()
    return name.split()[-1] if name else ""


def known_cities(slots: Iterable[dict]) -> List[str]:
    return sorted({slot_city(s) for s in slots if slot_city(s)})


def _normalize_city(value, cities: List[str]) -> Optional[str]:
    if not value:
        return None
    v = _CITY_ALIASES.get(str(value).strip().lower(), str(value).strip().lower())
    for c in cities:
        if c.lower() == v:
            return c
    return None


def normalize(raw: dict, cities: List[str]) -> Dict[str, object]:
    """Keep only well-formed values; anything else becomes None."""
    action = str(raw.get("action") or "").strip().lower()
    action = {"booking": "book", "reserve": "book", "cancellation": "cancel",
              "faq": "question", "ask": "question"}.get(action, action)
    vaccine = str(raw.get("vaccine") or "").strip().upper()
    try:
        dose = int(raw.get("dose")) if raw.get("dose") not in (None, "", "null") else None
    except (TypeError, ValueError):
        dose = None
    booking_id = raw.get("booking_id")
    booking_id = str(booking_id) if booking_id and _BOOKING_ID_RE.fullmatch(str(booking_id)) else None
    return {
        "action": action if action in ACTIONS else None,
        "vaccine": vaccine if vaccine in VACCINES else None,
        "city": _normalize_city(raw.get("city"), cities),
        "dose": dose if dose in (1, 2, 3) else None,
        "booking_id": booking_id,
    }


def rule_based_intent(text: str, cities: List[str]) -> Dict[str, object]:
    """Deterministic keyword extractor (also the fallback when no LLM is available)."""
    t = text.lower()
    if _QUESTION_RE.search(t) and not _BOOKING_ID_RE.search(text):
        action = "question"  # "how do I cancel?" asks about cancelling; it is not a request
    elif _CANCEL_RE.search(t):
        action = "cancel"
    elif _BOOK_RE.search(t):
        action = "book"
    else:
        action = None
    vaccine = next((v for v in VACCINES if v.lower() in t), None)
    city = None
    for word, canonical in list(_CITY_ALIASES.items()) + [(c.lower(), c.lower()) for c in cities]:
        if re.search(rf"\b{re.escape(word)}\b", t):
            city = _normalize_city(canonical, cities)
            break
    dose = next((d for d, pat in _DOSE_WORDS if re.search(pat, t)), None)
    m = _BOOKING_ID_RE.search(text)
    return {"action": action, "vaccine": vaccine, "city": city, "dose": dose,
            "booking_id": m.group(0) if m else None}


def parse_llm_json(answer: str, cities: List[str]) -> Dict[str, object]:
    """Extract and normalise the JSON object in a model reply (empty dict on failure)."""
    m = _JSON_RE.search(answer or "")
    if not m:
        return {}
    try:
        raw = json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}
    return normalize(raw, cities) if isinstance(raw, dict) else {}


def _grounded(field: str, value, text: str, rules: Dict[str, object]) -> bool:
    """Is a value the model returned actually supported by the user's words?

    Small models sometimes fill in a plausible but unrequested value (e.g. a
    vaccine the user never named); such values are discarded.
    """
    t = text.lower()
    if field == "vaccine":
        return str(value).lower() in t
    if field == "city":
        return rules.get("city") == value or str(value).lower() in t
    if field == "dose":
        return bool(_DOSE_HINT_RE.search(t))
    if field == "booking_id":
        return str(value).lower() in t
    return True


def merge(llm: Dict[str, object], rules: Dict[str, object], text: str = "") -> Dict[str, object]:
    """Combine the model's intent with the rule-based one.

    The model understands free phrasing, so its action is used when valid.
    For the fields (vaccine, city, dose, booking id) explicit evidence found
    by the rules wins, and the model only fills gaps with values that are
    grounded in the text. Two safety overrides keep a model mistake from changing state wrongly:
    a sentence the rules read as a question is never executed, and an
    explicit cancellation is never turned into a new booking.
    """
    out = {}
    for k in rules:
        v, r = llm.get(k), rules.get(k)
        if k == "action":
            out[k] = v if v is not None else r
        elif r is not None:
            out[k] = r
        elif v is not None and (not text or _grounded(k, v, text, rules)):
            out[k] = v
        else:
            out[k] = None
    if rules.get("action") == "question":
        out["action"] = "question"
    elif rules.get("action") == "cancel" and out["action"] == "book":
        out["action"] = "cancel"
    return out


def intent_context(slots: Iterable[dict]) -> str:
    """Allowed values sent to the LLM with the sentence."""
    return f"vaccines: {', '.join(VACCINES)}; cities: {', '.join(known_cities(slots))}"


def _describe(intent: dict) -> str:
    parts = [p for p in (
        intent.get("vaccine"),
        f"dose {intent['dose']}" if intent.get("dose") else None,
        f"in {intent['city']}" if intent.get("city") else None,
    ) if p]
    return " ".join(parts) or "any vaccine"


def _matches(item: dict, intent: dict) -> bool:
    if intent.get("vaccine") and item.get("vaccine_name") != intent["vaccine"]:
        return False
    if intent.get("dose") and int(item.get("dose_number") or 0) != intent["dose"]:
        return False
    if intent.get("city") and intent["city"].lower() not in str(item.get("center_name", "")).lower():
        return False
    return True


def resolve(intent: dict, slots: Dict[str, dict], my_bookings: Dict[str, dict]) -> dict:
    """Decide what to do. Returns one of:
        {"kind": "book",    "slot_id": ...}
        {"kind": "cancel",  "booking_id": ...}
        {"kind": "question"}
        {"kind": "clarify", "message": ...}   (ambiguous or nothing matches)
    """
    action = intent.get("action")
    if action == "book":
        if not any(intent.get(k) for k in ("vaccine", "city", "dose")):
            return {"kind": "clarify", "message": "Which vaccine, dose and city would you like? "
                    "For example: 'book COVAXIN dose 2 in Mumbai'."}
        matching = {sid: s for sid, s in slots.items() if _matches(s, intent)}
        open_ = {sid: s for sid, s in matching.items() if int(s.get("available_count", 0)) > 0}
        if len(open_) == 1:
            return {"kind": "book", "slot_id": next(iter(open_))}
        if len(open_) > 1:
            options = "; ".join(f"{s['name']} ({sid}, {s['available_count']} open)" for sid, s in sorted(open_.items()))
            return {"kind": "clarify", "message": f"Several slots match {_describe(intent)}: {options}. "
                    "Please say which vaccine, dose or city you want."}
        if matching:
            return {"kind": "clarify", "message": f"Sorry, every slot for {_describe(intent)} is sold out."}
        return {"kind": "clarify", "message": f"There is no slot for {_describe(intent)}. "
                f"Available: {', '.join(sorted({s['name'] for s in slots.values()}))}."}

    if action == "cancel":
        confirmed = {bid: b for bid, b in my_bookings.items() if b.get("status") == "confirmed"}
        if intent.get("booking_id"):
            bid = next((b for b in confirmed if b.upper() == intent["booking_id"].upper()), None)
            if bid:
                return {"kind": "cancel", "booking_id": bid}
            return {"kind": "clarify", "message": f"You have no active appointment {intent['booking_id']}."}
        candidates = {bid: b for bid, b in confirmed.items() if _matches(b, intent)}
        if len(candidates) == 1:
            return {"kind": "cancel", "booking_id": next(iter(candidates))}
        if not confirmed:
            return {"kind": "clarify", "message": "You have no active appointments to cancel."}
        if not candidates:
            return {"kind": "clarify", "message": f"None of your appointments is for {_describe(intent)}."}
        options = "; ".join(f"{bid} ({b['vaccine_name']} dose {b['dose_number']}, {b['center_name']}, "
                            f"{b['scheduled_date']})" for bid, b in sorted(candidates.items()))
        return {"kind": "clarify", "message": f"You have several matching appointments: {options}. "
                "Which one should I cancel? You can give the booking ID."}

    if action == "question":
        return {"kind": "question"}
    return {"kind": "clarify", "message": "I can book or cancel vaccination appointments, or answer "
            "questions. Try 'book a COVISHIELD first dose in Delhi' or 'cancel my COVAXIN appointment'."}

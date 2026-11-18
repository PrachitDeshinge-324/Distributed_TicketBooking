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
import difflib
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
# Information requests that don't start with a question word ("difference in covishield
# and covaxin", "tell me about corbevax", "covaxin vs covishield", "documents needed").
_INFO_RE = re.compile(
    r"\b(difference|differences|differ|different|compare|comparison|vs|versus|better|best|explain|"
    r"tell me|about|info|information|details|gap|interval|side effects?|safe|safety|efficacy|"
    r"documents?|id proof|price|cost|fee|refund|eligib\w*|age limit|rules?|policy|meaning)\b", re.I)
_DOSE_HINT_RE = re.compile(
    r"\b([123]|one|two|three|first|second|third|1st|2nd|3rd|booster|precaution)\b", re.I)
_BOOKING_ID_RE = re.compile(r"\bAPPT-[A-Za-z0-9-]+\b", re.I)
_SLOT_ID_RE = re.compile(r"\bSLOT-[A-Za-z0-9-]+\b", re.I)
_JSON_RE = re.compile(r"\{.*\}", re.S)
# "none", "no thanks", "forget it" in reply to a question ends the conversation.
_DISMISS_RE = re.compile(
    r"^\s*(none|no|nope|nah|neither|nothing|no thanks|no thank you|never ?mind|forget it|leave it|"
    r"stop|skip|cancel that|not now|none of them|none of these)\s*[.!]*\s*$", re.I)
# "at Pune" / "in Pune": a place the user named. Words that follow "in"/"at" but are not places:
_PLACE_RE = re.compile(r"\b(?:in|at|near)\s+([a-z][a-z]+)\b", re.I)
_NOT_PLACES = {"the", "a", "an", "my", "any", "morning", "evening", "afternoon", "night", "noon",
               "centre", "center", "hospital", "clinic", "time", "all", "least", "once", "advance",
               "dose", "slot", "general", "private", "government", "january", "february", "march",
               "april", "may", "june", "july", "august", "september", "october", "november", "december"}


def slot_city(slot: dict) -> str:
    """City of a slot, taken from its centre name ("KEM Hospital Mumbai" -> "Mumbai")."""
    name = str(slot.get("center_name", "")).strip()
    return name.split()[-1] if name else ""


def known_cities(slots: Iterable[dict]) -> List[str]:
    return sorted({slot_city(s) for s in slots if slot_city(s)})


_GENERIC_CENTRE_WORDS = {"hospital", "hospitals", "centre", "center", "clinic", "medical", "college",
                         "institute", "health", "the", "of", "and", "govt", "government"}


def centre_aliases(slots: Iterable[dict]) -> Dict[str, str]:
    """Centre-name words -> city, so "book at KEM" or "at AIIMS" finds the city."""
    out = {}
    for s in slots:
        city = slot_city(s)
        for w in re.findall(r"[a-z]+", str(s.get("center_name", "")).lower()):
            if len(w) >= 3 and w != city.lower() and w not in _GENERIC_CENTRE_WORDS:
                out[w] = city
    return out


def _normalize_city(value, cities: List[str], fuzzy: bool = True) -> Optional[str]:
    """Known city for a name, alias or misspelling ("Bengaluru", "banglore"), else None."""
    if not value:
        return None
    v = _CITY_ALIASES.get(str(value).strip().lower(), str(value).strip().lower())
    for c in cities:
        if c.lower() == v:
            return c
    if fuzzy and len(v) >= 4:
        lowered = {c.lower(): c for c in cities}
        close = difflib.get_close_matches(v, list(lowered) + list(_CITY_ALIASES), n=1, cutoff=0.8)
        if close:
            return _normalize_city(close[0], cities, fuzzy=False)
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


def rule_based_intent(text: str, cities: List[str], centres: Optional[Dict[str, str]] = None) -> Dict[str, object]:
    """Deterministic keyword extractor (also the fallback when no LLM is available).

    centres maps centre-name words to their city ("kem" -> "Mumbai"), see centre_aliases().
    """
    t = text.lower()
    if _QUESTION_RE.search(t) and not _BOOKING_ID_RE.search(text):
        action = "question"  # "how do I cancel?" asks about cancelling; it is not a request
    elif _CANCEL_RE.search(t):
        action = "cancel"
    elif _BOOK_RE.search(t):
        action = "book"
    elif _INFO_RE.search(t) or t.rstrip().endswith("?"):
        action = "question"
    else:
        action = None
    vaccine = next((v for v in VACCINES if v.lower() in t), None)
    city = None
    for word, canonical in (list(_CITY_ALIASES.items()) + [(c.lower(), c.lower()) for c in cities]
                            + list((centres or {}).items())):
        if re.search(rf"\b{re.escape(word)}\b", t):
            city = _normalize_city(canonical, cities)
            break
    if city is None:  # misspelt city ("banglore")
        for w in re.findall(r"[a-z]{4,}", t):
            if w not in _NOT_PLACES and not any(w == v.lower() for v in VACCINES):
                city = _normalize_city(w, cities)
                if city:
                    break
    dose = next((d for d, pat in _DOSE_WORDS if re.search(pat, t)), None)
    if dose is None and re.fullmatch(r"\s*(?:dose\s*)?([123])\s*[.!]?\s*", t):
        dose = int(re.search(r"[123]", t).group(0))  # "2" in reply to "which dose?"
    m = _BOOKING_ID_RE.search(text)
    return {"action": action, "vaccine": vaccine, "city": city, "dose": dose,
            "booking_id": m.group(0) if m else None}


def is_dismissal(text: str) -> bool:
    return bool(_DISMISS_RE.match(text or ""))


def find_slot_id(text: str, slots: Dict[str, dict]) -> Optional[str]:
    """A real slot id the user typed (case-insensitive), else None."""
    by_upper = {sid.upper(): sid for sid in slots}
    for m in _SLOT_ID_RE.finditer(text or ""):
        if m.group(0).upper() in by_upper:
            return by_upper[m.group(0).upper()]
    return None


def unknown_place(text: str, cities: List[str], llm_city: Optional[str] = None,
                  centres: Optional[Dict[str, str]] = None) -> Optional[str]:
    """A place the user asked for that has no vaccination centre ("book covishield at Pune")."""
    known = {c.lower() for c in cities} | set(_CITY_ALIASES) | set(centres or {})
    vaccines = {v.lower() for v in VACCINES}
    t = (text or "").lower()
    if llm_city and str(llm_city).strip().lower() in t and _normalize_city(llm_city, cities) is None:
        return str(llm_city).strip().title()
    for m in _PLACE_RE.finditer(t):
        word = m.group(1)
        if (word not in known and word not in vaccines and word not in _NOT_PLACES
                and _normalize_city(word, cities) is None):
            return word.title()
    return None


def is_bare_reply(text: str, rules: Dict[str, object], slot_id: Optional[str]) -> bool:
    """A short answer with no verb ("delhi", "dose 1", "the second one", a slot id):
    it only makes sense as a reply to the assistant's previous question."""
    if rules.get("action") is not None or len((text or "").split()) > 4:
        return False
    return bool(slot_id or any(rules.get(k) for k in ("vaccine", "city", "dose", "booking_id"))
                or _DOSE_HINT_RE.search(text or ""))


def continue_intent(pending: Dict[str, object], new: Dict[str, object]) -> Dict[str, object]:
    """Fill the pending request with what the follow-up adds; the follow-up wins on conflicts."""
    out = dict(pending)
    for k, v in new.items():
        if k != "action" and v is not None:
            out[k] = v
    return out


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


def raw_llm_city(answer: str) -> Optional[str]:
    """The city exactly as the model wrote it, even one with no centre ("Pune")."""
    m = _JSON_RE.search(answer or "")
    try:
        raw = json.loads(m.group(0)) if m else None
    except json.JSONDecodeError:
        return None
    city = raw.get("city") if isinstance(raw, dict) else None
    return str(city) if city else None


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


def choose_message(intent: dict, options: Dict[str, dict]) -> str:
    """Ask only about what actually differs between the matching slots."""
    doses = sorted({int(s.get("dose_number") or 0) for s in options.values()})
    places = sorted({slot_city(s) for s in options.values()})
    vaccines = sorted({s.get("vaccine_name") for s in options.values()})
    def price(s):
        p = float(s.get("price") or 0)
        return f", Rs.{p:.0f} paid at booking" if p > 0 else ", free"
    listing = "; ".join(f"{s['name']} ({sid}, {s['available_count']} open{price(s)})"
                        for sid, s in sorted(options.items()))
    if len(options) == 1:
        return f"Open: {listing}. Say 'yes' to book it, or 'none' to stop."
    if len(vaccines) == 1 and len(places) == 1 and len(doses) > 1:
        ask = "Which dose do you want: " + " or ".join(f"Dose {d}" for d in doses) + "?"
    elif len(vaccines) == 1 and len(doses) == 1 and len(places) > 1:
        ask = "Which city: " + ", ".join(places) + "?"
    else:
        ask = "Which one do you want? Reply with the dose, city or slot ID."
    return f"{len(options)} slots match {_describe(intent)}: {listing}. {ask} (Say 'none' to stop.)"


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
        # One appointment per dose: don't offer a dose the user already holds.
        held = {int(b.get("dose_number") or 0): b for b in my_bookings.values() if b.get("status") == "confirmed"}
        bookable = {sid: s for sid, s in open_.items() if int(s.get("dose_number") or 0) not in held}
        if open_ and not bookable:
            blocking = sorted({int(s.get("dose_number") or 0) for s in open_.values()})
            held_text = " and ".join(
                f"a Dose {d} appointment ({held[d]['booking_id']}, {held[d]['vaccine_name']} at "
                f"{held[d]['center_name']} on {held[d]['scheduled_date']})" for d in blocking)
            return {"kind": "clarify", "message": (
                f"You already hold {held_text}. Only one appointment per dose is allowed; "
                "cancel it first if you want to book that dose elsewhere.")}
        if len(bookable) == 1:
            sid = next(iter(bookable))
            if len(open_) > 1 and not intent.get("dose"):
                # Other doses were left out because the user holds them: offer the one
                # that is left instead of booking a dose the user never asked for.
                return {"kind": "clarify", "offer_slot": sid, "message": (
                    f"You already hold Dose {', '.join(str(d) for d in sorted(held))}. "
                    + choose_message(intent, bookable))}
            return {"kind": "book", "slot_id": sid}
        if len(bookable) > 1:
            return {"kind": "clarify", "message": choose_message(intent, bookable)}
        if matching:
            return {"kind": "clarify", "message": f"Sorry, every slot for {_describe(intent)} is sold out."}
        # Say where the vaccine/dose IS available instead of listing everything.
        elsewhere = {sid: s for sid, s in slots.items() if int(s.get("available_count", 0)) > 0
                     and _matches(s, {**intent, "city": None})}
        if intent.get("city") and elsewhere:
            where = ", ".join(sorted({slot_city(s) for s in elsewhere.values()}))
            return {"kind": "clarify", "message": f"There is no slot for {_describe(intent)}. "
                    f"{_describe({**intent, 'city': None})} is available in: {where}. Which city would you like?"}
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
            return {"kind": "clarify", "message": "You have no active appointments to cancel. To make one, "
                                                  "say for example 'book a COVAXIN first dose in Mumbai'."}
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

"""Natural-language booking and cancellation (LLM intent -> validated Raft command)."""
import json

from dist_ticket_booking.business.booking_service import DEFAULT_SLOT_STORE, BookingService
from dist_ticket_booking.grpc.service import TicketClient, TicketClientServicer
from dist_ticket_booking.grpc import ticket_service_pb2
from dist_ticket_booking.llm.intent import known_cities, merge, parse_llm_json, resolve, rule_based_intent
from test_raft_safety import _wait_converged_digest
from test_raft_stress import _wait_for_leader, cluster  # noqa: F401  (pytest fixture)

CITIES = known_cities(DEFAULT_SLOT_STORE.values())


def _slots():
    return BookingService().get_availability()["tickets"]


def test_rule_based_extraction():
    assert rule_based_intent("book me a covaxin second dose in Bombay", CITIES) == {
        "action": "book", "vaccine": "COVAXIN", "city": "Mumbai", "dose": 2, "booking_id": None}
    assert rule_based_intent("how do I cancel a booking?", CITIES)["action"] == "question"
    assert rule_based_intent("can you cancel my Delhi appointment", CITIES)["action"] == "cancel"
    assert rule_based_intent("cancel APPT-SLOT-CVS-D1-1-bob-7", CITIES)["booking_id"] == "APPT-SLOT-CVS-D1-1-bob-7"


def test_model_output_is_validated_and_grounded():
    # Invalid values from the model are dropped...
    assert parse_llm_json('sure! {"action": "book", "vaccine": "PFIZER", "city": "Paris", "dose": 7}', CITIES) == {
        "action": "book", "vaccine": None, "city": None, "dose": None, "booking_id": None}
    assert parse_llm_json("not json at all", CITIES) == {}
    # ...and valid-looking values the user never said are ignored.
    text = "reserve a slot for dose 1 in delhi"
    hallucinated = {"action": "book", "vaccine": "COVAXIN", "city": "Delhi", "dose": 1, "booking_id": None}
    intent = merge(hallucinated, rule_based_intent(text, CITIES), text)
    assert intent["vaccine"] is None
    assert resolve(intent, _slots(), {}) == {"kind": "book", "slot_id": "SLOT-CVS-D1-110001"}


def test_model_mistakes_cannot_change_state_wrongly():
    q = "how do I cancel a booking?"
    assert merge({"action": "cancel"}, rule_based_intent(q, CITIES), q)["action"] == "question"
    c = "cancel my covaxin appointment"
    assert merge({"action": "book", "vaccine": "COVAXIN"}, rule_based_intent(c, CITIES), c)["action"] == "cancel"


def test_resolution_asks_when_ambiguous():
    slots = _slots()
    plan = resolve({"action": "book", "vaccine": "COVISHIELD"}, slots, {})
    assert plan["kind"] == "clarify" and "SLOT-CVS-D1-110001" in plan["message"]
    assert resolve({"action": "book"}, slots, {})["kind"] == "clarify"
    slots["SLOT-CBX-BOS-560001"]["available_count"] = 0
    assert "sold out" in resolve({"action": "book", "vaccine": "CORBEVAX"}, slots, {})["message"]

    mine = {
        "A": {"status": "confirmed", "vaccine_name": "COVAXIN", "dose_number": 1, "center_name": "KEM Hospital Mumbai",
              "scheduled_date": "d"},
        "B": {"status": "confirmed", "vaccine_name": "COVAXIN", "dose_number": 2, "center_name": "KEM Hospital Mumbai",
              "scheduled_date": "d"},
    }
    assert resolve({"action": "cancel", "vaccine": "COVAXIN"}, slots, mine)["kind"] == "clarify"
    assert resolve({"action": "cancel", "dose": 2}, slots, mine) == {"kind": "cancel", "booking_id": "B"}
    assert resolve({"action": "cancel", "booking_id": "Z"}, slots, mine)["kind"] == "clarify"


class _WrongLLM:
    """A model that always tries to book COVAXIN in Mumbai, whatever was asked."""

    def extract_intent(self, request_id, text, allowed=""):
        return {"answer": json.dumps({"action": "book", "vaccine": "COVAXIN", "city": "Mumbai", "dose": 1})}

    def get_llm_answer(self, request_id, query, context=""):
        return {"answer": "stub answer"}


def test_wrong_model_cannot_book_something_else():
    svc = TicketClientServicer(llm_service=_WrongLLM())
    svc.Register(ticket_service_pb2.RegisterRequest(username="zoe", password="pw"), None)
    tok = svc.Login(ticket_service_pb2.LoginRequest(username="zoe", password="pw"), None).token

    def say(text):
        return svc.Post(ticket_service_pb2.PostRequest(
            token=tok, type="assistant", data=json.dumps({"text": text}), request_id=text), None)

    res = say("book the corbevax booster in bangalore")
    assert res.status == "accepted" and "CORBEVAX" in res.message
    assert say("how do I cancel a booking?").status == "success"  # answered, nothing executed
    assert not any(b["vaccine_name"] == "COVAXIN" for b in svc.booking_service.bookings.values())


def test_book_and_cancel_in_natural_language_through_raft(cluster):  # noqa: F811
    """End to end on a 3-node cluster (no LLM server: rule-based extraction)."""
    _wait_for_leader(cluster)
    follower = next(n for n in cluster if not n.raft_node.is_leader)
    c = TicketClient.for_cluster([f"localhost:{follower.client_port}"] +
                                 [f"localhost:{n.client_port}" for n in cluster if n is not follower])
    tok = c.login("asha", "pw").token

    res = c.assistant(tok, "please book me a COVAXIN second dose in Bombay")
    assert res.status == "accepted", res.message
    assert "Booking ID: APPT-SLOT-CVX-D2" in res.message
    res = c.assistant(tok, "I also want the corbevax booster in Bengaluru")
    assert res.status == "accepted", res.message

    res = c.assistant(tok, "cancel my appointment")  # two bookings -> must ask which
    assert res.status == "clarify" and "COVAXIN" in res.message and "CORBEVAX" in res.message

    rid = "nl-cancel-1"
    res = c.assistant(tok, "I don't need the Mumbai booking anymore", request_id=rid)
    assert res.status == "success" and "COVAXIN" in res.message
    again = c.assistant(tok, "I don't need the Mumbai booking anymore", request_id=rid)
    assert (again.status, again.message) == (res.status, res.message)  # retry: same reply, applied once

    states = _wait_converged_digest(cluster)
    assert len({s["state_digest"] for s in states}) == 1
    for n in cluster:
        mine = {b["vaccine_name"]: b["status"] for b in n.service.bookings.values() if b["user_id"] == "asha"}
        assert mine == {"COVAXIN": "cancelled", "CORBEVAX": "confirmed"}
        assert n.service.get_availability("SLOT-CVX-D2-400001")["available_count"] == 25
    c.close()


def test_assistant_conversation_follow_ups():
    """Replies to the assistant's questions continue the request; 'none' ends it;
    an unknown city is reported; a dose the user did not ask for is offered, not booked."""
    svc = TicketClientServicer()  # no LLM: rule-based extraction
    svc.Register(ticket_service_pb2.RegisterRequest(username="kim", password="pw"), None)
    tok = svc.Login(ticket_service_pb2.LoginRequest(username="kim", password="pw"), None).token
    n = iter(range(1000))

    def say(text):
        return svc.Post(ticket_service_pb2.PostRequest(
            token=tok, type="assistant", data=json.dumps({"text": text}), request_id=f"r{next(n)}"), None)

    res = say("book covishield at pune")
    assert res.status == "clarify" and "no vaccination slots in Pune" in res.message
    assert "won't book" in say("none").message
    assert "Which dose" in say("book covishield at delhi").message
    res = say("dose 1")                                       # follow-up completes the request
    assert res.status == "accepted" and "COVISHIELD Dose 1" in res.message and "AIIMS Delhi" in res.message
    res = say("book covishield dose 1 in hyderabad")          # one appointment per dose
    assert res.status == "clarify" and "already hold a Dose 1" in res.message
    res = say("book covishield")                              # only Dose 2 is left: offered, not booked
    assert res.status == "clarify" and "Say 'yes'" in res.message
    assert not any(b["dose_number"] == 2 for b in svc.booking_service.bookings.values())
    res = say("yes")
    assert res.status == "accepted" and "COVISHIELD Dose 2" in res.message
    res = say("book covaxin dose 1 in hyderabad")
    assert res.status == "clarify" and "available in: Mumbai" in res.message


def test_information_requests_without_question_word_are_questions():
    for text in ("difference in covishield and covaxin", "covaxin vs covishield",
                 "tell me about corbevax", "documents needed", "covaxin gap"):
        assert rule_based_intent(text, CITIES)["action"] == "question", text
    assert rule_based_intent("covishield", CITIES)["action"] is None  # a bare reply, not a question


def test_misspelt_city_centre_names_and_no_unasked_paid_booking():
    """'banglore' and 'KEM' are understood; a slot the user did not name a vaccine for
    (here a paid one) is offered and booked only after 'yes'; '2' answers 'which dose?'."""
    from dist_ticket_booking.llm.intent import centre_aliases
    svc = TicketClientServicer()
    svc.Register(ticket_service_pb2.RegisterRequest(username="lee", password="pw"), None)
    tok = svc.Login(ticket_service_pb2.LoginRequest(username="lee", password="pw"), None).token
    n = iter(range(1000))

    def say(text):
        return svc.Post(ticket_service_pb2.PostRequest(
            token=tok, type="assistant", data=json.dumps({"text": text}), request_id=f"q{next(n)}"), None)

    slots = svc.booking_service.get_availability()["tickets"].values()
    assert centre_aliases(slots)["kem"] == "Mumbai"
    res = say("book in banglore")
    assert res.status == "clarify" and "Say 'yes'" in res.message and "Rs.250" in res.message
    assert not svc.booking_service.bookings                    # nothing booked or charged yet
    assert say("yes").status == "accepted"
    assert "Which dose" in say("book covaxin at kem").message
    res = say("2")
    assert res.status == "accepted" and "COVAXIN Dose 2" in res.message and "KEM" in res.message


def test_medical_questions_are_not_answered_by_the_model():
    from dist_ticket_booking.llm.domain_llm import _grounded
    invented = ("The booster dose is mandatory because it must be administered at least 9 months after "
                "completing Dose 2 to ensure adequate immune response against the current vaccine strain.")
    out = _grounded(invented, "why booster is required")
    assert "medical advice" in out and "9 months" in out and "strain" not in out
    made_up = "COVAXIN is extremely effective against omicron variants and causes minimal fever reactions overall."
    assert _grounded(made_up, "what is covaxin") != made_up    # invented content is replaced


def test_also_one_for_another_city_repeats_the_last_booking():
    """'also one for bangalore' after a booking reuses its vaccine and dose; it is not an FAQ."""
    svc = TicketClientServicer()
    svc.Register(ticket_service_pb2.RegisterRequest(username="lee", password="pw"), None)
    tok = svc.Login(ticket_service_pb2.LoginRequest(username="lee", password="pw"), None).token
    n = iter(range(1000))

    def say(text):
        return svc.Post(ticket_service_pb2.PostRequest(
            token=tok, type="assistant", data=json.dumps({"text": text}), request_id=f"r{next(n)}"), None)

    assert say("book covishield dose 1 in delhi").status == "accepted"
    res = say("also one for banglore")
    # Vaccine and dose carried over from the last booking; Bangalore has no such slot, so it is reported.
    assert res.status == "clarify" and "COVISHIELD dose 1 in Bangalore" in res.message

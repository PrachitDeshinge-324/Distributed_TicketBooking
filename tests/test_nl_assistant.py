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

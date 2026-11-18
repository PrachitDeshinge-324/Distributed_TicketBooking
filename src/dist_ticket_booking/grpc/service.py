"""gRPC servicers and client wrapper for ticket booking."""
import json
import logging
import os
import re
import threading
import time
import uuid
from typing import Dict, Any, Optional

import grpc

from . import ticket_service_pb2
from . import ticket_service_pb2_grpc
from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.business.payment import DEFAULT_METHOD, SUPPORTED_METHODS, DECLINED_TEST_METHOD
from dist_ticket_booking.core.auth import (
    DEFAULT_ADMIN_CODE, ROLE_ADMIN, ROLE_CITIZEN, ROLES, admin_signup_code, credential_digest,
    register_user, role_of, validate_username, verify_credential,
)
from dist_ticket_booking.llm.domain_llm import STATE_MARKER, DomainLLM, _keyword_fallback
from dist_ticket_booking.llm.intent import (
    centre_aliases, choose_message, continue_intent, find_slot_id, intent_context, is_bare_reply, is_dismissal, known_cities,
    merge, parse_llm_json, raw_llm_city, resolve, rule_based_intent, slot_city, unknown_place,
)

logger = logging.getLogger(__name__)

# "yes" / "book it" in reply to the assistant's question confirms the pending request.
_CONFIRM_RE = re.compile(r"^\s*(yes|yeah|yep|yup|ok|okay|sure|confirm|go ahead|please do|do it|book it|"
                         r"yes please|that one|this one)\s*[.!]*\s*$", re.I)

# Deadline for app-server -> LLM-server RPCs. If the model is slower than
# this (e.g. CPU-only inference), clients get a keyword-based answer instead
# of hanging. Override with the LLM_TIMEOUT_S environment variable.
LLM_TIMEOUT_S = float(os.environ.get("LLM_TIMEOUT_S", "30"))
# At most this many FAQ queries per app node wait on the LLM at once. Extra
# queries get the instant keyword answer, so a slow model can never tie up
# the gRPC worker threads that bookings and availability reads need.
LLM_MAX_INFLIGHT = int(os.environ.get("LLM_MAX_INFLIGHT", "4"))
_llm_slots = threading.BoundedSemaphore(LLM_MAX_INFLIGHT)


_UNAVAILABLE_MSG = (
    "Cluster unavailable: cannot reach a majority of nodes (no quorum). "
    "No changes were made; please retry shortly."
)


def _raft_status(res: dict):
    """Map a Raft propose() result to (status, message) for clients."""
    status = res.get("status", "error")
    if status == "unavailable":
        return "unavailable", _UNAVAILABLE_MSG
    return status, res.get("message", "")


class PayloadError(ValueError):
    """A Post/ProcessBusinessRequest payload does not match its type's schema."""


def _parse_payload(raw: str) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise PayloadError(f"data is not valid JSON: {e.msg}")
    if not isinstance(data, dict):
        raise PayloadError("data must be a JSON object")
    return data


def _field(data: dict, name: str, kind, required: bool = True, default=None):
    """Typed field access for JSON payloads, with a clear error on mismatch."""
    if name not in data or data[name] is None:
        if required:
            raise PayloadError(f"missing required field {name!r}")
        return default
    value = data[name]
    if kind is int:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise PayloadError(f"field {name!r} must be an integer")
        try:
            return int(value)
        except ValueError:
            raise PayloadError(f"field {name!r} must be an integer")
    if not isinstance(value, kind) or (kind is str and not value.strip()):
        raise PayloadError(f"field {name!r} must be a non-empty {kind.__name__}")
    return value


# Schema per Post type (documented in the README):
#   booking    {"ticket_id": str, "quantity": int = 1, "priority": int = 0, "payment_method": str = "upi"}
#   cancel     {"booking_id": str}
#   assistant  {"text": str}
def _booking_command(user_id: str, data: dict, request_id: str) -> dict:
    method = _field(data, "payment_method", str, required=False, default=DEFAULT_METHOD).lower()
    if method not in SUPPORTED_METHODS + (DECLINED_TEST_METHOD,):
        raise PayloadError(f"payment_method must be one of {', '.join(SUPPORTED_METHODS)}")
    return {
        "user_id": user_id,
        "ticket_id": _field(data, "ticket_id", str),
        "quantity": _field(data, "quantity", int, required=False, default=1),
        "priority": _field(data, "priority", int, required=False, default=0),
        "payment_method": method,
        "request_id": request_id,
        # Stamped once, here on the leader, and replicated with the command:
        # every replica evaluates the time rules against the same instant.
        "requested_at": time.time(),
    }


def _cancel_command(user_id: str, data: dict, request_id: str) -> dict:
    return {
        "user_id": user_id,
        "booking_id": _field(data, "booking_id", str),
        "request_id": request_id,
        "requested_at": time.time(),
    }


def _fallback_answer(query: str, context: str = "") -> str:
    prompt = f"{query}\n\nContext: {context}" if context else query
    return _keyword_fallback(prompt)


class TicketClientServicer(ticket_service_pb2_grpc.TicketClientServiceServicer):
    """Client-facing gRPC handler."""

    def __init__(
        self,
        booking_service: Optional[BookingService] = None,
        sessions: Optional[Dict[str, str]] = None,
        llm_stub: Optional[ticket_service_pb2_grpc.LLMServiceStub] = None,
        llm_service: Optional[DomainLLM] = None,
        raft_node: Optional[Any] = None,
    ):
        self.raft_node = raft_node
        if raft_node is not None:
            self.booking_service = raft_node.state_machine.booking_service
            self.sessions = raft_node.state_machine.sessions
        else:
            self.booking_service = booking_service or BookingService()
            self.sessions = sessions if sessions is not None else {}
        self.llm_stub = llm_stub
        self.llm_service = llm_service
        self._users: Dict[str, dict] = {}  # standalone (non-Raft) mode only
        self._tl = threading.local()  # per-request info for log lines (answer source)
        # Assistant conversations: the request each user is being asked to complete
        # ("which dose?"), so a reply like "delhi" or "dose 1" continues it. Kept on
        # the serving node only; it shapes how a sentence is read, never the
        # replicated state, so losing it on failover just means asking again.
        self._pending: Dict[str, tuple] = {}
        self._pending_lock = threading.Lock()

    def _verify_token(self, token: str) -> Optional[str]:
        if self.raft_node is not None:
            return self.raft_node.state_machine.get_session_user(token)
        return self.sessions.get(token)

    def _role(self, username: str) -> Optional[str]:
        users = self.raft_node.state_machine.users if self.raft_node is not None else self._users
        return role_of(users, username)

    @staticmethod
    def _redirect(res: dict, raft_node):
        return ticket_service_pb2.StatusResponse(
            status="redirect", message="not_leader",
            leader_hint=res.get("leader_hint", "") or (raft_node.get_leader_address() or ""),
        )

    def _write(self, command_type: str, command: dict, standalone):
        """Replicate a write through Raft (or run `standalone()` without Raft)
        and turn the result into a StatusResponse."""
        if self.raft_node is None:
            res = standalone()
            return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))
        res = self.raft_node.propose(command_type, command)
        if res.get("message") == "not_leader":
            return self._redirect(res, self.raft_node)
        status, message = _raft_status(res)
        return ticket_service_pb2.StatusResponse(status=status, message=message)

    def Register(self, request, context):
        problem = validate_username(request.username)
        if problem or not request.password:
            return ticket_service_pb2.StatusResponse(status="error", message=problem or "password required")
        role = request.role or ROLE_CITIZEN
        if role not in ROLES:
            return ticket_service_pb2.StatusResponse(status="error", message=f"role must be one of {', '.join(ROLES)}")
        if role == ROLE_ADMIN and request.admin_code != admin_signup_code():
            return ticket_service_pb2.StatusResponse(status="unauthorized", message="invalid admin sign-up code")
        credential = credential_digest(request.username, request.password)
        return self._write(
            "register", {"username": request.username, "credential": credential, "role": role},
            lambda: register_user(self._users, request.username, credential, role),
        )

    def Login(self, request, context):
        if not (request.username and request.password):
            return ticket_service_pb2.LoginResponse(status="unauthorized", message="username and password required")
        # Only a salted digest leaves this function; the plain password is
        # never written to the Raft log.
        credential = credential_digest(request.username, request.password)

        if self.raft_node is not None:
            token = str(uuid.uuid4())
            res = self.raft_node.propose(
                "login", {"username": request.username, "token": token, "credential": credential}
            )
            if res.get("message") == "not_leader":
                return ticket_service_pb2.LoginResponse(
                    status="redirect",
                    leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                )
            if res.get("status") == "success":
                return ticket_service_pb2.LoginResponse(
                    status="success", token=res.get("token", token), role=res.get("role", ROLE_CITIZEN))
            status, message = _raft_status(res)
            return ticket_service_pb2.LoginResponse(status=status, message=message)

        ok = verify_credential(self._users, request.username, credential)
        if ok is None:
            return ticket_service_pb2.LoginResponse(
                status="unknown_user", message=f"no account named {request.username!r}; register first")
        if not ok:
            return ticket_service_pb2.LoginResponse(status="unauthorized", message="invalid username or password")
        token = str(uuid.uuid4())
        self.sessions[token] = request.username
        return ticket_service_pb2.LoginResponse(status="success", token=token, role=self._role(request.username))

    def Logout(self, request, context):
        if self.raft_node is not None:
            res = self.raft_node.propose("logout", {"token": request.token})
            if res.get("message") == "not_leader":
                return ticket_service_pb2.StatusResponse(
                    status="redirect",
                    message="not_leader",
                    leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                )
            status, message = _raft_status(res)
            return ticket_service_pb2.StatusResponse(status=status, message=message)

        if request.token in self.sessions:
            del self.sessions[request.token]
            return ticket_service_pb2.StatusResponse(status="success", message="Logged out")
        return ticket_service_pb2.StatusResponse(status="error", message="Invalid token")

    def Post(self, request, context):
        user_id = self._verify_token(request.token)
        if not user_id:
            return ticket_service_pb2.StatusResponse(status="unauthorized", message="Invalid token")

        try:
            data = _parse_payload(request.data)
            if request.type == "booking":
                cmd = _booking_command(user_id, data, request.request_id)
                return self._write("create_booking", cmd, lambda: self.booking_service.create_booking(
                    user_id, cmd["ticket_id"], cmd["quantity"], cmd["priority"],
                    now=cmd["requested_at"], payment_method=cmd["payment_method"]))

            if request.type == "cancel":
                cmd = _cancel_command(user_id, data, request.request_id)
                return self._write("cancel_booking", cmd, lambda: self.booking_service.cancel_booking(
                    user_id, cmd["booking_id"], now=cmd["requested_at"],
                    is_admin=self._role(user_id) == ROLE_ADMIN))

            if request.type == "assistant":
                return self._assistant(user_id, _field(data, "text", str), request.request_id)

            return ticket_service_pb2.StatusResponse(
                status="error", message=f"Unknown type {request.type!r} (use booking, cancel or assistant)")
        except PayloadError as e:
            return ticket_service_pb2.StatusResponse(status="invalid", message=str(e))
        except Exception as e:
            logger.error(f"Post error: {e}")
            return ticket_service_pb2.StatusResponse(status="error", message=str(e))

    def Get(self, request, context):
        if request.type in ("cluster_status", "status", "health"):
            if self.raft_node is not None:
                status_dict = self.raft_node.get_status()
                status_dict["leader_hint"] = self.raft_node.get_leader_address() or ""
                return ticket_service_pb2.GetResponse(
                    status="success",
                    items=[ticket_service_pb2.DataItem(id=self.raft_node.node_id, data=json.dumps(status_dict))],
                    leader_hint=status_dict["leader_hint"],
                )
            return ticket_service_pb2.GetResponse(
                status="success",
                items=[ticket_service_pb2.DataItem(id="standalone", data=json.dumps({"state": "standalone"}))],
            )

        if self.raft_node is not None and not self.raft_node.is_leader:
            leader_hint = self.raft_node.get_leader_address() or ""
            return ticket_service_pb2.GetResponse(status="redirect", leader_hint=leader_hint)
        if self.raft_node is not None and not self.raft_node.has_quorum_contact():
            # Cut off from the majority: a newer leader may exist elsewhere, so
            # our data could be stale. Refuse rather than return it.
            return ticket_service_pb2.GetResponse(status="unavailable")
        if self.raft_node is not None and not self.raft_node.wait_until_ready_for_reads():
            # New leader still catching up (or just lost leadership): let the
            # client retry rather than serve possibly stale data.
            return ticket_service_pb2.GetResponse(
                status="redirect", leader_hint=self.raft_node.get_leader_address() or ""
            )

        user_id = self._verify_token(request.token)
        if not user_id:
            return ticket_service_pb2.GetResponse(status="unauthorized")

        try:
            if request.type == "availability":
                params = json.loads(request.params) if request.params else {}
                ticket_id = params.get("ticket_id")
                availability = self.booking_service.get_availability(ticket_id)
                if ticket_id:
                    items = [ticket_service_pb2.DataItem(id=ticket_id, data=json.dumps(availability))]
                else:
                    items = [
                        ticket_service_pb2.DataItem(id=tid, data=json.dumps(info))
                        for tid, info in availability.get("tickets", {}).items()
                    ]
                return ticket_service_pb2.GetResponse(status="success", items=items)

            elif request.type == "my_bookings":
                mine = [
                    ticket_service_pb2.DataItem(id=bid, data=json.dumps(b))
                    for bid, b in sorted(self.booking_service.bookings.items())
                    if b.get("user_id") == user_id
                ]
                return ticket_service_pb2.GetResponse(status="success", items=mine)

            elif request.type == "all_bookings":
                # Help-desk view: administrators only.
                if self._role(user_id) != ROLE_ADMIN:
                    return ticket_service_pb2.GetResponse(status="forbidden")
                items = [ticket_service_pb2.DataItem(id=bid, data=json.dumps(b))
                         for bid, b in sorted(self.booking_service.bookings.items())]
                return ticket_service_pb2.GetResponse(status="success", items=items)

            elif request.type == "faq":
                params = json.loads(request.params) if request.params else {}
                query = params.get("query", request.params or "")
                live_context = self._build_live_context(query, user_id)
                t0 = time.monotonic()
                answer = self._query_llm(query, live_context)
                logger.info(f"FAQ    user '{user_id}': {query!r} -> answered in {time.monotonic() - t0:.1f}s "
                            f"({getattr(self._tl, 'source', 'LLM')})")
                items = [ticket_service_pb2.DataItem(id="faq_answer", data=answer)]
                return ticket_service_pb2.GetResponse(status="success", items=items)

            return ticket_service_pb2.GetResponse(status="error")
        except Exception as e:
            logger.error(f"Get error: {e}")
            return ticket_service_pb2.GetResponse(status="error")

    # ── Natural-language booking assistant ──

    def _execute(self, command_type: str, command: dict) -> dict:
        """Run a write through Raft (or directly when running standalone)."""
        command["requested_at"] = time.time()  # leader's clock, replicated with the command
        if self.raft_node is not None:
            return self.raft_node.propose(command_type, command)
        if command_type == "create_booking":
            return self.booking_service.create_booking(
                command["user_id"], command["ticket_id"], command.get("quantity", 1), command.get("priority", 0),
                now=command["requested_at"])
        return self.booking_service.cancel_booking(command["user_id"], command["booking_id"],
                                                   now=command["requested_at"])

    def _extract_intent_llm(self, text: str, allowed: str, cities) -> dict:
        """Ask the LLM server for a JSON intent; {} if it is unavailable or busy."""
        if not (self.llm_stub or self.llm_service) or not _llm_slots.acquire(blocking=False):
            return {}
        try:
            if self.llm_stub:
                res = self.llm_stub.ExtractIntent(
                    ticket_service_pb2.LLMQueryRequest(request_id=str(uuid.uuid4()), query=text, context=allowed),
                    timeout=LLM_TIMEOUT_S,
                )
                answer = res.answer
            else:
                answer = self.llm_service.extract_intent(str(uuid.uuid4()), text, allowed)["answer"]
            out = parse_llm_json(answer, cities)
            if out:
                out["_raw_city"] = raw_llm_city(answer)  # reported if it has no centre ("Pune")
            return out
        except Exception as e:
            logger.warning(f"Intent extraction via LLM failed ({e}); using rule-based extraction")
            return {}
        finally:
            _llm_slots.release()

    def _assistant(self, user_id: str, text: str, request_id: str):
        """Book or cancel from a sentence ("cancel my covaxin appointment").

        The LLM only proposes an interpretation. It is validated against live,
        Raft-replicated data, and the action itself is the ordinary replicated
        create_booking / cancel_booking command carrying the client's
        request_id, so overbooking protection and exactly-once retries apply.
        """
        def reply(status, message, hint=""):
            return ticket_service_pb2.StatusResponse(status=status, message=message, leader_hint=hint)

        if not text.strip():
            return reply("error", "Please tell me what you would like to do.")
        if self.raft_node is not None:
            # Decide on the leader only, from up-to-date state (and before
            # spending an LLM call on a node that would have to redirect).
            if not self.raft_node.is_leader:
                return reply("redirect", "not_leader", self.raft_node.get_leader_address() or "")
            if not self.raft_node.has_quorum_contact():
                return reply("unavailable", _UNAVAILABLE_MSG)
            if not self.raft_node.wait_until_ready_for_reads():
                return reply("redirect", "not_leader", self.raft_node.get_leader_address() or "")

        def done(res: dict):
            """Reply for an executed command. Built from the replicated result
            only, so a retried request gets exactly the same reply."""
            if res.get("status") == "accepted":
                return reply("accepted", f"{res.get('message', '')} Booking ID: {res.get('booking_id')}.".strip())
            if res.get("status") == "success":
                return reply("success", f"{res.get('message', '')}".strip())
            if res.get("message") == "not_leader":
                return reply("redirect", "not_leader", res.get("leader_hint", ""))
            status, message = _raft_status(res)
            return reply(status, message)

        # Exactly-once: if this request_id was already executed (the client is
        # retrying after a lost reply), return that result instead of
        # interpreting the sentence again against the changed state.
        if self.raft_node is not None:
            prior = self.raft_node.state_machine.cached_result(request_id)
            if prior is not None:
                return done(prior)

        pending = self._take_pending(user_id)
        if is_dismissal(text):
            logger.info(f"ASSIST user '{user_id}': {text!r} -> DISMISS")
            return reply("success", "OK, I won't book or cancel anything. Tell me when you need something."
                         if pending else "OK. Tell me what you would like to book or cancel.")

        slots = self.booking_service.get_availability().get("tickets", {})
        mine = {bid: b for bid, b in self.booking_service.bookings.items() if b.get("user_id") == user_id}
        cities = known_cities(slots.values())
        centres = centre_aliases(slots.values())
        rules = rule_based_intent(text, cities, centres)
        slot_id = find_slot_id(text, slots)
        confirm = bool(pending and _CONFIRM_RE.match(text))
        # A single unknown word after "which city?" is a place we have no centre in ("pune").
        word_place = (text.strip().title() if pending and not confirm and not slot_id
                      and re.fullmatch(r"[A-Za-z]{3,}", text.strip()) and not any(rules.values()) else None)
        bare = confirm or bool(word_place) or is_bare_reply(text, rules, slot_id)
        # The model's intent is not needed when this is a reply to the assistant's question,
        # or when the rules already see a question (merge() would keep "question" anyway).
        skip_llm = (bare and pending) or rules["action"] == "question"
        llm = {} if skip_llm else self._extract_intent_llm(text, intent_context(slots.values()), cities)
        intent = merge(llm, rules, text)
        place = word_place or (None if intent.get("city") else unknown_place(text, cities, llm.get("_raw_city"), centres))
        # Naming a (different) vaccine, a booking ID or a slot starts a new request.
        fresh = not bare and any(intent.get(k) for k in ("vaccine", "booking_id")) or bool(slot_id and not bare)
        if pending and not fresh and (bare or (rules["action"] in (None, pending["action"])
                                               and intent["action"] == pending["action"])):
            # A reply to the assistant's question ("delhi", "dose 1", "yes", "book dose 2"):
            # complete the pending request with what this message adds.
            intent = continue_intent(pending, rules if bare else intent)
            if place:
                intent["city"] = None  # "at pune" replaces the earlier city
        elif bare:
            intent["action"] = "browse"  # "delhi" out of the blue: show the slots, don't book

        if intent["action"] in ("book", "browse") and place:
            plan = {"kind": "clarify", "message": (
                f"There are no vaccination slots in {place}. Slots are available in {', '.join(cities)}. "
                f"Which city would you like? (Say 'none' to stop.)")}
        elif confirm and pending.get("offer_slot"):
            plan = {"kind": "book", "slot_id": pending["offer_slot"]}   # "yes" to the offered slot
        elif intent["action"] in ("book", "browse") and slot_id:
            plan = ({"kind": "book", "slot_id": slot_id} if intent["action"] == "book" or pending
                    else {"kind": "clarify", "message": f"Do you want to book {slots[slot_id]['name']} "
                          f"({slot_id})? Say 'book {slot_id}' to confirm."})
        elif intent["action"] == "browse":
            plan = self._browse(intent, slots)
        else:
            plan = resolve(intent, slots, mine)
        if plan["kind"] == "book" and not intent.get("vaccine") and not slot_id and not confirm:
            # The user never named a vaccine ("book in Bangalore"): one slot matching is
            # not consent to book (and maybe pay for) it. Offer it and wait for "yes".
            plan = {"kind": "clarify", "offer_slot": plan["slot_id"],
                    "message": choose_message(intent, {plan["slot_id"]: slots[plan["slot_id"]]})}
        if plan["kind"] == "clarify" and intent["action"] in ("book", "cancel", "browse"):
            self._set_pending(user_id, {**intent, "action": "cancel" if intent["action"] == "cancel" else "book",
                                        "offer_slot": plan.get("offer_slot")})

        understood = ", ".join(f"{k}={v}" for k, v in intent.items() if v is not None and not k.startswith("_"))
        logger.info(f"ASSIST user '{user_id}': {text!r} -> understood [{understood or 'nothing'}] "
                    f"({'follow-up' if bare and pending else 'LLM' if llm else 'keyword rules'}) "
                    f"-> {plan['kind'].upper()}")

        if plan["kind"] == "clarify":
            return reply("clarify", plan["message"])
        if plan["kind"] == "question":
            return reply("success", self._query_llm(text, self._build_live_context(text, user_id)))

        if plan["kind"] == "book":
            return done(self._execute("create_booking", {
                "user_id": user_id, "ticket_id": plan["slot_id"], "quantity": 1,
                "priority": 0, "request_id": request_id,
            }))
        return done(self._execute("cancel_booking", {
            "user_id": user_id, "booking_id": plan["booking_id"], "request_id": request_id,
        }))

    _PENDING_TTL_S = 300  # a reply after 5 minutes starts a new conversation

    def _take_pending(self, user_id: str) -> Optional[dict]:
        with self._pending_lock:
            entry = self._pending.pop(user_id, None)
        if entry and time.monotonic() - entry[1] < self._PENDING_TTL_S:
            return entry[0]
        return None

    def _set_pending(self, user_id: str, intent: dict):
        with self._pending_lock:
            self._pending[user_id] = ({k: v for k, v in intent.items() if not k.startswith("_")}, time.monotonic())

    @staticmethod
    def _browse(intent: dict, slots: Dict[str, dict]) -> dict:
        """List open slots for a bare "delhi" / "covaxin" without booking anything."""
        def ok(s):
            return ((not intent.get("vaccine") or s.get("vaccine_name") == intent["vaccine"])
                    and (not intent.get("dose") or int(s.get("dose_number") or 0) == intent["dose"])
                    and (not intent.get("city") or slot_city(s) == intent["city"])
                    and int(s.get("available_count", 0)) > 0)
        found = {sid: s for sid, s in slots.items() if ok(s)}
        if not found:
            return {"kind": "clarify", "message": "No open slots match that. Try another city or vaccine."}
        return {"kind": "clarify", "message": choose_message(intent, found),
                "offer_slot": next(iter(found)) if len(found) == 1 else None}

    # Keywords that indicate the user is asking about live inventory
    _AVAILABILITY_KEYWORDS = (
        "available", "availability", "seat", "slot", "how many",
        "remaining", "left", "count", "vacant", "open", "book",
    )

    def _build_live_context(self, query: str, user_id: Optional[str] = None) -> str:
        """Live context for an FAQ: the focused availability summary (when the
        question is about availability) followed by the full live state after
        STATE_MARKER. The small model uses only the first part; the larger
        model reads the full state as its one page of live data."""
        return f"{self._focused_availability(query)}\n{STATE_MARKER}\n{self._full_state(user_id)}".strip()

    def _full_state(self, user_id: Optional[str]) -> str:
        """Every slot and the asking citizen's appointments, from Raft-replicated state."""
        from datetime import datetime
        from dist_ticket_booking.business.booking_service import IST
        lines = [f"Today: {datetime.now(IST).strftime('%Y-%m-%d')}", "Slots:"]
        for sid, s in sorted(self.booking_service.get_availability().get("tickets", {}).items()):
            price = "free" if not s.get("price") else f"Rs.{float(s['price']):.0f}"
            seats = f"{s.get('available_count', 0)} seats open" if s.get("available_count", 0) > 0 else "SOLD OUT"
            lines.append(f"- {s.get('name', sid)} | {sid} | dose {s.get('dose_number')} | {s.get('date')} "
                         f"{s.get('time')} | {seats} | {price}")
        mine = [b for b in self.booking_service.bookings.values()
                if user_id and b.get("user_id") == user_id and b.get("status") == "confirmed"]
        lines.append("This citizen's appointments:")
        lines += [f"- {b['booking_id']}: {b['vaccine_name']} dose {b['dose_number']} at {b['center_name']} on "
                  f"{b['scheduled_date']} {b.get('scheduled_time', '')}" for b in mine] or ["- none"]
        return "\n".join(lines)

    def _focused_availability(self, query: str) -> str:
        """A plain-text summary of live slot inventory if the query seems to
        be about availability; otherwise an empty string."""
        q = query.lower()
        if not any(kw in q for kw in self._AVAILABILITY_KEYWORDS):
            return ""

        all_slots = self.booking_service.get_availability()
        tickets = all_slots.get("tickets", {})
        if not tickets:
            return ""

        # Focus the context on the slots the user asked about (vaccine, city,
        # centre, dose) so the small model isn't distracted by unrelated rows.
        words = {w for w in re.findall(r"[a-z0-9]+", q) if len(w) >= 4 or w.isdigit()}
        def _haystack(info):
            return " ".join(
                str(info.get(k, "")) for k in ("name", "vaccine_name", "center_name", "dose_number")
            ).lower()
        focused = {sid: i for sid, i in tickets.items() if words & set(re.findall(r"[a-z0-9]+", _haystack(i)))}
        if focused:
            tickets = focused

        lines = ["Current live slot availability:"]
        for slot_id, info in tickets.items():
            count = info.get("available_count", 0)
            name = info.get("name", slot_id)
            date = info.get("date", "")
            center = info.get("center_name", "")
            price = info.get("price", 0.0)
            price_str = "free of charge" if price == 0.0 else f"paid, Rs. {price:.0f}"
            if count > 0:
                lines.append(f"- {name}: {count} open slot(s) on {date}; price: {price_str}")
            else:
                lines.append(f"- {name}: SOLD OUT on {date}")
        return "\n".join(lines)

    def _query_llm(self, query: str, context: str = "") -> str:
        self._tl.source = "LLM"
        if not (self.llm_stub or self.llm_service):
            self._tl.source = "keyword fallback, no LLM"
            return _fallback_answer(query, context)
        if not _llm_slots.acquire(blocking=False):
            logger.debug("LLM busy (max in-flight reached); using keyword fallback")
            self._tl.source = "keyword fallback, LLM busy"
            return _fallback_answer(query, context)
        try:
            return self._query_llm_unthrottled(query, context)
        finally:
            _llm_slots.release()

    def _query_llm_unthrottled(self, query: str, context: str = "") -> str:
        if self.llm_stub:
            try:
                req = ticket_service_pb2.LLMQueryRequest(
                    request_id=str(uuid.uuid4()),
                    query=query,
                    context=context or "TicketClientFAQ",
                )
                res = self.llm_stub.GetLLMAnswer(req, timeout=LLM_TIMEOUT_S)
                return res.answer
            except grpc.RpcError as e:
                logger.warning(f"LLM server unavailable ({e.code().name}); using keyword fallback")
                self._tl.source = "keyword fallback, LLM down"
            except Exception as e:
                logger.warning(f"LLM server error: {e}; using keyword fallback")
                self._tl.source = "keyword fallback, LLM error"
        elif self.llm_service:
            res = self.llm_service.get_llm_answer(str(uuid.uuid4()), query, context)
            return res.get("answer", "")
        return _fallback_answer(query, context)


class TicketAppServicer(ticket_service_pb2_grpc.TicketAppServiceServicer):
    """Internal application servicer for business requests."""

    def __init__(
        self,
        booking_service: Optional[BookingService] = None,
        llm_stub: Optional[ticket_service_pb2_grpc.LLMServiceStub] = None,
        llm_service: Optional[DomainLLM] = None,
        raft_node: Optional[Any] = None,
    ):
        self.raft_node = raft_node
        if raft_node is not None:
            self.booking_service = raft_node.state_machine.booking_service
        else:
            self.booking_service = booking_service or BookingService()
        self.llm_stub = llm_stub
        self.llm_service = llm_service

    def _write(self, command_type: str, command: dict, standalone):
        if self.raft_node is None:
            res = standalone()
            return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))
        res = self.raft_node.propose(command_type, command)
        if res.get("message") == "not_leader":
            return TicketClientServicer._redirect(res, self.raft_node)
        status, message = _raft_status(res)
        return ticket_service_pb2.StatusResponse(status=status, message=message)

    def ProcessBusinessRequest(self, request, context):
        """Internal application-server API (called by trusted services, so
        the acting user is named in the payload rather than by a token)."""
        try:
            payload = _parse_payload(request.payload)
            action = payload.get("action", "booking")

            if action == "booking":
                user_id = _field(payload, "user_id", str)
                cmd = _booking_command(user_id, payload, request.request_id)
                return self._write("create_booking", cmd, lambda: self.booking_service.create_booking(
                    user_id, cmd["ticket_id"], cmd["quantity"], cmd["priority"],
                    now=cmd["requested_at"], payment_method=cmd["payment_method"]))

            elif action == "cancel":
                user_id = _field(payload, "user_id", str)
                cmd = _cancel_command(user_id, payload, request.request_id)
                return self._write("cancel_booking", cmd, lambda: self.booking_service.cancel_booking(
                    user_id, cmd["booking_id"], now=cmd["requested_at"]))

            elif action in ("faq", "llm"):
                query = payload.get("query", "")
                if self.llm_stub:
                    try:
                        llm_res = self.llm_stub.GetLLMAnswer(
                            ticket_service_pb2.LLMQueryRequest(
                                request_id=request.request_id, query=query, context=request.context
                            ),
                            timeout=LLM_TIMEOUT_S,
                        )
                        return ticket_service_pb2.StatusResponse(status="success", message=llm_res.answer)
                    except grpc.RpcError as e:
                        logger.warning(f"LLM server unavailable ({e.code().name}); using keyword fallback")
                        return ticket_service_pb2.StatusResponse(
                            status="success", message=_fallback_answer(query)
                        )
                elif self.llm_service:
                    ans = self.llm_service.get_llm_answer(request.request_id, query)["answer"]
                    return ticket_service_pb2.StatusResponse(status="success", message=ans)

            return ticket_service_pb2.StatusResponse(
                status="error", message=f"Unknown action {action!r} (use booking, cancel or faq)")
        except PayloadError as e:
            return ticket_service_pb2.StatusResponse(status="invalid", message=str(e))
        except Exception as e:
            logger.error(f"ProcessBusinessRequest error: {e}")
            return ticket_service_pb2.StatusResponse(status="error", message=str(e))

    # ── Cluster administration ──

    def _admin_reply(self, res: dict):
        if res.get("message") == "not_leader":
            return ticket_service_pb2.StatusResponse(
                status="redirect", message="not_leader",
                leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
            )
        return ticket_service_pb2.StatusResponse(
            status=res.get("status", "error"), message=res.get("message", ""),
            leader_hint=res.get("leader_hint", ""),
        )

    def _require_admin(self, token: str):
        """None if `token` belongs to an administrator, else an error reply."""
        sm = self.raft_node.state_machine
        user = sm.get_session_user(token)
        if not user:
            return ticket_service_pb2.StatusResponse(status="unauthorized", message="admin login required")
        if sm.get_role(user) != ROLE_ADMIN:
            return ticket_service_pb2.StatusResponse(status="forbidden", message=f"{user!r} is not an administrator")
        return None

    def ChangeMembership(self, request, context):
        if self.raft_node is None:
            return ticket_service_pb2.StatusResponse(status="error", message="not running Raft")
        if (denied := self._require_admin(request.token)) is not None:
            return denied
        return self._admin_reply(self.raft_node.change_membership(
            request.action, request.node_id, request.host, request.raft_port, request.client_port))

    def TransferLeadership(self, request, context):
        if self.raft_node is None:
            return ticket_service_pb2.StatusResponse(status="error", message="not running Raft")
        if (denied := self._require_admin(request.token)) is not None:
            return denied
        return self._admin_reply(self.raft_node.transfer_leadership(request.target_node))

    def GetLLMAnswer(self, request, context):
        if self.llm_stub:
            try:
                return self.llm_stub.GetLLMAnswer(request, timeout=LLM_TIMEOUT_S)
            except grpc.RpcError:
                return ticket_service_pb2.GetLLMAnswerResponse(
                    request_id=request.request_id, answer=_fallback_answer(request.query)
                )
        if self.llm_service:
            ans = self.llm_service.get_llm_answer(request.request_id, request.query, request.context)
            return ticket_service_pb2.GetLLMAnswerResponse(request_id=request.request_id, answer=ans["answer"])
        return ticket_service_pb2.GetLLMAnswerResponse(request_id=request.request_id, answer="Service unavailable")


class LLMServicer(ticket_service_pb2_grpc.LLMServiceServicer):
    """Servicer for the standalone LLM service."""

    def __init__(self, llm_service: DomainLLM):
        self.llm_service = llm_service

    def GetLLMAnswer(self, request, context):
        t0 = time.monotonic()
        logger.info(f"FAQ question: {request.query!r}")
        result = self.llm_service.get_llm_answer(request.request_id, request.query, request.context)
        logger.info(f"   answer ({time.monotonic() - t0:.1f}s): {result['answer']}")
        return ticket_service_pb2.GetLLMAnswerResponse(
            request_id=result["request_id"],
            answer=result["answer"],
        )

    def ExtractIntent(self, request, context):
        t0 = time.monotonic()
        logger.info(f"Booking request: {request.query!r}")
        result = self.llm_service.extract_intent(request.request_id, request.query, request.context)
        logger.info(f"   intent ({time.monotonic() - t0:.1f}s): {result['answer']}")
        return ticket_service_pb2.GetLLMAnswerResponse(request_id=result["request_id"], answer=result["answer"])


class TicketClient:
    """Client interface for interacting with the ticket server with auto leader redirection.

    Election-aware: while the cluster has no known leader (redirect without a
    hint) or the node we were redirected to is unreachable, the client backs
    off and retries, returning to its original seed address when needed.
    Writes carry a request_id that stays the same across retries and the
    replicated state machine de-duplicates on it, so a retried booking or
    cancel is applied exactly once even if the original reply was lost.
    """

    RETRY_BACKOFF_S = 0.3
    MAX_RETRY_WAIT_S = 6.0  # covers an election (600-1200 ms) plus a couple of retries

    def __init__(self, host: str = "localhost", port: int = 50051, seeds: Optional[list] = None):
        """`seeds` (optional) lists other cluster nodes as "host:port"; the client
        rotates through them if the node it is talking to becomes unreachable."""
        first = f"{host}:{port}"
        self.seeds = [first] + [t for t in (seeds or []) if t != first]
        self._seed_idx = 0
        self.seed_target = first
        self.token: Optional[str] = None
        self.channel = None
        self._connect(self.seed_target)

    @classmethod
    def for_cluster(cls, targets: list) -> "TicketClient":
        """Client for a cluster given as ["host:port", ...]."""
        host, port = targets[0].rsplit(":", 1)
        return cls(host=host, port=int(port), seeds=targets[1:])

    def _next_seed(self):
        self._seed_idx = (self._seed_idx + 1) % len(self.seeds)
        self.seed_target = self.seeds[self._seed_idx]
        self._connect(self.seed_target)

    def _connect(self, target: str):
        if self.channel is not None:
            try:
                self.channel.close()
            except Exception:
                pass
        self.target = target
        parts = target.split(":")
        self.host = parts[0]
        self.port = int(parts[1]) if len(parts) > 1 else 50051
        from dist_ticket_booking.raft.node import CHANNEL_OPTIONS
        self.channel = grpc.insecure_channel(self.target, options=CHANNEL_OPTIONS)
        self.client_stub = ticket_service_pb2_grpc.TicketClientServiceStub(self.channel)
        self.app_stub = ticket_service_pb2_grpc.TicketAppServiceStub(self.channel)

    def _reconnect_to_leader(self, leader_hint: str):
        if not leader_hint or leader_hint == self.target:
            return
        logger.info(f"[TicketClient] Redirecting from {self.target} to leader at {leader_hint}")
        self._connect(leader_hint)

    def close(self):
        try:
            self.channel.close()
        except Exception:
            pass

    def _call(self, method: str, request, max_redirects: int = 3, stub: str = "client_stub"):
        """Invoke an RPC, following redirects and riding out elections.

        The retry window starts at the first failure, not at the first attempt:
        a slow call (e.g. an LLM answer) that dies with its server must still
        get the full window to find the new leader."""
        deadline = None
        redirects = 0
        res = None
        while True:
            try:
                res = getattr(getattr(self, stub), method)(request)
            except grpc.RpcError as e:
                # The server is unreachable or went away mid-call. Retrying is
                # safe: reads are idempotent and writes carry a request_id.
                if deadline is None:
                    deadline = time.monotonic() + self.MAX_RETRY_WAIT_S
                if e.code() != grpc.StatusCode.UNAVAILABLE or time.monotonic() >= deadline:
                    raise
                if self.target != self.seed_target:
                    self._connect(self.seed_target)  # leader we were sent to is gone
                elif len(self.seeds) > 1:
                    self._next_seed()  # our seed node is down: try another one
                time.sleep(self.RETRY_BACKOFF_S)
                continue

            if deadline is None:
                deadline = time.monotonic() + self.MAX_RETRY_WAIT_S
            if res.status == "unavailable" and len(self.seeds) > 1 and time.monotonic() < deadline:
                # This node cannot reach a majority (e.g. it is on the minority
                # side of a partition); another node may have a working leader.
                # Writes keep their request_id, so retrying elsewhere is safe.
                self._next_seed()
                time.sleep(self.RETRY_BACKOFF_S)
                continue
            if res.status != "redirect":
                return res
            if res.leader_hint and res.leader_hint != self.target and redirects < max_redirects:
                redirects += 1
                self._reconnect_to_leader(res.leader_hint)
                continue
            # No leader known yet (election in progress): back off and retry.
            if time.monotonic() >= deadline:
                return res
            time.sleep(self.RETRY_BACKOFF_S)

    def register(self, username: str, password: str, role: str = ROLE_CITIZEN, admin_code: str = ""):
        """Create an account (role "citizen", or "admin" with the sign-up code)."""
        req = ticket_service_pb2.RegisterRequest(username=username, password=password, role=role,
                                                 admin_code=admin_code)
        return self._call("Register", req)

    def login(self, username: str, password: str = "password", max_redirects: int = 3,
              auto_register: bool = True, role: str = ROLE_CITIZEN, admin_code: str = ""):
        """Log in. The server never creates accounts implicitly; for demo and
        load-test convenience this client registers the account first when
        the server answers `unknown_user` (pass auto_register=False to
        disable). A wrong password for an existing account is never retried."""
        req = ticket_service_pb2.LoginRequest(username=username, password=password)
        res = self._call("Login", req, max_redirects)
        if res.status == "unknown_user" and auto_register:
            reg = self.register(username, password, role, admin_code)
            # "already taken" means another client registered the name in
            # the meantime: just log in. Any other failure is reported as is.
            if reg.status != "success" and "already taken" not in reg.message:
                return ticket_service_pb2.LoginResponse(status=reg.status, message=reg.message)
            res = self._call("Login", req, max_redirects)
        if res.status == "success" and res.token:
            self.token = res.token
        return res

    def login_admin(self, username: str = "admin", password: str = "admin-password",
                    admin_code: str = DEFAULT_ADMIN_CODE):
        """Log in as an administrator, creating the admin account if needed."""
        return self.login(username, password, role=ROLE_ADMIN, admin_code=admin_code)

    def logout(self, token: Optional[str] = None, max_redirects: int = 3):
        tok = token or self.token
        req = ticket_service_pb2.LogoutRequest(token=tok or "")
        res = self._call("Logout", req, max_redirects)
        if tok == self.token and res.status == "success":
            self.token = None
        return res

    def post(self, token: str, req_type: str, data: Any, max_redirects: int = 3,
             request_id: Optional[str] = None):
        """Send a write. The same `request_id` is used for every retry, so the
        cluster applies the write at most once even if a reply is lost; pass an
        earlier id explicitly to learn the outcome of an uncertain request."""
        payload = data if isinstance(data, str) else json.dumps(data)
        req = ticket_service_pb2.PostRequest(
            token=token, type=req_type, data=payload, request_id=request_id or uuid.uuid4().hex
        )
        return self._call("Post", req, max_redirects)

    def assistant(self, token: str, text: str, request_id: Optional[str] = None):
        """Natural-language request, e.g. "book a COVAXIN second dose in Mumbai".

        Returns a StatusResponse: accepted / success (done), clarify (the
        assistant needs more detail, see message), or an error status."""
        return self.post(token, "assistant", {"text": text}, request_id=request_id)

    def get(self, token: str, req_type: str, params: Optional[Any] = None, max_redirects: int = 3):
        param_str = None
        if params is not None:
            param_str = params if isinstance(params, str) else json.dumps(params)
        req = ticket_service_pb2.GetRequest(token=token, type=req_type, params=param_str)
        return self._call("Get", req, max_redirects)

    # ── Cluster administration ──

    def change_membership(self, action: str, node_id: str, host: str = "",
                          raft_port: int = 0, client_port: int = 0):
        """Add or remove one server (sent to the leader). Requires login_admin()."""
        req = ticket_service_pb2.MembershipChangeRequest(
            action=action, node_id=node_id, host=host, raft_port=raft_port, client_port=client_port,
            token=self.token or "")
        return self._call("ChangeMembership", req, stub="app_stub")

    def transfer_leadership(self, target_node: str = ""):
        """Ask the leader to hand leadership to `target_node` (default: best follower)."""
        req = ticket_service_pb2.TransferLeadershipRequest(target_node=target_node, token=self.token or "")
        return self._call("TransferLeadership", req, stub="app_stub")

    def process_business_request(self, request_id: str, payload: Any, context: str = "", max_redirects: int = 3):
        payload_str = payload if isinstance(payload, str) else json.dumps(payload)
        req = ticket_service_pb2.BusinessRequest(request_id=request_id, payload=payload_str, context=context)
        for _ in range(max_redirects):
            res = self.app_stub.ProcessBusinessRequest(req)
            if res.status == "redirect" and res.leader_hint:
                self._reconnect_to_leader(res.leader_hint)
                continue
            return res
        return res


class TicketServiceStub:
    """In-memory stub used for unit tests."""

    def __init__(self, ticket_store=None):
        self.booking_service = BookingService(ticket_store=ticket_store)

    def create_booking(self, user_id: str, ticket_id: str, quantity: int = 1, priority: int = 0):
        return self.booking_service.create_booking(user_id, ticket_id, quantity, priority)

    def get_ticket_availability(self, ticket_id: str):
        return self.booking_service.get_availability(ticket_id)

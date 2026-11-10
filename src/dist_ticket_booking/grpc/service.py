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
from dist_ticket_booking.llm.domain_llm import DomainLLM, _keyword_fallback

logger = logging.getLogger(__name__)

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

    def _verify_token(self, token: str) -> Optional[str]:
        if self.raft_node is not None:
            return self.raft_node.state_machine.get_session_user(token)
        return self.sessions.get(token)

    def Login(self, request, context):
        if not (request.username and (request.password == "password" or len(request.password) > 0)):
            return ticket_service_pb2.LoginResponse(status="unauthorized")

        if self.raft_node is not None:
            token = str(uuid.uuid4())
            res = self.raft_node.propose("login", {"username": request.username, "token": token})
            if res.get("message") == "not_leader":
                return ticket_service_pb2.LoginResponse(
                    status="redirect",
                    leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                )
            if res.get("status") == "success":
                return ticket_service_pb2.LoginResponse(status="success", token=res.get("token", token))
            status, message = _raft_status(res)
            return ticket_service_pb2.LoginResponse(status=status, message=message)

        token = str(uuid.uuid4())
        self.sessions[token] = request.username
        return ticket_service_pb2.LoginResponse(status="success", token=token)

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
            data = json.loads(request.data) if request.data else {}
            if request.type == "booking":
                ticket_id = data.get("ticket_id")
                quantity = int(data.get("quantity", 1))
                priority = int(data.get("priority", 0))

                if self.raft_node is not None:
                    res = self.raft_node.propose(
                        "create_booking",
                        {
                            "user_id": user_id,
                            "ticket_id": ticket_id,
                            "quantity": quantity,
                            "priority": priority,
                            "request_id": request.request_id,
                        },
                    )
                    if res.get("message") == "not_leader":
                        return ticket_service_pb2.StatusResponse(
                            status="redirect",
                            message="not_leader",
                            leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                        )
                    status, message = _raft_status(res)
                    return ticket_service_pb2.StatusResponse(status=status, message=message)

                res = self.booking_service.create_booking(user_id, ticket_id, quantity, priority)
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            elif request.type == "cancel":
                booking_id = data.get("booking_id")
                if self.raft_node is not None:
                    res = self.raft_node.propose(
                        "cancel_booking",
                        {
                            "user_id": user_id,
                            "booking_id": booking_id,
                            "request_id": request.request_id,
                        },
                    )
                    if res.get("message") == "not_leader":
                        return ticket_service_pb2.StatusResponse(
                            status="redirect",
                            message="not_leader",
                            leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                        )
                    status, message = _raft_status(res)
                    return ticket_service_pb2.StatusResponse(status=status, message=message)

                res = self.booking_service.cancel_booking(user_id, booking_id)
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            return ticket_service_pb2.StatusResponse(status="error", message=f"Unknown type: {request.type}")
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

            elif request.type == "faq":
                params = json.loads(request.params) if request.params else {}
                query = params.get("query", request.params or "")
                live_context = self._build_live_context(query)
                answer = self._query_llm(query, live_context)
                items = [ticket_service_pb2.DataItem(id="faq_answer", data=answer)]
                return ticket_service_pb2.GetResponse(status="success", items=items)

            return ticket_service_pb2.GetResponse(status="error")
        except Exception as e:
            logger.error(f"Get error: {e}")
            return ticket_service_pb2.GetResponse(status="error")

    # Keywords that indicate the user is asking about live inventory
    _AVAILABILITY_KEYWORDS = (
        "available", "availability", "seat", "slot", "how many",
        "remaining", "left", "count", "vacant", "open", "book",
    )

    def _build_live_context(self, query: str) -> str:
        """Return a plain-text summary of live slot inventory if the query
        seems to be about availability; otherwise return an empty string.
        """
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
        if not (self.llm_stub or self.llm_service):
            return _fallback_answer(query, context)
        if not _llm_slots.acquire(blocking=False):
            logger.info("LLM busy (max in-flight reached); using keyword fallback")
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
            except Exception as e:
                logger.warning(f"LLM server error: {e}; using keyword fallback")
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

    def ProcessBusinessRequest(self, request, context):
        try:
            payload = json.loads(request.payload) if request.payload else {}
            action = payload.get("action", "booking")

            if action == "booking":
                user_id = payload.get("user_id", "user-system")
                ticket_id = payload.get("ticket_id")
                quantity = int(payload.get("quantity", 1))
                priority = int(payload.get("priority", 0))

                if self.raft_node is not None:
                    res = self.raft_node.propose(
                        "create_booking",
                        {
                            "user_id": user_id,
                            "ticket_id": ticket_id,
                            "quantity": quantity,
                            "priority": priority,
                            "request_id": request.request_id,
                        },
                    )
                    if res.get("message") == "not_leader":
                        return ticket_service_pb2.StatusResponse(
                            status="redirect",
                            message="not_leader",
                            leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                        )
                    status, message = _raft_status(res)
                    return ticket_service_pb2.StatusResponse(status=status, message=message)

                res = self.booking_service.create_booking(user_id, ticket_id, quantity, priority)
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            elif action == "cancel":
                if self.raft_node is not None:
                    res = self.raft_node.propose(
                        "cancel_booking",
                        {
                            "user_id": payload.get("user_id"),
                            "booking_id": payload.get("booking_id"),
                        },
                    )
                    if res.get("message") == "not_leader":
                        return ticket_service_pb2.StatusResponse(
                            status="redirect",
                            message="not_leader",
                            leader_hint=res.get("leader_hint", "") or (self.raft_node.get_leader_address() or ""),
                        )
                    status, message = _raft_status(res)
                    return ticket_service_pb2.StatusResponse(status=status, message=message)

                res = self.booking_service.cancel_booking(payload.get("user_id"), payload.get("booking_id"))
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

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

            return ticket_service_pb2.StatusResponse(status="processed", message=f"Processed request {request.request_id}")
        except Exception as e:
            logger.error(f"ProcessBusinessRequest error: {e}")
            return ticket_service_pb2.StatusResponse(status="error", message=str(e))

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
        result = self.llm_service.get_llm_answer(request.request_id, request.query, request.context)
        return ticket_service_pb2.GetLLMAnswerResponse(
            request_id=result["request_id"],
            answer=result["answer"],
        )


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
        self.channel = grpc.insecure_channel(self.target)
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

    def _call(self, method: str, request, max_redirects: int = 3):
        """Invoke a client RPC, following redirects and riding out elections."""
        deadline = time.monotonic() + self.MAX_RETRY_WAIT_S
        redirects = 0
        res = None
        while True:
            try:
                res = getattr(self.client_stub, method)(request)
            except grpc.RpcError as e:
                # Connection-level failure: request never reached a server.
                if e.code() != grpc.StatusCode.UNAVAILABLE or time.monotonic() >= deadline:
                    raise
                if self.target != self.seed_target:
                    self._connect(self.seed_target)  # leader we were sent to is gone
                elif len(self.seeds) > 1:
                    self._next_seed()  # our seed node is down: try another one
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

    def login(self, username: str, password: str = "password", max_redirects: int = 3):
        req = ticket_service_pb2.LoginRequest(username=username, password=password)
        res = self._call("Login", req, max_redirects)
        if res.status == "success" and res.token:
            self.token = res.token
        return res

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

    def get(self, token: str, req_type: str, params: Optional[Any] = None, max_redirects: int = 3):
        param_str = None
        if params is not None:
            param_str = params if isinstance(params, str) else json.dumps(params)
        req = ticket_service_pb2.GetRequest(token=token, type=req_type, params=param_str)
        return self._call("Get", req, max_redirects)

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

"""gRPC servicers and client wrapper for ticket booking."""
import json
import logging
import uuid
from typing import Dict, Any, Optional

import grpc

from . import ticket_service_pb2
from . import ticket_service_pb2_grpc
from dist_ticket_booking.business.booking_service import BookingService
from dist_ticket_booking.llm.domain_llm import DomainLLM

logger = logging.getLogger(__name__)


class TicketClientServicer(ticket_service_pb2_grpc.TicketClientServiceServicer):
    """Client-facing gRPC handler."""

    def __init__(
        self,
        booking_service: BookingService,
        sessions: Dict[str, str],
        llm_stub: Optional[ticket_service_pb2_grpc.LLMServiceStub] = None,
        llm_service: Optional[DomainLLM] = None,
    ):
        self.booking_service = booking_service
        self.sessions = sessions
        self.llm_stub = llm_stub
        self.llm_service = llm_service

    def _verify_token(self, token: str) -> Optional[str]:
        return self.sessions.get(token)

    def Login(self, request, context):
        if request.username and (request.password == "password" or len(request.password) > 0):
            token = str(uuid.uuid4())
            self.sessions[token] = request.username
            return ticket_service_pb2.LoginResponse(status="success", token=token)
        return ticket_service_pb2.LoginResponse(status="unauthorized")

    def Logout(self, request, context):
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
                res = self.booking_service.create_booking(user_id, ticket_id, quantity, priority)
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            elif request.type == "cancel":
                booking_id = data.get("booking_id")
                res = self.booking_service.cancel_booking(user_id, booking_id)
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            return ticket_service_pb2.StatusResponse(status="error", message=f"Unknown type: {request.type}")
        except Exception as e:
            logger.error(f"Post error: {e}")
            return ticket_service_pb2.StatusResponse(status="error", message=str(e))

    def Get(self, request, context):
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
        The summary is injected as context into the LLM prompt so the model
        can answer with real numbers instead of generic advice.
        """
        q = query.lower()
        if not any(kw in q for kw in self._AVAILABILITY_KEYWORDS):
            return ""

        all_slots = self.booking_service.get_availability()
        tickets = all_slots.get("tickets", {})
        if not tickets:
            return ""

        lines = ["Current live slot availability:"]
        for slot_id, info in tickets.items():
            count = info.get("available_count", 0)
            name = info.get("name", slot_id)
            date = info.get("date", "")
            center = info.get("center_name", "")
            price = info.get("price", 0.0)
            price_str = "Free" if price == 0.0 else f"Rs. {price:.0f}"
            status = "available" if count > 0 else "sold out"
            lines.append(
                f"- {name}: {count} slot(s) {status} | Date: {date} | "
                f"Center: {center} | {price_str}"
            )
        return "\n".join(lines)

    def _query_llm(self, query: str, context: str = "") -> str:
        if self.llm_stub:
            try:
                req = ticket_service_pb2.LLMQueryRequest(
                    request_id=str(uuid.uuid4()),
                    query=query,
                    context=context or "TicketClientFAQ",
                )
                res = self.llm_stub.GetLLMAnswer(req)
                return res.answer
            except Exception as e:
                logger.warning(f"Could not reach LLM server: {e}")
        if self.llm_service:
            res = self.llm_service.get_llm_answer(str(uuid.uuid4()), query, context)
            return res.get("answer", "")
        return "Service unavailable"


class TicketAppServicer(ticket_service_pb2_grpc.TicketAppServiceServicer):
    """Internal application servicer for business requests."""

    def __init__(
        self,
        booking_service: BookingService,
        llm_stub: Optional[ticket_service_pb2_grpc.LLMServiceStub] = None,
        llm_service: Optional[DomainLLM] = None,
    ):
        self.booking_service = booking_service
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
                res = self.booking_service.create_booking(user_id, ticket_id, quantity, priority)
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            elif action == "cancel":
                res = self.booking_service.cancel_booking(payload.get("user_id"), payload.get("booking_id"))
                return ticket_service_pb2.StatusResponse(status=res["status"], message=res.get("message", ""))

            elif action in ("faq", "llm"):
                query = payload.get("query", "")
                if self.llm_stub:
                    llm_res = self.llm_stub.GetLLMAnswer(
                        ticket_service_pb2.LLMQueryRequest(request_id=request.request_id, query=query, context=request.context)
                    )
                    return ticket_service_pb2.StatusResponse(status="success", message=llm_res.answer)
                elif self.llm_service:
                    ans = self.llm_service.get_llm_answer(request.request_id, query)["answer"]
                    return ticket_service_pb2.StatusResponse(status="success", message=ans)

            return ticket_service_pb2.StatusResponse(status="processed", message=f"Processed request {request.request_id}")
        except Exception as e:
            logger.error(f"ProcessBusinessRequest error: {e}")
            return ticket_service_pb2.StatusResponse(status="error", message=str(e))

    def GetLLMAnswer(self, request, context):
        if self.llm_stub:
            return self.llm_stub.GetLLMAnswer(request)
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
    """Client interface for interacting with the ticket server."""

    def __init__(self, host: str = "localhost", port: int = 50051):
        self.target = f"{host}:{port}"
        self.channel = grpc.insecure_channel(self.target)
        self.client_stub = ticket_service_pb2_grpc.TicketClientServiceStub(self.channel)
        self.app_stub = ticket_service_pb2_grpc.TicketAppServiceStub(self.channel)
        self.token: Optional[str] = None

    def close(self):
        self.channel.close()

    def login(self, username: str, password: str = "password"):
        req = ticket_service_pb2.LoginRequest(username=username, password=password)
        res = self.client_stub.Login(req)
        if res.status == "success" and res.token:
            self.token = res.token
        return res

    def logout(self, token: Optional[str] = None):
        tok = token or self.token
        req = ticket_service_pb2.LogoutRequest(token=tok or "")
        res = self.client_stub.Logout(req)
        if tok == self.token:
            self.token = None
        return res

    def post(self, token: str, req_type: str, data: Any):
        payload = data if isinstance(data, str) else json.dumps(data)
        req = ticket_service_pb2.PostRequest(token=token, type=req_type, data=payload)
        return self.client_stub.Post(req)

    def get(self, token: str, req_type: str, params: Optional[Any] = None):
        param_str = None
        if params is not None:
            param_str = params if isinstance(params, str) else json.dumps(params)
        req = ticket_service_pb2.GetRequest(token=token, type=req_type, params=param_str)
        return self.client_stub.Get(req)

    def process_business_request(self, request_id: str, payload: Any, context: str = ""):
        payload_str = payload if isinstance(payload, str) else json.dumps(payload)
        req = ticket_service_pb2.BusinessRequest(request_id=request_id, payload=payload_str, context=context)
        return self.app_stub.ProcessBusinessRequest(req)


class TicketServiceStub:
    """In-memory stub used for unit tests."""

    def __init__(self, ticket_store=None):
        self.booking_service = BookingService(ticket_store=ticket_store)

    def create_booking(self, user_id: str, ticket_id: str, quantity: int = 1, priority: int = 0):
        return self.booking_service.create_booking(user_id, ticket_id, quantity, priority)

    def get_ticket_availability(self, ticket_id: str):
        return self.booking_service.get_availability(ticket_id)


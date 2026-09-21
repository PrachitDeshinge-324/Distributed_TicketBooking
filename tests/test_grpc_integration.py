"""Integration tests for gRPC services and concurrency."""
import concurrent.futures
import json
import uuid
import pytest
import grpc

from dist_ticket_booking.business.booking_service import BookingService, VaccinationService
from dist_ticket_booking.core.models import VaccineSlot, Ticket
from dist_ticket_booking.grpc import ticket_service_pb2
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.grpc.service import TicketClientServicer, TicketAppServicer, LLMServicer, TicketClient
from dist_ticket_booking.llm.domain_llm import DomainLLM


@pytest.fixture(scope="module")
def running_services():
    llm_service = DomainLLM()
    llm_server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=5))
    ticket_service_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(llm_service), llm_server)
    llm_port = llm_server.add_insecure_port("[::]:0")
    llm_server.start()

    llm_channel = grpc.insecure_channel(f"localhost:{llm_port}")
    llm_stub = ticket_service_pb2_grpc.LLMServiceStub(llm_channel)

    ticket_store = {
        "SLOT-CVS-D1-TEST": VaccineSlot(
            slot_id="SLOT-CVS-D1-TEST",
            vaccine_name="COVISHIELD",
            center_name="Test Health Center",
            center_pincode="000001",
            dose_number=1,
            date="2026-11-01",
            available_count=2,
            price=0.0,
        ),
    }
    booking_service = VaccinationService(ticket_store=ticket_store)
    sessions = {}

    app_server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    ticket_service_pb2_grpc.add_TicketClientServiceServicer_to_server(
        TicketClientServicer(booking_service, sessions, llm_stub=llm_stub, llm_service=llm_service),
        app_server,
    )
    ticket_service_pb2_grpc.add_TicketAppServiceServicer_to_server(
        TicketAppServicer(booking_service, llm_stub=llm_stub, llm_service=llm_service),
        app_server,
    )
    app_port = app_server.add_insecure_port("[::]:0")
    app_server.start()

    yield {"app_port": app_port, "llm_port": llm_port, "booking_service": booking_service}

    app_server.stop(0)
    llm_channel.close()
    llm_server.stop(0)


def test_full_client_workflow_via_grpc(running_services):
    port = running_services["app_port"]
    client = TicketClient(host="localhost", port=port)

    try:
        login_res = client.login("test_user", "password")
        assert login_res.status == "success"
        token = login_res.token
        assert token is not None

        get_res = client.get(token, "availability", {"ticket_id": "SLOT-CVS-D1-TEST"})
        assert get_res.status == "success"
        assert len(get_res.items) == 1
        data = json.loads(get_res.items[0].data)
        assert data["available_count"] == 2

        faq_res = client.get(token, "faq", {"query": "How do I cancel my vaccine appointment?"})
        assert faq_res.status == "success"
        assert len(faq_res.items) == 1
        # LLM answer should be a non-empty string
        assert len(faq_res.items[0].data) > 0

        post_res = client.post(token, "booking", {"ticket_id": "SLOT-CVS-D1-TEST", "quantity": 1})
        assert post_res.status == "accepted"

        get_after = client.get(token, "availability", {"ticket_id": "SLOT-CVS-D1-TEST"})
        data_after = json.loads(get_after.items[0].data)
        assert data_after["available_count"] == 1

        biz_res = client.process_business_request(
            request_id="REQ-001",
            payload={"action": "booking", "user_id": "test_user", "ticket_id": "SLOT-CVS-D1-TEST", "quantity": 1},
        )
        assert biz_res.status == "accepted"

        # now slot should be sold out
        post_rejected = client.post(token, "booking", {"ticket_id": "SLOT-CVS-D1-TEST", "quantity": 1})
        assert post_rejected.status == "rejected"

        logout_res = client.logout(token)
        assert logout_res.status == "success"

        unauth_res = client.get(token, "availability", {"ticket_id": "TEST-SEAT"})
        assert unauth_res.status == "unauthorized"

    finally:
        client.close()


def test_concurrency_control_prevents_overbooking():
    """Verify that multiple concurrent threads booking the same seat cannot overbook."""
    # Simulate 10 citizens racing for 3 available vaccine slots (overbooking prevention)
    service = VaccinationService({"SLOT-CVS-RUSH": {
        "name": "COVISHIELD Dose 1 — Rush Test Center",
        "vaccine_name": "COVISHIELD",
        "center_name": "Rush Test Center",
        "center_pincode": "999999",
        "dose_number": 1,
        "date": "2026-11-01",
        "available_count": 3,
        "price": 0.0,
        "status": "available",
    }})

    def book():
        return service.create_booking(user_id=str(uuid.uuid4()), ticket_id="SLOT-CVS-RUSH", quantity=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(book) for _ in range(10)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    accepted = [r for r in results if r["status"] == "accepted"]
    rejected = [r for r in results if r["status"] == "rejected"]

    assert len(accepted) == 3
    assert len(rejected) == 7
    assert service.get_availability("SLOT-CVS-RUSH")["available_count"] == 0

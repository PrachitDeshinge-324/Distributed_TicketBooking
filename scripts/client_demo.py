"""Sample client workflow to test the booking service endpoints."""
import json
import logging
import sys

from dist_ticket_booking.grpc.service import TicketClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run(host="localhost", port=50051):
    logger.info(f"Connecting to server at {host}:{port}")
    client = TicketClient(host=host, port=port)

    try:
        login_res = client.login("alice", "password")
        token = login_res.token
        if not token:
            logger.error("Login failed, aborting")
            return
        logger.info(f"Logged in successfully. Session token: {token[:8]}...")

        # check initial slot availability
        avail_res = client.get(token, "availability", {"ticket_id": "SLOT-CVS-D1-110001"})
        for item in avail_res.items:
            logger.info(f"Availability for {item.id}: {item.data}")

        # query support chatbot
        for query in ["How to cancel a booking?", "What documents do I need?"]:
            logger.info(f"FAQ question: {query}")
            faq_res = client.get(token, "faq", {"query": query})
            for item in faq_res.items:
                logger.info(f"Answer: {item.data}")

        # reserve a vaccine slot
        booking_payload = {"ticket_id": "SLOT-CVS-D1-110001", "quantity": 1, "priority": 1}
        post_res = client.post(token, "booking", booking_payload)
        logger.info(f"Booking status: {post_res.status} ({post_res.message})")

        # check updated availability
        updated = client.get(token, "availability", {"ticket_id": "SLOT-CVS-D1-110001"})
        for item in updated.items:
            logger.info(f"Updated availability: {item.data}")

        # test business request directly
        biz_res = client.process_business_request(
            request_id="req-test-1",
            payload={"action": "booking", "user_id": "alice", "ticket_id": "SLOT-CVX-D1-400001", "quantity": 1},
        )
        logger.info(f"Business request status: {biz_res.status} ({biz_res.message})")

        logout_res = client.logout(token)
        logger.info(f"Logout status: {logout_res.status}")

    finally:
        client.close()


if __name__ == "__main__":
    h = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    p = int(sys.argv[2]) if len(sys.argv) > 2 else 50051
    run(host=h, port=p)


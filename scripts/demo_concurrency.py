"""Simulate concurrent client requests to verify lock safety."""
import concurrent.futures
import json
import logging
import sys
import time

from dist_ticket_booking.grpc.service import TicketClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def book_seat(user_id, ticket_id, host, port):
    client = TicketClient(host=host, port=port)
    try:
        login = client.login(user_id, "password")
        if not login.token:
            return {"user": user_id, "status": "login_failed"}

        res = client.post(login.token, "booking", {"ticket_id": ticket_id, "quantity": 1})
        client.logout(login.token)
        return {"user": user_id, "status": res.status, "message": res.message}
    except Exception as e:
        return {"user": user_id, "status": "error", "message": str(e)}
    finally:
        client.close()


def run(host="localhost", port=50051, num_clients=10):
    admin = TicketClient(host=host, port=port)
    admin_login = admin.login("admin", "password")
    if not admin_login.token:
        logger.error("Could not connect to server")
        return

    data = json.loads(admin.get(admin_login.token, "availability", {"ticket_id": "SEAT-A1"}).items[0].data)
    initial_count = data.get("available_count", 0)
    logger.info(f"Target seat SEAT-A1 initial available count: {initial_count}")
    logger.info(f"Sending {num_clients} concurrent booking requests...")
    admin.logout()
    admin.close()

    start_time = time.time()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [
            executor.submit(book_seat, f"user_{i + 1}", "SEAT-A1", host, port)
            for i in range(num_clients)
        ]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    elapsed = time.time() - start_time
    accepted = [r for r in results if r["status"] == "accepted"]
    rejected = [r for r in results if r["status"] == "rejected"]

    logger.info(f"Completed in {elapsed:.3f}s: {len(accepted)} accepted, {len(rejected)} rejected")
    for r in results:
        logger.info(f"  {r['user']}: {r['status']} ({r.get('message', '')})")

    admin = TicketClient(host=host, port=port)
    admin_login = admin.login("admin", "password")
    final_data = json.loads(admin.get(admin_login.token, "availability", {"ticket_id": "SEAT-A1"}).items[0].data)
    remaining = final_data.get("available_count", 0)
    admin.logout()
    admin.close()

    logger.info(f"Remaining seats: {remaining}")
    expected_accepted = min(initial_count, num_clients)
    if len(accepted) == expected_accepted and remaining == (initial_count - expected_accepted):
        logger.info("Concurrency test passed: no race conditions or overbooking detected.")
    else:
        logger.warning(f"Mismatch: expected {expected_accepted} accepted, got {len(accepted)}")


if __name__ == "__main__":
    h = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    p = int(sys.argv[2]) if len(sys.argv) > 2 else 50051
    run(host=h, port=p)

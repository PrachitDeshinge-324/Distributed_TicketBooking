"""Simulate concurrent client requests to verify per-slot lock safety."""
import concurrent.futures
import json
import logging
import sys
import time

from dist_ticket_booking.grpc.service import TicketClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# A small slot so we can easily provoke rejections
TARGET_SLOT = "SLOT-CBX-BOS-560001"   # starts with 3 available slots


def book_slot(user_id, slot_id, host, port):
    client = TicketClient(host=host, port=port)
    try:
        login = client.login(user_id, "password")
        if not login.token:
            return {"user": user_id, "status": "login_failed"}
        res = client.post(login.token, "booking", {"ticket_id": slot_id, "quantity": 1})
        client.logout(login.token)
        return {"user": user_id, "status": res.status, "message": res.message}
    except Exception as exc:
        return {"user": user_id, "status": "error", "message": str(exc)}
    finally:
        client.close()


def run(host="localhost", port=50051, num_clients=10):
    # Fetch initial availability
    admin = TicketClient(host=host, port=port)
    admin_login = admin.login("admin", "password")
    if not admin_login.token:
        logger.error("Cannot connect to server")
        return

    raw = admin.get(admin_login.token, "availability", {"ticket_id": TARGET_SLOT})
    if not raw.items:
        logger.error(f"Slot '{TARGET_SLOT}' not found on server")
        admin.logout(admin_login.token)
        admin.close()
        return
    data = json.loads(raw.items[0].data)
    initial_count = data.get("available_count", 0)
    logger.info(f"Slot '{TARGET_SLOT}' initial available count: {initial_count}")
    logger.info(f"Sending {num_clients} concurrent booking requests (expecting {min(initial_count, num_clients)} accepted)...")
    admin.logout(admin_login.token)
    admin.close()

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures_list = [
            executor.submit(book_slot, f"user_{i + 1}", TARGET_SLOT, host, port)
            for i in range(num_clients)
        ]
        results = [f.result() for f in concurrent.futures.as_completed(futures_list)]

    elapsed = time.time() - start_time
    accepted = [r for r in results if r["status"] == "accepted"]
    rejected = [r for r in results if r["status"] == "rejected"]
    errors   = [r for r in results if r["status"] not in ("accepted", "rejected")]

    logger.info(f"Completed in {elapsed:.3f}s | accepted={len(accepted)} rejected={len(rejected)} errors={len(errors)}")
    for r in sorted(results, key=lambda x: x["user"]):
        logger.info(f"  {r['user']}: {r['status']} — {r.get('message', '')[:80]}")

    # Verify remaining inventory
    admin = TicketClient(host=host, port=port)
    admin_login = admin.login("admin", "password")
    final_raw = admin.get(admin_login.token, "availability", {"ticket_id": TARGET_SLOT})
    remaining = json.loads(final_raw.items[0].data).get("available_count", 0)
    admin.logout(admin_login.token)
    admin.close()

    logger.info(f"Remaining slots: {remaining} (initial={initial_count})")
    expected_accepted = min(initial_count, num_clients)
    if len(accepted) == expected_accepted and remaining == (initial_count - expected_accepted):
        logger.info("Concurrency test PASSED — no race conditions or overbooking.")
    else:
        logger.warning(
            f"Mismatch: expected {expected_accepted} accepted, "
            f"got {len(accepted)}; remaining={remaining}"
        )


if __name__ == "__main__":
    h = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    p = int(sys.argv[2]) if len(sys.argv) > 2 else 50051
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    run(host=h, port=p, num_clients=n)

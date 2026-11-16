"""Client node walkthrough: every client function of the assignment, step by step.

Runs login -> get(availability) -> get(faq) -> post(booking) -> get(my_bookings)
-> post(assistant, plain English) -> post(cancel) -> logout against the cluster,
printing which node answered each call. Any node can be given: followers
redirect to the Raft leader automatically.

    python scripts/client_demo.py                       # local 3-node cluster
    python scripts/client_demo.py --nodes 10.0.0.2:50051,10.0.0.3:50053,10.0.0.4:50055
    python scripts/client_demo.py --user ravi --slot SLOT-CVX-D1-400001
"""
import argparse
import json
import logging
import os
import sys
import time
import uuid

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import grpc  # noqa: E402

from dist_ticket_booking.grpc.service import TicketClient  # noqa: E402

DEFAULT_NODES = "localhost:50051,localhost:50053,localhost:50055"


def step(n, title):
    print(f"\n[{n}] {title}")


def show(res, client, t0):
    print(f"    -> {res.status.upper()}  (served by {client.target}, {(time.monotonic() - t0) * 1000:.0f} ms)")


def availability_table(items):
    print(f"    {'SLOT':<22}{'VACCINE':<12}{'DOSE':<6}{'CENTRE':<28}{'DATE':<12}{'OPEN':>5}{'PRICE':>8}")
    for i in items:
        d = json.loads(i.data)
        price = "free" if not d["price"] else f"Rs.{d['price']:.0f}"
        print(f"    {i.id:<22}{d['vaccine_name']:<12}{d['dose_number']:<6}{d['center_name']:<28}"
              f"{d['date']:<12}{d['available_count']:>5}{price:>8}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", default=DEFAULT_NODES, help="comma-separated client endpoints (any nodes)")
    parser.add_argument("--user", default="alice")
    parser.add_argument("--password", default="password")
    parser.add_argument("--slot", default="SLOT-CVS-D1-110001", help="slot to book in step 4")
    args = parser.parse_args()
    logging.disable(logging.WARNING)

    targets = [t.strip() for t in args.nodes.split(",") if t.strip()]
    client = TicketClient.for_cluster(targets)
    print(f"CLIENT NODE -> cluster {', '.join(targets)}")
    try:
        step(1, f"login('{args.user}', password)")
        t0 = time.monotonic()
        res = client.login(args.user, args.password)
        show(res, client, t0)
        if res.status != "success":
            sys.exit(f"    login failed: {res.message}")
        token = res.token
        print(f"    session token {token[:8]}... (replicated to every node through Raft)")

        step(2, "get(token, 'availability')")
        t0 = time.monotonic()
        res = client.get(token, "availability")
        show(res, client, t0)
        availability_table(res.items)

        step(3, "get(token, 'faq', 'How do I cancel a booking?')   -> LLM server")
        t0 = time.monotonic()
        res = client.get(token, "faq", {"query": "How do I cancel a booking?"})
        show(res, client, t0)
        print(f"    answer: {res.items[0].data if res.items else '(none)'}")

        step(4, f"post(token, 'booking', {{ticket_id: {args.slot}}})")
        t0 = time.monotonic()
        res = client.post(token, "booking", {"ticket_id": args.slot, "quantity": 1})
        show(res, client, t0)
        print(f"    {res.message}")

        step(5, "post(token, 'assistant', 'I want the corbevax booster in Bengaluru')   -> LLM + Raft")
        t0 = time.monotonic()
        res = client.assistant(token, "I want the corbevax booster in Bengaluru", request_id=uuid.uuid4().hex)
        show(res, client, t0)
        print(f"    {res.message}")

        step(6, "get(token, 'my_bookings')")
        t0 = time.monotonic()
        res = client.get(token, "my_bookings")
        show(res, client, t0)
        mine = [json.loads(i.data) for i in res.items]
        for b in mine:
            print(f"    {b['booking_id']:<32}{b['vaccine_name']:<12}dose {b['dose_number']}  "
                  f"{b['center_name']:<28}{b['status']}")

        confirmed = [b for b in mine if b["status"] == "confirmed"]
        if confirmed:
            bid = confirmed[0]["booking_id"]
            step(7, f"post(token, 'cancel', {{booking_id: {bid}}})")
            t0 = time.monotonic()
            res = client.post(token, "cancel", {"booking_id": bid})
            show(res, client, t0)
            print(f"    {res.message}")

        step(8, "get(token, 'availability', SLOT)   (seat counts after the changes)")
        res = client.get(token, "availability")
        availability_table(res.items)

        step(9, "logout(token)")
        t0 = time.monotonic()
        res = client.logout(token)
        show(res, client, t0)
        print("\nDone. Every write above appears as a COMMITTED line in all three node terminals.")
    except grpc.RpcError:
        sys.exit("\nCannot reach the cluster at " + args.nodes + ".\n"
                 "Start the three node terminals first (see README: 'Run the demo in 5 terminals').")
    finally:
        client.close()


if __name__ == "__main__":
    main()

"""One client action per command, for testing the system by hand.

Every command logs in first (creating the account on first use), does one
thing, and prints the result and which node served it. Any node can be
contacted: followers redirect to the Raft leader automatically.

    python scripts/client.py availability
    python scripts/client.py book SLOT-CVS-D1-110001
    python scripts/client.py book SLOT-CBX-BOS-560001 --pay card           # paid slot (Rs. 250)
    python scripts/client.py book SLOT-CBX-BOS-560001 --pay card_declined  # payment failure path
    python scripts/client.py my-bookings
    python scripts/client.py cancel APPT-SLOT-CVS-D1-alice-4
    python scripts/client.py faq "How do I cancel a booking?"
    python scripts/client.py assist "book a covaxin second dose in Mumbai"
    python scripts/client.py all-bookings --admin                          # admin-only view
    python scripts/client.py status                                        # every node's Raft role

Options: --user NAME (default alice), --password PW, --admin (log in as the
administrator), --nodes host:port,... (default: the local 3-node cluster).
"""
import argparse
import json
import logging
import os
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import grpc  # noqa: E402

from dist_ticket_booking.monitoring.console import setup_console  # noqa: E402

setup_console()
from dist_ticket_booking.grpc import ticket_service_pb2 as pb  # noqa: E402
from dist_ticket_booking.grpc import ticket_service_pb2_grpc as stubs  # noqa: E402
from dist_ticket_booking.grpc.service import TicketClient  # noqa: E402

DEFAULT_NODES = "localhost:50051,localhost:50053,localhost:50055"


def show_status(targets):
    print(f"{'ADDRESS':<18}{'NODE':<9}{'ROLE':<11}{'TERM':>5}{'COMMIT':>8}  STATE HASH")
    for t in targets:
        ch = grpc.insecure_channel(t)
        try:
            st = json.loads(stubs.TicketClientServiceStub(ch).Get(
                pb.GetRequest(type="cluster_status"), timeout=1.0).items[0].data)
            role = "* LEADER" if st["state"] == "leader" else st["state"]
            print(f"{t:<18}{st['node_id']:<9}{role:<11}{st['current_term']:>5}{st['commit_index']:>8}  "
                  f"{st['state_digest']}")
        except Exception:
            print(f"{t:<18}{'-':<9}{'DOWN':<11}")
        finally:
            ch.close()


def bookings_table(items):
    if not items:
        print("  (no bookings)")
    for i in items:
        b = json.loads(i.data)
        print(f"  {b['booking_id']:<34}{b['user_id']:<10}{b['vaccine_name']:<12}dose {b['dose_number']}  "
              f"{b['scheduled_date']}  {b['status']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=("status", "availability", "book", "cancel", "my-bookings",
                                       "all-bookings", "faq", "assist"))
    ap.add_argument("arg", nargs="?", default="", help="slot id, booking id, question or sentence")
    ap.add_argument("--user", default="alice")
    ap.add_argument("--password", default="password")
    ap.add_argument("--admin", action="store_true", help="log in as the administrator account 'admin'")
    ap.add_argument("--pay", default="upi", help="payment method for paid slots: upi, card, netbanking, "
                                                  "card_declined")
    ap.add_argument("--nodes", default=DEFAULT_NODES)
    args = ap.parse_args()
    logging.disable(logging.WARNING)
    targets = [t.strip() for t in args.nodes.split(",") if t.strip()]

    if args.action == "status":
        return show_status(targets)
    needs_arg = {"book": "a slot id", "cancel": "a booking id", "faq": "a question", "assist": "a sentence"}
    if args.action in needs_arg and not args.arg:
        sys.exit(f"'{args.action}' needs {needs_arg[args.action]}")

    client = TicketClient.for_cluster(targets)
    try:
        login = client.login_admin() if args.admin else client.login(args.user, args.password)
        if login.status != "success":
            sys.exit(f"login failed: {login.status} {login.message}")
        user = "admin" if args.admin else args.user
        print(f"logged in as {user} ({login.role})")
        t0 = time.monotonic()
        if args.action == "availability":
            res = client.get(login.token, "availability")
            print(f"  {'SLOT':<22}{'VACCINE':<12}{'DOSE':<6}{'CENTRE':<28}{'DATE':<12}{'OPEN':>5}{'PRICE':>8}")
            for i in res.items:
                d = json.loads(i.data)
                price = "free" if not d["price"] else f"Rs.{d['price']:.0f}"
                print(f"  {i.id:<22}{d['vaccine_name']:<12}{d['dose_number']:<6}{d['center_name']:<28}"
                      f"{d['date']:<12}{d['available_count']:>5}{price:>8}")
        elif args.action == "book":
            res = client.post(login.token, "booking", {"ticket_id": args.arg, "payment_method": args.pay})
            print(f"  {res.status.upper()}: {res.message}")
        elif args.action == "cancel":
            res = client.post(login.token, "cancel", {"booking_id": args.arg})
            print(f"  {res.status.upper()}: {res.message}")
        elif args.action == "my-bookings":
            res = client.get(login.token, "my_bookings")
            bookings_table(res.items)
        elif args.action == "all-bookings":
            res = client.get(login.token, "all_bookings")
            if res.status == "forbidden":
                print("  FORBIDDEN: only administrators can list every booking (add --admin)")
            else:
                bookings_table(res.items)
        elif args.action == "faq":
            res = client.get(login.token, "faq", {"query": args.arg})
            print(f"  Q: {args.arg}\n  A: {res.items[0].data if res.items else '(no answer)'}")
        else:
            res = client.assistant(login.token, args.arg)
            print(f"  {res.status.upper()}: {res.message}")
        print(f"(served by the leader at {client.target} in {(time.monotonic() - t0) * 1000:.0f} ms)")
        client.logout(login.token)
    except grpc.RpcError:
        sys.exit(f"Cannot reach the cluster at {args.nodes}. Start the node terminals first.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

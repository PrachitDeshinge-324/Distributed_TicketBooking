"""Natural-language booking assistant (chat with the cluster).

Type what you want in plain English; the LLM server interprets it, the app
server checks it against live Raft-replicated data, and the booking or
cancellation is committed through Raft like any other write.

    python scripts/assistant.py                       # interactive chat
    python scripts/assistant.py --user asha "book a COVAXIN second dose in Mumbai"

Try: "I want my first covishield jab in Hyderabad", "cancel my covaxin
appointment", "what documents do I need?", "show my bookings", "quit".
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


def show_bookings(client, token):
    res = client.get(token, "my_bookings")
    items = [json.loads(i.data) for i in res.items]
    if not items:
        print("  (no appointments)")
    for b in items:
        print(f"  {b['booking_id']:<34} {b['vaccine_name']:<11} dose {b['dose_number']}  "
              f"{b['center_name']:<26} {b['scheduled_date']}  {b['status']}")


def say(client, token, text):
    t0 = time.monotonic()
    res = client.assistant(token, text, request_id=uuid.uuid4().hex)
    mark = {"accepted": "✔", "success": "✔", "clarify": "?"}.get(res.status, "✘")
    print(f"assistant [{mark} {res.status}, {time.monotonic() - t0:.1f}s]: {res.message}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("text", nargs="*", help="one request (omit for an interactive chat)")
    parser.add_argument("--nodes", default=DEFAULT_NODES, help="comma-separated client endpoints")
    parser.add_argument("--user", default="demo-citizen")
    parser.add_argument("--password", default="password")
    args = parser.parse_args()
    logging.disable(logging.WARNING)

    client = TicketClient.for_cluster([t.strip() for t in args.nodes.split(",") if t.strip()])
    try:
        login = client.login(args.user, args.password)
        if login.status != "success":
            sys.exit(f"login failed: {login.status} {login.message}")
        token = login.token
        if args.text:
            say(client, token, " ".join(args.text))
            return
        print(f"Logged in as {args.user}. Ask me to book or cancel an appointment ('quit' to exit).")
        while True:
            try:
                text = input("you> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if text.lower() in ("quit", "exit", "bye"):
                break
            if text.lower() in ("bookings", "my bookings", "show my bookings"):
                show_bookings(client, token)
            elif text:
                say(client, token, text)
        client.logout(token)
    except grpc.RpcError:
        sys.exit("\nCannot reach the cluster at " + args.nodes + ".\n"
                 "Start the three node terminals first (see README: 'Run the demo in 5 terminals').")
    finally:
        client.close()


if __name__ == "__main__":
    main()

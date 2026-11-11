"""Ask the FAQ assistant one question through the cluster (handy for live demos).

The request goes client -> any app node (redirected to the Raft leader) ->
LLM server, with live, Raft-replicated availability injected as context.

Usage:
    python scripts/ask.py "How many slots are left for CORBEVAX?"
    python scripts/ask.py --book SLOT-CBX-BOS-560001 "How many slots are left for CORBEVAX?"
    python scripts/ask.py --port 50053 "How do I cancel a booking?"     # via a follower
"""
import argparse
import logging
import os
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dist_ticket_booking.grpc.service import TicketClient  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("question", help="question for the FAQ assistant")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051, help="any app node's client port")
    parser.add_argument("--book", metavar="SLOT_ID", help="book one seat in this slot before asking")
    parser.add_argument("--user", default="demo-citizen")
    args = parser.parse_args()
    logging.disable(logging.WARNING)

    client = TicketClient(host=args.host, port=args.port,
                          seeds=["localhost:50051", "localhost:50053", "localhost:50055"])
    try:
        token = client.login(args.user).token
        if args.book:
            res = client.post(token, "booking", {"ticket_id": args.book, "quantity": 1})
            print(f"Booking {args.book}: {res.status} - {res.message}\n")
        t0 = time.monotonic()
        res = client.get(token, "faq", {"query": args.question})
        answer = res.items[0].data if res.items else "(no answer)"
        print(f"Q: {args.question}")
        print(f"A: {answer}")
        print(f"\n(answered in {time.monotonic() - t0:.1f}s via leader {client.target})")
    finally:
        client.close()


if __name__ == "__main__":
    main()

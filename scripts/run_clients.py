"""Client node (Node 5): simulates many concurrent users against the cluster.

Each simulated user logs in and runs a random mix of operations for the
given duration: check availability, book a slot, cancel one of its own
bookings, and (optionally) ask the LLM FAQ assistant. Clients know every
cluster node, follow leader redirects and ride out leader elections.

At the end it prints per-operation statistics and a consistency audit:
for every slot,  available_before - available_after == accepted - cancelled
and availability never drops below zero (no overbooking, no lost updates).
Writes whose outcome is uncertain (e.g. the leader died before replying) are
re-sent with the same request_id; the cluster de-duplicates, so each write
is applied exactly once and the audit can account for every one of them.
The audit assumes this script is the only writer during the run.

Usage:
    python scripts/run_clients.py                                   # 20 users, 20 s, local 3-node cluster
    python scripts/run_clients.py --users 50 --duration 30 --faq
    python scripts/run_clients.py --nodes 10.0.0.2:50051,10.0.0.3:50053,10.0.0.4:50055
"""
import argparse
import json
import logging
import os
import random
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dist_ticket_booking.grpc.service import TicketClient  # noqa: E402

DEFAULT_NODES = "localhost:50051,localhost:50053,localhost:50055"
FAQ_QUERIES = [
    "How to cancel a booking?",
    "What seats are available?",
    "Which documents do I need?",
    "What is the gap between covishield doses?",
    "Are there COVAXIN slots in Mumbai?",
]


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.outcomes = defaultdict(Counter)   # op -> status -> count
        self.latency = defaultdict(list)       # op -> [seconds]
        self.booked = Counter()                # slot -> accepted bookings
        self.cancelled = Counter()             # slot -> successful cancels

    def record(self, op, status, seconds):
        with self.lock:
            self.outcomes[op][status] += 1
            self.latency[op].append(seconds)


def _percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(pct / 100.0 * (len(values) - 1))))]


def fetch_availability(targets):
    """slot_id -> available_count, read through the leader."""
    c = TicketClient.for_cluster(targets)
    try:
        tok = c.login("audit-user").token
        res = c.get(tok, "availability")
        return {item.id: json.loads(item.data)["available_count"] for item in res.items}
    finally:
        c.close()


def user_session(uid, targets, slots, deadline, stats, faq, think_s):
    rng = random.Random(uid)
    client = TicketClient.for_cluster(rng.sample(targets, len(targets)))  # spread initial load

    def timed(op, fn):
        t0 = time.monotonic()
        try:
            res = fn()
            status = res.status
        except Exception as exc:  # cluster fully unreachable for > retry window
            res, status = None, f"exception:{type(exc).__name__}"
        stats.record(op, status, time.monotonic() - t0)
        return res

    def uncertain(res):
        """Outcome unknown: the write may or may not have been applied."""
        if res is None or res.status in ("redirect", "unavailable"):
            return True
        return res.status == "error" and res.message == "lost leadership"

    def write(op, kind, payload):
        # Re-send with the SAME request_id until the outcome is known; the
        # cluster de-duplicates, so a write that already happened is not repeated.
        rid = uuid.uuid4().hex
        res = timed(op, lambda: client.post(token, kind, payload, request_id=rid))
        for _ in range(8):
            if not uncertain(res):
                break
            time.sleep(0.5)
            res = timed(op + "_retry", lambda: client.post(token, kind, payload, request_id=rid))
        return res

    login = timed("login", lambda: client.login(f"user{uid:03d}"))
    if not login or login.status != "success":
        client.close()
        return
    token = login.token

    ops = ["availability"] * 4 + ["book"] * 3 + ["cancel"] * 2 + (["faq"] if faq else [])
    while time.monotonic() < deadline:
        op = rng.choice(ops)
        if op == "availability":
            timed(op, lambda: client.get(token, "availability", {"ticket_id": rng.choice(slots)}))
        elif op == "book":
            slot = rng.choice(slots)
            res = write(op, "booking", {"ticket_id": slot, "quantity": 1})
            if res is not None and res.status == "accepted":
                with stats.lock:
                    stats.booked[slot] += 1
        elif op == "cancel":
            res = timed("my_bookings", lambda: client.get(token, "my_bookings"))
            mine = [json.loads(i.data) for i in res.items] if res is not None and res.status == "success" else []
            confirmed = [b for b in mine if b.get("status") == "confirmed"]
            if confirmed:
                b = rng.choice(confirmed)
                out = write(op, "cancel", {"booking_id": b["booking_id"]})
                if out is not None and out.status in ("accepted", "success"):
                    with stats.lock:
                        stats.cancelled[b["ticket_id"]] += 1
        elif op == "faq":
            timed(op, lambda: client.get(token, "faq", {"query": rng.choice(FAQ_QUERIES)}))
        time.sleep(rng.uniform(0, think_s))

    timed("logout", lambda: client.logout(token))
    client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", default=DEFAULT_NODES, help="comma-separated host:port client endpoints")
    parser.add_argument("--users", type=int, default=20, help="concurrent simulated users")
    parser.add_argument("--duration", type=float, default=20.0, help="seconds to run")
    parser.add_argument("--faq", action="store_true", help="include LLM FAQ queries in the mix")
    parser.add_argument("--think", type=float, default=0.2, help="max think time between ops (s)")
    args = parser.parse_args()
    logging.disable(logging.WARNING)

    targets = [t.strip() for t in args.nodes.split(",") if t.strip()]
    before = fetch_availability(targets)
    slots = sorted(before)
    print(f"[client node] {args.users} users x {args.duration:.0f}s against {targets}")
    print(f"[client node] slots: {', '.join(f'{s}={before[s]}' for s in slots)}")

    stats = Stats()
    deadline = time.monotonic() + args.duration
    threads = [
        threading.Thread(target=user_session,
                         args=(i, targets, slots, deadline, stats, args.faq, args.think), daemon=True)
        for i in range(args.users)
    ]
    for t in threads:
        t.start()

    start = time.monotonic()
    while any(t.is_alive() for t in threads):
        time.sleep(1.0)
        with stats.lock:
            total = sum(sum(c.values()) for c in stats.outcomes.values())
            booked = sum(stats.booked.values())
        print(f"  t={time.monotonic() - start:5.1f}s  ops={total:5d}  bookings_accepted={booked}", flush=True)
        if time.monotonic() > deadline + 30:
            print("  [warn] some sessions did not finish in time")
            break

    print("\n[client node] results")
    print(f"  {'operation':<14}{'count':>7}{'p50 ms':>9}{'p95 ms':>9}  outcomes")
    for op in sorted(stats.outcomes):
        lat = stats.latency[op]
        outcomes = ", ".join(f"{k}={v}" for k, v in stats.outcomes[op].most_common())
        print(f"  {op:<14}{len(lat):>7}{_percentile(lat, 50) * 1000:>9.0f}{_percentile(lat, 95) * 1000:>9.0f}  {outcomes}")

    after = fetch_availability(targets)
    print("\n[client node] consistency audit (per slot: before - after == accepted - cancelled)")
    ok = True
    for s in slots:
        expected = before[s] - after.get(s, 0)
        net = stats.booked[s] - stats.cancelled[s]
        good = expected == net and after.get(s, 0) >= 0
        ok &= good
        print(f"  {'OK ' if good else 'BAD'} {s:<22} before={before[s]:>3} after={after.get(s, 0):>3} "
              f"accepted={stats.booked[s]:>3} cancelled={stats.cancelled[s]:>3}")
    print(f"\n[client node] {'AUDIT PASSED: no overbooking, no lost updates' if ok else 'AUDIT FAILED'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

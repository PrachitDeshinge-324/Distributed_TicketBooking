"""End-to-end Milestone 2 verification against real multi-process clusters.

Runs a sequence of checks, each printing PASS/FAIL:
  1. LLM integration   - LLM server + 3 Raft nodes: FAQ via leader / follower,
                         live-availability answers, concurrent queries,
                         FAQ after leader failover, LLM-server outage fallback.
  2. Fault tolerance   - follower crash, loss of quorum (fail-fast, clear error),
                         quorum restored, full-cluster crash + recovery from disk.
  3. Five-node cluster - tolerates 2 simultaneous failures; rejoined nodes converge.
  4. Log compaction    - leader snapshots its log; a follower that missed the
                         compacted entries catches up via InstallSnapshot.

Usage:
    python scripts/verify_milestone2.py              # all sections
    python scripts/verify_milestone2.py --skip-llm   # no LLM server (faster)
"""
import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import grpc  # noqa: E402

from dist_ticket_booking.grpc import ticket_service_pb2 as pb  # noqa: E402
from dist_ticket_booking.grpc import ticket_service_pb2_grpc  # noqa: E402
from dist_ticket_booking.grpc.service import TicketClient  # noqa: E402
from scripts.run_cluster import ClusterManager, DEFAULT_CLUSTER_SPEC, build_cluster_spec  # noqa: E402

SLOT = "SLOT-CBX-BOS-560001"  # paid CORBEVAX slot, small capacity
LLM_PORT = 50060

_results = []


def check(name, cond, info=""):
    _results.append(bool(cond))
    detail = f"  | {str(info)[:160]}" if info not in ("", None) else ""
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{detail}", flush=True)


# ── helpers ────────────────────────────────────────────────────────────────

def find_leader(specs):
    for s in specs:
        c = TicketClient(port=s["client_port"])
        try:
            r = c.client_stub.Get(pb.GetRequest(token="probe", type="availability"), timeout=2)
            if r.status != "redirect":
                return s
        except Exception:
            pass
        finally:
            c.close()
    return None


def wait_leader(specs, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader = find_leader(specs)
        if leader:
            return leader
        time.sleep(0.3)
    return None


def wait_llm_ready(timeout=120.0):
    """Block until the LLM server answers (model load can take a while)."""
    ch = grpc.insecure_channel(f"localhost:{LLM_PORT}")
    stub = ticket_service_pb2_grpc.LLMServiceStub(ch)
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            try:
                stub.GetLLMAnswer(pb.LLMQueryRequest(request_id="warmup", query="hello"), timeout=60)
                return True
            except grpc.RpcError:
                time.sleep(1)
        return False
    finally:
        ch.close()


def login(port, user):
    c = TicketClient(port=port)
    res = c.login(user)
    return c, res


def availability(port):
    c, res = login(port, "auditor")
    try:
        r = c.get(res.token, "availability", {"ticket_id": SLOT})
        return json.loads(r.items[0].data)["available_count"]
    finally:
        c.close()


def book(port, user):
    c, res = login(port, user)
    try:
        if res.status != "success":
            return res.status, res.message
        r = c.post(res.token, "booking", {"ticket_id": SLOT, "quantity": 1})
        return r.status, r.message
    except Exception as exc:
        return "exception", str(exc)
    finally:
        c.close()


def faq(port, user, query):
    c, res = login(port, user)
    try:
        r = c.get(res.token, "faq", {"query": query})
        return r.status, (r.items[0].data if r.items else "")
    finally:
        c.close()


def new_manager(spec, data_dir, with_llm, node_args=None):
    return ClusterManager(python_bin=sys.executable, nodes=spec, data_dir=data_dir,
                          with_llm=with_llm, llm_port=LLM_PORT, node_args=node_args)


def node_status(port):
    """cluster_status of one node (works on followers too)."""
    ch = grpc.insecure_channel(f"localhost:{port}")
    try:
        stub = ticket_service_pb2_grpc.TicketClientServiceStub(ch)
        r = stub.Get(pb.GetRequest(token="", type="cluster_status"), timeout=2)
        return json.loads(r.items[0].data)
    except Exception:
        return None
    finally:
        ch.close()


# ── sections ───────────────────────────────────────────────────────────────

def section_llm(data_dir):
    print("\n[1] LLM integration (LLM server + 3 Raft nodes)")
    spec = DEFAULT_CLUSTER_SPEC
    m = new_manager(spec, data_dir, with_llm=True)
    try:
        m.start_all()
        check("LLM server ready", wait_llm_ready())
        leader = wait_leader(spec)
        check("leader elected", leader, leader and leader["node_id"])
        lp = leader["client_port"]

        for q, expect in [
            ("How to cancel a booking?", "24"),
            ("Which documents do I need?", "Aadhaar"),
            ("What is the gap between covishield doses?", "84"),
        ]:
            st, ans = faq(lp, "faq-user", q)
            check(f"FAQ: {q}", st == "success" and expect in ans, ans)

        st, ans = faq(lp, "faq-user", "How many slots are left for CORBEVAX?")
        live = availability(lp)
        check("live availability reflected in answer", st == "success" and str(live) in ans, ans)

        follower = next(s for s in spec if s is not leader)
        st, ans = faq(follower["client_port"], "faq-user", "How to cancel a booking?")
        check("FAQ via follower (auto-redirect)", st == "success" and ans, ans)

        out = []
        threads = [threading.Thread(target=lambda i=i: out.append(
            faq(lp, f"c{i}", "How to cancel a booking?"))) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(120)
        check("8 concurrent FAQ queries", len(out) == 8 and all(s == "success" and a for s, a in out))

        m.kill_node(leader["node_id"])
        rest = [s for s in spec if s is not leader]
        new_leader = wait_leader(rest)
        check("new leader after leader kill", new_leader, new_leader and new_leader["node_id"])
        st, ans = faq(new_leader["client_port"], "faq-user", "How to cancel a booking?")
        check("FAQ after failover", st == "success" and ans, ans)

        m.llm_proc.kill()
        time.sleep(1)
        st, ans = faq(new_leader["client_port"], "faq-user", "How to cancel a booking?")
        check("LLM server down -> keyword fallback answer", st == "success" and "24 hours" in ans, ans)
        st, msg = book(new_leader["client_port"], "booker")
        check("bookings unaffected by LLM outage", st == "accepted", msg)
    finally:
        m.stop_all()


def section_fault_tolerance(data_dir):
    print("\n[2] Fault tolerance (3 Raft nodes)")
    spec = DEFAULT_CLUSTER_SPEC
    m = new_manager(spec, data_dir, with_llm=False)
    try:
        m.start_all()
        leader = wait_leader(spec)
        check("leader elected", leader, leader and leader["node_id"])
        lp = leader["client_port"]
        base = availability(lp)
        check("booking committed", book(lp, "a")[0] == "accepted")

        f1, f2 = [s for s in spec if s is not leader]
        m.kill_node(f1["node_id"])
        time.sleep(1)
        check("1 follower down: writes still commit (2/3 quorum)", book(lp, "b")[0] == "accepted")

        m.kill_node(f2["node_id"])
        time.sleep(1.5)
        t0 = time.time()
        st, msg = book(lp, "c")
        elapsed = time.time() - t0
        check("no quorum: write rejected with clear error",
              st == "unavailable" and "quorum" in msg, (st, msg))
        check("no quorum: fails fast (< 2 s)", elapsed < 2.0, f"{elapsed:.2f}s")

        m.start_node(f1["node_id"])
        m.start_node(f2["node_id"])
        leader2 = wait_leader(spec)
        check("leader after quorum restored", leader2, leader2 and leader2["node_id"])
        time.sleep(2)
        after = availability(leader2["client_port"])
        check("acknowledged bookings retained", after == base - 2, (base, after))

        for nid in list(m.processes):
            m.kill_node(nid)
        time.sleep(1)
        m.start_all()
        leader3 = wait_leader(spec)
        check("leader after full-cluster crash", leader3, leader3 and leader3["node_id"])
        time.sleep(2)
        recovered = availability(leader3["client_port"])
        check("state recovered from persistent log", recovered == after, (after, recovered))
        check("bookings work after recovery", book(leader3["client_port"], "d")[0] == "accepted")
    finally:
        m.stop_all()


def section_five_nodes(data_dir):
    print("\n[3] Five-node cluster")
    spec = build_cluster_spec(5)
    m = new_manager(spec, data_dir, with_llm=False)
    try:
        m.start_all()
        leader = wait_leader(spec)
        check("5-node leader elected", leader, leader and leader["node_id"])
        base = availability(leader["client_port"])
        check("booking committed", book(leader["client_port"], "a")[0] == "accepted")

        follower = next(s for s in spec if s is not leader)
        m.kill_node(leader["node_id"])
        m.kill_node(follower["node_id"])
        rest = [s for s in spec if s not in (leader, follower)]
        new_leader = wait_leader(rest)
        check("leader + follower down (2/5): new leader", new_leader, new_leader and new_leader["node_id"])
        np_ = new_leader["client_port"]
        check("state preserved", availability(np_) == base - 1)
        check("booking on new leader", book(np_, "b")[0] == "accepted")

        m.start_node(leader["node_id"])
        m.start_node(follower["node_id"])
        time.sleep(4)
        check("rejoined nodes converge", availability(np_) == base - 2)
    finally:
        m.stop_all()


def section_log_compaction(data_dir):
    print("\n[4] Log compaction & InstallSnapshot (3 Raft nodes, snapshot every 20 entries)")
    spec = DEFAULT_CLUSTER_SPEC
    m = new_manager(spec, data_dir, with_llm=False, node_args=["--snapshot-threshold", "20"])
    try:
        m.start_all()
        leader = wait_leader(spec)
        check("leader elected", leader, leader and leader["node_id"])
        lp = leader["client_port"]
        base = availability(lp)
        lagger = next(s for s in spec if s is not leader)
        m.kill_node(lagger["node_id"])

        # ~60 committed entries while the follower is down -> leader compacts
        c = TicketClient(port=lp)
        for i in range(30):
            tok = c.login(f"snap{i}").token
            c.logout(tok)
        c.close()
        st = node_status(lp)
        check("leader compacted its log", st and st["log_length"] < 25, st and f"log_length={st['log_length']}")
        check("booking still works after compaction", book(lp, "post-snap")[0] == "accepted")

        m.start_node(lagger["node_id"])
        deadline = time.time() + 15
        leader_applied = lag = None
        while time.time() < deadline:
            leader_applied = node_status(lp)["last_applied"]
            lag = node_status(lagger["client_port"])
            if lag and lag["last_applied"] == leader_applied:
                break
            time.sleep(0.3)
        check("lagging follower caught up via snapshot",
              lag and lag["last_applied"] == leader_applied,
              lag and f"follower={lag['last_applied']} leader={leader_applied}")

        # The caught-up follower must hold the same data: make it the leader.
        m.kill_node(leader["node_id"])
        rest = [s for s in spec if s is not leader]
        new_leader = wait_leader(rest)
        check("state intact after failing over past the snapshot",
              new_leader and availability(new_leader["client_port"]) == base - 1,
              new_leader and new_leader["node_id"])
    finally:
        m.stop_all()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-llm", action="store_true", help="skip the LLM section")
    parser.add_argument("--verbose", action="store_true", help="show node logs")
    args = parser.parse_args()
    if not args.verbose:
        logging.disable(logging.CRITICAL)
        os.environ.setdefault("RAFT_LOG_LEVEL", "WARNING")

    root = tempfile.mkdtemp(prefix="m2verify_")
    try:
        if not args.skip_llm:
            section_llm(os.path.join(root, "llm"))
        section_fault_tolerance(os.path.join(root, "ft"))
        section_five_nodes(os.path.join(root, "five"))
        section_log_compaction(os.path.join(root, "snap"))
    finally:
        shutil.rmtree(root, ignore_errors=True)

    passed, total = sum(_results), len(_results)
    print(f"\n{'ALL CHECKS PASSED' if passed == total else 'SOME CHECKS FAILED'}: {passed}/{total}")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()

"""Full-system demo in the assignment's suggested 5-node layout.

    Node 1  LLM server (Qwen3.5-0.8B)          :50060
    Node 2  App server + Raft (initial leader)  node-1  client :50051  raft :50052
    Node 3  App server + Raft follower          node-2  client :50053  raft :50054
    Node 4  App server + Raft follower          node-3  client :50055  raft :50056
    Node 5  Client node: N concurrent users     scripts/run_clients.py

Timeline (default 30 s of client load):
    t= 0s  client node starts (bookings, cancels, availability, FAQ)
    t=10s  the current Raft leader is killed (SIGKILL)
    t=18s  the killed node is restarted and catches up
    end    client node prints stats + consistency audit; cluster status shown

Usage:
    python scripts/demo_full_system.py                 # with the LLM
    python scripts/demo_full_system.py --no-llm        # Raft only
    python scripts/demo_full_system.py --users 40 --duration 40
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from scripts.monitor_cluster import probe_llm, probe_node, render_dashboard  # noqa: E402
from scripts.run_cluster import ClusterManager, DEFAULT_CLUSTER_SPEC  # noqa: E402
from scripts.verify_milestone2 import wait_leader, wait_llm_ready  # noqa: E402

LLM_PORT = 50060


def banner(text):
    print("\n" + "=" * 98 + f"\n  {text}\n" + "=" * 98, flush=True)


def wait_raft_leader(specs, timeout=15.0):
    """Poll each node's Raft role directly (no read barrier), for accurate timing."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for spec in specs:
            if probe_node(spec, timeout=0.3)["role"] == "★ LEADER":
                return spec
        time.sleep(0.05)
    return None


def show_cluster(with_llm):
    infos = [probe_node(n) for n in DEFAULT_CLUSTER_SPEC]
    render_dashboard(infos, clear=False, llm_status=probe_llm(port=LLM_PORT) if with_llm else None)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-llm", action="store_true", help="skip the LLM server node")
    parser.add_argument("--users", type=int, default=20, help="concurrent users on the client node")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds of client load")
    parser.add_argument("--kill-at", type=float, default=10.0, help="seconds into the load to kill the leader")
    parser.add_argument("--restart-at", type=float, default=18.0, help="seconds into the load to restart it")
    args = parser.parse_args()
    with_llm = not args.no_llm
    sys.stdout.reconfigure(line_buffering=True)

    os.environ.setdefault("RAFT_LOG_LEVEL", "WARNING")  # keep node output quiet
    data_dir = tempfile.mkdtemp(prefix="demo_full_")
    manager = ClusterManager(python_bin=sys.executable, nodes=DEFAULT_CLUSTER_SPEC,
                             data_dir=data_dir, with_llm=with_llm, llm_port=LLM_PORT)
    client_proc = None
    try:
        banner("STEP 1: starting Node 1 (LLM) and Nodes 2-4 (Raft app servers)")
        manager.start_all()
        if with_llm:
            print("  waiting for the LLM model to load...", flush=True)
            print(f"  LLM ready: {wait_llm_ready()}")
        leader = wait_leader(DEFAULT_CLUSTER_SPEC)
        print(f"  Raft leader elected: {leader['node_id'] if leader else 'NONE'}")
        show_cluster(with_llm)

        banner(f"STEP 2: Node 5 client node - {args.users} concurrent users for {args.duration:.0f}s")
        cmd = [sys.executable, os.path.join(PROJECT_ROOT, "scripts", "run_clients.py"),
               "--users", str(args.users), "--duration", str(args.duration)]
        if with_llm:
            cmd.append("--faq")
        client_proc = subprocess.Popen(cmd)
        t0 = time.monotonic()

        time.sleep(max(0.0, args.kill_at - (time.monotonic() - t0)))
        leader = wait_raft_leader(DEFAULT_CLUSTER_SPEC) or leader
        banner(f"STEP 3: killing the Raft leader {leader['node_id']} under load (SIGKILL)")
        killed_at = time.monotonic()
        manager.kill_node(leader["node_id"])
        survivors = [s for s in DEFAULT_CLUSTER_SPEC if s is not leader]
        new_leader = wait_raft_leader(survivors)
        print(f"  new leader elected: {new_leader['node_id'] if new_leader else 'NONE'} "
              f"({time.monotonic() - killed_at:.1f}s after the kill)", flush=True)
        show_cluster(with_llm)

        time.sleep(max(0.0, args.restart_at - (time.monotonic() - t0)))
        banner(f"STEP 4: restarting {leader['node_id']} - it rejoins as a follower and catches up")
        manager.start_node(leader["node_id"])

        client_rc = client_proc.wait()
        client_proc = None
        time.sleep(1.5)  # let the restarted node apply the last entries

        banner("STEP 5: final cluster state (all nodes should show the same APPLIED index)")
        show_cluster(with_llm)
        print("\n  " + ("DEMO PASSED: no overbooking, no lost updates across a leader crash"
                       if client_rc == 0 else "DEMO FAILED: see client audit above"))
        sys.exit(client_rc)
    finally:
        if client_proc is not None:
            client_proc.kill()
        manager.stop_all()
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    main()

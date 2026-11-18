"""Automatic leader-failure demo with a pass/fail summary (no manual steps).

Runs against the local 3-node cluster that is already running (started by
run_demo.bat / run_demo.sh, or by hand in terminals T2-T4):

  1. shows the cluster: who leads, term, commit index, replica state hash
  2. starts concurrent users (book / cancel / availability) on the client node
  3. after a few seconds CRASHES the current leader (kill -9 / taskkill /F)
  4. measures how long the two survivors take to elect a new leader
  5. restarts the crashed node from its saved data (in its own terminal window,
     a new window, or in the background); it rejoins as a follower and catches up
  6. audits the result: no overbooking, no lost update, all replicas identical
  7. prints a checklist with PASS / FAIL for each point

    python scripts/failover_demo.py
    python scripts/failover_demo.py --users 10 --duration 30
    python scripts/failover_demo.py --restart window       # open a new window for the restarted node
    python scripts/failover_demo.py --restart background   # no window for the restarted node
    python scripts/failover_demo.py --restart none         # print the restart command instead

Crashing a process only works for nodes on this machine.
"""
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # before importing grpc: hide its INFO notices

import grpc  # noqa: E402

from dist_ticket_booking.monitoring.console import setup_console  # noqa: E402

COLOR = setup_console()
from dist_ticket_booking.grpc import ticket_service_pb2 as pb  # noqa: E402
from dist_ticket_booking.grpc import ticket_service_pb2_grpc as stubs  # noqa: E402
from scripts.demo_launcher import (  # noqa: E402
    IS_WIN, NODES, RUN_DIR, demo_env, open_terminals, peers_arg, restart_request_file,
    restart_waiting_file, script_text,
)
from scripts.run_clients import Stats, fetch_availability, user_session  # noqa: E402

TARGETS = [f"localhost:{cport}" for _, cport, _ in NODES]
GREEN, RED, BOLD, RESET = ("\033[32m", "\033[31m", "\033[1m", "\033[0m") if COLOR else ("", "", "", "")


def say(msg=""):
    print(msg, flush=True)


def banner(text):
    say(f"\n{BOLD}{'=' * 78}\n  {text}\n{'=' * 78}{RESET}")


def node_status(cport, timeout=0.5):
    ch = grpc.insecure_channel(f"localhost:{cport}")
    try:
        r = stubs.TicketClientServiceStub(ch).Get(pb.GetRequest(type="cluster_status"), timeout=timeout)
        return json.loads(r.items[0].data)
    except Exception:
        return None
    finally:
        ch.close()


def cluster():
    return {nid: node_status(cport) for nid, cport, _ in NODES}


def show(states):
    say(f"  {'NODE':<8}{'ROLE':<11}{'TERM':>5}{'COMMIT':>8}  STATE HASH")
    for nid, st in states.items():
        if st is None:
            say(f"  {nid:<8}{RED}{'DOWN':<11}{RESET}")
        else:
            role = f"{'* LEADER' if st['state'] == 'leader' else st['state']}"
            say(f"  {nid:<8}{role:<11}{st['current_term']:>5}{st['commit_index']:>8}  {st['state_digest']}")


def find_leader(exclude=(), timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for nid, cport, _ in NODES:
            if nid in exclude:
                continue
            st = node_status(cport, timeout=0.3)
            if st and st["state"] == "leader":
                return st
        time.sleep(0.05)
    return None


def crash(pid):
    """Kill the process the hard way, like a power failure (no clean shutdown)."""
    if IS_WIN:
        subprocess.call(["taskkill", "/F", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        os.kill(pid, signal.SIGKILL)


def restart(nid, mode):
    """Restart a node WITHOUT --fresh, so it recovers from its saved log."""
    cport, rport = next((c, r) for n, c, r in NODES if n == nid)
    cmd = [sys.executable, "scripts/run_node.py", "--node-id", nid, "--client-port", str(cport),
           "--raft-port", str(rport), "--peers", peers_arg(nid)]
    shown = "python " + " ".join(cmd[1:])
    if mode == "none":
        say(f"  restart it yourself (same command, no --fresh):\n    {shown}")
        return
    if mode == "same":
        # The node's own terminal (from the demo launcher) waits after its process
        # ends; ask it to rerun the node there instead of opening another window.
        if restart_waiting_file(nid).exists():
            restart_request_file(nid).write_text(str(time.time()))
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if node_status(cport) is not None:
                    say(f"  restarted {nid} in its own terminal window (same command, without --fresh)")
                    return
                time.sleep(0.3)
            restart_request_file(nid).unlink(missing_ok=True)
            say(f"  {nid}'s window did not restart it; restarting in the background instead")
        else:
            say(f"  no waiting terminal for {nid}; restarting in the background instead")
        mode = "background"
    RUN_DIR.mkdir(exist_ok=True)
    if mode == "window":
        title = f"{nid} (restarted after crash)"
        path = RUN_DIR / f"restart_{nid}{'.bat' if IS_WIN else '.sh'}"
        with open(path, "w", newline="") as f:
            f.write(script_text(title, [cmd], shown))
        if not IS_WIN:
            path.chmod(0o755)
        try:
            if open_terminals([title], [path]):
                say(f"  restarted {nid} in a new window (same command, without --fresh)")
                return
        except Exception:
            pass
        say("  could not open a new window; restarting in the background instead")
    logs = RUN_DIR / "logs"
    logs.mkdir(exist_ok=True)
    log = open(logs / f"{nid}_restarted.log", "w", encoding="utf-8")
    extra = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if IS_WIN else {"start_new_session": True}
    proc = subprocess.Popen(cmd, cwd=PROJECT_ROOT, env=demo_env(), stdout=log, stderr=subprocess.STDOUT, **extra)
    pid_file = RUN_DIR / "restarted.json"  # `demo_launcher.py stop` cleans these up
    pids = json.loads(pid_file.read_text()) if pid_file.exists() else []
    pid_file.write_text(json.dumps(pids + [proc.pid]))
    say(f"  restarted {nid} in the background (pid {proc.pid}, log .demo/logs/{nid}_restarted.log)")


def ok_ops(stats):
    with stats.lock:
        return sum(n for op, c in stats.outcomes.items() if op in ("book", "cancel", "availability")
                   for status, n in c.items() if not status.startswith("exception"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--users", type=int, default=6, help="concurrent users during the test")
    ap.add_argument("--duration", type=float, default=20.0, help="seconds of client load")
    ap.add_argument("--kill-at", type=float, default=5.0, help="seconds into the load to crash the leader")
    ap.add_argument("--restart-at", type=float, default=10.0, help="seconds into the load to restart it")
    ap.add_argument("--restart", choices=("same", "window", "background", "none"), default="same",
                    help="how to restart the crashed node (default: in its own terminal window "
                         "when started by the demo launcher, otherwise in the background)")
    args = ap.parse_args()
    logging.disable(logging.WARNING)
    checks = []

    def check(name, passed, detail=""):
        checks.append((name, bool(passed), detail))
        mark = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        say(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""))

    banner("AUTOMATIC LEADER FAILURE TEST")
    before_state = cluster()
    show(before_state)
    leader = find_leader(timeout=5)
    if leader is None:
        sys.exit("\nNo leader found. Start the three Raft nodes first (run_demo.bat / run_demo.sh, "
                 "or README: 'Run the demo in 5 terminals').")
    old_id, old_term = leader["node_id"], leader["current_term"]
    if not leader.get("pid"):
        sys.exit("This node does not report its process id (old version running?); restart the cluster.")

    banner(f"STEP 1  {args.users} users start booking / cancelling for {args.duration:.0f}s")
    before = fetch_availability(TARGETS)
    slots = sorted(before)
    stats = Stats()
    start = time.monotonic()
    deadline = start + args.duration
    users = [threading.Thread(target=user_session, args=(i, TARGETS, slots, deadline, stats, False, 0.3),
                              daemon=True) for i in range(args.users)]
    for t in users:
        t.start()

    time.sleep(max(0.0, args.kill_at - (time.monotonic() - start)))
    leader = find_leader(timeout=5) or leader  # leadership may have moved meanwhile
    old_id, old_term, pid = leader["node_id"], leader["current_term"], leader["pid"]
    banner(f"STEP 2  t={time.monotonic() - start:.0f}s  CRASHING the leader {old_id} (pid {pid}, term {old_term})")
    ops_at_kill = ok_ops(stats)
    with stats.lock:
        booked_at_kill = sum(stats.booked.values())
    killed_at = time.monotonic()
    crash(pid)
    say(f"  {old_id} killed (no clean shutdown). Its terminal shows the process ended.")

    new = find_leader(exclude=(old_id,), timeout=15)
    elect_s = time.monotonic() - killed_at
    banner("STEP 3  the two survivors elect a new leader")
    check("new leader elected by the survivors", new is not None and new["node_id"] != old_id,
          f"{new['node_id']} in term {new['current_term']}, {elect_s:.1f}s after the crash" if new else "none")
    if new is None:
        sys.exit(1)
    check("new term is higher than the crashed leader's", new["current_term"] > old_term,
          f"term {old_term} -> {new['current_term']}")

    time.sleep(max(0.0, args.restart_at - (time.monotonic() - start)))
    banner(f"STEP 4  t={time.monotonic() - start:.0f}s  restarting {old_id} from its saved data")
    restart(old_id, args.restart)

    say(f"\n  clients keep running until t={args.duration:.0f}s ...")
    for t in users:
        t.join(timeout=max(1.0, deadline - time.monotonic() + 30))
    with stats.lock:
        booked_after = sum(stats.booked.values()) - booked_at_kill
        exceptions = sum(n for c in stats.outcomes.values() for s, n in c.items() if s.startswith("exception"))

    banner("STEP 5  results")
    check("clients kept working through the failover",
          ok_ops(stats) > ops_at_kill and booked_after > 0 and exceptions == 0,
          f"{ok_ops(stats) - ops_at_kill} operations and {booked_after} bookings after the crash, "
          f"{exceptions} client errors")

    after = fetch_availability(TARGETS)
    bad = [s for s in slots if before[s] - after.get(s, 0) != stats.booked[s] - stats.cancelled[s]
           or after.get(s, 0) < 0]
    accepted, cancelled = sum(stats.booked.values()), sum(stats.cancelled.values())
    check("no overbooking and no lost updates (seat audit, every slot)", not bad,
          f"{accepted} bookings and {cancelled} cancellations all accounted for" if not bad
          else f"mismatch in {', '.join(bad)}")

    states, rejoined = {}, False
    wait_until = time.monotonic() + (25 if args.restart != "none" else 0)
    while True:
        states = cluster()
        live = [s for s in states.values() if s]
        rejoined = (states.get(old_id) is not None and states[old_id]["state"] == "follower"
                    and len(live) == len(NODES)
                    and len({(s["commit_index"], s["state_digest"]) for s in live}) == 1)
        if rejoined or time.monotonic() >= wait_until:
            break
        time.sleep(0.3)
    if args.restart != "none":
        check(f"crashed node {old_id} recovered from disk and caught up as a follower", rejoined,
              f"commit {states[old_id]['commit_index']}" if states.get(old_id) else "not running")
    live = [s for s in states.values() if s]
    check("all running replicas hold identical data (same state hash)",
          len({(s["commit_index"], s["state_digest"]) for s in live}) == 1,
          f"{len(live)} nodes, hash {live[0]['state_digest']}" if live else "")
    say("")
    show(states)

    passed = all(p for _, p, _ in checks)
    say(f"\n{BOLD}{'=' * 78}")
    if passed:
        say(f"  {GREEN}ALL {len(checks)} CHECKS PASSED{RESET}{BOLD}: leader {old_id} crashed, "
            f"{new['node_id']} took over in {elect_s:.1f}s, no data lost")
    else:
        failed = [name for name, p, _ in checks if not p]
        say(f"  {RED}{len(failed)} OF {len(checks)} CHECKS FAILED{RESET}{BOLD}: {'; '.join(failed)}")
    say(f"{'=' * 78}{RESET}")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

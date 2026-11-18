"""Cluster administration: membership changes and leadership transfer.

Implements the operator side of Raft section 6 (cluster membership changes,
one server at a time) and of leadership transfer (Raft thesis 3.10).

Examples (local 3-node cluster from scripts/run_cluster.py):

    # 1. start a 4th node that waits to be added (it replicates but does not vote)
    python scripts/run_node.py --node-id node-4 --client-port 50057 --raft-port 50058 --join \\
        --peers "node-1:localhost:50052:50051,node-2:localhost:50054:50053,node-3:localhost:50056:50055"

    # 2. add it, then remove another node
    python scripts/cluster_admin.py add node-4 --host localhost --raft-port 50058 --client-port 50057
    python scripts/cluster_admin.py remove node-3

    # planned maintenance: move leadership away before restarting the leader
    python scripts/cluster_admin.py transfer            # to the most up-to-date follower
    python scripts/cluster_admin.py transfer node-2

    python scripts/cluster_admin.py status

    # simulate a crash of the current leader (local nodes only, kill -9 / taskkill /F)
    python scripts/cluster_admin.py kill-leader

Membership changes and leadership transfer require an administrator
session. The tool logs in as --admin-user (default "admin"), creating the
admin account on first use with the cluster's admin sign-up code
(--admin-code, default: the ADMIN_SIGNUP_CODE environment variable, or
"aos-admin").
"""
import argparse
import json
import logging
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import grpc  # noqa: E402

from dist_ticket_booking.monitoring.console import setup_console  # noqa: E402

setup_console()
from dist_ticket_booking.grpc import ticket_service_pb2 as pb  # noqa: E402
from dist_ticket_booking.grpc import ticket_service_pb2_grpc  # noqa: E402
from dist_ticket_booking.core.auth import admin_signup_code  # noqa: E402
from dist_ticket_booking.grpc.service import TicketClient  # noqa: E402

DEFAULT_NODES = "localhost:50051,localhost:50053,localhost:50055"


def show_status(targets):
    print(f"{'ADDRESS':<18}{'NODE':<9}{'ROLE':<11}{'TERM':>5}{'COMMIT':>8}  MEMBERS")
    for t in targets:
        ch = grpc.insecure_channel(t)
        try:
            stub = ticket_service_pb2_grpc.TicketClientServiceStub(ch)
            r = stub.Get(pb.GetRequest(type="cluster_status"), timeout=1.5)
            st = json.loads(r.items[0].data)
            role = st["state"] if st.get("voting", True) else f"{st['state']}*"
            print(f"{t:<18}{st['node_id']:<9}{role:<11}{st['current_term']:>5}{st['commit_index']:>8}  "
                  f"{', '.join(st.get('members', []))}")
        except Exception:
            print(f"{t:<18}{'-':<9}{'DOWN':<11}")
        finally:
            ch.close()
    print("(* = not a voting member)")


def kill_leader(targets):
    """Crash the leader the hard way (no clean shutdown) to test failover."""
    import signal
    import subprocess
    import time
    for t in targets:
        ch = grpc.insecure_channel(t)
        try:
            st = json.loads(ticket_service_pb2_grpc.TicketClientServiceStub(ch).Get(
                pb.GetRequest(type="cluster_status"), timeout=1.0).items[0].data)
        except Exception:
            continue
        finally:
            ch.close()
        if st["state"] != "leader":
            continue
        pid = st.get("pid")
        if not pid:
            sys.exit("the leader does not report its process id; restart it with the current code")
        if os.name == "nt":
            subprocess.call(["taskkill", "/F", "/PID", str(pid)], stdout=subprocess.DEVNULL)
        else:
            os.kill(pid, signal.SIGKILL)
        print(f"killed leader {st['node_id']} (pid {pid}, term {st['current_term']}) at {t}")
        print("waiting for the survivors to elect a new leader ...")
        killed = time.monotonic()
        while time.monotonic() - killed < 15:
            for other in targets:
                if other == t:
                    continue
                c2 = grpc.insecure_channel(other)
                try:
                    s2 = json.loads(ticket_service_pb2_grpc.TicketClientServiceStub(c2).Get(
                        pb.GetRequest(type="cluster_status"), timeout=0.3).items[0].data)
                    if s2["state"] == "leader":
                        print(f"new leader: {s2['node_id']} in term {s2['current_term']} "
                              f"({time.monotonic() - killed:.1f}s after the crash)")
                        return
                except Exception:
                    pass
                finally:
                    c2.close()
            time.sleep(0.05)
        sys.exit("no new leader within 15 s")
    sys.exit("no leader found among " + ", ".join(targets))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", default=DEFAULT_NODES, help="comma-separated client endpoints to contact")
    parser.add_argument("--admin-user", default="admin")
    parser.add_argument("--admin-password", default="admin-password")
    parser.add_argument("--admin-code", default=admin_signup_code(),
                        help="admin sign-up code (only needed the first time the admin account is used)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    add = sub.add_parser("add", help="add one server to the cluster")
    add.add_argument("node_id")
    add.add_argument("--host", default="localhost")
    add.add_argument("--raft-port", type=int, required=True)
    add.add_argument("--client-port", type=int, required=True)
    rem = sub.add_parser("remove", help="remove one server from the cluster")
    rem.add_argument("node_id")
    tr = sub.add_parser("transfer", help="hand leadership to another member")
    tr.add_argument("target", nargs="?", default="")
    sub.add_parser("status", help="show every node's role, term and membership view")
    sub.add_parser("kill-leader", help="crash the current leader process (nodes on this machine only)")
    args = parser.parse_args()
    logging.disable(logging.WARNING)

    targets = [t.strip() for t in args.nodes.split(",") if t.strip()]
    if args.cmd == "status":
        show_status(targets)
        return
    if args.cmd == "kill-leader":
        return kill_leader(targets)

    client = TicketClient.for_cluster(targets)
    try:
        login = client.login_admin(args.admin_user, args.admin_password, args.admin_code)
        if login.status != "success":
            print(f"admin login failed: {login.status} - {login.message}")
            sys.exit(1)
        if args.cmd == "add":
            res = client.change_membership("add", args.node_id, args.host, args.raft_port, args.client_port)
        elif args.cmd == "remove":
            res = client.change_membership("remove", args.node_id)
        else:
            res = client.transfer_leadership(args.target)
    finally:
        client.close()
    print(f"{args.cmd}: {res.status} - {res.message}")
    sys.exit(0 if res.status == "success" else 1)


if __name__ == "__main__":
    main()

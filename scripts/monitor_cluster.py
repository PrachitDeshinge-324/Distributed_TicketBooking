"""Real-time terminal dashboard monitoring cluster nodes, roles, terms, and logs."""
import argparse
import json
import os
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import grpc
from dist_ticket_booking.grpc import ticket_service_pb2, ticket_service_pb2_grpc
from scripts.run_cluster import DEFAULT_CLUSTER_SPEC


def probe_node(node_spec, timeout=0.5):
    """Probe a single node's client gRPC endpoint to determine status, term, and role."""
    client_addr = f"localhost:{node_spec['client_port']}"
    channel = grpc.insecure_channel(client_addr)
    stub = ticket_service_pb2_grpc.TicketClientServiceStub(channel)

    info = {
        "node_id": node_spec["node_id"],
        "client_port": node_spec["client_port"],
        "raft_port": node_spec["raft_port"],
        "status": "OFFLINE",
        "role": "DOWN",
        "term": "-",
        "commit": "-",
        "log_len": "-",
        "leader_hint": "-",
    }

    try:
        req = ticket_service_pb2.GetRequest(token="", type="cluster_status")
        res = stub.Get(req, timeout=timeout)

        if res.status == "success" and res.items:
            data = json.loads(res.items[0].data)
            info["status"] = "ONLINE"
            raw_state = str(data.get("state") or "follower").lower()

            if raw_state == "leader":
                info["role"] = "★ LEADER"
                info["leader_hint"] = "(self)"
            elif raw_state == "candidate":
                info["role"] = "CANDIDATE"
                info["leader_hint"] = "-"
            else:
                info["role"] = "FOLLOWER"
                info["leader_hint"] = res.leader_hint or data.get("leader_id") or "-"

            info["term"] = data.get("current_term") if data.get("current_term") is not None else "-"
            info["commit"] = data.get("commit_index") if data.get("commit_index") is not None else "-"
            info["log_len"] = data.get("log_length") if data.get("log_length") is not None else "-"

        elif res.status == "redirect":
            info["status"] = "ONLINE"
            info["role"] = "FOLLOWER"
            info["leader_hint"] = res.leader_hint or "-"

    except grpc.RpcError:
        info["status"] = "DOWN / CRASHED"
        info["role"] = "-"
    finally:
        channel.close()

    return info


def render_dashboard(nodes_info):
    """Render terminal ASCII dashboard table."""
    os.system("clear" if os.name != "nt" else "cls")
    print("=" * 88)
    print("           DISTRIBUTED TICKET BOOKING SYSTEM — RAFT CLUSTER MONITOR             ")
    print("=" * 88)
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}  |  Nodes Configured: {len(nodes_info)}")
    print("-" * 88)
    header = (
        f"{'NODE ID':<9} | {'STATUS':<14} | {'ROLE':<10} | {'TERM':<5} | "
        f"{'COMMIT':<6} | {'LOGS':<5} | {'CLIENT':<7} | {'RAFT':<7} | {'LEADER HINT':<14}"
    )
    print(header)
    print("-" * 88)

    for n in nodes_info:
        node_id = str(n.get("node_id") or "-")
        status = str(n.get("status") or "-")
        role = str(n.get("role") or "-")
        term = str(n.get("term") if n.get("term") is not None else "-")
        commit = str(n.get("commit") if n.get("commit") is not None else "-")
        log_len = str(n.get("log_len") if n.get("log_len") is not None else "-")
        client_port = str(n.get("client_port") or "-")
        raft_port = str(n.get("raft_port") or "-")
        leader_hint = str(n.get("leader_hint") or "-")

        row = (
            f"{node_id:<9} | {status:<14} | {role:<10} | {term:<5} | "
            f"{commit:<6} | {log_len:<5} | {client_port:<7} | {raft_port:<7} | {leader_hint:<14}"
        )
        print(row)

    print("=" * 88)
    print("Leader Rule: In any term, Raft strictly guarantees at most ONE leader.")
    print("Press Ctrl+C to exit monitor.\n")


def main():
    parser = argparse.ArgumentParser(description="Monitor live Raft cluster status")
    parser.add_argument("--once", action="store_true", help="Print dashboard once and exit")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds")
    args = parser.parse_args()

    try:
        while True:
            infos = [probe_node(spec) for spec in DEFAULT_CLUSTER_SPEC]
            render_dashboard(infos)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


if __name__ == "__main__":
    main()

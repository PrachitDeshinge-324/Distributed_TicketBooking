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
from scripts.run_cluster import build_cluster_spec


def probe_llm(host="localhost", port=50060, timeout=1.0):
    """'ONLINE' if the LLM server accepts gRPC connections, else 'DOWN'.

    Connection-level check only, so the monitor never triggers model inference.
    """
    channel = grpc.insecure_channel(f"{host}:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout)
        return f"ONLINE ({host}:{port})"
    except grpc.FutureTimeoutError:
        return f"DOWN ({host}:{port})"
    finally:
        channel.close()


def probe_node(node_spec, timeout=0.5, host="localhost"):
    """Probe a single node's client gRPC endpoint to determine status, term, and role."""
    client_addr = f"{node_spec.get('host', host)}:{node_spec['client_port']}"
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
        "applied": "-",
        "digest": "-",
        "leader_hint": "-",
    }

    try:
        req = ticket_service_pb2.GetRequest(token="", type="cluster_status")
        res = stub.Get(req, timeout=timeout)

        if res.status == "success" and res.items:
            data = json.loads(res.items[0].data)
            info["status"] = "ONLINE"
            info["node_id"] = data.get("node_id") or info["node_id"]
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
            info["applied"] = data.get("last_applied") if data.get("last_applied") is not None else "-"
            info["digest"] = data.get("state_digest") or "-"

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


def render_dashboard(nodes_info, clear=True, llm_status=None):
    """Render terminal ASCII dashboard table."""
    if clear:
        os.system("clear" if os.name != "nt" else "cls")
    print("=" * 98)
    print("           DISTRIBUTED TICKET BOOKING SYSTEM — RAFT CLUSTER MONITOR             ")
    print("=" * 98)
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}  |  Nodes Configured: {len(nodes_info)}")
    print("-" * 98)
    header = (
        f"{'NODE ID':<9} | {'STATUS':<14} | {'ROLE':<10} | {'TERM':<5} | "
        f"{'COMMIT':<6} | {'APPLIED':<7} | {'LOGS':<5} | {'STATE DIGEST':<12} | {'CLIENT':<6} | {'LEADER HINT':<15}"
    )
    print(header)
    print("-" * 98)

    for n in nodes_info:
        node_id = str(n.get("node_id") or "-")
        status = str(n.get("status") or "-")
        role = str(n.get("role") or "-")
        term = str(n.get("term") if n.get("term") is not None else "-")
        commit = str(n.get("commit") if n.get("commit") is not None else "-")
        log_len = str(n.get("log_len") if n.get("log_len") is not None else "-")
        applied = str(n.get("applied") if n.get("applied") is not None else "-")
        digest = str(n.get("digest") or "-")
        client_port = str(n.get("client_port") or "-")
        raft_port = str(n.get("raft_port") or "-")
        leader_hint = str(n.get("leader_hint") or "-")

        row = (
            f"{node_id:<9} | {status:<14} | {role:<10} | {term:<5} | "
            f"{commit:<6} | {applied:<7} | {log_len:<5} | {digest:<12} | {client_port:<6} | {leader_hint:<15}"
        )
        print(row)

    print("=" * 98)
    digests = {n.get("digest") for n in nodes_info
               if n.get("digest") not in (None, "-") and n.get("applied") == max(
                   (m.get("applied") for m in nodes_info if isinstance(m.get("applied"), int)), default=None)}
    if digests:
        print("Replicas at the latest applied index: "
              + ("IDENTICAL state" if len(digests) == 1 else f"DIVERGED ({len(digests)} different states!)"))
    if llm_status is not None:
        print(f"LLM server (Node 1): {llm_status}")
    print("Leader Rule: In any term, Raft strictly guarantees at most ONE leader.")
    if clear:
        print("Press Ctrl+C to exit monitor.\n")


def main():
    parser = argparse.ArgumentParser(description="Monitor live Raft cluster status")
    parser.add_argument("--once", action="store_true", help="Print dashboard once and exit")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds")
    parser.add_argument("--nodes", type=int, default=3, help="Number of Raft nodes (default 3)")
    parser.add_argument("--host", default="localhost", help="Host running the nodes (default localhost)")
    parser.add_argument("--addrs", default="",
                        help="Multi-machine: comma-separated client endpoints host:port (overrides --nodes/--host)")
    parser.add_argument("--llm", default="localhost:50060", help="LLM server host:port ('' to skip)")
    args = parser.parse_args()
    if args.addrs:
        spec = []
        for addr in [a.strip() for a in args.addrs.split(",") if a.strip()]:
            h, p = addr.rsplit(":", 1)
            spec.append({"node_id": addr, "host": h, "client_port": int(p), "raft_port": "-"})
    else:
        spec = build_cluster_spec(args.nodes)

    try:
        while True:
            infos = [probe_node(n, host=args.host) for n in spec]
            llm = None
            if args.llm:
                h, p = args.llm.rsplit(":", 1)
                llm = probe_llm(h, int(p))
            render_dashboard(infos, clear=not args.once, llm_status=llm)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


if __name__ == "__main__":
    main()

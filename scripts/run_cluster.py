"""Cluster orchestrator: launches and manages a multi-node Raft cluster."""
import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ClusterManager] %(message)s",
)
logger = logging.getLogger("ClusterManager")

DEFAULT_CLUSTER_SPEC = [
    {"node_id": "node-1", "client_port": 50051, "raft_port": 50052},
    {"node_id": "node-2", "client_port": 50053, "raft_port": 50054},
    {"node_id": "node-3", "client_port": 50055, "raft_port": 50056},
]


def build_cluster_spec(n: int, llm_port: int = 50060):
    """Return a node spec list for an n-node cluster (ports avoid the LLM port)."""
    if n <= 3:
        return DEFAULT_CLUSTER_SPEC[:max(n, 1)]
    spec, port = [], 50051
    for i in range(n):
        if port <= llm_port <= port + 1:
            port = llm_port + 1
        spec.append({"node_id": f"node-{i + 1}", "client_port": port, "raft_port": port + 1})
        port += 2
    return spec


class ClusterManager:
    """Manages cluster node processes."""

    def __init__(
        self,
        python_bin: str = sys.executable,
        nodes=None,
        data_dir: str = "./raft_data",
        with_llm: bool = False,
        llm_port: int = 50060,
    ):
        self.python_bin = python_bin
        self.nodes_spec = nodes or DEFAULT_CLUSTER_SPEC
        self.data_dir = data_dir
        self.with_llm = with_llm
        self.llm_port = llm_port
        self.processes: Dict[str, subprocess.Popen] = {}
        self.llm_proc: Optional[subprocess.Popen] = None

    def _build_peers_str(self, current_node_id: str) -> str:
        peer_specs = []
        for n in self.nodes_spec:
            if n["node_id"] != current_node_id:
                peer_specs.append(f"{n['node_id']}:localhost:{n['raft_port']}:{n['client_port']}")
        return ",".join(peer_specs)

    def start_llm(self):
        if not self.with_llm:
            return
        logger.info(f"Starting standalone LLM server on port {self.llm_port}...")
        script_path = os.path.join(os.path.dirname(__file__), "run_llm_server.py")
        self.llm_proc = subprocess.Popen(
            [self.python_bin, script_path, "--port", str(self.llm_port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        time.sleep(1.0)

    def start_node(self, node_id: str):
        spec = next((n for n in self.nodes_spec if n["node_id"] == node_id), None)
        if not spec:
            logger.error(f"Unknown node_id: {node_id}")
            return

        if node_id in self.processes and self.processes[node_id].poll() is None:
            logger.warning(f"Node {node_id} is already running!")
            return

        peers_str = self._build_peers_str(node_id)
        script_path = os.path.join(os.path.dirname(__file__), "run_node.py")
        cmd = [
            self.python_bin,
            script_path,
            "--node-id",
            node_id,
            "--client-port",
            str(spec["client_port"]),
            "--raft-port",
            str(spec["raft_port"]),
            "--peers",
            peers_str,
            "--data-dir",
            self.data_dir,
            "--llm-port",
            str(self.llm_port),
        ]

        logger.info(f"Starting node {node_id} (client:{spec['client_port']}, raft:{spec['raft_port']})...")
        proc = subprocess.Popen(cmd)
        self.processes[node_id] = proc

    def start_all(self):
        self.start_llm()
        for spec in self.nodes_spec:
            self.start_node(spec["node_id"])
            time.sleep(0.3)
        logger.info("All cluster nodes launched. Waiting for leader election...")

    def kill_node(self, node_id: str, force: bool = True):
        proc = self.processes.get(node_id)
        if not proc or proc.poll() is not None:
            logger.warning(f"Node {node_id} is not currently running.")
            return

        logger.info(f"Killing node {node_id} (PID {proc.pid})...")
        if force:
            proc.kill()
        else:
            proc.terminate()
        proc.wait()
        logger.info(f"Node {node_id} terminated.")

    def restart_node(self, node_id: str):
        self.kill_node(node_id)
        time.sleep(0.5)
        logger.info(f"Restarting node {node_id}...")
        self.start_node(node_id)

    def stop_all(self):
        logger.info("Stopping all cluster processes...")
        for node_id, proc in list(self.processes.items()):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
        self.processes.clear()

        if self.llm_proc and self.llm_proc.poll() is None:
            self.llm_proc.terminate()
            try:
                self.llm_proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.llm_proc.kill()
            self.llm_proc = None
        logger.info("All cluster processes stopped.")


def main():
    parser = argparse.ArgumentParser(description="Manage Distributed Ticket Booking Raft Cluster")
    parser.add_argument("--nodes", type=int, default=3, help="Number of nodes (default 3)")
    parser.add_argument("--with-llm", action="store_true", help="Also start LLM server")
    parser.add_argument("--data-dir", type=str, default="./raft_data", help="Raft persistence folder")
    parser.add_argument("--clean", action="store_true", help="Clean data directory before start")
    args = parser.parse_args()

    if args.clean and os.path.exists(args.data_dir):
        import shutil
        shutil.rmtree(args.data_dir)
        logger.info(f"Cleaned data directory: {args.data_dir}")

    cluster_spec = build_cluster_spec(args.nodes)
    manager = ClusterManager(
        python_bin=sys.executable,
        nodes=cluster_spec,
        data_dir=args.data_dir,
        with_llm=args.with_llm,
    )

    def handle_signal(sig, frame):
        logger.info("\nReceived exit signal. Shutting down cluster...")
        manager.stop_all()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    manager.start_all()

    print("\nCluster is active. Press Ctrl+C to terminate.")
    print("Nodes:")
    for n in cluster_spec:
        print(f"  - {n['node_id']}: Client port {n['client_port']} | Raft port {n['raft_port']}")
    print("")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop_all()


if __name__ == "__main__":
    main()

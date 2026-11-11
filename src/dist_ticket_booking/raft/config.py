"""Raft cluster configuration and peer discovery."""
import os
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class PeerInfo:
    """Connection info for a peer Raft node."""
    node_id: str
    raft_host: str
    raft_port: int
    client_host: str
    client_port: int

    @property
    def raft_address(self) -> str:
        return f"{self.raft_host}:{self.raft_port}"

    @property
    def client_address(self) -> str:
        return f"{self.client_host}:{self.client_port}"


@dataclass
class ClusterConfig:
    """Raft cluster configuration."""
    node_id: str
    peers: Dict[str, PeerInfo] = field(default_factory=dict)

    # Raft timing parameters (milliseconds)
    heartbeat_interval_ms: int = 100
    # Wider than the paper's 150-300 ms: Python nodes on a shared laptop can
    # stall for a few hundred ms (GC, model loading), which would otherwise
    # trigger spurious elections. Failover still completes in ~1 s.
    election_timeout_min_ms: int = 600
    election_timeout_max_ms: int = 1200

    # Log compaction: take a snapshot once this many entries are in the log
    # (0 disables automatic snapshots).
    snapshot_threshold: int = 200

    # Persistence directory
    data_dir: str = "./raft_data"

    def __post_init__(self):
        os.makedirs(self.data_dir, exist_ok=True)

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1  # peers + self

    @property
    def majority(self) -> int:
        return self.cluster_size // 2 + 1

    @classmethod
    def from_args(
        cls,
        node_id: str,
        peers_str: str,
        data_dir: str = "./raft_data",
        heartbeat_ms: int = 100,
        election_min_ms: int = 600,
        election_max_ms: int = 1200,
        snapshot_threshold: int = 200,
    ) -> "ClusterConfig":
        """Parse peers from CLI format: 'node-2:localhost:50054:50053,node-3:localhost:50056:50055'

        Format per peer: <node_id>:<host>:<raft_port>:<client_port>
        """
        peers = {}
        if peers_str:
            for peer_spec in peers_str.split(","):
                spec = peer_spec.strip()
                if not spec:
                    continue
                parts = spec.split(":")
                if len(parts) != 4:
                    raise ValueError(
                        f"Invalid peer spec '{spec}'. "
                        f"Expected format: node_id:host:raft_port:client_port"
                    )
                pid, host, raft_port, client_port = (
                    parts[0],
                    parts[1],
                    int(parts[2]),
                    int(parts[3]),
                )
                peers[pid] = PeerInfo(pid, host, raft_port, host, client_port)
        return cls(
            node_id=node_id,
            peers=peers,
            data_dir=data_dir,
            heartbeat_interval_ms=heartbeat_ms,
            election_timeout_min_ms=election_min_ms,
            election_timeout_max_ms=election_max_ms,
            snapshot_threshold=snapshot_threshold,
        )

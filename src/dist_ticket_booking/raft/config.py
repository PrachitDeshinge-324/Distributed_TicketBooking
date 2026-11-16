"""Raft cluster configuration and peer discovery."""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


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

    def to_dict(self) -> dict:
        return {"node_id": self.node_id, "host": self.raft_host,
                "raft_port": self.raft_port, "client_port": self.client_port}

    @classmethod
    def from_dict(cls, d: dict) -> "PeerInfo":
        return cls(d["node_id"], d["host"], int(d["raft_port"]), d["host"], int(d["client_port"]))


@dataclass
class ClusterConfig:
    """Raft cluster configuration."""
    node_id: str
    peers: Dict[str, PeerInfo] = field(default_factory=dict)
    # This node's own address. Needed to write membership-change entries,
    # since every member's address is recorded in the log.
    self_info: Optional[PeerInfo] = None
    # False while this node is not part of the cluster configuration (a new
    # node waiting to be added, or a node that was removed). Such a node
    # replicates the log but never votes for itself or counts toward a majority.
    voting: bool = True

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
        """Number of voting members in the current configuration."""
        return len(self.peers) + (1 if self.voting else 0)

    @property
    def majority(self) -> int:
        return self.cluster_size // 2 + 1

    @property
    def is_solo(self) -> bool:
        """Single-member cluster: this node alone forms a majority."""
        return self.voting and not self.peers

    def member_list(self) -> List[dict]:
        """Current members (including self when voting), as stored in config entries."""
        members = [p.to_dict() for p in self.peers.values()]
        if self.voting and self.self_info is not None:
            members.append(self.self_info.to_dict())
        return sorted(members, key=lambda m: m["node_id"])

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
        host: str = "localhost",
        raft_port: int = 0,
        client_port: int = 0,
        voting: bool = True,
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
        self_info = PeerInfo(node_id, host, raft_port, host, client_port) if raft_port else None
        return cls(
            node_id=node_id,
            peers=peers,
            self_info=self_info,
            voting=voting,
            data_dir=data_dir,
            heartbeat_interval_ms=heartbeat_ms,
            election_timeout_min_ms=election_min_ms,
            election_timeout_max_ms=election_max_ms,
            snapshot_threshold=snapshot_threshold,
        )

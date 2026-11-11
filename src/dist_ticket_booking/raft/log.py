"""Raft log with JSON persistence and log compaction (snapshotting)."""
import json
import logging
import os
import threading
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)


@dataclass
class LogEntry:
    """A single entry in the Raft log."""
    term: int
    index: int
    command_type: str  # "create_booking", "cancel_booking", "login", "logout", "noop"
    command: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LogEntry":
        return cls(
            term=d["term"],
            index=d["index"],
            command_type=d["command_type"],
            command=d.get("command", {}),
        )

    def to_proto(self):
        """Convert to protobuf RaftLogEntry."""
        from dist_ticket_booking.grpc import ticket_service_pb2
        return ticket_service_pb2.RaftLogEntry(
            term=self.term,
            index=self.index,
            command_type=self.command_type,
            command_json=json.dumps(self.command),
        )

    @classmethod
    def from_proto(cls, proto) -> "LogEntry":
        """Create from protobuf RaftLogEntry."""
        return cls(
            term=proto.term,
            index=proto.index,
            command_type=proto.command_type,
            command=json.loads(proto.command_json) if proto.command_json else {},
        )


class RaftLog:
    """Manages the ordered Raft log with JSON persistence and log compaction.

    Entries are indexed by their global logical index.
    Log compaction discards entries up to `last_included_index`
    after they are committed and saved to stable snapshot storage.
    """

    def __init__(self, data_dir: str, node_id: str):
        self._entries: List[LogEntry] = []
        self._lock = threading.Lock()
        self._persist_path = os.path.join(data_dir, f"{node_id}_log.json")
        self._snapshot_path = os.path.join(data_dir, f"{node_id}_snapshot.json")
        self.last_included_index: int = -1
        self.last_included_term: int = 0
        self.snapshot: Optional[dict] = None  # in-memory copy of the latest snapshot
        self._load()

    def append(self, entry: LogEntry) -> int:
        """Append a log entry. Returns the entry's index."""
        with self._lock:
            self._entries.append(entry)
            self._persist()
            return entry.index

    def get(self, index: int) -> Optional[LogEntry]:
        """Get entry by global log index. Returns None if out of range or compacted."""
        with self._lock:
            for e in self._entries:
                if e.index == index:
                    return e
            return None

    def last_index(self) -> int:
        """Return the index of the last log entry, or snapshot's last_included_index if log is empty."""
        with self._lock:
            if self._entries:
                return self._entries[-1].index
            return self.last_included_index

    def last_term(self) -> int:
        """Return the term of the last log entry, or snapshot's last_included_term."""
        with self._lock:
            if self._entries:
                return self._entries[-1].term
            return self.last_included_term

    def term_at(self, index: int) -> Optional[int]:
        """Term of the entry at `index`, including the snapshot boundary.

        Returns None if the index is neither in the log nor the last
        compacted index (i.e. unknown to this node).
        """
        if index < 0:
            return 0
        with self._lock:
            if index == self.last_included_index:
                return self.last_included_term
            for e in self._entries:
                if e.index == index:
                    return e.term
            return None

    def slice_from(self, start_index: int) -> List[LogEntry]:
        """Return entries from start_index onwards (inclusive)."""
        with self._lock:
            return [e for e in self._entries if e.index >= start_index]

    def truncate_from(self, index: int):
        """Remove all entries from index onwards (for conflict resolution)."""
        with self._lock:
            old_count = len(self._entries)
            self._entries = [e for e in self._entries if e.index < index]
            if len(self._entries) < old_count:
                logger.warning(
                    f"Truncated log from index {index} (had {old_count}, now {len(self._entries)})"
                )
                self._persist()

    def create_snapshot(self, last_included_index: int, last_included_term: int, state_data: dict):
        """Save state machine snapshot and compact log entries up to last_included_index."""
        with self._lock:
            snapshot_content = {
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "state": state_data,
            }
            try:
                with open(self._snapshot_path, "w") as f:
                    json.dump(snapshot_content, f)
                self.last_included_index = last_included_index
                self.last_included_term = last_included_term
                self.snapshot = snapshot_content

                # Discard compacted entries
                self._entries = [e for e in self._entries if e.index > last_included_index]
                self._persist()
                logger.info(
                    f"Snapshot created at index {last_included_index} (term {last_included_term}). "
                    f"Compacted log now contains {len(self._entries)} entries."
                )
            except OSError as e:
                logger.error(f"Failed to create snapshot: {e}")

    def install_snapshot(self, last_included_index: int, last_included_term: int, state_data: dict):
        """Install a snapshot received from the leader (InstallSnapshot RPC).

        If the log already has an entry matching the snapshot's last index and
        term, the entries after it are kept; otherwise the whole log is
        discarded (Raft paper, Figure 13, steps 6-7).
        """
        with self._lock:
            keep_suffix = any(
                e.index == last_included_index and e.term == last_included_term for e in self._entries
            )
        self.create_snapshot(last_included_index, last_included_term, state_data)
        if not keep_suffix:
            with self._lock:
                self._entries = []
                self._persist()

    def load_snapshot(self) -> Optional[dict]:
        """Load snapshot file from disk if present."""
        if not os.path.exists(self._snapshot_path):
            return None
        try:
            with open(self._snapshot_path, "r") as f:
                data = json.load(f)
            self.last_included_index = data.get("last_included_index", -1)
            self.last_included_term = data.get("last_included_term", 0)
            self.snapshot = data
            return data
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load snapshot from {self._snapshot_path}: {e}")
            return None

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def _persist(self):
        """Write log to JSON file."""
        try:
            data = [e.to_dict() for e in self._entries]
            with open(self._persist_path, "w") as f:
                json.dump(data, f)
        except OSError as e:
            logger.error(f"Failed to persist log: {e}")

    def _load(self):
        """Load snapshot and log from JSON files on startup."""
        self.load_snapshot()
        if not os.path.exists(self._persist_path):
            return
        try:
            with open(self._persist_path, "r") as f:
                data = json.load(f)
            self._entries = [LogEntry.from_dict(d) for d in data]
            logger.info(
                f"Loaded {len(self._entries)} log entries (snapshot up to {self.last_included_index}) "
                f"from {self._persist_path}"
            )
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load log from {self._persist_path}: {e}")
            self._entries = []

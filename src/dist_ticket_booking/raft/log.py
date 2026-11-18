"""Raft log with append-only durable persistence and log compaction (snapshotting).

On disk the log is a JSON-lines file, one entry per line. Appending writes
only the new lines and fsyncs them, so the cost of an append does not grow
with the length of the log. The rare operations that remove entries
(conflict truncation, compaction, installing a snapshot) rewrite the file
atomically.
"""
import json
import logging
import os
import threading
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any

from dist_ticket_booking.raft.storage import (
    CorruptStateError, atomic_write_json, atomic_write_text, read_json,
)

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
    """Manages the ordered Raft log with durable persistence and log compaction.

    Entries are indexed by their global logical index and are always
    contiguous, so lookups are O(1) offsets from the first retained entry.
    Log compaction discards entries up to `last_included_index`
    after they are committed and saved to stable snapshot storage.

    Every mutation reaches stable storage (fsync) before the method returns,
    so an acknowledged entry survives a crash. Appends cost O(new entries).
    A crash in the middle of an append can only leave a torn *last* line;
    that entry was never acknowledged, so it is dropped on load.
    """

    def __init__(self, data_dir: str, node_id: str):
        self._entries: List[LogEntry] = []
        self._lock = threading.Lock()
        self._persist_path = os.path.join(data_dir, f"{node_id}_log.jsonl")
        # Whole-file JSON format used before the append-only log; migrated on load.
        self._legacy_path = os.path.join(data_dir, f"{node_id}_log.json")
        self._snapshot_path = os.path.join(data_dir, f"{node_id}_snapshot.json")
        self.last_included_index: int = -1
        self.last_included_term: int = 0
        self.snapshot: Optional[dict] = None  # in-memory copy of the latest snapshot
        self._load()

    def append(self, entry: LogEntry) -> int:
        """Append a log entry. Returns the entry's index."""
        with self._lock:
            self._entries.append(entry)
            self._append_to_disk([entry])
            return entry.index

    def extend(self, entries: List[LogEntry]) -> None:
        """Append several entries with a single disk write."""
        if not entries:
            return
        with self._lock:
            self._entries.extend(entries)
            self._append_to_disk(entries)

    def _find(self, index: int) -> Optional[LogEntry]:
        """Entry at a global index, or None. Caller holds self._lock."""
        if not self._entries:
            return None
        pos = index - self._entries[0].index
        if 0 <= pos < len(self._entries):
            entry = self._entries[pos]
            if entry.index == index:
                return entry
        return None

    def get(self, index: int) -> Optional[LogEntry]:
        """Get entry by global log index. Returns None if out of range or compacted."""
        with self._lock:
            return self._find(index)

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
            entry = self._find(index)
            return entry.term if entry else None

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

    def create_snapshot(self, last_included_index: int, last_included_term: int, state_data: dict,
                        config: Optional[list] = None):
        """Save state machine snapshot and compact log entries up to last_included_index.

        `config` is the cluster membership as of last_included_index: once the
        entries are compacted, the configuration entries among them are gone.
        """
        with self._lock:
            snapshot_content = {
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "state": state_data,
                "config": config,
            }
            try:
                # Snapshot first, then the shortened log: if we crash in
                # between, _load() drops the entries the snapshot covers.
                atomic_write_json(self._snapshot_path, snapshot_content)
                self.last_included_index = last_included_index
                self.last_included_term = last_included_term
                self.snapshot = snapshot_content

                # Discard compacted entries
                self._entries = [e for e in self._entries if e.index > last_included_index]
                self._persist()
                logger.debug(
                    f"Snapshot created at index {last_included_index} (term {last_included_term}). "
                    f"Compacted log now contains {len(self._entries)} entries."
                )
            except OSError as e:
                logger.error(f"Failed to create snapshot: {e}")

    def install_snapshot(self, last_included_index: int, last_included_term: int, state_data: dict,
                         config: Optional[list] = None):
        """Install a snapshot received from the leader (InstallSnapshot RPC).

        If the log already has an entry matching the snapshot's last index and
        term, the entries after it are kept; otherwise the whole log is
        discarded (Raft paper, Figure 13, steps 6-7).
        """
        with self._lock:
            keep_suffix = any(
                e.index == last_included_index and e.term == last_included_term for e in self._entries
            )
        self.create_snapshot(last_included_index, last_included_term, state_data, config)
        if not keep_suffix:
            with self._lock:
                self._entries = []
                self._persist()

    def load_snapshot(self) -> Optional[dict]:
        """Load snapshot file from disk if present."""
        if not os.path.exists(self._snapshot_path):
            return None
        data = read_json(self._snapshot_path)
        self.last_included_index = data.get("last_included_index", -1)
        self.last_included_term = data.get("last_included_term", 0)
        self.snapshot = data
        return data

    def latest_config(self, up_to: Optional[int] = None):
        """(index, members) of the newest configuration entry at or below `up_to`
        (whole log if None), falling back to the snapshot's configuration.
        Returns None if neither has one."""
        with self._lock:
            for e in reversed(self._entries):
                if e.command_type == "config" and (up_to is None or e.index <= up_to):
                    return e.index, e.command["members"]
            if self.snapshot and self.snapshot.get("config"):
                return self.last_included_index, self.snapshot["config"]
            return None

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    @staticmethod
    def _encode(entries: List[LogEntry]) -> str:
        return "".join(json.dumps(e.to_dict(), separators=(",", ":")) + "\n" for e in entries)

    def _append_to_disk(self, entries: List[LogEntry]):
        """Durably append entries to the log file. Caller holds self._lock."""
        try:
            with open(self._persist_path, "a", encoding="utf-8", newline="\n") as f:
                f.write(self._encode(entries))
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logger.error(f"Failed to persist log: {e}")
            raise

    def _persist(self):
        """Rewrite the whole log atomically (used after entries were removed).
        Caller holds self._lock."""
        try:
            atomic_write_text(self._persist_path, self._encode(self._entries))
        except OSError as e:
            logger.error(f"Failed to persist log: {e}")
            raise

    def _read_entries(self) -> List[LogEntry]:
        """Parse the log file. A torn final line (crash during an append) is
        dropped; damage anywhere else raises CorruptStateError."""
        try:
            with open(self._persist_path, "r", encoding="utf-8", newline="") as f:
                text = f.read()
        except OSError as exc:
            raise CorruptStateError(f"stable storage file {self._persist_path} is unreadable: {exc}") from exc
        lines = text.split("\n")
        complete, tail = lines[:-1], lines[-1]  # tail == "" when the file ends with a newline
        entries = []
        for n, line in enumerate(complete, 1):
            try:
                entries.append(LogEntry.from_dict(json.loads(line)))
            except (ValueError, KeyError, TypeError) as exc:
                raise CorruptStateError(
                    f"stable storage file {self._persist_path} is corrupt at line {n}: {exc}") from exc
        if tail.strip():
            logger.warning(f"Dropping a torn last line in {self._persist_path} "
                           f"(crash during an append; that entry was never acknowledged)")
            atomic_write_text(self._persist_path, self._encode(entries))
        return entries

    def _load(self):
        """Load snapshot and log from disk on startup.

        Raises CorruptStateError if a file exists but cannot be parsed: a
        node must never restart with an empty log it once acknowledged.
        """
        self.load_snapshot()
        if not os.path.exists(self._persist_path) and os.path.exists(self._legacy_path):
            entries = [LogEntry.from_dict(d) for d in read_json(self._legacy_path)]
            atomic_write_text(self._persist_path, self._encode(entries))
            os.remove(self._legacy_path)
        if not os.path.exists(self._persist_path):
            return
        entries = self._read_entries()
        # A crash between writing a snapshot and rewriting the log leaves
        # entries the snapshot already covers; they are safe to drop.
        self._entries = [e for e in entries if e.index > self.last_included_index]
        if self._entries and self._entries[0].index != self.last_included_index + 1:
            # Entries are missing between the snapshot and the log (e.g. the
            # snapshot file was deleted). Starting anyway would leave this node
            # unable to apply anything while reporting progress.
            raise CorruptStateError(
                f"{self._persist_path} starts at entry {self._entries[0].index} but the snapshot "
                f"only covers up to {self.last_included_index}; delete this node's data directory "
                f"(or start it with --fresh) so it re-syncs from the leader")
        logger.info(
            f"Loaded {len(self._entries)} log entries (snapshot up to {self.last_included_index}) "
            f"from {self._persist_path}"
        )

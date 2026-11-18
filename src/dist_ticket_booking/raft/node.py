"""Core Raft consensus node implementation.

Implements the Raft protocol as described in the paper:
'In Search of an Understandable Consensus Algorithm' by Ongaro & Ousterhout.

Responsibilities:
- Leader election with randomized timeouts
- Log replication via AppendEntries RPCs
- Commit index advancement on majority confirmation
- State machine application of committed entries
- Persistent state (currentTerm, votedFor, log) for crash recovery
- Log compaction with InstallSnapshot (section 7)
- Cluster membership changes, one server at a time (section 6; thesis 4.1)
- Pre-Vote, leadership transfer, leader lease and CheckQuorum (thesis 3.10,
  6.4.1, 9.6) for availability and linearizable reads
"""
import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, Optional, Set

import grpc

from dist_ticket_booking.grpc import ticket_service_pb2
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.raft.config import ClusterConfig, PeerInfo
from dist_ticket_booking.raft.log import RaftLog, LogEntry
from dist_ticket_booking.raft.state_machine import StateMachine
from dist_ticket_booking.raft.storage import atomic_write_json, read_json

logger = logging.getLogger(__name__)


# gRPC's default reconnect back-off grows to minutes. A peer that was down (or
# started a few seconds later, as when nodes are launched by hand in separate
# terminals) must be retried quickly, or elections and catch-up stall.
CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 200),
    ("grpc.min_reconnect_backoff_ms", 200),
    ("grpc.max_reconnect_backoff_ms", 500),
]


class NodeState(Enum):
    """Raft node states."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class PendingRequest:
    """Tracks a client request waiting for Raft commit."""
    event: threading.Event = field(default_factory=threading.Event)
    result: Optional[dict] = None


def describe_entry(entry: LogEntry, result: dict) -> str:
    """One readable line per committed command, for the node's terminal."""
    c, status = entry.command, result.get("status", "?")
    t = entry.command_type
    if t == "create_booking":
        outcome = f"booking {result['booking_id']}" if status == "accepted" else result.get("message", "")
        return f"BOOK   {c.get('ticket_id')} for user '{c.get('user_id')}' -> {status.upper()} ({outcome})"
    if t == "cancel_booking":
        return f"CANCEL {c.get('booking_id')} by user '{c.get('user_id')}' -> {status.upper()}"
    if t == "login":
        return f"LOGIN  user '{c.get('username')}' -> {status.upper()}"
    if t == "logout":
        return f"LOGOUT -> {status.upper()}"
    if t == "noop":
        return f"no-op of the term-{entry.term} leader (confirms all earlier entries)"
    if t == "config":
        return f"MEMBERSHIP -> {', '.join(m['node_id'] for m in c.get('members', []))}"
    return f"{t} -> {status}"


class RaftNode:
    """Complete Raft consensus node.

    Thread safety: all state mutations are protected by self._lock (RLock).
    Timer threads for elections and heartbeats run as daemon threads.
    """

    def __init__(self, config: ClusterConfig, booking_service):
        self.config = config
        self.node_id = config.node_id

        # ── Persistent state (survives restarts) ──
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log = RaftLog(config.data_dir, config.node_id)

        # ── Volatile state (all servers) ──
        self.state = NodeState.FOLLOWER
        self.commit_index: int = -1
        self.leader_id: Optional[str] = None

        # ── Volatile state (leader only) ──
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        # Per peer: send time (monotonic) of the newest AppendEntries it
        # acknowledged in our current term. Using the *send* time is what makes
        # the read lease safe: the follower got the message no earlier than that.
        self._last_ack: Dict[str, float] = {}
        # When we last became leader; gives CheckQuorum a grace period.
        self._leader_since: float = 0.0
        # Followers: last time we heard from a current leader (None = never).
        self._last_leader_contact: Optional[float] = None
        # Index of the no-op entry appended when we became leader; reads are
        # served only once it is committed (Raft paper, section 8).
        self._noop_index: int = -1
        # Highest commit index persisted to disk (lower bound on true commits).
        self._persisted_commit: int = -1

        # ── State machine ──
        self.state_machine = StateMachine(booking_service)

        # ── Peer gRPC channels and stubs ──
        self._peer_channels: Dict[str, grpc.Channel] = {}
        self._peer_stubs: Dict[str, ticket_service_pb2_grpc.RaftServiceStub] = {}

        # ── Pending client requests (leader only) ──
        self._pending_requests: Dict[int, PendingRequest] = {}

        # ── Cluster membership (Raft section 6) ──
        # Membership the node was started with; used until the log contains a
        # configuration entry. Index of the configuration currently in force
        # (-1 = bootstrap configuration).
        self._bootstrap_peers = dict(config.peers)
        self._bootstrap_voting = config.voting
        self._config_index: int = -1

        # ── Leadership transfer (thesis 3.10) ──
        self._transfer_target: Optional[str] = None

        self._replaying = False

        # ── Fault injection (tests / demos) ──
        # Peers this node is "partitioned" from: RPCs to and from them are
        # dropped, which simulates a network partition without killing anyone.
        self._blocked_peers: Set[str] = set()

        # ── Threading ──
        self._lock = threading.RLock()
        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._running = False

        # ── Persistence path for term/votedFor ──
        self._state_path = os.path.join(config.data_dir, f"{config.node_id}_state.json")
        self._load_persistent_state()
        self._refresh_membership()

    # ================================================================
    #  LIFECYCLE
    # ================================================================

    def start(self):
        """Start the Raft node. Connects to peers and begins election timer."""
        with self._lock:
            if self._running:
                return
            self._running = True
            logger.info(f"[{self.node_id}] Starting Raft node (term={self.current_term})")
            self._connect_to_peers()
            self._replay_committed_entries()
            self._reset_election_timer()

    def stop(self):
        """Gracefully stop the Raft node."""
        with self._lock:
            if not self._running:
                return
            logger.info(f"[{self.node_id}] Stopping Raft node")
            self._running = False
            self._cancel_election_timer()
            self._stop_heartbeat()
            # Wake up any pending requests
            for pending in self._pending_requests.values():
                pending.result = {"status": "error", "message": "node shutting down"}
                pending.event.set()
            self._pending_requests.clear()
            # Close peer channels
            for ch in self._peer_channels.values():
                try:
                    ch.close()
                except Exception:
                    pass
            self._peer_channels.clear()
            self._peer_stubs.clear()

    # ── Fault injection ──

    def set_partition(self, blocked_peers: Iterable[str]) -> None:
        """Simulate a network partition: drop all Raft RPCs to/from these peers.

        Pass an empty iterable to heal the partition.
        """
        with self._lock:
            self._blocked_peers = set(blocked_peers)
            logger.warning(f"[{self.node_id}] Partitioned from: {sorted(self._blocked_peers) or 'nobody'}")

    def is_partitioned_from(self, peer_id: str) -> bool:
        return peer_id in self._blocked_peers

    def _connect_to_peers(self):
        """Establish gRPC channels to all peer nodes."""
        for peer_id, peer_info in self.config.peers.items():
            if peer_id not in self._peer_channels:
                addr = peer_info.raft_address
                logger.debug(f"[{self.node_id}] Connecting to peer {peer_id} at {addr}")
                channel = grpc.insecure_channel(addr, options=CHANNEL_OPTIONS)
                self._peer_channels[peer_id] = channel
                self._peer_stubs[peer_id] = ticket_service_pb2_grpc.RaftServiceStub(channel)

    # ================================================================
    #  CLUSTER MEMBERSHIP (Raft section 6, single-server changes)
    # ================================================================
    #
    # A configuration is the list of voting members with their addresses. It
    # is stored as an ordinary log entry ("config") and, as the paper
    # prescribes, a server uses the newest configuration in its log whether
    # or not that entry is committed. Changing one server at a time keeps any
    # majority of the old configuration overlapping any majority of the new
    # one, so two leaders can never be elected in the same term (thesis 4.1).

    def _refresh_membership(self):
        """Adopt the newest configuration in the log/snapshot. Under self._lock."""
        found = self.log.latest_config()
        if found is None:
            # No configuration entry yet: the command-line membership applies.
            index, new_peers, voting = -1, dict(self._bootstrap_peers), self._bootstrap_voting
        else:
            index, members = found
            new_peers = {m["node_id"]: PeerInfo.from_dict(m) for m in members if m["node_id"] != self.node_id}
            voting = any(m["node_id"] == self.node_id for m in members)
        self._config_index = index

        old_ids, new_ids = set(self.config.peers), set(new_peers)
        if old_ids == new_ids and voting == self.config.voting:
            return
        self.config.peers = new_peers
        self.config.voting = voting
        logger.info(
            f"[{self.node_id}] Configuration (index {index}): members="
            f"{sorted(new_ids | ({self.node_id} if voting else set()))}"
        )
        for pid in old_ids - new_ids:
            ch = self._peer_channels.pop(pid, None)
            self._peer_stubs.pop(pid, None)
            for d in (self.next_index, self.match_index, self._last_ack):
                d.pop(pid, None)
            if ch is not None:
                try:
                    ch.close()
                except Exception:
                    pass
        if self._running:
            self._connect_to_peers()
        if self.state == NodeState.LEADER:
            for pid in new_ids - old_ids:
                self.next_index[pid] = self.log.last_index() + 1
                self.match_index[pid] = -1
                self._last_ack[pid] = time.monotonic()  # grace period for the newcomer

    def _members_at(self, index: int) -> Optional[list]:
        """Membership in force at `index` (None = the bootstrap configuration)."""
        found = self.log.latest_config(up_to=index)
        return found[1] if found else None

    def change_membership(self, action: str, node_id: str, host: str = "",
                          raft_port: int = 0, client_port: int = 0, timeout: float = 10.0) -> dict:
        """Add or remove ONE server (leader only). Blocks until committed."""
        with self._lock:
            if self.state != NodeState.LEADER:
                return {"status": "error", "message": "not_leader",
                        "leader_hint": self.get_leader_address() or ""}
            if self.config.self_info is None:
                return {"status": "error", "message": "this node was started without its own address"}
            if self.commit_index < self._noop_index:
                # Thesis 4.1 (bug fix): a leader may not change the configuration
                # until it has committed an entry in its own term.
                return {"status": "unavailable", "message": "leader not ready yet, retry"}
            if self._config_index > self.commit_index:
                return {"status": "error", "message": "another membership change is still in progress"}
            members = {m["node_id"]: m for m in self.config.member_list()}
            if action == "add":
                if node_id in members:
                    return {"status": "error", "message": f"{node_id} is already a member"}
                if not (host and raft_port and client_port):
                    return {"status": "error", "message": "add needs host, raft_port and client_port"}
                members[node_id] = PeerInfo(node_id, host, raft_port, host, client_port).to_dict()
            elif action == "remove":
                if node_id not in members:
                    return {"status": "error", "message": f"{node_id} is not a member"}
                if len(members) == 1:
                    return {"status": "error", "message": "cannot remove the last member"}
                del members[node_id]
            else:
                return {"status": "error", "message": f"unknown action {action!r}"}
            new_members = sorted(members.values(), key=lambda m: m["node_id"])
        logger.info(f"[{self.node_id}] Membership change: {action} {node_id}")
        return self.propose("config", {"members": new_members}, timeout=timeout)

    def _replay_committed_entries(self):
        """On startup, load snapshot if present and replay committed entries to state machine."""
        snapshot = self.log.load_snapshot()
        if snapshot:
            self.state_machine.restore_from_snapshot(snapshot)
            self.commit_index = max(self.commit_index, self.log.last_included_index)
            logger.info(
                f"[{self.node_id}] Restored state machine from snapshot up to index {self.state_machine.last_applied}"
            )

        # Replay only entries known to be committed. Entries after that may be
        # uncommitted leftovers that a new leader will overwrite; they are
        # applied later, once the leader's commit index covers them.
        known_committed = min(self._persisted_commit, self.log.last_index())
        if known_committed > self.commit_index:
            self.commit_index = known_committed
            self._replaying = True
            try:
                self._apply_committed_entries()
            finally:
                self._replaying = False
            logger.info(
                f"[{self.node_id}] Replayed committed log entries up to index {known_committed}. "
                f"State machine now at index {self.state_machine.last_applied}"
            )

    def take_snapshot(self) -> bool:
        """Take a snapshot of current state machine and compact log up to last_applied."""
        with self._lock:
            if self.state_machine.last_applied < 0:
                return False
            applied_idx = self.state_machine.last_applied
            if applied_idx <= self.log.last_included_index:
                return False  # nothing new to compact
            term = self.log.term_at(applied_idx)
            if term is None:
                term = self.current_term
            # Deep, point-in-time copy: the snapshot must not share mutable
            # objects (e.g. booking records) with the live state machine, or
            # later changes would leak into a snapshot labelled with an older
            # index and corrupt any follower that installs it.
            data = json.loads(json.dumps(self.state_machine.get_snapshot_data()))
            self.log.create_snapshot(applied_idx, term, data, config=self._members_at(applied_idx))
            logger.info(f"[{self.node_id}] Snapshot taken at index {applied_idx}: log compacted "
                        f"to {len(self.log)} entries")
            return True

    def _maybe_snapshot(self):
        """Compact the log once it exceeds the configured threshold. Under self._lock."""
        threshold = self.config.snapshot_threshold
        if threshold > 0 and len(self.log) >= threshold:
            self.take_snapshot()

    # ================================================================
    #  PERSISTENCE (currentTerm + votedFor)
    # ================================================================

    def _persist_state(self):
        """Persist currentTerm and votedFor to disk (Raft requirement)."""
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            # Not required by Raft, but lets a restarted node rebuild its
            # in-memory state machine up to entries known to be committed
            # (and never beyond: uncommitted entries may be overwritten).
            "commit_index": self._persisted_commit,
        }
        try:
            atomic_write_json(self._state_path, data)
        except OSError as e:
            # Answering an RPC without a durable term/vote could let this node
            # vote twice after a restart, so fail loudly instead of continuing.
            logger.critical(f"[{self.node_id}] Failed to persist state: {e}")
            raise

    def _load_persistent_state(self):
        """Load currentTerm and votedFor from disk on startup."""
        if not os.path.exists(self._state_path):
            return
        data = read_json(self._state_path)  # raises CorruptStateError if unreadable
        self.current_term = data.get("current_term", 0)
        self.voted_for = data.get("voted_for")
        self._persisted_commit = data.get("commit_index", -1)
        logger.info(
            f"[{self.node_id}] Loaded persistent state: "
            f"term={self.current_term}, votedFor={self.voted_for}"
        )

    # ================================================================
    #  ELECTION TIMER
    # ================================================================

    def _reset_election_timer(self):
        """Reset the randomized election timeout."""
        self._cancel_election_timer()
        if not self._running:
            return
        timeout = random.uniform(
            self.config.election_timeout_min_ms / 1000.0,
            self.config.election_timeout_max_ms / 1000.0,
        )
        self._election_timer = threading.Timer(timeout, self._election_timeout_handler)
        self._election_timer.daemon = True
        self._election_timer.start()

    def _cancel_election_timer(self):
        if self._election_timer is not None:
            self._election_timer.cancel()
            self._election_timer = None

    def _election_timeout_handler(self):
        """Called when election timeout expires — start an election."""
        if not self._running:
            return
        with self._lock:
            if self.state == NodeState.LEADER:
                return  # leaders do not run election timers
            if not self.config.voting:
                # Not (or no longer) a member: never disrupt the cluster.
                self._reset_election_timer()
                return
        self._start_election()

    # ================================================================
    #  LEADER ELECTION
    # ================================================================

    def _pre_vote(self) -> bool:
        """Pre-Vote round (Raft thesis 9.6).

        Ask the peers whether they *would* vote for us in the next term,
        without anyone changing state. Only if a majority says yes do we
        increment our term. A node cut off by a partition therefore cannot
        keep inflating its term and force a needless re-election when the
        partition heals.
        """
        with self._lock:
            if not self._running or self.state == NodeState.LEADER or not self.config.voting:
                return False
            term = self.current_term + 1
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()
            peers = list(self.config.peers)
            needed = self.config.majority
        granted = [1]  # ourselves
        done = threading.Event()
        count_lock = threading.Lock()

        def ask(peer_id):
            stub = self._peer_stubs.get(peer_id)
            if not stub or self.is_partitioned_from(peer_id):
                return
            try:
                resp = stub.RequestVote(ticket_service_pb2.RequestVoteRequest(
                    from_node=self.node_id, to_node=peer_id, term=term,
                    last_log_index=last_log_index, last_log_term=last_log_term, pre_vote=True,
                ), timeout=0.4)
            except Exception:
                return
            if resp.vote_granted:
                with count_lock:
                    granted[0] += 1
                    if granted[0] >= needed:
                        done.set()

        threads = [threading.Thread(target=ask, args=(p,), daemon=True) for p in peers]
        for t in threads:
            t.start()
        deadline = time.monotonic() + 0.5
        while not done.is_set() and time.monotonic() < deadline and any(t.is_alive() for t in threads):
            done.wait(0.01)
        ok = granted[0] >= needed
        if not ok:
            logger.info(f"[{self.node_id}] Pre-vote for term {term} failed "
                        f"({granted[0]}/{needed}); staying at term {self.current_term}")
        return ok

    def _start_election(self, leadership_transfer: bool = False):
        """Transition to candidate and solicit votes from all peers."""
        if not leadership_transfer and not self.config.is_solo and not self._pre_vote():
            with self._lock:
                self._reset_election_timer()
            return
        with self._lock:
            if not self._running or self.state == NodeState.LEADER or not self.config.voting:
                return
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self._last_leader_contact = None
            self._persist_state()

            term = self.current_term
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()
            logger.info(
                f"[{self.node_id}] Starting election for term {term} "
                f"(lastLogIndex={last_log_index}, lastLogTerm={last_log_term})"
            )

        # Single-node cluster becomes leader immediately
        if self.config.is_solo:
            with self._lock:
                if self.state == NodeState.CANDIDATE and self.current_term == term:
                    self._become_leader()
            return

        votes_received = 1  # vote for self (only touched under self._lock)

        def request_vote_from(peer_id: str):
            nonlocal votes_received
            stub = self._peer_stubs.get(peer_id)
            if not stub or self.is_partitioned_from(peer_id):
                return
            try:
                response = stub.RequestVote(
                    ticket_service_pb2.RequestVoteRequest(
                        from_node=self.node_id,
                        to_node=peer_id,
                        term=term,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term,
                        leadership_transfer=leadership_transfer,
                    ),
                    timeout=0.4,
                )
                with self._lock:
                    if response.term > self.current_term:
                        self._step_down(response.term)
                        return
                    if (
                        response.vote_granted
                        and self.state == NodeState.CANDIDATE
                        and self.current_term == term
                    ):
                        votes_received += 1
                        # Take office the moment a majority is reached rather
                        # than waiting for slow or dead peers to time out.
                        if votes_received >= self.config.majority:
                            self._become_leader()
            except Exception as e:
                logger.debug(f"[{self.node_id}] Vote request to {peer_id} failed: {e}")

        # Send RequestVote RPCs in parallel
        threads = []
        for peer_id in list(self.config.peers.keys()):
            t = threading.Thread(
                target=request_vote_from, args=(peer_id,), daemon=True
            )
            t.start()
            threads.append(t)

        # One shared deadline for the whole round (not 0.5 s per peer), and
        # stop waiting as soon as the election is decided either way.
        deadline = time.monotonic() + 0.5
        for t in threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            with self._lock:
                if self.state != NodeState.CANDIDATE or self.current_term != term:
                    return  # won (became leader) or stepped down
            t.join(timeout=remaining)

        with self._lock:
            if self.state == NodeState.CANDIDATE and self.current_term == term:
                logger.info(
                    f"[{self.node_id}] Election for term {term} failed "
                    f"(got {votes_received}/{self.config.majority} votes)"
                )
                self._reset_election_timer()

    def _become_leader(self):
        """Transition to leader state. Must be called under self._lock."""
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        self._leader_since = time.monotonic()

        next_idx = self.log.last_index() + 1
        for peer_id in self.config.peers:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = -1
            # No acknowledgements yet in this term: the first heartbeat round
            # (sent right below) establishes quorum contact and the read lease.
            self._last_ack[peer_id] = 0.0

        self._cancel_election_timer()
        logger.info(
            f"[{self.node_id}] ★ BECAME LEADER for term {self.current_term} "
            f"(log_len={len(self.log)})"
        )

        # Append a no-op in our own term. A leader may only commit entries from
        # its current term directly; committing this no-op also commits every
        # earlier entry, so recovered state becomes visible without waiting for
        # the next client write (Raft paper, sections 5.4.2 and 8).
        noop = LogEntry(
            term=self.current_term,
            index=self.log.last_index() + 1,
            command_type="noop",
            command={},
        )
        self.log.append(noop)
        self._noop_index = noop.index
        self._transfer_target = None
        if self.config.is_solo:
            self.commit_index = noop.index
            self._apply_committed_entries()

        self._start_heartbeat()
        self._send_append_entries_to_all()

    def _step_down(self, new_term: int):
        """Step down to follower upon discovering a higher term.

        Must be called under self._lock.
        """
        was_leader = self.state == NodeState.LEADER
        self._transfer_target = None
        if new_term > self.current_term:
            # Only a new term resets our vote; clearing it within the same term
            # would let this node vote twice and allow two leaders per term.
            self.current_term = new_term
            self.voted_for = None
        self.state = NodeState.FOLLOWER
        self._persist_state()

        if was_leader:
            logger.info(f"[{self.node_id}] Stepped down from leader (new term {new_term})")
            self._stop_heartbeat()
            for pending in self._pending_requests.values():
                pending.result = {"status": "error", "message": "lost leadership"}
                pending.event.set()
            self._pending_requests.clear()

        self._reset_election_timer()

    # ================================================================
    #  HEARTBEAT (Leader Only)
    # ================================================================

    def _start_heartbeat(self):
        """Start the heartbeat sender thread."""
        # A fresh stop event per leadership term: the previous loop (if any)
        # keeps its own, already-set event and can never be revived.
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, args=(self._heartbeat_stop,), daemon=True
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat(self):
        """Signal the heartbeat sender to exit.

        Does not join: callers hold self._lock, which the heartbeat thread may
        be waiting for, so joining here would stall the node.
        """
        self._heartbeat_stop.set()
        self._heartbeat_thread = None

    def _heartbeat_loop(self, stop: threading.Event):
        """Periodically send AppendEntries (heartbeats) to all peers."""
        interval = self.config.heartbeat_interval_ms / 1000.0
        while not stop.is_set() and self._running:
            self._send_append_entries_to_all()
            stop.wait(timeout=interval)

    # ================================================================
    #  LOG REPLICATION (Leader)
    # ================================================================

    def _send_append_entries_to_all(self):
        """Leader sends AppendEntries to every peer."""
        with self._lock:
            if self.state != NodeState.LEADER:
                return

        for peer_id in list(self.config.peers.keys()):
            if peer_id in self._peer_stubs:
                threading.Thread(
                    target=self._send_append_entries_to_peer,
                    args=(peer_id,),
                    daemon=True,
                ).start()

    def _send_append_entries_to_peer(self, peer_id: str):
        """Send AppendEntries RPC to a single peer."""
        with self._lock:
            if self.state != NodeState.LEADER:
                return

            next_idx = self.next_index.get(peer_id, 0)
            if next_idx <= self.log.last_included_index:
                # The entries this follower needs were compacted: ship the snapshot.
                send_snapshot = True
            else:
                send_snapshot = False
                prev_index = next_idx - 1
                prev_term = self.log.term_at(prev_index) or 0
                entries = self.log.slice_from(next_idx)
            term = self.current_term
            commit = self.commit_index

        stub = self._peer_stubs.get(peer_id)
        if not stub or self.is_partitioned_from(peer_id):
            return
        if send_snapshot:
            self._send_install_snapshot(peer_id, stub)
            return

        sent_at = time.monotonic()
        try:
            response = stub.AppendEntries(
                ticket_service_pb2.AppendEntriesRequest(
                    from_node=self.node_id,
                    to_node=peer_id,
                    term=term,
                    prev_log_index=prev_index,
                    prev_log_term=prev_term,
                    commit_index=commit,
                    entries=[e.to_proto() for e in entries],
                ),
                timeout=0.4,
            )

            with self._lock:
                if response.term > self.current_term:
                    self._step_down(response.term)
                    return

                if self.state != NodeState.LEADER:
                    return

                self._last_ack[peer_id] = max(self._last_ack.get(peer_id, 0.0), sent_at)
                if response.entry_appended:
                    if entries:
                        # max(): replies can arrive out of order, and an older
                        # one must not move matchIndex backwards.
                        self.match_index[peer_id] = max(
                            self.match_index.get(peer_id, -1), response.match_index
                        )
                        self.next_index[peer_id] = self.match_index[peer_id] + 1
                    self._try_advance_commit_index()
                else:
                    # Log inconsistency: back up. The follower reports its last
                    # index, so jump straight past entries it cannot have.
                    self.next_index[peer_id] = max(
                        0, min(self.next_index[peer_id] - 1, response.match_index + 1)
                    )
        except Exception as e:
            logger.debug(f"[{self.node_id}] AppendEntries to {peer_id} failed: {e}")

    def _send_install_snapshot(self, peer_id: str, stub):
        """Send the leader's snapshot to a follower that is behind the compacted log."""
        with self._lock:
            if self.state != NodeState.LEADER or not self.log.snapshot:
                return
            snap = self.log.snapshot
            term = self.current_term
            request = ticket_service_pb2.InstallSnapshotRequest(
                from_node=self.node_id,
                to_node=peer_id,
                term=term,
                last_included_index=snap["last_included_index"],
                last_included_term=snap["last_included_term"],
                data=json.dumps(snap["state"]),
                config_json=json.dumps(snap.get("config") or self._members_at(snap["last_included_index"])),
            )
        sent_at = time.monotonic()
        try:
            response = stub.InstallSnapshot(request, timeout=2.0)
        except Exception as e:
            logger.debug(f"[{self.node_id}] InstallSnapshot to {peer_id} failed: {e}")
            return
        with self._lock:
            if response.term > self.current_term:
                self._step_down(response.term)
                return
            if self.state != NodeState.LEADER:
                return
            self._last_ack[peer_id] = max(self._last_ack.get(peer_id, 0.0), sent_at)
            if response.success:
                self.match_index[peer_id] = max(self.match_index.get(peer_id, -1), response.match_index)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                logger.info(
                    f"[{self.node_id}] Installed snapshot on {peer_id} up to index {response.match_index}"
                )
                self._try_advance_commit_index()

    def _try_advance_commit_index(self):
        """Check if any new entries can be committed (majority replicated).

        Must be called under self._lock.
        """
        for n in range(self.commit_index + 1, self.log.last_index() + 1):
            entry = self.log.get(n)
            if entry is None:
                break
            if entry.term != self.current_term:
                continue

            replicated = 1 if self.config.voting else 0  # the leader has it, if it is a member
            for peer_id in self.config.peers:
                if self.match_index.get(peer_id, -1) >= n:
                    replicated += 1

            if replicated >= self.config.majority:
                self.commit_index = n

        self._apply_committed_entries()

        if (self.state == NodeState.LEADER and not self.config.voting
                and self.commit_index >= self._config_index):
            # The configuration that removed us is committed: hand over by
            # stepping down; the remaining members elect a new leader (thesis 4.2.2).
            logger.info(f"[{self.node_id}] Removed from the cluster; stepping down")
            self._step_down(self.current_term)

    def _apply_committed_entries(self):
        """Apply all committed but unapplied entries to the state machine."""
        while self.state_machine.last_applied < self.commit_index:
            next_index = self.state_machine.last_applied + 1
            entry = self.log.get(next_index)
            if entry is None:
                break

            result = self.state_machine.apply(entry)
            # Replaying our own log after a restart is summarised once by the
            # caller instead of printing every entry again.
            (logger.debug if self._replaying else logger.info)(
                f"[{self.node_id}] COMMITTED #{next_index}: {describe_entry(entry, result)}"
            )

            if next_index in self._pending_requests:
                pending = self._pending_requests.pop(next_index)
                pending.result = result
                pending.event.set()

        if self.commit_index > self._persisted_commit:
            self._persisted_commit = self.commit_index
            self._persist_state()
        self._maybe_snapshot()

    # ================================================================
    #  RPC HANDLERS (called by RaftServiceServicer)
    # ================================================================

    def handle_request_vote(self, request) -> ticket_service_pb2.RequestVoteResponse:
        """Handle incoming RequestVote RPC from a candidate."""
        with self._lock:
            my_last_term, my_last_index = self.log.last_term(), self.log.last_index()
            candidate_log_ok = (
                request.last_log_term > my_last_term
                or (request.last_log_term == my_last_term and request.last_log_index >= my_last_index)
            )
            leader_alive = self.state == NodeState.LEADER or (
                self.state == NodeState.FOLLOWER
                and self._last_leader_contact is not None
                and time.monotonic() - self._last_leader_contact
                < self.config.election_timeout_min_ms / 1000.0
            )

            if request.pre_vote:
                # Pre-Vote: answer "would I vote for you?" without touching
                # our term, vote or timers.
                return ticket_service_pb2.RequestVoteResponse(
                    from_node=self.node_id,
                    to_node=request.from_node,
                    term=self.current_term,
                    vote_granted=(request.term > self.current_term and candidate_log_ok
                                  and not leader_alive),
                )

            # Raft thesis 4.2.3 / 6.4.1: while we still hear from a live leader,
            # ignore vote requests entirely (without adopting their term). A
            # partitioned or flaky node then cannot depose a healthy leader, and
            # a leader's read lease stays valid for one minimum election timeout.
            # A leadership transfer is the one deliberate exception.
            if (
                not request.leadership_transfer
                and self.state == NodeState.FOLLOWER
                and leader_alive
            ):
                return ticket_service_pb2.RequestVoteResponse(
                    from_node=self.node_id,
                    to_node=request.from_node,
                    term=self.current_term,
                    vote_granted=False,
                )

            if request.term > self.current_term:
                self._step_down(request.term)

            vote_granted = False

            if request.term < self.current_term:
                vote_granted = False
            elif self.voted_for is None or self.voted_for == request.from_node:
                my_last_term = self.log.last_term()
                my_last_index = self.log.last_index()

                log_ok = (
                    request.last_log_term > my_last_term
                    or (
                        request.last_log_term == my_last_term
                        and request.last_log_index >= my_last_index
                    )
                )

                if log_ok:
                    self.voted_for = request.from_node
                    vote_granted = True
                    self._persist_state()
                    self._reset_election_timer()
                    logger.info(
                        f"[{self.node_id}] Voted for {request.from_node} in term {self.current_term}"
                    )

            return ticket_service_pb2.RequestVoteResponse(
                from_node=self.node_id,
                to_node=request.from_node,
                term=self.current_term,
                vote_granted=vote_granted,
            )

    def handle_append_entries(self, request) -> ticket_service_pb2.AppendEntriesResponse:
        """Handle incoming AppendEntries RPC (heartbeat or log replication)."""
        with self._lock:
            if request.term < self.current_term:
                return ticket_service_pb2.AppendEntriesResponse(
                    from_node=self.node_id,
                    to_node=request.from_node,
                    term=self.current_term,
                    entry_appended=False,
                    match_index=self.log.last_index(),
                )

            if request.term > self.current_term or self.state != NodeState.FOLLOWER:
                self._step_down(request.term)
            self.leader_id = request.from_node
            self._last_leader_contact = time.monotonic()
            self._reset_election_timer()

            # Log consistency check (snapshot-aware: anything at or below the
            # snapshot boundary is committed and therefore matches).
            if request.prev_log_index > self.log.last_included_index:
                prev_term = self.log.term_at(request.prev_log_index)
                if prev_term is None or prev_term != request.prev_log_term:
                    return ticket_service_pb2.AppendEntriesResponse(
                        from_node=self.node_id,
                        to_node=request.from_node,
                        term=self.current_term,
                        entry_appended=False,
                        match_index=self.log.last_index(),
                    )

            # Append new entries (handling conflict truncations)
            new_entries = []
            truncated = False
            for proto_entry in request.entries:
                entry = LogEntry.from_proto(proto_entry)
                if entry.index <= self.log.last_included_index:
                    continue  # already covered by our snapshot
                if new_entries:
                    new_entries.append(entry)  # everything after the first new entry is new
                    continue
                existing = self.log.get(entry.index)
                if existing is not None and existing.term != entry.term:
                    # Conflict: drop our entry and everything after it (Raft 5.3).
                    self.log.truncate_from(entry.index)
                    truncated = True
                    existing = None
                if existing is None:
                    new_entries.append(entry)
            self.log.extend(new_entries)  # one durable write for the whole batch
            if truncated or any(e.command_type == "config" for e in new_entries):
                self._refresh_membership()  # configurations take effect when appended

            # Index of the last entry this RPC vouches for. Using our own
            # last_index() here would be unsafe: stale entries from an old
            # term beyond the leader's range could be counted (or applied)
            # as if they matched the leader's log (Raft Figure 2, rule 5).
            last_new_index = max(request.prev_log_index + len(request.entries), self.log.last_included_index)

            # Update commit index
            new_commit = min(request.commit_index, last_new_index)
            if new_commit > self.commit_index:  # commitIndex never moves backwards
                self.commit_index = new_commit
                self._apply_committed_entries()

            return ticket_service_pb2.AppendEntriesResponse(
                from_node=self.node_id,
                to_node=request.from_node,
                term=self.current_term,
                entry_appended=True,
                match_index=last_new_index,
            )

    def handle_install_snapshot(self, request) -> ticket_service_pb2.InstallSnapshotResponse:
        """Handle InstallSnapshot RPC from the leader (Raft paper, Figure 13)."""
        with self._lock:
            def reply(success: bool):
                return ticket_service_pb2.InstallSnapshotResponse(
                    from_node=self.node_id,
                    to_node=request.from_node,
                    term=self.current_term,
                    success=success,
                    match_index=max(self.log.last_included_index, self.state_machine.last_applied),
                )

            if request.term < self.current_term:
                return reply(False)
            if request.term > self.current_term or self.state != NodeState.FOLLOWER:
                self._step_down(request.term)
            self.leader_id = request.from_node
            self._last_leader_contact = time.monotonic()
            self._reset_election_timer()

            if request.last_included_index <= self.state_machine.last_applied:
                return reply(True)  # we already have everything in this snapshot

            state = json.loads(request.data)
            config = json.loads(request.config_json) if request.config_json else None
            self.log.install_snapshot(request.last_included_index, request.last_included_term, state, config)
            self._refresh_membership()
            self.state_machine.restore_from_snapshot(state)
            self.state_machine.last_applied = request.last_included_index
            self.commit_index = max(self.commit_index, request.last_included_index)
            logger.info(
                f"[{self.node_id}] Installed snapshot from {request.from_node} "
                f"up to index {request.last_included_index}"
            )
            self._apply_committed_entries()
            return reply(True)

    def handle_timeout_now(self, request) -> ticket_service_pb2.TimeoutNowResponse:
        """TimeoutNow RPC (thesis 3.10): the leader asks us to take over now."""
        with self._lock:
            ok = (
                self._running
                and request.term == self.current_term
                and self.state == NodeState.FOLLOWER
                and self.config.voting
            )
            reply = ticket_service_pb2.TimeoutNowResponse(
                from_node=self.node_id, to_node=request.from_node, term=self.current_term, success=ok,
            )
        if ok:
            logger.info(f"[{self.node_id}] TimeoutNow from {request.from_node}: starting election")
            threading.Thread(target=self._start_election, kwargs={"leadership_transfer": True},
                             daemon=True).start()
        return reply

    def transfer_leadership(self, target: str = "", timeout: Optional[float] = None) -> dict:
        """Hand leadership to `target` (or the most up-to-date follower).

        Thesis 3.10: stop accepting new requests, bring the target's log fully
        up to date, then send it TimeoutNow so it wins the next election at
        once. Used for planned maintenance, so a leader can be restarted
        without the cluster waiting out an election timeout.
        """
        timeout = timeout or self.config.election_timeout_max_ms / 1000.0 * 2
        with self._lock:
            if self.state != NodeState.LEADER:
                return {"status": "error", "message": "not_leader",
                        "leader_hint": self.get_leader_address() or ""}
            if not self.config.peers:
                return {"status": "error", "message": "no other member to transfer to"}
            if not target:
                target = max(self.config.peers, key=lambda p: self.match_index.get(p, -1))
            if target not in self.config.peers:
                return {"status": "error", "message": f"{target} is not a voting member"}
            self._transfer_target = target
            term = self.current_term
        logger.info(f"[{self.node_id}] Transferring leadership to {target}")
        deadline = time.monotonic() + timeout
        try:
            # 1. Catch the target up (new client writes are refused meanwhile).
            while time.monotonic() < deadline:
                with self._lock:
                    if self.state != NodeState.LEADER or self.current_term != term:
                        break
                    caught_up = self.match_index.get(target, -1) >= self.log.last_index()
                if caught_up:
                    break
                self._send_append_entries_to_peer(target)
                time.sleep(0.02)
            # 2. Tell it to start an election immediately.
            stub = self._peer_stubs.get(target)
            if stub is not None and self.is_leader:
                try:
                    stub.TimeoutNow(ticket_service_pb2.TimeoutNowRequest(
                        from_node=self.node_id, to_node=target, term=term), timeout=0.5)
                except Exception as e:
                    logger.warning(f"[{self.node_id}] TimeoutNow to {target} failed: {e}")
            # 3. Wait until the target has taken over.
            while time.monotonic() < deadline:
                with self._lock:
                    if self.state != NodeState.LEADER and self.leader_id == target:
                        return {"status": "success", "message": f"leadership transferred to {target}",
                                "leader_hint": self.get_leader_address() or ""}
                time.sleep(0.02)
            return {"status": "error", "message": f"transfer to {target} timed out"}
        finally:
            with self._lock:
                if self.current_term == term:
                    self._transfer_target = None

    # ================================================================
    #  CLIENT REQUEST INTERFACE
    # ================================================================

    def has_read_lease(self) -> bool:
        """True if a majority confirmed our leadership within the minimum
        election timeout (leader lease, Raft thesis 6.4.1).

        Followers ignore vote requests for that long after hearing from us,
        so no other leader can exist yet and a local read is linearizable.
        Assumes clocks on the nodes advance at roughly the same rate.
        """
        with self._lock:
            if self.state != NodeState.LEADER:
                return False
            if self.config.is_solo:
                return True
            lease = self.config.election_timeout_min_ms / 1000.0
            now = time.monotonic()
            live = 1 + sum(1 for t in self._last_ack.values() if now - t < lease)
            return live >= self.config.majority

    def wait_until_ready_for_reads(self, timeout: float = 3.0) -> bool:
        """Block until this leader may serve a read without it being stale.

        Two conditions (Raft paper section 8): the leader has committed the
        no-op of its term, so it knows every earlier committed entry; and it
        holds a read lease, so it has not been replaced by a newer leader on
        the other side of a network partition.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if self.state != NodeState.LEADER:
                    return False
                if self.commit_index >= self._noop_index and self.has_read_lease():
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.02)

    def has_quorum_contact(self) -> bool:
        """True if a majority (including self) acked us within the election timeout.

        A leader cut off from the majority cannot commit anything; this lets
        it reject requests immediately with a clear error instead of hanging.
        """
        with self._lock:
            if self.config.is_solo:
                return True
            # One max election timeout (~12 missed heartbeats): a follower that is
            # silent for this long would itself have started an election.
            window = self.config.election_timeout_max_ms / 1000.0
            now = time.monotonic()
            if now - self._leader_since <= window:
                return True  # just won a majority of votes: grace period
            live = 1 + sum(1 for t in self._last_ack.values() if now - t <= window)
            return live >= self.config.majority

    def propose(self, command_type: str, command: dict, timeout: float = 5.0) -> dict:
        """Propose a new command to the Raft cluster.

        Only the leader can propose. Blocks until the entry is committed
        by a majority or the timeout expires.
        """
        with self._lock:
            if self.state != NodeState.LEADER:
                return {
                    "status": "error",
                    "message": "not_leader",
                    "leader_hint": self.get_leader_address() or "",
                }
            if self._transfer_target is not None:
                # Leadership is being handed over: send the client to the new leader.
                peer = self.config.peers.get(self._transfer_target)
                return {"status": "error", "message": "not_leader",
                        "leader_hint": peer.client_address if peer else ""}
            if not self.has_quorum_contact():
                return {"status": "unavailable", "message": "no_quorum"}

            entry = LogEntry(
                term=self.current_term,
                index=self.log.last_index() + 1,
                command_type=command_type,
                command=command,
            )
            self.log.append(entry)
            if command_type == "config":
                self._refresh_membership()  # takes effect on append, not on commit

            pending = PendingRequest()
            self._pending_requests[entry.index] = pending

            logger.debug(
                f"[{self.node_id}] Proposed entry {entry.index}: "
                f"type={command_type}, term={entry.term}"
            )

        # For single-node cluster, commit immediately
        if self.config.is_solo:
            with self._lock:
                self.commit_index = entry.index
                self._apply_committed_entries()
        else:
            # Trigger immediate replication
            self._send_append_entries_to_all()

        # Wait for commit
        if pending.event.wait(timeout=timeout):
            return pending.result or {"status": "error", "message": "no result"}
        else:
            with self._lock:
                self._pending_requests.pop(entry.index, None)
            return {"status": "unavailable", "message": "commit_timeout"}

    def get_leader_address(self) -> Optional[str]:
        """Return 'host:port' of current leader for client redirect."""
        if self.leader_id is None:
            return None
        if self.leader_id == self.node_id:
            return None
        peer = self.config.peers.get(self.leader_id)
        if peer:
            return peer.client_address
        return None

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self.state == NodeState.LEADER

    def get_status(self) -> dict:
        """Return current node status for monitoring."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "current_term": self.current_term,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.state_machine.last_applied,
                "log_length": len(self.log),
                "voted_for": self.voted_for,
                "state_digest": self.state_machine.digest(),
                "members": sorted(set(self.config.peers) | ({self.node_id} if self.config.voting else set())),
                "voting": self.config.voting,
                # Process id, so local demo tools can crash the current leader.
                "pid": os.getpid(),
            }

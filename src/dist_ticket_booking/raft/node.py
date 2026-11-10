"""Core Raft consensus node implementation.

Implements the Raft protocol as described in the paper:
'In Search of an Understandable Consensus Algorithm' by Ongaro & Ousterhout.

Responsibilities:
- Leader election with randomized timeouts
- Log replication via AppendEntries RPCs
- Commit index advancement on majority confirmation
- State machine application of committed entries
- Persistent state (currentTerm, votedFor, log) for crash recovery
"""
import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Any

import grpc

from dist_ticket_booking.grpc import ticket_service_pb2
from dist_ticket_booking.grpc import ticket_service_pb2_grpc
from dist_ticket_booking.raft.config import ClusterConfig
from dist_ticket_booking.raft.log import RaftLog, LogEntry
from dist_ticket_booking.raft.state_machine import StateMachine

logger = logging.getLogger(__name__)


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
        # Last time (monotonic) each peer acknowledged us in our current term.
        # Used for fail-fast quorum checks ("CheckQuorum").
        self._last_ack: Dict[str, float] = {}
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

        # ── Threading ──
        self._lock = threading.RLock()
        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._running = False

        # ── Persistence path for term/votedFor ──
        self._state_path = os.path.join(config.data_dir, f"{config.node_id}_state.json")
        self._load_persistent_state()

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

    def _connect_to_peers(self):
        """Establish gRPC channels to all peer nodes."""
        for peer_id, peer_info in self.config.peers.items():
            if peer_id not in self._peer_channels:
                addr = peer_info.raft_address
                logger.info(f"[{self.node_id}] Connecting to peer {peer_id} at {addr}")
                channel = grpc.insecure_channel(addr)
                self._peer_channels[peer_id] = channel
                self._peer_stubs[peer_id] = ticket_service_pb2_grpc.RaftServiceStub(channel)

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
            self._apply_committed_entries()
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
            self.log.create_snapshot(applied_idx, term, data)
            logger.info(f"[{self.node_id}] Successfully took snapshot at index {applied_idx} (term {term})")
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
        try:
            data = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                # Not required by Raft, but lets a restarted node rebuild its
                # in-memory state machine up to entries known to be committed
                # (and never beyond: uncommitted entries may be overwritten).
                "commit_index": self._persisted_commit,
            }
            with open(self._state_path, "w") as f:
                json.dump(data, f)
        except OSError as e:
            logger.error(f"[{self.node_id}] Failed to persist state: {e}")

    def _load_persistent_state(self):
        """Load currentTerm and votedFor from disk on startup."""
        if not os.path.exists(self._state_path):
            return
        try:
            with open(self._state_path, "r") as f:
                data = json.load(f)
            self.current_term = data.get("current_term", 0)
            self.voted_for = data.get("voted_for")
            self._persisted_commit = data.get("commit_index", -1)
            logger.info(
                f"[{self.node_id}] Loaded persistent state: "
                f"term={self.current_term}, votedFor={self.voted_for}"
            )
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"[{self.node_id}] Failed to load state: {e}")

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
        self._start_election()

    # ================================================================
    #  LEADER ELECTION
    # ================================================================

    def _start_election(self):
        """Transition to candidate and solicit votes from all peers."""
        with self._lock:
            if not self._running or self.state == NodeState.LEADER:
                return
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self._persist_state()

            term = self.current_term
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()
            logger.info(
                f"[{self.node_id}] Starting election for term {term} "
                f"(lastLogIndex={last_log_index}, lastLogTerm={last_log_term})"
            )

        # Single-node cluster becomes leader immediately
        if self.config.cluster_size == 1:
            with self._lock:
                if self.state == NodeState.CANDIDATE and self.current_term == term:
                    self._become_leader()
            return

        votes_received = 1  # vote for self
        votes_lock = threading.Lock()
        election_won = threading.Event()

        def request_vote_from(peer_id: str):
            nonlocal votes_received
            stub = self._peer_stubs.get(peer_id)
            if not stub:
                return
            try:
                response = stub.RequestVote(
                    ticket_service_pb2.RequestVoteRequest(
                        from_node=self.node_id,
                        to_node=peer_id,
                        term=term,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term,
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
                        with votes_lock:
                            votes_received += 1
                            if votes_received >= self.config.majority:
                                election_won.set()
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

        for t in threads:
            t.join(timeout=0.5)

        with self._lock:
            if (
                self.state == NodeState.CANDIDATE
                and self.current_term == term
                and votes_received >= self.config.majority
            ):
                self._become_leader()
            elif self.state == NodeState.CANDIDATE and self.current_term == term:
                logger.info(
                    f"[{self.node_id}] Election for term {term} failed "
                    f"(got {votes_received}/{self.config.majority} votes)"
                )
                self._reset_election_timer()

    def _become_leader(self):
        """Transition to leader state. Must be called under self._lock."""
        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        next_idx = self.log.last_index() + 1
        for peer_id in self.config.peers:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = -1
            # We just won a majority of votes, so start with a fresh grace period.
            self._last_ack[peer_id] = time.monotonic()

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
        if self.config.cluster_size == 1:
            self.commit_index = noop.index
            self._apply_committed_entries()

        self._start_heartbeat()
        self._send_append_entries_to_all()

    def _step_down(self, new_term: int):
        """Step down to follower upon discovering a higher term.

        Must be called under self._lock.
        """
        was_leader = self.state == NodeState.LEADER
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
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat(self):
        """Stop the heartbeat sender thread."""
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=0.5)
        self._heartbeat_thread = None

    def _heartbeat_loop(self):
        """Periodically send AppendEntries (heartbeats) to all peers."""
        interval = self.config.heartbeat_interval_ms / 1000.0
        while not self._heartbeat_stop.is_set() and self._running:
            self._send_append_entries_to_all()
            self._heartbeat_stop.wait(timeout=interval)

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
        if not stub:
            return
        if send_snapshot:
            self._send_install_snapshot(peer_id, stub)
            return

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

                self._last_ack[peer_id] = time.monotonic()
                if response.entry_appended:
                    if entries:
                        self.match_index[peer_id] = response.match_index
                        self.next_index[peer_id] = response.match_index + 1
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
            )
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
            self._last_ack[peer_id] = time.monotonic()
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

            replicated = 1  # leader has it
            for peer_id in self.config.peers:
                if self.match_index.get(peer_id, -1) >= n:
                    replicated += 1

            if replicated >= self.config.majority:
                self.commit_index = n

        self._apply_committed_entries()

    def _apply_committed_entries(self):
        """Apply all committed but unapplied entries to the state machine."""
        while self.state_machine.last_applied < self.commit_index:
            next_index = self.state_machine.last_applied + 1
            entry = self.log.get(next_index)
            if entry is None:
                break

            result = self.state_machine.apply(entry)
            logger.info(
                f"[{self.node_id}] Applied entry {next_index}: "
                f"type={entry.command_type}, result_status={result.get('status')}"
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
            for proto_entry in request.entries:
                entry = LogEntry.from_proto(proto_entry)
                if entry.index <= self.log.last_included_index:
                    continue  # already covered by our snapshot
                existing = self.log.get(entry.index)
                if existing and existing.term != entry.term:
                    self.log.truncate_from(entry.index)
                if not self.log.get(entry.index):
                    self.log.append(entry)

            # Index of the last entry this RPC vouches for. Using our own
            # last_index() here would be unsafe: stale entries from an old
            # term beyond the leader's range could be counted (or applied)
            # as if they matched the leader's log (Raft Figure 2, rule 5).
            last_new_index = max(request.prev_log_index + len(request.entries), self.log.last_included_index)

            # Update commit index
            if request.commit_index > self.commit_index:
                self.commit_index = min(request.commit_index, last_new_index)
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
            self._reset_election_timer()

            if request.last_included_index <= self.state_machine.last_applied:
                return reply(True)  # we already have everything in this snapshot

            state = json.loads(request.data)
            self.log.install_snapshot(request.last_included_index, request.last_included_term, state)
            self.state_machine.restore_from_snapshot(state)
            self.state_machine.last_applied = request.last_included_index
            self.commit_index = max(self.commit_index, request.last_included_index)
            logger.info(
                f"[{self.node_id}] Installed snapshot from {request.from_node} "
                f"up to index {request.last_included_index}"
            )
            self._apply_committed_entries()
            return reply(True)

    # ================================================================
    #  CLIENT REQUEST INTERFACE
    # ================================================================

    def wait_until_ready_for_reads(self, timeout: float = 3.0) -> bool:
        """Block until this leader has committed its term's no-op entry.

        Before that, a new leader might not yet know about entries committed
        by its predecessor, so serving reads could return stale data.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if self.state != NodeState.LEADER:
                    return False
                if self.commit_index >= self._noop_index:
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
            if self.config.cluster_size == 1:
                return True
            # One max election timeout (~12 missed heartbeats): a follower that is
            # silent for this long would itself have started an election.
            window = self.config.election_timeout_max_ms / 1000.0
            now = time.monotonic()
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
            if not self.has_quorum_contact():
                return {"status": "unavailable", "message": "no_quorum"}

            entry = LogEntry(
                term=self.current_term,
                index=self.log.last_index() + 1,
                command_type=command_type,
                command=command,
            )
            self.log.append(entry)

            pending = PendingRequest()
            self._pending_requests[entry.index] = pending

            logger.info(
                f"[{self.node_id}] Proposed entry {entry.index}: "
                f"type={command_type}, term={entry.term}"
            )

        # For single-node cluster, commit immediately
        if self.config.cluster_size == 1:
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
            }

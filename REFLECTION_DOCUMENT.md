# Project Reflection Document

**Course**: CS G623 Advanced Operating Systems · Semester I 2026–2027  
**Institution**: BITS Pilani  
**Project Architecture**: Option A — Distributed Ticket Booking System (Vaccine Appointment Booking)  
**Team Members**: Namish Sudeep Kumar (2026H1030059P), Prachit Suresh Deshinge (2026H1030079P)  

---

## 1. System Overview

Our project implements an end-to-end distributed ticket booking platform that guarantees strong data consistency and high availability despite node crashes. The system integrates:
1. **gRPC inter-service communication** for low-latency, typed RPC calls.
2. **Raft Consensus Protocol** across application server nodes for leader election, log replication, and failure recovery.
3. **Domain-Specific AI/LLM Server** running on an independent node to answer FAQs and to turn plain-English requests into bookings and cancellations.
4. **Replicated State Machine** ensuring slot inventory and user sessions remain consistent across all surviving replicas.

---

## 2. Individual Contributions

We are a two-member team. Rather than splitting the work by milestone, we split it by **layer of the system**, and each of us owned that layer from Milestone 1 through Milestone 2. Member 1 owned the *distributed layer* (how nodes talk to each other and agree); Member 2 owned the *application layer* (what the system books, how the replicated state behaves, and the AI features). The boundary between the two layers is the `propose()` / `StateMachine.apply()` interface, which we designed together.

### Member 1: Namish Sudeep Kumar (2026H1030059P) — Distributed layer: communication, consensus and cluster operations

| | Milestone 1 | Milestone 2 |
|---|---|---|
| **Communication** | Wrote the protobuf contract (`ticket_service.proto`) for the client, application and LLM services; built the gRPC server bootstrap | Added the Raft service (`RequestVote`, `AppendEntries`, `InstallSnapshot`, `TimeoutNow`) and the admin RPCs; leader-following, election-aware `TicketClient` (redirects, back-off, seed rotation) |
| **Sessions** | Login/logout flow and session tokens | Made sessions part of the replicated state so a token works on any node after failover |
| **Consensus** | — | `RaftNode`: randomized election timeouts, Pre-Vote, election restriction, log replication with conflict truncation, commit rules and the leader no-op, CheckQuorum and the read lease |
| **Durability** | — | Crash-safe stable storage (atomic `fsync` + rename), commit-safe restart, log compaction and `InstallSnapshot` |
| **Cluster operations** | — | Single-server membership changes, leadership transfer, `run_node.py --join`, `run_cluster.py`, `cluster_admin.py`, `monitor_cluster.py` |
| **Testing** | gRPC integration tests | Raft unit tests, partition fault injection (`set_partition`) and the partition / membership / Pre-Vote test suites |

**Key learnings**
- How much of Raft's safety lives in small details: `votedFor` must survive a crash, a follower may only trust entries the leader's RPC vouches for, and a leader may only count replicas for entries of its own term.
- Killing a process is the easy failure. A leader that is alive but cut off is harder, because it still believes it leads; serving reads safely needed a lease, not just a role check.
- Membership changes are where intuition fails: changing more than one server at a time can produce two disjoint majorities, which is why we change one server at a time.

### Member 2: Prachit Suresh Deshinge (2026H1030079P) — Application layer: booking logic, replicated state machine and AI

| | Milestone 1 | Milestone 2 |
|---|---|---|
| **Booking logic** | Vaccine-slot inventory, booking and cancellation, per-slot locks against race conditions, mock payment | Made booking deterministic so every replica produces identical results (booking IDs derived from the log index) |
| **State machine** | — | `StateMachine`: applies committed entries, exactly-once de-duplication by `request_id`, snapshot/restore of the business state, state digest for replica comparison |
| **Authentication** | Credential checks at login | Salted password hashing with the check run inside the replicated state machine, so every replica agrees and no plain-text password enters the log |
| **LLM server** | Standalone Qwen3.5-0.8B server with batched inference, domain policy prompt and keyword fallback | Live Raft data as context, grounding guardrails for policy numbers and seat counts, back-pressure and timeouts so a slow model never blocks bookings |
| **Natural-language booking** | — | Intent extraction on the LLM server, validation and grounding of the model output, resolution against live slots and the user's bookings, `assistant.py` |
| **Load and demos** | Concurrency demo | Client node with consistency audit (`run_clients.py`), stress tests, 5-node demo, `verify_milestone2.py` |

**Key learnings**
- A replicated state machine must be completely deterministic: anything random or time-based (our early booking IDs used `uuid4`) makes replicas diverge silently.
- "Exactly once" is an end-to-end property: a retried request has to return the original result even if the state has changed since — we found this the hard way with the natural-language assistant.
- A small LLM is useful for understanding phrasing but must never be trusted to act: its output is a proposal that the application validates against real data.

### Joint work
- Designing the boundary between the two layers (`propose()` → log entry → `apply()`), the request/response formats and the error statuses (`redirect`, `unavailable`, `clarify`).
- Debugging failover and partition scenarios together, reviewing each other's code, the README, and the demo video.

---

## 3. Key Challenges & Technical Insights

1. **Raft Leader Election in Local Multi-Process Clusters**:
   - *Challenge*: Simultaneous startup of nodes could cause split votes if timeouts were uniform.
   - *Resolution*: Randomized election timeouts ensure that one node usually times out first and wins cleanly. We started with the paper's 150–300 ms, but Python processes on a shared laptop can pause for a few hundred milliseconds (garbage collection, loading the LLM), which caused spurious elections; we widened the range to 600–1200 ms with 100 ms heartbeats. Failover still completes in about a second.

2. **Strong Consistency for Write Operations**:
   - *Challenge*: Client writes must only acknowledge once safely committed by a cluster majority.
   - *Resolution*: Employed a synchronization barrier (`threading.Event`) per log index. The leader blocks the client call until the majority confirms replication via `AppendEntries`, preventing dirty writes and phantom reads.

3. **Client Failover Transparency**:
   - *Challenge*: Clients connecting to follower nodes or experiencing leader failure should not crash.
   - *Resolution*: Followers return `"redirect"` status with the `leader_hint`. The client library transparently intercepts this hint and reconnects to the new leader without throwing application-level errors.

4. **Crash Recovery & Re-synchronization**:
   - *Challenge*: When a killed leader restarts, its local log might be stale.
   - *Resolution*: The node reloads persistent state from disk, restarts as follower, and receives missing entries from the current leader's `AppendEntries` heartbeats.

5. **Crash-Safe Stable Storage**:
   - *Challenge*: Our first version rewrote the JSON log in place. A `SIGKILL` in the middle of that write would leave a truncated file, and the node would restart with an empty log, forgetting entries it had already acknowledged — which breaks Raft's safety argument.
   - *Resolution*: Term and vote are written to a temporary file, `fsync`-ed and atomically renamed over the old one. The log became an append-only JSON-lines file: each append writes and `fsync`s only the new entries, so its cost no longer grows with the log, and a crash mid-append can only tear the last line, which held an entry that was never acknowledged and is dropped on restart. Damage anywhere else makes the node refuse to start instead of silently starting empty.

6. **Stale Reads During a Network Partition**:
   - *Challenge*: Killing a process is not the hardest failure. A leader that is alive but cut off from the others still believes it is the leader, while the majority elects a new one and keeps booking. Reads served by the old leader would show seats that were already sold.
   - *Resolution*: A leader serves reads only while it holds a lease — a majority acknowledged a heartbeat (timed from when it was sent) within the minimum election timeout — and followers ignore vote requests while they still hear from a live leader, so no second leader can appear within that window. We added fault injection (`set_partition`) to test this without killing processes.

7. **Retries Without Double Booking**:
   - *Challenge*: If the leader commits a booking and then crashes before replying, the client cannot tell whether it succeeded, and a naive retry books twice.
   - *Resolution*: Each write carries a client-generated `request_id` that stays the same across retries; the replicated state machine remembers results by `request_id` and returns the original result for duplicates.

8. **Changing the Cluster While It Runs**:
   - *Challenge*: Adding or removing a node changes what a "majority" is. If some nodes use the old membership and others the new one, two leaders could be elected in the same term.
   - *Resolution*: Membership is stored as a log entry and used as soon as it is appended; we change one server at a time (Raft thesis §4.1), so old and new majorities always overlap. New nodes join as non-voters, and a leader that removes itself steps down once the change commits.

9. **Letting an LLM Book Appointments Safely**:
   - *Challenge*: The 0.8B model usually extracts intents correctly, but in testing it invented a vaccine the user never mentioned, and could label "how do I cancel?" as a cancellation.
   - *Resolution*: The model only proposes an intent. The app server keeps only values grounded in the user's words, never executes questions, matches against real slots and the user's own bookings, asks when ambiguous, and commits the result through the same Raft path as every other write.

10. **Time-Based Rules in a Replicated State Machine**:
   - *Challenge*: Rules such as "cancellation closes 24 hours before the appointment" depend on the current time, but replicas apply a committed entry at different moments (a restarted node replays it much later). Reading the local clock inside `apply()` would let replicas decide differently.
   - *Resolution*: The leader stamps each booking and cancellation with `requested_at` before replicating it, and every replica evaluates the rule against that value. The same idea keeps the mock payment gateway deterministic: its decision depends only on the command, and its ledger is part of the snapshot and the replica digest.

---

## 4. Potential Future Enhancements

1. **Joint consensus**: we change membership one server at a time; joint consensus (Raft §6) would allow replacing several servers in one step.
2. **Catch-up rounds for new servers**: let a joining node replicate as a learner until it is nearly up to date before it becomes a voter (thesis §4.2.1).
3. **Incremental snapshots**: Snapshots (already implemented, with `InstallSnapshot`) are sent as one message; chunking them would support large state.
4. **Multi-turn assistant**: remember the conversation so a follow-up like "the second one" resolves a clarification question.
5. **Mutual TLS (mTLS)**: Enforcing encrypted and authenticated gRPC channels between nodes for zero-trust production deployments.

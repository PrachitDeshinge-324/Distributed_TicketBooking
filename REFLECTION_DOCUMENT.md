# Project Reflection Document

**Course**: CS G623 Advanced Operating Systems · Semester I 2026–2027  
**Institution**: BITS Pilani  
**Project Architecture**: Option A — Distributed Ticket Booking System (Vaccine Appointment Booking)  
**Team Members**: [Add Names & BITS IDs]  

---

## 1. System Overview

Our project implements an end-to-end distributed ticket booking platform that guarantees strong data consistency and high availability despite node crashes. The system integrates:
1. **gRPC inter-service communication** for low-latency, typed RPC calls.
2. **Raft Consensus Protocol** across application server nodes for leader election, log replication, and failure recovery.
3. **Domain-Specific AI/LLM Server** running on an independent node to provide FAQ customer support.
4. **Replicated State Machine** ensuring slot inventory and user sessions remain consistent across all surviving replicas.

---

## 2. Individual Contributions

*(Customize the member names and assignments below according to your team's division of responsibilities)*

### Team Member 1: [Name / ID]
- **Focus Area**: gRPC Architecture & LLM Integration (Milestone 1 Lead)
- **Specific Contributions**:
  - Authored the Protocol Buffer specification (`ticket_service.proto`) defining client, application, LLM, and Raft RPC services.
  - Implemented the client-facing `TicketClient` wrapper and `TicketClientServicer`.
  - Built the independent CPU-optimized LLM node (`DomainLLM`) with batched request scheduling and prompt-based vaccination booking rules.
  - Developed initial contract and integration tests (`test_contracts.py`, `test_grpc_integration.py`).
- **Key Learnings**:
  - Gained hands-on experience structuring microservice communication using protobuf contracts.
  - Understood batched inference techniques for local LLM deployment to reduce CPU overhead.

### Team Member 2: [Name / ID]
- **Focus Area**: Core Raft Consensus Protocol (Milestone 2 Core)
- **Specific Contributions**:
  - Designed and implemented the `RaftNode` state machine with `FOLLOWER`, `CANDIDATE`, and `LEADER` transitions.
  - Developed the randomized election timeout handler to avoid split votes in candidate state.
  - Implemented `RequestVote` RPC handlers, vote granting safety checks, and term management.
  - Built the peer connection management and inter-node gRPC channels.
- **Key Learnings**:
  - Understood distributed clock synchronization concepts and logical terms in consensus protocols.
  - Mastered concurrency control, thread synchronization (`threading.RLock`), and timer thread lifecycles in Python.

### Team Member 3: [Name / ID]
- **Focus Area**: Log Replication, State Machine, Persistence & Fault Tolerance
- **Specific Contributions**:
  - Implemented `RaftLog` with JSON persistence for `currentTerm`, `votedFor`, and log entries.
  - Developed `StateMachine` applying committed log entries to slot inventory and session storage.
  - Built the cluster orchestrator (`run_cluster.py`) and fault tolerance demo (`demo_fault_tolerance.py`).
  - Implemented follower log consistency checks, log conflict truncation, and catch-up synchronization on node reboot.
- **Key Learnings**:
  - Learned how Write-Ahead Logging (WAL) and idempotent state machine replay enable crash recovery.
  - Experienced distributed failure scenarios first-hand, including leader crash (`SIGKILL`), failover, and zero-loss state re-sync.

---

## 3. Key Challenges & Technical Insights

1. **Raft Leader Election in Local Multi-Process Clusters**:
   - *Challenge*: Simultaneous startup of nodes could cause split votes if timeouts were uniform.
   - *Resolution*: Randomized election timeouts between 150ms and 300ms ensured clean single-candidate elections.

2. **Strong Consistency for Write Operations**:
   - *Challenge*: Client writes must only acknowledge once safely committed by a cluster majority.
   - *Resolution*: Employed a synchronization barrier (`threading.Event`) per log index. The leader blocks the client call until the majority confirms replication via `AppendEntries`, preventing dirty writes and phantom reads.

3. **Client Failover Transparency**:
   - *Challenge*: Clients connecting to follower nodes or experiencing leader failure should not crash.
   - *Resolution*: Followers return `"redirect"` status with the `leader_hint`. The client library transparently intercepts this hint and reconnects to the new leader without throwing application-level errors.

4. **Crash Recovery & Re-synchronization**:
   - *Challenge*: When a killed leader restarts, its local log might be stale.
   - *Resolution*: The node reloads persistent state from disk, restarts as follower, and receives missing entries from the current leader's `AppendEntries` heartbeats.

---

## 4. Potential Future Enhancements

1. **Dynamic Cluster Membership**: Implementing Raft Joint Consensus ($\alpha$ configuration changes) to add/remove nodes without cluster downtime.
2. **Log Compaction / Snapshotting**: Implementing periodic snapshotting to compress old committed log entries and conserve disk storage.
3. **Mutual TLS (mTLS)**: Enforcing encrypted and authenticated gRPC channels between nodes for zero-trust production deployments.

# Python Package Overview

`dist_ticket_booking` is the Python package behind the Distributed Ticket
Booking System. See the top-level `README.md` for setup and demos.

| Module | Contents |
|---|---|
| `raft/` | Raft consensus: `node.py` (elections with Pre-Vote, replication, commit, read lease, CheckQuorum, snapshots, membership changes, leadership transfer), `log.py` (persistent log + compaction), `state_machine.py` (applies committed commands, exactly-once dedupe), `storage.py` (atomic, fsync-ed stable storage), `config.py` (cluster/peer config) |
| `grpc/` | `service.py` (client/app/LLM servicers, leader-following `TicketClient`), `raft_service.py` (Raft RPC servicer), generated protobuf stubs |
| `business/` | Vaccine-slot inventory, booking/cancel, mock payment, per-slot locks |
| `core/` | Data models and password hashing (`auth.py`) |
| `llm/` | Domain LLM (Qwen3.5-0.8B): FAQ answers with grounding guardrails, intent extraction for natural-language booking (`intent.py` validates it), keyword fallback |
| `coordination/`, `scheduler/` | Lock-ownership tracker and FIFO dispatcher used by the booking service |
| `simulation/`, `monitoring/` | Milestone 1 scenario runner and event logger |

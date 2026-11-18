# Python Package Overview

`dist_ticket_booking` is the Python package behind the Distributed Ticket
Booking System. See the top-level `README.md` for setup and demos.

| Module | Contents |
|---|---|
| `raft/` | Raft consensus: `node.py` (elections with Pre-Vote, replication, commit, read lease, CheckQuorum, snapshots, membership changes, leadership transfer), `log.py` (append-only, fsync-ed log + compaction), `state_machine.py` (applies committed commands, exactly-once dedupe), `storage.py` (atomic, fsync-ed stable storage), `config.py` (cluster/peer config) |
| `grpc/` | `service.py` (client/app/LLM servicers, leader-following `TicketClient`), `raft_service.py` (Raft RPC servicer), generated protobuf stubs |
| `business/` | `booking_service.py` (slot inventory, booking rules, per-slot locks), `payment.py` (mock gateway with replicated ledger: capture, refund, decline) |
| `core/` | `models.py` (VaccineSlot), `auth.py` (accounts, citizen/admin roles, password hashing) |
| `llm/` | Domain LLM (Qwen3.5-0.8B): FAQ answers grounded on the booking-policy knowledge base (`knowledge.py`) with an answer cache, intent extraction for natural-language booking (`intent.py` validates it), keyword fallback |
| `monitoring/` | `console.py`: UTF-8 and ANSI-colour terminal setup for the scripts on every OS |

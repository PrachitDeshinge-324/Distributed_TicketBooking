# Distributed Vaccine Booking System

> **CS G623 — Advanced Operating Systems** | BITS Pilani | Milestone 1  
> Architecture A: Distributed Ticket Booking System

A distributed, gRPC-based vaccine slot booking platform demonstrating core distributed systems concepts — mutual exclusion, concurrent resource allocation, inter-process communication, and LLM-powered customer support — implemented in Python.

> **Note on LLM performance:** The language model (`Qwen2.5-0.5B-Instruct`) runs entirely on the local machine hosting Node 1. No external API or internet connection is required after the first model download. Response latency depends on the hardware: a modern CPU takes a few seconds per query; a GPU (CUDA/MPS) reduces this to under a second.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Running the System](#running-the-system)
- [Testing](#testing)
- [Key Design Decisions](#key-design-decisions)
- [Milestone Roadmap](#milestone-roadmap)

---

## Overview

The system models the real-world problem of multiple users concurrently competing for limited vaccine appointment slots. It is built as a distributed multi-node architecture where nodes communicate exclusively through **gRPC**, enforcing correctness under high concurrency without using a database.

**What it demonstrates:**

- Inter-process communication via gRPC across three logical nodes
- Thread-safe booking using per-resource locks (no global bottleneck)
- Priority-aware request queuing via `Dispatcher` and `LockManager`
- Offline domain LLM (`Qwen2.5-0.5B-Instruct`) with **batched inference** on a dedicated server node
- Provably zero overbooking under concurrent load (tested with 10 simultaneous clients)

---

## Architecture

The system is split across three logical nodes:

```
┌─────────────────────┐         gRPC          ┌──────────────────────────┐
│   Node 5 — Clients  │ ──────────────────── ▶ │  Node 2 — App Server     │
│  (multiple clients) │    login / post / get   │  port 50051              │
└─────────────────────┘                         │  • Booking logic         │
                                                │  • Per-slot locking      │
                                                │  • Payment validation    │
                                                │  • Session management    │
                                                └────────────┬─────────────┘
                                                             │ gRPC (getLLMAnswer)
                                                             ▼
                                                ┌──────────────────────────┐
                                                │  Node 1 — LLM Server     │
                                                │  port 50052              │
                                                │  • Qwen2.5-0.5B (local)  │
                                                │  • Batched FAQ queries   │
                                                │  • Fully offline         │
                                                └──────────────────────────┘
```

### RPC Contracts (per Assessment PDF)

| Direction | Function | Signature |
|---|---|---|
| Client → App Server | Login | `login(username, password)` → `loginResponse(status, token)` |
| Client → App Server | Logout | `logout(token)` → `status` |
| Client → App Server | Post | `post(token, type, data)` → `status` |
| Client → App Server | Get | `get(token, type, params)` → `getResponse(status, List[(id, data)])` |
| App Server (internal) | ProcessBusinessRequest | `processBusinessRequest(requestId, payload, context)` |
| App Server → LLM Server | GetLLMAnswer | `getLLMAnswer(requestId, query, context)` → `getLLMAnswerResponse(requestId, answer)` |

---

## Technology Stack

| Component | Technology |
|---|---|
| Inter-node communication | gRPC + Protocol Buffers |
| Language | Python 3.11+ |
| LLM inference | HuggingFace `transformers` — `Qwen/Qwen2.5-0.5B-Instruct` (local, offline) |
| Concurrency | Per-slot `threading.Lock`; `queue.Queue` with batched LLM worker |
| Testing | `pytest` |
| Packaging | `pyproject.toml` (editable install) |

---

## Project Structure

```
.
├── proto/
│   └── ticket_service.proto          # gRPC service and message definitions
├── src/dist_ticket_booking/
│   ├── business/
│   │   └── booking_service.py        # Core booking logic, per-slot locks
│   ├── coordination/
│   │   └── lock_manager.py           # Logical lock ownership tracking
│   ├── core/
│   │   └── models.py                 # Dataclasses: VaccineSlot, Appointment, etc.
│   ├── grpc/
│   │   ├── service.py                # gRPC servicers + TicketClient wrapper
│   │   ├── ticket_service_pb2.py     # Generated protobuf stubs
│   │   └── ticket_service_pb2_grpc.py
│   ├── llm/
│   │   └── domain_llm.py             # Offline HuggingFace LLM, batched worker thread
│   ├── monitoring/
│   │   └── logger.py
│   ├── scheduler/
│   │   └── dispatcher.py             # FIFO priority request queue
│   └── simulation/
│       └── scenario_runner.py
├── scripts/
│   ├── run_llm_server.py             # Start Node 1 (LLM, port 50052)
│   ├── run_server.py                 # Start Node 2 (App Server, port 50051)
│   ├── client_demo.py                # Sample client workflow
│   ├── demo_concurrency.py           # 10-thread concurrent booking test
│   ├── run_all_demo.py               # Orchestrate full demo
│   └── generate_grpc.sh              # Regenerate gRPC stubs from proto
├── tests/
│   ├── test_contracts.py             # Model and lock manager unit tests
│   ├── test_business_and_grpc_contracts.py  # Business logic + dispatcher tests
│   └── test_grpc_integration.py      # End-to-end gRPC + concurrency tests
└── docs/
    ├── progress-report.pdf           # Milestone 1 presentation (PDF)
    └── progress-report.pptx          # Milestone 1 presentation (PowerPoint)
```

---

## Setup

**Requirements:** Python 3.11+

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # On Windows: .venv\Scripts\activate

# Install the package and all dependencies
pip install -e .
```

**First run:** When `run_llm_server.py` starts for the first time, it downloads the Qwen model weights (~1 GB) into your HuggingFace cache (`~/.cache/huggingface/hub/`). All subsequent starts load from that cache — **no internet required**.

To regenerate gRPC stubs after modifying `ticket_service.proto`:

```bash
bash scripts/generate_grpc.sh
```

---

## Running the System

### Automated Demo (recommended)

```bash
python scripts/run_all_demo.py
```

Starts both servers, runs the client demo, then the concurrency test — all in sequence.

### Manual (three separate terminals)

**Terminal 1 — LLM Server (Node 1):**
```bash
python scripts/run_llm_server.py --port 50052
# Loads model from local cache; no internet needed after first run
```

**Terminal 2 — Application Server (Node 2):**
```bash
python scripts/run_server.py --port 50051 --llm-host localhost --llm-port 50052
```

**Terminal 3 — Clients (Node 5):**
```bash
# Single client walkthrough
python scripts/client_demo.py

# 10 concurrent clients racing for 3 slots (verifies zero overbooking)
python scripts/demo_concurrency.py
```

---

## Testing

```bash
# Run all unit tests (no running server needed)
pytest tests/test_contracts.py tests/test_business_and_grpc_contracts.py -v

# Run everything including gRPC integration tests (requires running server)
pytest tests/ -v
```

**Test results (13 / 13 passing):**

| Test file | Coverage | Count |
|---|---|---|
| `test_contracts.py` | Data models, `LockManager` | 4 |
| `test_business_and_grpc_contracts.py` | Booking logic, `Dispatcher`, LLM stub, integration | 9 |
| `test_grpc_integration.py` | Live gRPC server, concurrent booking | — |

---

## Key Design Decisions

### Per-slot locking (not global)
One `threading.Lock` per slot. Concurrent users booking **different** slots run fully in parallel. Only contenders for the **same** slot are serialized — the only case requiring serialization.

### LLM batching with offline-first loading
The LLM worker collects requests arriving within a 60 ms window and processes them as a single batched forward pass. This gives roughly N× throughput when N clients query simultaneously, compared to N sequential calls. The model loads from local cache (`local_files_only=True`) and only falls back to downloading on a cache miss.

### Dispatcher + LockManager integration
Every booking request is enqueued, dequeued when the slot lock is acquired, and the logical lock is released after the transaction. Both `Dispatcher` (FIFO priority queue) and `LockManager` (ownership tracker) are live, not placeholders.

### No database for Milestone 1
All state is in-memory. The assessment evaluates the distributed gRPC architecture and concurrency control, not persistence. Replicated state is a Milestone 2 concern.

---

## Milestone Roadmap

| Milestone | Focus | Status |
|---|---|---|
| **1** | gRPC architecture, per-slot concurrency, batched offline LLM | ✅ Complete |
| **2** | Raft consensus, leader election, state replication, fault tolerance | Planned |

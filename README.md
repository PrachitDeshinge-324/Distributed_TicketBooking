# Distributed Ticket Booking System

Project for CS G623 (Advanced Operating Systems).
Milestone 1 focuses on core client-server communication using gRPC, local concurrency control for seat reservations, mock payment handling, and a domain LLM service for customer support FAQs.

## Architecture

The project consists of three components communicating via gRPC:

- **LLM Server (Node 1)**: Independent service listening on port 50052, providing answers to user questions (e.g., cancellation policies, seat queries).
- **Application Server (Node 2)**: Core business logic server listening on port 50051. Manages ticket inventory, enforces lock-based concurrency control to prevent overbooking, handles mock payment validation, and proxies FAQ queries to Node 1.
- **Client Node (Node 5)**: Client implementation for user interactions (login, check availability, book seats, ask FAQs, logout).

## Setup

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

To recompile proto definitions if modified:

```bash
bash scripts/generate_grpc.sh
```

## Running the System

### Automated Demo

Run both servers and the client test scenarios in a single command:

```bash
python scripts/run_all_demo.py
```

### Manual Execution

Run each node in separate terminals:

1. **Terminal 1 - LLM Server**:
   ```bash
   python scripts/run_llm_server.py --port 50052
   ```

2. **Terminal 2 - Application Server**:
   ```bash
   python scripts/run_server.py --port 50051 --llm-port 50052
   ```

3. **Terminal 3 - Client Demo**:
   ```bash
   python scripts/client_demo.py
   python scripts/demo_concurrency.py
   ```

## Testing

Run all unit and integration tests:

```bash
pytest tests/ -v
```

Tests include:
- Contract tests for models and lock manager (`tests/test_contracts.py`)
- Business logic, validation, and LLM stub tests (`tests/test_business_and_grpc_contracts.py`)
- End-to-end gRPC integration and concurrent booking tests (`tests/test_grpc_integration.py`)

## Project Structure

```
├── proto/
│   └── ticket_service.proto
├── src/dist_ticket_booking/
│   ├── business/
│   │   └── booking_service.py
│   ├── coordination/
│   │   └── lock_manager.py
│   ├── core/
│   │   └── models.py
│   ├── grpc/
│   │   ├── service.py
│   │   ├── ticket_service_pb2.py
│   │   └── ticket_service_pb2_grpc.py
│   └── llm/
│       └── domain_llm.py
├── scripts/
│   ├── run_llm_server.py
│   ├── run_server.py
│   ├── client_demo.py
│   ├── demo_concurrency.py
│   ├── run_all_demo.py
│   └── generate_grpc.sh
└── tests/
    ├── test_contracts.py
    ├── test_business_and_grpc_contracts.py
    └── test_grpc_integration.py
```


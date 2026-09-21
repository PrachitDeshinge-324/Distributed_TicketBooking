# Milestone 1 Plan: Distributed Ticket Booking System

Reference: **Advanced Operating Systems (CS G623) Project Assignment**  
Topic: **Architecture A - Distributed Ticket Booking System**

---

## Milestone 1 Objectives & Scope

Milestone 1 establishes the foundation for the distributed ticket booking system using **Python** and the **gRPC framework**, integrating a dedicated **domain-specific LLM server** for customer support FAQs, and enforcing **concurrency control** during real-time seat reservations.

### Key Objectives
- **Basic gRPC Service Architecture**: Inter-service communication strictly implemented via gRPC in Python.
- **Client-Server Communication & Authentication**: Session management with `login` and `logout`.
- **Fundamental Business Logic & Concurrency Control**: Real-time seat reservation with concurrency control to prevent race conditions and overbooking.
- **Payment Processing**: Integrated mock payment processing gateway.
- **Domain LLM Integration**: Customer support chatbot running on an independent server (Node 1) answering FAQ queries.
- **Clean Node Separation**:
  - **Node 1: AI/LLM Server**: Operates independently, serving `getLLMAnswer`.
  - **Node 2: Application Server**: Manages core business logic, seat inventory, and concurrency locks.
  - **Node 5: Client Node (multiple)**: Simulates concurrent user interactions.

---

## Core RPC Function Signatures (from Assessment PDF)

### 1. Client Functions (Client -> Application Server)
- `login(username, password)` -> `loginResponse(status, Optional(token))`
  - Authenticates a user and issues a session token.
- `logout(token)` -> `status`
  - Terminates the user session and invalidates the token.
- `post(token, type, data)` -> `status`
  - Sends new requests (e.g. seat booking, cancellation).
- `get(token, type, Optional(params))` -> `getResponse(status, List[(id, data)])`
  - Retrieves system data (seat availability, FAQs).

### 2. Application Server Functions
- `loginResponse(status, Optional(token))`
  - Returns result of login request.
- `getResponse(status, List[(id, data)])`
  - Returns requested items to client.
- `processBusinessRequest(requestId, payload, context)`
  - Handles domain-specific operations (seat reservation, cancellations, FAQ queries) and interacts with the LLM server over gRPC.

### 3. LLM Server Functions (Node 1)
- `getLLMAnswer(requestId, query, context)` -> `getLLMAnswerResponse(requestId, answer)`
  - Handles customer support queries (e.g., "How to cancel a booking?", "What seats are available?").

---

## Milestone 1 Deliverables Checklist

- [x] **gRPC Service Definitions**: `proto/ticket_service.proto` with `TicketClientService`, `TicketAppService`, and `LLMService`.
- [x] **Client-Server Communication**: gRPC stubs generated and verified.
- [x] **Authentication & Sessions**: Token-based authentication in `TicketClientServicer`.
- [x] **Real-time Seat Reservation with Concurrency Control**: Thread-safe mutex in `BookingService` preventing race conditions.
- [x] **Mock Payment Processing**: Instant payment verification in booking workflow.
- [x] **LLM Integration**: Node 1 independent server with domain-specific FAQ responses and local Ollama support.
- [x] **Concurrent Client Simulation**: Multi-threaded client test proving zero overbooking under high concurrency.
- [x] **Full Automated Test Suite**: 12 passing unit and integration tests.

---

## Next Milestone (Deferred to Milestone 2)

- **Milestone 2: Raft Consensus & Fault Tolerance**
  - Leader election mechanism
  - Booking state replication across nodes using Raft
  - Strong consistency and failure detection (demo consistency after leader node kill)


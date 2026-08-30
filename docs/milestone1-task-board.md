# Milestone 1 Task Board

## Objective

Complete the repository setup and shared architecture foundation for the Python-first Distributed Ticket Booking System without implementing the full booking logic yet.

## Working Rules

- Keep the repository scoped to Milestone 1 only.
- Do not build a web/UI layer.
- Keep all design decisions Python-first and AOS/distributed-systems focused.
- Use the shared contract files before implementing any behavior.
- Integrate only after both contributors agree on the same model and semantics.

## Person A: Core Coordination and Scheduler

### 1. Shared Domain Model

File:

- src/dist_ticket_booking/core/models.py

Tasks:

- finalize the Ticket dataclass fields
- finalize the BookingRequest dataclass fields
- finalize the Booking dataclass fields
- define the SystemState container for all shared runtime state
- decide the booking status values: pending, confirmed, rejected, failed
- document assumptions in comments above each class

Functions/classes to work on:

- Ticket
- BookingRequest
- Booking
- SystemState

### 2. Lock and Synchronization Logic

File:

- src/dist_ticket_booking/coordination/lock_manager.py

Tasks:

- define lock acquisition semantics
- define what happens when a process waits for a ticket lock
- decide whether waiting is queue-based or priority-based
- implement a simple lock owner and waiting-queue model
- write the acquire and release logic as placeholders only in Milestone 1

Functions to work on:

- LockManager.**init**
- LockManager.acquire
- LockManager.release
- LockManager.queue_status

### 3. Scheduling and Request Ordering

File:

- src/dist_ticket_booking/scheduler/dispatcher.py

Tasks:

- define how requests are ordered before processing
- decide whether ordering is FIFO, priority-based, or fairness-based
- design how the queue handles multiple simultaneous booking requests
- prepare the dispatcher to accept and return the next request

Functions to work on:

- Dispatcher.**init**
- Dispatcher.enqueue
- Dispatcher.next_request

### 4. Contract Review Checkpoint

Before moving to deeper implementation, Person A and Person B must agree on:

- ticket ID format
- user ID format
- booking request structure
- inventory state representation
- lock contention handling rules

## Person B: Simulation and Validation

### 1. Scenario Simulation

File:

- src/dist_ticket_booking/simulation/scenario_runner.py

Tasks:

- define 2 to 3 simulation scenarios for ticket contention
- represent multiple concurrent booking attempts
- decide how event logs are stored for later debugging
- simulate request ordering effects under a shared lock model

Functions to work on:

- ScenarioRunner.**init**
- ScenarioRunner.add_event
- ScenarioRunner.run

### 2. Monitoring and Debugging

File:

- src/dist_ticket_booking/monitoring/logger.py

Tasks:

- create a consistent logging format for events
- record lock acquisition, queue updates, and booking decisions
- define event names such as request_received, lock_acquired, queued, booking_success
- keep logs simple but enough for debugging distributed behavior

Functions to work on:

- SystemLogger.**init**
- SystemLogger.log

### 3. Contract and Regression Tests

File:

- tests/test_contracts.py

Tasks:

- validate the Ticket model contract
- validate the BookingRequest contract
- add tests for booking state transitions
- add tests for queue ordering assumptions
- add tests for lock conflict handling placeholders

Functions to work on:

- test_ticket_model_contract
- test_booking_request_contract

### 4. Validation Checkpoint

Before integration, Person B should verify:

- the shared model matches Person A’s assumptions
- the simulation logs reflect correct event flow
- no test fails when the shared contract is used across modules

## Shared Files to Review Together

- README.md
- docs/architecture.md
- docs/workflow.md
- docs/contribution-split.md

## Final Milestone 1 Deliverable

The team should complete Milestone 1 when all of the following are true:

- Python package structure exists and is coherent
- shared ticket and booking model is agreed
- coordination and scheduler skeletons are in place
- simulation and monitoring placeholders are available
- tests validate the current contract
- documentation clearly states what remains for later milestones

## What Not to Do in Milestone 1

- no frontend development
- no API buildout
- no database integration
- no complete distributed runtime logic
- no final production-grade system implementation

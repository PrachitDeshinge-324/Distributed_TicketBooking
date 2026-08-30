# System Architecture

## 1. Overview

The Distributed Ticket Booking System is modeled as a distributed computing problem rather than a web application. The system is designed to study how multiple processes coordinate access to scarce resources while preserving correctness and consistency.

## 2. Core Architectural Focus

### Process Layer

- independent logical processes simulate users or booking agents
- each process may request access to a shared resource
- processes communicate through a controlled coordination mechanism

### Coordination Layer

- mutual exclusion for critical sections
- distributed lock coordination
- event ordering and request queue management
- agreement protocols for shared decisions

### Scheduling Layer

- resource allocation decisions
- fairness and ordering policies
- queue management for ticket availability updates
- process dispatch or event-handling order

### State and Monitoring Layer

- current booking state
- ticket inventory state
- lock ownership and waiting queues
- log records for debugging and analysis

## 3. Key AOS and Distributed Systems Concepts

### Mutual Exclusion

The system must ensure that booking updates are serialized correctly when multiple processes compete for the same ticket inventory.

### Distributed Coordination

Different processes may independently attempt to reserve a ticket; the system needs a consistent mechanism for deciding who proceeds without violating concurrency assumptions.

### Synchronization

The design should make race conditions explicit and explain how coordination prevents inconsistent updates.

### Fault Tolerance Awareness

Milestone 1 focuses on the architectural model for how the system can respond to failures or missed messages, even if those behaviors are not yet fully implemented.

## 4. Suggested Module Boundaries

### Ticket Model

Responsible for:

- ticket attributes
- availability checks
- pricing and inventory metadata
- state transitions

### Booking Engine

Responsible for:

- request handling
- booking validation
- state transitions and conflict management
- event triggers for processing decisions

### Coordination Manager

Responsible for:

- lock acquisition
- request ordering
- distributed decision protocols
- coordination state tracking

### Scheduler

Responsible for:

- process ordering
- fairness policy
- event dispatch
- low-level execution sequencing

## 5. Communication Model

During Milestone 1, the project uses a conceptual distributed model rather than a web-based client-server architecture.

- processes interact through internal Python modules
- state changes are coordinated through shared or simulated coordination logic
- the design keeps interfaces explicit to support later implementation

## 6. Milestone 1 Deliverables

- Python package structure
- shared ticket and booking models
- coordination and scheduler skeletons
- documentation of system assumptions
- parallel task planning for two members
- test placeholders for distributed behavior

## 7. Design Principles

- clear separation of concerns
- explicit concurrency boundaries
- modular Python architecture
- easy parallel development for two contributors
- extensibility for later implementation stages

## 8. Status

This design describes the intended architecture for Milestone 1 only. The final distributed algorithms and runtime behavior are intentionally left for later milestones.

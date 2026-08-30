# Team Workflow for Milestone 1

## Parallel Work Strategy

This project is intentionally split so that two members can contribute at the same time without a blocking dependency chain.

### Person A: Core Distributed Systems Logic

Focus areas:

- ticket model design
- booking state model
- lock and synchronization skeletons
- process coordination abstractions
- shared state and ordering rules

### Person B: Simulation and Validation Layer

Focus areas:

- simulation workflow
- testing scaffolding
- logging and metrics hooks
- scenario definitions for race conditions
- documentation of correctness assumptions

## Dependency Handling

To avoid waiting:

- define the shared ticket and booking model first
- agree on state transitions before implementing logic
- keep all coordination algorithms as placeholders at first
- avoid deep implementation until the shared design is stable

## Suggested Task Split

### Task Group 1: Shared Domain Contract

- ticket entity fields
- booking lifecycle states
- process or user identity model
- request/response conventions for simulation events

### Task Group 2: Coordination and Scheduler Skeleton

- lock manager definitions
- fairness or ordering policy placeholders
- scheduler interfaces
- process handling stubs

### Task Group 3: Simulation and Logging

- scenario runner skeleton
- event recorder placeholders
- logs for state transitions
- detection of contention cases

### Task Group 4: Documentation

- architecture notes
- milestone checklist
- contribution split and working assumptions

## Collaboration Rules

- commit small changes often
- keep one source of truth for the shared model
- avoid editing the same contracts without notice
- clearly document concurrency assumptions
- integrate once both branches are aligned on the core design

## Success Condition for Milestone 1

Both contributors should be able to work in parallel while still aligning around a consistent Python-first distributed-systems design.

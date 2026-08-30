# Contribution Split for Two Members

## Member 1: Coordination and Core Logic

Responsible for:

- shared ticket and booking models
- lock manager design
- synchronization abstractions
- scheduler interfaces
- state transition rules
- correctness assumptions for distributed access

Suggested branch:

- `feature/core-coordination`

## Member 2: Simulation and Validation

Responsible for:

- scenario runner design
- event logging and monitoring hooks
- test cases for race conditions
- request ordering and fairness simulation
- documentation of observed behavior

Suggested branch:

- `feature/simulation-validation`

## Shared Contract to Agree on

Both members should align on these before deeper implementation:

- ticket attributes and inventory state
- booking request fields
- booking lifecycle states
- process IDs and queue ordering
- locking or coordination semantics for contention

## Avoiding Waiting Phases

- do not block on UI or API decisions during Milestone 1
- keep the Python model and simulation interfaces stable
- define core contracts before implementing behavior
- integrate once both branches agree on state and coordination semantics

## Completion Rule

Milestone 1 can be considered complete when both contributors have a Python-first skeleton and the shared distributed-system contract is documented.

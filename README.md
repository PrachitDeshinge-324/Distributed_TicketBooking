# Distributed Ticket Booking System

This repository is built for the Advanced Operating Systems and Distributed Systems coursework. The focus is not on a web frontend or an API service layer; the emphasis is on the core distributed-system behavior, concurrency control, coordination, and process-level design.

## Primary Language

Python is the primary language for this project. The codebase is structured to support simulation, coordination logic, scheduling experiments, and distributed-system modules instead of UI or web development.

## Project Goal

The project models a distributed ticket booking system with emphasis on:

- process synchronization
- distributed coordination
- mutual exclusion and locking
- leader election or distributed decision making
- failure tolerance and consistency awareness
- event-driven simulation of ticket booking behavior

## Milestone 1 Scope

This repository is limited to Milestone 1 only.

Current focus:

- architecture planning
- module boundary definitions
- Python project skeleton
- shared contracts and interfaces
- parallel task split for two contributors
- test and simulation placeholders

No full implementation is included yet. This phase is intentionally setup-only.

## Two-Person Workflow

To avoid waiting phases:

- Person A: distributed coordination, scheduler, and lock-related logic
- Person B: simulation, logging, validation, and testing scaffolding
- Both work from the same shared contracts and data models
- Keep interfaces explicit before implementing behavior

## Repository Layout

- src/dist_ticket_booking/ — Python package for the core system
- docs/ — design, milestone, and workflow notes
- tests/ — test placeholders and contract checks
- scripts/ — local utility scripts for running examples or diagnostics

## Python Project Structure

- core/ — shared models and system state
- coordination/ — locks, synchronization, leader election, message coordination
- scheduler/ — resource and process scheduling logic
- simulation/ — event simulation for distributed ticket booking behavior
- monitoring/ — logs, metrics, and system-state observation
- utils/ — helper functions and reusable Python components

## Initial Setup Rules

- keep modules isolated
- define data contracts early
- use Python dataclasses and typed interfaces where useful
- avoid full business logic before the shared model is agreed
- commit incremental progress in small pieces

## Branching Strategy

- main — clean integration branch
- feature/core-models — shared ticket and booking domain models
- feature/coordination — distributed algorithms and locking skeleton
- feature/simulation — simulation and validation layer
- feature/docs-architecture — design and milestone documentation

## Quick Start

1. Open the repo in VS Code
2. Review the docs folder
3. Start with the Python package under src/
4. Split the work according to the contribution plan
5. Keep Milestone 1 limited to architecture and skeleton logic

## Developer File Map

Person A should start here:

- src/dist_ticket_booking/core/models.py
- src/dist_ticket_booking/coordination/lock_manager.py
- src/dist_ticket_booking/scheduler/dispatcher.py

Person B should start here:

- src/dist_ticket_booking/simulation/scenario_runner.py
- src/dist_ticket_booking/monitoring/logger.py
- tests/test_contracts.py

Shared reference file:

- docs/architecture.md

## Notes

This repo is intentionally a boilerplate foundation for the Advanced Operating Systems and Distributed Systems assignment. The actual distributed algorithms and ticket-booking behavior are planned for later milestones.

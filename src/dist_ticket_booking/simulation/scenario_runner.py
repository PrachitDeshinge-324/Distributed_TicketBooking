"""Scenario runner placeholder.

Owner: Person B (Simulation and Validation)
Primary responsibility: simulate distributed booking contention and state changes.

Functions to implement in this file:
- ScenarioRunner.__init__
- ScenarioRunner.add_event
- ScenarioRunner.run

TODO for Milestone 1:
- define at least 2-3 simulation scenarios for ticket contention
- log request order, lock acquisition, and final booking state
- verify correctness under concurrent request generation
"""


class ScenarioRunner:
    """Runs simulation scenarios for booking activity."""

    def __init__(self):
        self.events = []

    def add_event(self, event_name: str, details: dict | None = None):
        self.events.append({"event": event_name, "details": details or {}})
        return self.events

    def run(self):
        """Placeholder run method for future simulation logic."""
        return {"status": "ready", "events": self.events}

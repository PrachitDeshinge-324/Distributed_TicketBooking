"""Monitoring and log placeholder.

Owner: Person B (Simulation and Validation)
Primary responsibility: record request decisions and coordination traces.

Functions to implement in this file:
- SystemLogger.__init__
- SystemLogger.log

TODO for Milestone 1:
- define event categories such as request, lock-acquired, queue-update, booking-success
- ensure log output supports debugging of concurrency problems
- keep logs readable for later analysis and report writing
"""


class SystemLogger:
    """Simple log sink for system activity."""

    def __init__(self):
        self.records = []

    def log(self, event: str, payload: dict | None = None):
        self.records.append({"event": event, "payload": payload or {}})
        return self.records

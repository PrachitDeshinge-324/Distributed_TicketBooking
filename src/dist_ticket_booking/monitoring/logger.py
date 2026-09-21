"""Event logging helper for tracking system events."""


class SystemLogger:
    """Simple log recorder for system activity."""

    def __init__(self):
        self.records = []

    def log(self, event: str, payload: dict | None = None):
        self.records.append({"event": event, "payload": payload or {}})
        return self.records

    def last_event(self):
        if not self.records:
            return None
        return self.records[-1]

    def clear(self):
        self.records = []
        return self.records

"""Scenario runner for simulating booking request flows."""


class ScenarioRunner:
    """Runs simulation scenarios for booking activity."""

    def __init__(self):
        self.events = []

    def add_event(self, event_name: str, details: dict | None = None):
        self.events.append({"event": event_name, "details": details or {}})
        return self.events

    def run(self, requests=None):
        """Execute a simple scenario over a list of requests.

        The current implementation is deliberately minimal and intended for the
        Milestone 1 simulation layer.
        """
        if requests is None:
            requests = []

        for request in requests:
            self.add_event("request_received", {"request_id": getattr(request, "request_id", "unknown")})

        return {"status": "ready", "events": self.events, "request_count": len(requests)}

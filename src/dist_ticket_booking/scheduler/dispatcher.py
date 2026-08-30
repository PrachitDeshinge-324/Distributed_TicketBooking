"""Scheduler placeholder.

Owner: Person A (Core Coordination)
Primary responsibility: decide the ordering of booking requests.

Functions to implement in this file:
- Dispatcher.__init__
- Dispatcher.enqueue
- Dispatcher.next_request

TODO for Milestone 1:
- define fairness and priority semantics
- decide whether requests are processed by FIFO, priority, or round-robin rules
- ensure queue ordering is consistent with locking logic
"""


class Dispatcher:
    """Simple scheduling layer placeholder."""

    def __init__(self):
        self.queue = []

    def enqueue(self, request):
        self.queue.append(request)
        return self.queue

    def next_request(self):
        if not self.queue:
            return None
        return self.queue.pop(0)

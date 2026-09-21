"""FIFO queue-based request dispatcher."""


class Dispatcher:
    """Queue-based scheduler for booking requests."""

    def __init__(self):
        self.queue = []

    def enqueue(self, request):
        self.queue.append(request)
        return self.queue

    def next_request(self):
        if not self.queue:
            return None
        return self.queue.pop(0)

    def size(self):
        return len(self.queue)

    def peek(self):
        if not self.queue:
            return None
        return self.queue[0]

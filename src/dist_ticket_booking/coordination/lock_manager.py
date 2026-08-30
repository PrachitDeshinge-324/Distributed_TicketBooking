"""Lock manager placeholder.

Owner: Person A (Core Coordination)
Primary responsibility: coordinate access to shared ticket inventory safely.

Functions to implement in this file:
- LockManager.__init__
- LockManager.acquire
- LockManager.release
- LockManager.queue_status

TODO for Milestone 1:
- define lock acquisition semantics
- decide between simple queue-based locking and priority-based ordering
- keep state transitions explicit for distributed booking contention
"""


class LockManager:
    """Simple coordination placeholder for distributed lock logic."""

    def __init__(self):
        self.lock_owner = None
        self.wait_queue = []

    def acquire(self, process_id: str):
        """Request a lock for a process."""
        return {"process_id": process_id, "status": "requested"}

    def release(self, process_id: str):
        """Release a lock owned by a process."""
        return {"process_id": process_id, "status": "released"}

    def queue_status(self):
        """Return current waiting queue information."""
        return list(self.wait_queue)

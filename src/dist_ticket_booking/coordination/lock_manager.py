"""Queue-based lock manager for coordinating access to ticket inventory."""


class LockManager:
    """Queue-based coordination for lock logic.

    A process can acquire the lock only when the current owner is free.
    If another process already owns the lock, later requests are queued.
    """

    def __init__(self):
        self.lock_owner = None
        self.wait_queue = []

    def acquire(self, process_id: str):
        """Request a lock for a process."""
        if self.lock_owner is None:
            self.lock_owner = process_id
            return {"process_id": process_id, "status": "acquired"}

        if process_id not in self.wait_queue:
            self.wait_queue.append(process_id)
        return {"process_id": process_id, "status": "queued"}

    def release(self, process_id: str):
        """Release a lock owned by a process."""
        if self.lock_owner == process_id:
            self.lock_owner = None
            if self.wait_queue:
                next_process = self.wait_queue.pop(0)
                self.lock_owner = next_process
                return {"process_id": process_id, "status": "released", "next_owner": next_process}
            return {"process_id": process_id, "status": "released"}

        return {"process_id": process_id, "status": "not_owner"}

    def queue_status(self):
        """Return current waiting queue information."""
        return list(self.wait_queue)

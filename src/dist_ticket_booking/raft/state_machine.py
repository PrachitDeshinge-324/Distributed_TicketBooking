"""State machine that applies committed Raft log entries to the BookingService."""
import logging
import uuid
from typing import Dict, Any, Optional

from dist_ticket_booking.raft.log import LogEntry

logger = logging.getLogger(__name__)


class StateMachine:
    """The 'state machine' in Raft terminology.

    Receives committed log entries and applies them deterministically
    to the BookingService. Every node applies the same sequence of
    committed entries, so their state converges.
    """

    def __init__(self, booking_service):
        self.booking_service = booking_service
        self.sessions: Dict[str, str] = {}  # token -> username (replicated)
        self.last_applied: int = -1
        self._results: Dict[int, dict] = {}  # log_index -> result cache

    def apply(self, entry: LogEntry) -> dict:
        """Apply a committed log entry. Returns the operation result.

        Idempotent — re-applying an already-applied entry returns
        the cached result without side effects.
        """
        if entry.index <= self.last_applied:
            return self._results.get(entry.index, {})

        cmd = entry.command
        result: Dict[str, Any] = {}

        try:
            if entry.command_type == "create_booking":
                result = self.booking_service.create_booking(
                    user_id=cmd["user_id"],
                    ticket_id=cmd["ticket_id"],
                    quantity=int(cmd.get("quantity", 1)),
                    priority=int(cmd.get("priority", 0)),
                    # Deterministic id: every replica must assign the same
                    # booking_id so cancels still work after leader failover.
                    booking_id=f"APPT-{cmd['ticket_id'][:12]}-{cmd['user_id'][:6]}-{entry.index}",
                )
            elif entry.command_type == "cancel_booking":
                result = self.booking_service.cancel_booking(
                    user_id=cmd["user_id"],
                    booking_id=cmd["booking_id"],
                )
            elif entry.command_type == "login":
                token = cmd.get("token") or str(uuid.uuid4())
                self.sessions[token] = cmd["username"]
                result = {"status": "success", "token": token}
            elif entry.command_type == "logout":
                token = cmd.get("token", "")
                if token in self.sessions:
                    del self.sessions[token]
                    result = {"status": "success", "message": "Logged out"}
                else:
                    result = {"status": "error", "message": "Invalid token"}
            elif entry.command_type == "noop":
                result = {"status": "success"}
            else:
                logger.warning(f"Unknown command type: {entry.command_type}")
                result = {"status": "error", "message": f"Unknown command: {entry.command_type}"}
        except Exception as e:
            logger.error(f"Error applying entry {entry.index}: {e}")
            result = {"status": "error", "message": str(e)}

        self.last_applied = entry.index
        self._results[entry.index] = result
        logger.debug(f"Applied entry {entry.index} (type={entry.command_type}, term={entry.term})")
        return result

    def get_session_user(self, token: str) -> Optional[str]:
        """Look up username for a session token."""
        return self.sessions.get(token)

    def get_snapshot_data(self) -> dict:
        """Capture state machine state for Raft log compaction."""
        ticket_store_data = {}
        for sid, slot in self.booking_service.ticket_store.items():
            if isinstance(slot, dict):
                ticket_store_data[sid] = dict(slot)
            else:
                ticket_store_data[sid] = {
                    "name": getattr(slot, "name", ""),
                    "vaccine_name": getattr(slot, "vaccine_name", ""),
                    "center_name": getattr(slot, "center_name", ""),
                    "center_pincode": getattr(slot, "center_pincode", ""),
                    "dose_number": getattr(slot, "dose_number", 0),
                    "date": getattr(slot, "date", ""),
                    "available_count": getattr(slot, "available_count", 0),
                    "price": getattr(slot, "price", 0.0),
                    "status": getattr(slot, "status", "available"),
                }
        return {
            "ticket_store": ticket_store_data,
            "bookings": dict(self.booking_service.bookings),
            "sessions": dict(self.sessions),
            "last_applied": self.last_applied,
        }

    def restore_from_snapshot(self, snapshot_data: dict):
        """Restore state machine from snapshot."""
        if not snapshot_data:
            return
        state = snapshot_data.get("state", snapshot_data)
        if "ticket_store" in state:
            self.booking_service.ticket_store = state["ticket_store"]
        if "bookings" in state:
            self.booking_service.bookings = state["bookings"]
        if "sessions" in state:
            self.sessions = state["sessions"]
        self.last_applied = state.get("last_applied", self.last_applied)
        logger.info(f"State machine restored from snapshot up to index {self.last_applied}")

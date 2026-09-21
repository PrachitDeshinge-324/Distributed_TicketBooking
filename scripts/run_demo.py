"""Simple demo script for the distributed ticket booking system.

Purpose:
- demonstrate a minimal Milestone 1 request flow
- show how the dispatcher and logger work together
- provide a quick way to check the project still runs as expected
"""

from dist_ticket_booking.core.models import BookingRequest
from dist_ticket_booking.scheduler.dispatcher import Dispatcher
from dist_ticket_booking.simulation.scenario_runner import ScenarioRunner
from dist_ticket_booking.monitoring.logger import SystemLogger


def main():
    logger = SystemLogger()
    dispatcher = Dispatcher()
    runner = ScenarioRunner()

    requests = [
        BookingRequest("R-001", "U-001", "T-001", quantity=1, priority=1),
        BookingRequest("R-002", "U-002", "T-001", quantity=2, priority=2),
        BookingRequest("R-003", "U-003", "T-001", quantity=1, priority=3),
    ]

    for request in requests:
        dispatcher.enqueue(request)
        logger.log("request_added", {"request_id": request.request_id})

    runner.run(requests)

    print("Dispatcher queue size:", dispatcher.size())
    print("Next request:", dispatcher.peek().request_id if dispatcher.peek() else None)
    print("Last log entry:", logger.last_event())
    print("Scenario result:", runner.run(requests))


if __name__ == "__main__":
    main()

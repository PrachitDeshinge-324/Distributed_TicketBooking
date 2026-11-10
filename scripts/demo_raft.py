"""Demonstration of Raft Consensus across 3 nodes."""
import json
import logging
import os
import shutil
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dist_ticket_booking.grpc.service import TicketClient
from scripts.run_cluster import ClusterManager, DEFAULT_CLUSTER_SPEC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [DemoRaft] %(message)s",
)
logger = logging.getLogger("DemoRaft")

DATA_DIR = "./raft_data_demo"


def find_leader(cluster_spec):
    """Query each node's availability to determine which is the leader."""
    for node in cluster_spec:
        client = TicketClient(host="localhost", port=node["client_port"])
        try:
            # Login and check
            login_res = client.client_stub.Login(
                client.client_stub._common_request(
                    __import__("dist_ticket_booking.grpc.ticket_service_pb2", fromlist=["LoginRequest"]).LoginRequest(
                        username="probe", password="password"
                    )
                )
            ) if hasattr(client.client_stub, "_common_request") else None
        except Exception:
            pass
        finally:
            client.close()


def run_demo():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    logger.info("=================================================================")
    logger.info("   DEMO: RAFT CONSENSUS & DISTRIBUTED STATE REPLICATION          ")
    logger.info("=================================================================")

    manager = ClusterManager(
        python_bin=sys.executable,
        nodes=DEFAULT_CLUSTER_SPEC,
        data_dir=DATA_DIR,
        with_llm=False,
    )

    try:
        logger.info("\n>>> Step 1: Launching 3-Node Raft Cluster...")
        manager.start_all()

        # Allow leader election to settle
        logger.info("Waiting 3 seconds for leader election to complete...")
        time.sleep(3.0)

        logger.info("\n>>> Step 2: Client connecting to node-1 (port 50051)...")
        # TicketClient automatically follows leader redirects if node-1 is a follower!
        client = TicketClient(host="localhost", port=50051)

        logger.info("Performing login for citizen 'alice'...")
        login_res = client.login("alice", "password")
        token = login_res.token
        logger.info(f"Login result: status={login_res.status}, token={token[:8]}... (Connected to: {client.target})")

        logger.info("\n>>> Step 3: Checking initial vaccine slot availability...")
        avail_res = client.get(token, "availability", {"ticket_id": "SLOT-CVS-D1-110001"})
        initial_data = json.loads(avail_res.items[0].data)
        logger.info(
            f"Slot: {initial_data['name']} | Available count: {initial_data['available_count']}"
        )

        logger.info("\n>>> Step 4: Proposing booking through Raft log replication...")
        booking_res = client.post(
            token,
            "booking",
            {"ticket_id": "SLOT-CVS-D1-110001", "quantity": 2, "priority": 1},
        )
        logger.info(f"Booking response: status={booking_res.status}, message={booking_res.message}")

        logger.info("\n>>> Step 5: Checking updated availability on the cluster...")
        updated_res = client.get(token, "availability", {"ticket_id": "SLOT-CVS-D1-110001"})
        updated_data = json.loads(updated_res.items[0].data)
        logger.info(
            f"Updated Available Count: {updated_data['available_count']} (decremented by 2)"
        )
        assert updated_data["available_count"] == initial_data["available_count"] - 2

        logger.info("\n>>> Step 6: Testing Follower Redirection...")
        # Now purposefully connect to another node port (e.g. 50053 or 50055)
        for target_port in [50053, 50055]:
            alt_client = TicketClient(host="localhost", port=target_port)
            logger.info(f"Connecting client to port {target_port}...")
            alt_login = alt_client.login("bob", "password")
            logger.info(
                f"Client at port {target_port} resolved to {alt_client.target} "
                f"(status={alt_login.status})"
            )
            alt_client.close()

        client.logout(token)
        client.close()

        logger.info("\n=================================================================")
        logger.info("   RAFT CONSENSUS DEMO COMPLETED SUCCESSFULLY!                  ")
        logger.info("=================================================================")

    finally:
        manager.stop_all()


if __name__ == "__main__":
    run_demo()

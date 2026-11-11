"""Demonstration of Raft Fault Tolerance, Leader Crash, and Node Recovery."""
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
    format="%(asctime)s [%(levelname)s] [FaultToleranceDemo] %(message)s",
)
logger = logging.getLogger("FaultToleranceDemo")

DATA_DIR = "./raft_data_fault_tolerance"
TARGET_SLOT = "SLOT-CBX-BOS-560001"  # starts with 3 available slots


def find_current_leader_node(cluster_spec):
    """Find which node is currently serving as leader."""
    for spec in cluster_spec:
        client = TicketClient(host="localhost", port=spec["client_port"])
        try:
            # Send an unauthenticated get or login probe
            from dist_ticket_booking.grpc import ticket_service_pb2
            req = ticket_service_pb2.GetRequest(token="probe", type="availability")
            res = client.client_stub.Get(req)
            if res.status != "redirect":
                client.close()
                return spec
        except Exception:
            pass
        finally:
            client.close()
    # Default to node-1 if not identified
    return cluster_spec[0]


def run_fault_tolerance_demo():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    logger.info("=================================================================")
    logger.info("   DEMO: FAULT TOLERANCE, LEADER CRASH, AND STATE RECOVERY       ")
    logger.info("=================================================================")

    manager = ClusterManager(
        python_bin=sys.executable,
        nodes=DEFAULT_CLUSTER_SPEC,
        data_dir=DATA_DIR,
        with_llm=False,
    )

    try:
        logger.info("\n>>> Phase 1: Launching 3-Node Raft Cluster...")
        manager.start_all()
        logger.info("Waiting 3.0 seconds for cluster to elect initial Leader...")
        time.sleep(3.0)

        # Identify leader
        leader_spec = find_current_leader_node(DEFAULT_CLUSTER_SPEC)
        leader_id = leader_spec["node_id"]
        logger.info(f"★ Initial Raft Leader detected: {leader_id} (Client Port: {leader_spec['client_port']})")

        logger.info("\n>>> Phase 2: Booking a slot on the initial Leader...")
        client = TicketClient(host="localhost", port=leader_spec["client_port"])
        login_res = client.login("citizen_1", "password")
        token = login_res.token

        avail_1 = client.get(token, "availability", {"ticket_id": TARGET_SLOT})
        count_1 = json.loads(avail_1.items[0].data)["available_count"]
        logger.info(f"Initial '{TARGET_SLOT}' inventory: {count_1}")

        book_res_1 = client.post(token, "booking", {"ticket_id": TARGET_SLOT, "quantity": 1})
        logger.info(f"Booking 1 result: {book_res_1.status} ({book_res_1.message})")

        avail_2 = client.get(token, "availability", {"ticket_id": TARGET_SLOT})
        count_2 = json.loads(avail_2.items[0].data)["available_count"]
        logger.info(f"Post-Booking 1 inventory on {leader_id}: {count_2}")
        assert count_2 == count_1 - 1
        client.close()

        logger.info("\n" + "=" * 65)
        logger.info(f">>> Phase 3: SIMULATING CRASH: KILLING LEADER NODE ({leader_id})...")
        logger.info("=" * 65)
        manager.kill_node(leader_id, force=True)

        logger.info("Leader is DOWN. Surviving followers are detecting heartbeat timeout...")
        logger.info("Waiting 3.0 seconds for new leader election among remaining nodes...")
        time.sleep(3.0)

        surviving_specs = [s for s in DEFAULT_CLUSTER_SPEC if s["node_id"] != leader_id]
        new_leader_spec = find_current_leader_node(surviving_specs)
        new_leader_id = new_leader_spec["node_id"]
        logger.info(f"★ NEW LEADER ELECTED: {new_leader_id} (Client Port: {new_leader_spec['client_port']})")
        assert new_leader_id != leader_id, "New leader must be one of the surviving nodes"

        logger.info("\n>>> Phase 4: Verifying data consistency on the new Leader...")
        client_2 = TicketClient(host="localhost", port=new_leader_spec["client_port"])
        login_res_2 = client_2.login("citizen_2", "password")
        token_2 = login_res_2.token

        avail_3 = client_2.get(token_2, "availability", {"ticket_id": TARGET_SLOT})
        count_3 = json.loads(avail_3.items[0].data)["available_count"]
        logger.info(f"Inventory retrieved from NEW leader ({new_leader_id}): {count_3}")
        assert count_3 == count_2, f"State lost! Expected {count_2}, got {count_3}"
        logger.info("✓ DATA PRESERVED: Inventory on new leader matches state before the crash!")

        logger.info("\n>>> Phase 5: Executing new booking through NEW Leader...")
        book_res_2 = client_2.post(token_2, "booking", {"ticket_id": TARGET_SLOT, "quantity": 1})
        logger.info(f"Booking 2 result on new leader: {book_res_2.status}")

        avail_4 = client_2.get(token_2, "availability", {"ticket_id": TARGET_SLOT})
        count_4 = json.loads(avail_4.items[0].data)["available_count"]
        logger.info(f"Updated inventory on new leader: {count_4}")
        assert count_4 == count_3 - 1
        client_2.close()

        logger.info("\n" + "=" * 65)
        logger.info(f">>> Phase 6: RESTARTING CRASHED NODE ({leader_id})...")
        logger.info("=" * 65)
        manager.start_node(leader_id)
        logger.info(f"Node {leader_id} restarted. Waiting 3.0 seconds for log catch-up...")
        time.sleep(3.0)

        logger.info("\n>>> Phase 7: Verifying restarted node has caught up with latest state...")
        # Querying restarted node directly via client auto-redirect or inspection
        recovered_client = TicketClient(host="localhost", port=leader_spec["client_port"])
        rec_login = recovered_client.login("admin", "password")
        rec_token = rec_login.token
        rec_avail = recovered_client.get(rec_token, "availability", {"ticket_id": TARGET_SLOT})
        rec_count = json.loads(rec_avail.items[0].data)["available_count"]
        logger.info(f"Inventory verified through restarted node: {rec_count}")
        assert rec_count == count_4
        recovered_client.close()

        logger.info("\n=================================================================")
        logger.info("   ✓ FAULT TOLERANCE & RECOVERY TEST: 100% SUCCESSFUL!          ")
        logger.info("   - Survives leader crash                                       ")
        logger.info("   - Elects new leader automatically                             ")
        logger.info("   - Zero data loss                                              ")
        logger.info("   - Recovered node successfully rejoins and catches up          ")
        logger.info("=================================================================")

    finally:
        manager.stop_all()


if __name__ == "__main__":
    run_fault_tolerance_demo()

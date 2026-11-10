"""Master demo orchestrator: executes the full end-to-end distributed system demonstration."""
import argparse
import os
import subprocess
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

PYTHON_BIN = sys.executable


def run_command(cmd, desc):
    print(f"\n{'='*70}")
    print(f"  STEP: {desc}")
    print(f"{'='*70}")
    ret = subprocess.run([PYTHON_BIN] + cmd, cwd=PROJECT_ROOT)
    if ret.returncode != 0:
        print(f"\n[ERROR] Step '{desc}' failed with exit code {ret.returncode}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Run complete Distributed Ticket Booking demonstrations")
    parser.add_argument("--skip-tests", action="store_true", help="Skip pytest test suite")
    parser.add_argument("--demo-only", type=str, choices=["raft", "fault-tolerance", "verify", "full"], help="Run a specific demo only")
    args = parser.parse_args()

    print("\n" + "#" * 70)
    print("  DISTRIBUTED TICKET BOOKING SYSTEM — COMPLETE SYSTEM DEMO")
    print("  CS G623 Advanced Operating Systems · BITS Pilani")
    print("  Milestone 1 + Milestone 2 (Raft Consensus & Fault Tolerance)")
    print("#" * 70)

    if not args.skip_tests and not args.demo_only:
        print("\n>>> Phase 1: Running Automated Test Suites (unit, integration, stress)...")
        res = subprocess.run([PYTHON_BIN, "-m", "pytest", "tests/", "-v"], cwd=PROJECT_ROOT)
        if res.returncode != 0:
            print("[WARNING] Some tests reported issues, proceeding to live demos...")

    if args.demo_only == "raft":
        run_command(["scripts/demo_raft.py"], "3-Node Raft Consensus & Replication Demo")
        return

    if args.demo_only == "fault-tolerance":
        run_command(["scripts/demo_fault_tolerance.py"], "Fault Tolerance, Leader Crash & Recovery Demo")
        return

    if args.demo_only == "full":
        ok = run_command(["scripts/demo_full_system.py"], "Full 5-Node System Demo (LLM + 3 Raft + client node)")
        sys.exit(0 if ok else 1)

    if args.demo_only == "verify":
        ok = run_command(["scripts/verify_milestone2.py"], "End-to-End Milestone 2 Verification")
        sys.exit(0 if ok else 1)

    # Run all demos sequentially
    print("\n>>> Phase 2: Live Raft Consensus & Multi-Node Replication...")
    ok1 = run_command(["scripts/demo_raft.py"], "Raft Consensus Replication & Follower Redirects")

    print("\n>>> Phase 3: Live Fault Tolerance, Leader Crash & State Machine Recovery...")
    ok2 = run_command(["scripts/demo_fault_tolerance.py"], "Leader Kill, Re-Election, Zero Data Loss, Node Catch-up")

    print("\n>>> Phase 4: End-to-End Verification (LLM, quorum loss, full restart, 5 nodes)...")
    ok3 = run_command(["scripts/verify_milestone2.py"], "End-to-End Milestone 2 Verification")

    print("\n>>> Phase 5: Full 5-Node System (LLM + 3 Raft nodes + client node, leader kill under load)...")
    ok4 = run_command(["scripts/demo_full_system.py"], "Full 5-Node System Demo")

    all_ok = ok1 and ok2 and ok3 and ok4
    print("\n" + "#" * 70)
    if all_ok:
        print("  ALL DEMONSTRATIONS AND VERIFICATIONS COMPLETED SUCCESSFULLY!")
    else:
        failed = [n for n, ok in (("raft demo", ok1), ("fault-tolerance demo", ok2),
                                                    ("verification", ok3), ("full-system demo", ok4)) if not ok]
        print(f"  SOME STEPS FAILED: {', '.join(failed)}")
    print("#" * 70 + "\n")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

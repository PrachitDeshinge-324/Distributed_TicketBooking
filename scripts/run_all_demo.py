"""Helper script to launch the servers and run tests."""
import subprocess
import sys
import time

PYTHON_BIN = sys.executable


def main():
    print("Starting LLM server on :50052...")
    llm_proc = subprocess.Popen(
        [PYTHON_BIN, "scripts/run_llm_server.py", "--port", "50052"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)

    print("Starting App server on :50051...")
    app_proc = subprocess.Popen(
        [PYTHON_BIN, "scripts/run_server.py", "--port", "50051", "--llm-port", "50052"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)

    try:
        print("\n--- Running Client Demo ---")
        ret1 = subprocess.run([PYTHON_BIN, "scripts/client_demo.py", "localhost", "50051"])
        if ret1.returncode != 0:
            print("Client demo returned non-zero exit code")

        print("\n--- Running Concurrency Test ---")
        ret2 = subprocess.run([PYTHON_BIN, "scripts/demo_concurrency.py", "localhost", "50051"])
        if ret2.returncode != 0:
            print("Concurrency test returned non-zero exit code")

    finally:
        print("\nStopping servers...")
        app_proc.terminate()
        llm_proc.terminate()
        app_proc.wait()
        llm_proc.wait()


if __name__ == "__main__":
    main()

"""Package the project for course submission (zip with README + code).

Works on Windows, macOS and Linux (standard library only):

    python scripts/package_submission.py

Only files tracked by git are included, so local-only material (the demo
virtualenv, Raft data directories, logs, caches, old zips) can never leak
into the archive. Run the test suite before packaging.
"""
import os
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZIP_NAME = "Distributed_TicketBooking_Submission.zip"
TOP = "Distributed_TicketBooking"  # folder name inside the zip


def git(*args) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout


def main() -> int:
    try:
        files = [f for f in git("ls-files", "-z").split("\0") if f]
    except (OSError, subprocess.CalledProcessError) as e:
        print(f"git is required to list the tracked files: {e}", file=sys.stderr)
        return 1
    if git("status", "--porcelain", "--untracked-files=no").strip():
        print("[warn] uncommitted changes: the archive contains the working-tree version of tracked files")
    untracked = [f for f in git("ls-files", "--others", "--exclude-standard").splitlines() if f]
    if untracked:
        print(f"[warn] {len(untracked)} untracked file(s) are NOT included (git add them if needed): "
              f"{', '.join(untracked[:5])}{' ...' if len(untracked) > 5 else ''}")

    out = os.path.join(ROOT, ZIP_NAME)
    if os.path.exists(out):
        os.remove(out)
    written = 0
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in files:
            path = os.path.join(ROOT, rel)
            if os.path.isfile(path):  # skips files deleted but not yet committed
                zf.write(path, f"{TOP}/{rel}")
                written += 1
    print(f"Created {out} ({written} files, {os.path.getsize(out) / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

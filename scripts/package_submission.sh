#!/usr/bin/env bash
# Package the project for course submission (zip with README + code).
#
# Only files tracked by git are included, so local-only material (slides,
# demo-recording notes, Raft data directories, virtualenvs, old zips) can
# never leak into the archive. Run the test suite before packaging.
set -euo pipefail

ZIP_NAME="Distributed_TicketBooking_Submission.zip"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "[warn] uncommitted changes: the archive contains the working-tree version of tracked files"
fi

rm -f "$ZIP_NAME"
git ls-files -z | xargs -0 zip -q "$ZIP_NAME"

echo "Created $PROJECT_DIR/$ZIP_NAME ($(git ls-files | wc -l | tr -d ' ') files)"
ls -lh "$ZIP_NAME"

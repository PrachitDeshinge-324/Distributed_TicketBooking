#!/usr/bin/env bash
set -e

# Package the project cleanly for course submission according to CS G623 guidelines.

ZIP_NAME="Distributed_TicketBooking_Submission.zip"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_DIR"

echo "=========================================================="
echo " Packaging Distributed Ticket Booking System for Submission"
echo "=========================================================="

# Remove any existing zip
rm -f "$ZIP_NAME"

# Create clean zip archive
zip -r "$ZIP_NAME" . \
    -x "*.git*" \
    -x "*__pycache__*" \
    -x "*.pytest_cache*" \
    -x "*.venv*" \
    -x "*env/*" \
    -x "*raft_data*" \
    -x "*raft_data_*/*" \
    -x "*.DS_Store*" \
    -x "*.egg-info*" \
    -x "dist/*" \
    -x "build/*"

echo ""
echo "=========================================================="
echo " Packaging Complete!"
echo " Generated archive: $PROJECT_DIR/$ZIP_NAME"
ls -lh "$ZIP_NAME"
echo "=========================================================="

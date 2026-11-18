#!/usr/bin/env bash
# Package the project for course submission (thin wrapper around
# package_submission.py, which also runs on Windows:
# `python scripts/package_submission.py`). Only git-tracked files are zipped.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
"${PYTHON:-python3}" scripts/package_submission.py

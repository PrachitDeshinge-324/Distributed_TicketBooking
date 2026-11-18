#!/bin/bash
# macOS: double-click this file in Finder to start the demo.
cd "$(dirname "$0")" || exit 1
bash ./run_demo.sh "$@"
status=$?
echo
if [ "$status" -eq 0 ]; then
    echo "Launcher finished. You can close this window (the demo keeps running in its own windows)."
else
    echo "The launcher stopped with an error (see the messages above)."
fi

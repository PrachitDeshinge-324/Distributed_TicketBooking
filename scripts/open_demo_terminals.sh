#!/usr/bin/env bash
# macOS only (optional): open the 5 demo terminals in Terminal.app, one window
# per node, each already in the project folder with the current Python.
# Elsewhere, open the five terminals by hand as described in README.md
# ("Run the Demo in 5 Terminals").
#
#   bash scripts/open_demo_terminals.sh            # fresh start (deletes old ./raft_data)
#   bash scripts/open_demo_terminals.sh --keep     # keep the saved Raft data
#   bash scripts/open_demo_terminals.sh --dry-run  # only print the commands
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-$(command -v python || command -v python3)}"
FRESH="--fresh"
DRY=0
for arg in "$@"; do
    case "$arg" in
        --keep) FRESH="" ;;
        --dry-run) DRY=1 ;;
        *) echo "unknown option: $arg"; exit 1 ;;
    esac
done

P1="node-2:localhost:50054:50053,node-3:localhost:50056:50055"
P2="node-1:localhost:50052:50051,node-3:localhost:50056:50055"
P3="node-1:localhost:50052:50051,node-2:localhost:50054:50053"

TITLES=("Node 1 - LLM server" "Node 2 - app + Raft (node-1)" "Node 3 - app + Raft (node-2)"
        "Node 4 - app + Raft (node-3)" "Node 5 - client")
CMDS=(
  "$PY scripts/run_llm_server.py"
  "$PY scripts/run_node.py --node-id node-1 --client-port 50051 --raft-port 50052 --peers $P1 $FRESH"
  "$PY scripts/run_node.py --node-id node-2 --client-port 50053 --raft-port 50054 --peers $P2 $FRESH"
  "$PY scripts/run_node.py --node-id node-3 --client-port 50055 --raft-port 50056 --peers $P3 $FRESH"
  "echo 'Client node. Try:  $PY scripts/client_demo.py   |   $PY scripts/run_clients.py --users 5 --duration 60 --think 1 --faq   |   $PY scripts/assistant.py'"
)

for i in "${!CMDS[@]}"; do
    full="cd '$DIR' && printf '\\\\033]0;${TITLES[$i]}\\\\007' && ${CMDS[$i]}"
    if [ "$DRY" = 1 ]; then
        echo "[${TITLES[$i]}] ${CMDS[$i]}"
    else
        osascript -e "tell application \"Terminal\" to do script \"${full//\"/\\\"}\"" >/dev/null
        [ "$i" = 0 ] && sleep 2   # let the LLM start loading first
    fi
done
[ "$DRY" = 1 ] || osascript -e 'tell application "Terminal" to activate'

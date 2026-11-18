#!/usr/bin/env bash
# ======================================================================
#  Distributed Ticket Booking - one-command demo for Linux and macOS
#
#      bash run_demo.sh            (macOS: or double-click run_demo.command)
#
#  1. Uses the project's own environment (.demo_venv) if it already exists.
#  2. Otherwise finds Python 3.10+ anywhere on this computer (PATH, Homebrew,
#     python.org, pyenv, conda/miniforge) and creates .demo_venv with it.
#  3. If there is no Python 3.10+ at all, downloads a private Python 3.11
#     into .demo_tools (no admin rights, nothing installed system-wide).
#  Then it installs the requirements, downloads the LLM once, and opens the
#  5 terminals:  T1 LLM server | T2, T3, T4 Raft nodes | T5 client.
#
#  Options:  --no-llm      skip PyTorch / the model (small install)
#            --keep        keep saved Raft data from a previous run
#            --background  no new windows, logs in .demo/logs
# ======================================================================
#  Your own Python installations, virtual environments and conda envs are
#  only read (to find a Python), never changed: everything is installed
#  into .demo_venv inside this folder.  Delete .demo_venv / .demo_tools /
#  .demo to remove every trace.
# ======================================================================
cd "$(dirname "$0")" || exit 1
ROOT="$(pwd)"
# Don't let an active venv/conda env or PYTHON*/PIP_* settings redirect anything.
unset PYTHONHOME PYTHONPATH PYTHONUSERBASE PYTHONSTARTUP VIRTUAL_ENV __PYVENV_LAUNCHER__
for v in $(env | sed -n 's/^\(PIP_[A-Za-z0-9_]*\)=.*/\1/p'); do unset "$v"; done
export PYTHONNOUSERSITE=1
VENV="$ROOT/.demo_venv"
TOOLS="$ROOT/.demo_tools"
LAUNCHER="scripts/demo_launcher.py"

run_with() {  # run_with <python> [args...]
    echo "Using Python $("$1" -c 'import sys; print(sys.version.split()[0])') ($1)"
    exec "$@"
}

# --- 1. existing project environment -----------------------------------
if [ -x "$VENV/bin/python" ]; then
    if "$VENV/bin/python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
        run_with "$VENV/bin/python" "$LAUNCHER" "$@"
    fi
fi
if [ -e "$VENV" ]; then
    if [ ! -f "$VENV/pyvenv.cfg" ]; then   # only ever delete a real virtual environment
        echo "$VENV exists but is not a virtual environment; not touching it."
        echo "Rename or remove it and run this again."
        exit 1
    fi
    echo "The existing .demo_venv does not work any more (its Python was moved or removed); recreating it."
    rm -rf "$VENV"
fi

# --- 2. find Python 3.10+ on this computer -------------------------------
BEST=""
BEST_RANK=99
consider() {
    local p="$1" v r
    [ -x "$p" ] || return 0
    # needs venv + ensurepip, which some Linux distributions ship separately
    v=$("$p" -c 'import sys, venv, ensurepip; print("%d.%d" % sys.version_info[:2])' 2>/dev/null) || return 0
    case "$v" in
        3.12) r=1 ;; 3.11) r=2 ;; 3.13) r=3 ;; 3.10) r=4 ;;
        3.1[4-9] | 3.[2-9][0-9]) r=5 ;;   # newer: works, but fewer PyTorch wheels
        *) return 0 ;;
    esac
    if [ "$r" -lt "$BEST_RANK" ]; then BEST="$p"; BEST_RANK="$r"; fi
}

if [ -z "$DEMO_FORCE_UV" ]; then   # DEMO_FORCE_UV=1 only exists to test step 3
    for n in python3.12 python3.11 python3.13 python3.10 python3.14 python3.15 python3 python; do
        p="$(command -v "$n" 2>/dev/null)" && consider "$p"
    done
    shopt -s nullglob
    for p in /opt/homebrew/bin/python3.[0-9]* /usr/local/bin/python3.[0-9]* /usr/bin/python3.[0-9]* \
             /Library/Frameworks/Python.framework/Versions/3.*/bin/python3 \
             "$HOME"/.pyenv/versions/*/bin/python3 \
             "$HOME"/miniforge3/bin/python3 "$HOME"/miniforge3/envs/*/bin/python3 \
             "$HOME"/mambaforge/bin/python3 "$HOME"/miniconda3/bin/python3 "$HOME"/miniconda3/envs/*/bin/python3 \
             "$HOME"/anaconda3/bin/python3 "$HOME"/anaconda3/envs/*/bin/python3 \
             /opt/homebrew/Caskroom/miniforge/base/bin/python3 /opt/homebrew/Caskroom/miniforge/base/envs/*/bin/python3 \
             /opt/conda/bin/python3 /opt/miniconda3/bin/python3 /opt/anaconda3/bin/python3 \
             "$HOME"/.local/share/uv/python/*/bin/python3; do
        case "$p" in *-config) continue ;; esac
        consider "$p"
    done
    shopt -u nullglob
fi

if [ -n "$BEST" ]; then
    run_with "$BEST" "$LAUNCHER" "$@"
fi

# --- 3. no usable Python: download a private one with uv -----------------
echo
echo "No Python 3.10+ found on this computer."
echo "Downloading a private Python 3.11 into .demo_tools (one time, about 40 MB; no admin rights needed) ..."
mkdir -p "$TOOLS"
export UV_CACHE_DIR="$TOOLS/cache" UV_PYTHON_INSTALL_DIR="$TOOLS/python" UV_NO_CONFIG=1
find_uv() {
    for c in "$TOOLS/uv" "$TOOLS/bin/uv"; do [ -x "$c" ] && { echo "$c"; return 0; }; done
    return 1
}
UV="$(find_uv)"
if [ -z "$UV" ]; then
    if command -v curl >/dev/null 2>&1; then
        curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$TOOLS" UV_NO_MODIFY_PATH=1 sh
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$TOOLS" UV_NO_MODIFY_PATH=1 sh
    fi
    UV="$(find_uv)"
fi
if [ -z "$UV" ] || ! "$UV" venv --seed --python 3.11 "$VENV"; then
    echo
    echo " Could not download Python automatically (no internet connection, or curl/wget missing)."
    case "$(uname -s)" in
        Darwin) echo " Install Python 3.11 from https://www.python.org/downloads/ (or: brew install python@3.11)" ;;
        *)      echo " Install it with:  sudo apt install python3 python3-venv python3-pip   (Debian/Ubuntu)"
                echo "              or:  sudo dnf install python3                              (Fedora)" ;;
    esac
    echo " and run this again."
    exit 1
fi
run_with "$VENV/bin/python" "$LAUNCHER" "$@"

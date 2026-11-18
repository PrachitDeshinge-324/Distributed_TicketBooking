#!/usr/bin/env python3
"""One-command demo launcher for Windows, macOS and Linux.

    python scripts/demo_launcher.py              # install if needed, open the 5 terminals
    python scripts/demo_launcher.py setup        # only create the environment and install
    python scripts/demo_launcher.py start --background    # no new windows: run everything here
    python scripts/demo_launcher.py stop         # stop a --background run (or leftovers)

Normally you use the wrappers instead: run_demo.bat (Windows), run_demo.sh
(Linux / macOS) or run_demo.command (macOS, double-click). They find a Python
3.10+ and call this file.

What `start` does
  1. creates a private virtual environment in .demo_venv (your own Python
     packages are never touched) and installs the requirements into it,
  2. downloads the LLM (Qwen3.5-0.8B, about 1.6 GB) into the Hugging Face cache,
  3. opens five terminals and starts, one per terminal:
       T1 LLM server | T2, T3, T4 Raft nodes | T5 client walkthrough, then
       the automatic leader-failure test (scripts/failover_demo.py)
     If no graphical terminal can be opened (SSH session, container, WSL
     without a display) it falls back to --background automatically.

Only the Python standard library is used here, so it runs before anything is
installed.
"""
import argparse
import hashlib
import json
import os
import platform
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV = ROOT / ".demo_venv"
RUN_DIR = ROOT / ".demo"          # generated per-terminal scripts, logs, pid file
IS_WIN = os.name == "nt"
OS_NAME = "windows" if IS_WIN else ("macos" if sys.platform == "darwin" else "linux")

LLM_PORT = 50060
NODES = [("node-1", 50051, 50052), ("node-2", 50053, 50054), ("node-3", 50055, 50056)]
MODEL = "Qwen/Qwen3.5-0.8B"
LLM_PACKAGES = ("torch", "transformers")


# ----------------------------------------------------------------- helpers

def say(msg=""):
    print(msg, flush=True)


def step(msg):
    say(f"\n=== {msg}")


def quote(arg: str) -> str:
    """Quote one argument for the shell of the current platform's generated scripts."""
    return subprocess.list2cmdline([arg]) if OS_NAME == "windows" else shlex.quote(arg)


def venv_python() -> Path:
    return VENV / ("Scripts/python.exe" if IS_WIN else "bin/python")


def venv_bin() -> Path:
    return VENV / ("Scripts" if IS_WIN else "bin")


def clean_env() -> dict:
    """This process's environment minus anything that could point Python or pip at the
    user's own environments (PYTHONHOME, PYTHONPATH, PIP_TARGET, PIP_USER, ...). Every
    install and every demo process runs with it, so only .demo_venv is ever touched."""
    env = {k: v for k, v in os.environ.items()
           if k not in ("PYTHONHOME", "PYTHONPATH", "PYTHONSTARTUP", "PYTHONUSERBASE",
                        "VIRTUAL_ENV", "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "__PYVENV_LAUNCHER__")
           and not k.startswith("PIP_")}
    env["PYTHONNOUSERSITE"] = "1"         # never read or write the user's ~/.local site-packages
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    return env


def is_our_venv(path: Path) -> bool:
    """Only a folder that really is a virtual environment may be deleted and recreated."""
    return (path / "pyvenv.cfg").is_file()


def demo_env() -> dict:
    env = clean_env()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def port_in_use(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


# ----------------------------------------------------------------- setup

def read_requirements():
    """Split requirements.txt into (core, llm) lists of requirement strings."""
    core, llm = [], []
    for line in (ROOT / "requirements.txt").read_text().splitlines():
        line = line.split("#")[0].strip()
        if not line:
            continue
        name = line.split("==")[0].split(">=")[0].strip().lower()
        (llm if name in LLM_PACKAGES else core).append(line)
    return core, llm


def unpinned(req: str) -> str:
    name = req.split("==")[0].strip()
    return "protobuf>=7.35.1" if name.lower() == "protobuf" else name


def run(cmd, **kw) -> int:
    kw.setdefault("env", clean_env())
    return subprocess.call([str(c) for c in cmd], **kw)


def ensure_python_version():
    if sys.version_info < (3, 10):
        sys.exit(f"Python 3.10 or newer is required (this is {platform.python_version()}). "
                 "Install it from https://www.python.org/downloads/ and run this again.")


def setup(args) -> Path:
    """Create the virtual environment and install dependencies. Returns the interpreter to use."""
    if args.python:
        return Path(args.python)
    ensure_python_version()
    py = venv_python()
    step(f"Environment: {VENV}")
    running_inside_venv = Path(sys.prefix).resolve() == VENV.resolve()
    if py.exists() and not running_inside_venv and run(
            [py, "-c", "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
        if not is_our_venv(VENV):
            sys.exit(f"{VENV} exists but is not a virtual environment; not touching it. "
                     "Rename or remove it and run again.")
        say("The existing .demo_venv does not work any more (its Python was moved or removed); recreating it.")
        shutil.rmtree(VENV, ignore_errors=True)
    if py.exists():
        say("Using the existing environment.")
    else:
        say(f"Creating a private virtual environment with Python {platform.python_version()} ...")
        if run([sys.executable, "-m", "venv", VENV]) != 0 or not py.exists():
            hint = ("  On Debian/Ubuntu run:  sudo apt install python3-venv python3-pip\n"
                    if OS_NAME == "linux" else "")
            sys.exit("Could not create the virtual environment.\n" + hint)

    core, llm = read_requirements()
    want_llm = not args.no_llm
    stamp_file = VENV / ".demo_install.json"
    digest = hashlib.sha256(((ROOT / "requirements.txt").read_text() + str(want_llm)).encode()).hexdigest()
    stamp = json.loads(stamp_file.read_text()) if stamp_file.exists() else {}
    if run([py, "-m", "pip", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
        run([py, "-m", "ensurepip", "--upgrade"])  # venvs made without pip
    if stamp.get("digest") == digest and not args.reinstall:
        say("Dependencies already installed (use --reinstall to redo).")
        llm_ok = stamp.get("llm_ok", False)
    else:
        step("Installing dependencies (first run only; PyTorch is large, this can take several minutes)")
        # --isolated: ignore the user's pip.conf / PIP_* settings (e.g. user=true, target=...),
        # so packages can only land inside .demo_venv
        pip = [py, "-m", "pip", "--isolated", "install", "--disable-pip-version-check"]
        if run(pip + core) != 0:
            say("Pinned versions failed; retrying with flexible versions ...")
            if run(pip + [unpinned(r) for r in core]) != 0:
                sys.exit("Installing the core packages (grpcio, protobuf) failed. "
                         "Check your internet connection and try again.")
        llm_ok = False
        if want_llm:
            if run(pip + llm) == 0 or run(pip + [unpinned(r) for r in llm]) == 0:
                llm_ok = True
            else:
                say("WARNING: PyTorch/transformers could not be installed. The demo still works; "
                    "the LLM will answer from built-in keyword rules.")
        stamp_file.write_text(json.dumps({"digest": digest, "llm_ok": llm_ok}))

    if run([py, "-c", "import grpc, google.protobuf"]) != 0:
        sys.exit("The environment is broken. Delete the .demo_venv folder and run again.")
    if want_llm and llm_ok and not args.no_model:
        step(f"LLM model {MODEL} (downloaded once, then cached offline)")
        code = (
            "from huggingface_hub import snapshot_download as d\n"
            f"m = {MODEL!r}\n"
            "try:\n    d(m, local_files_only=True); print('Model already cached.')\n"
            "except Exception:\n    print('Downloading ~1.6 GB, please wait ...'); d(m); print('Model ready.')\n"
        )
        if run([py, "-c", code], env=demo_env()) != 0:
            say("WARNING: could not download the model (offline?). The LLM node starts anyway "
                "and answers from keyword rules until the model is available.")
    return py


# ----------------------------------------------------------------- commands

def peers_arg(node_id: str) -> str:
    return ",".join(f"{n}:localhost:{rp}:{cp}" for n, cp, rp in NODES if n != node_id)


def build_terminals(py: str, fresh: bool, with_llm: bool, windows: bool = True):
    """[(title, [argv, ...], restart_hint)]. Commands inside one terminal run one after another."""
    me = str(py)
    terms = []
    if with_llm:
        terms.append(("T1 LLM server (node 1)", [[me, "scripts/run_llm_server.py"]],
                      "python scripts/run_llm_server.py"))
    for i, (nid, cport, rport) in enumerate(NODES):
        base = [me, "scripts/run_node.py", "--node-id", nid, "--client-port", str(cport),
                "--raft-port", str(rport), "--peers", peers_arg(nid)]
        keep = "python scripts/run_node.py " + " ".join(quote(a) for a in base[2:])
        terms.append((f"T{i + 2} Raft node {nid}", [base + (["--fresh"] if fresh else [])],
                      f"{keep}   (same command, without --fresh, restarts this node with its saved data)"))
    client = [[me, "scripts/demo_launcher.py", "wait"] + ([] if with_llm else ["--no-llm"]),
              [me, "scripts/client_demo.py"],
              # Crashes the leader automatically, restarts it, prints PASS/FAIL checks.
              [me, "scripts/failover_demo.py", "--restart", "window" if windows else "background"]]
    terms.append(("T5 client (node 5)", client,
                  "python scripts/failover_demo.py | python scripts/client.py --help | "
                  "python scripts/assistant.py"))
    return terms


def script_text(title, cmds, hint):
    """Contents of the per-terminal script for this OS."""
    if OS_NAME == "windows":
        L = ["@echo off", "chcp 65001 >nul", "set PYTHONHOME=", "set PYTHONPATH=", "set PYTHONUSERBASE=",
             "set PYTHONNOUSERSITE=1", "set PYTHONUTF8=1", "set PYTHONIOENCODING=utf-8",
             f"title {title}", f'cd /d "{ROOT}"', f'call "{venv_bin() / "activate.bat"}"' if VENV.exists() else "rem"]
        L += [" ".join(quote(a) for a in c) for c in cmds]
        L += ["echo.", "echo ------------------------------------------------------------------",
              "echo  Process ended. This window stays open - run it again with:",
              f"echo    {hint}".replace("|", "^|"), "echo ------------------------------------------------------------------"]
        return "\r\n".join(L) + "\r\n"
    L = ["#!/usr/bin/env bash", "unset PYTHONHOME PYTHONPATH PYTHONUSERBASE",
         "export PYTHONUTF8=1 PYTHONIOENCODING=utf-8 PYTHONNOUSERSITE=1",
         f"printf '\\033]0;%s\\007' {quote(title)}", f"cd {quote(str(ROOT))}", "trap : INT   # Ctrl+C stops the program, not this script (the window stays open)"]
    L += [" ".join(quote(a) for a in c) for c in cmds]
    L += [f"export PATH={quote(str(venv_bin()))}:\"$PATH\"", "echo",
          "echo '------------------------------------------------------------------'",
          "echo ' Process ended. This window stays open - run it again with:'",
          f"echo {quote('   ' + hint)}", "echo '------------------------------------------------------------------'",
          'exec "${SHELL:-bash}" -i']
    return "\n".join(L) + "\n"


def write_scripts(terms):
    RUN_DIR.mkdir(exist_ok=True)
    paths = []
    for i, (title, cmds, hint) in enumerate(terms):
        p = RUN_DIR / f"terminal_{i + 1}{'.bat' if IS_WIN else '.sh'}"
        with open(p, "w", newline="") as f:
            f.write(script_text(title, cmds, hint))
        if not IS_WIN:
            p.chmod(0o755)
        paths.append(p)
    return paths


def mac_screen_size():
    try:
        out = subprocess.run(
            ["osascript", "-l", "JavaScript", "-e",
             'ObjC.import("AppKit"); var f = $.NSScreen.mainScreen.frame; f.size.width + "," + f.size.height'],
            capture_output=True, text=True, timeout=10).stdout.strip()
        w, h = (int(float(x)) for x in out.split(","))
        return w, h
    except Exception:
        return None


def open_macos(titles, scripts):
    size = mac_screen_size()
    for i, script in enumerate(scripts):
        lines = [f'tell application "Terminal" to do script "bash " & quoted form of "{script}"']
        if size:  # tile the five windows: 3 columns x 2 rows
            w, h = size
            col, row = i % 3, i // 3
            x1, x2 = w * col // 3, w * (col + 1) // 3
            y1, y2 = 30 + (h - 30) * row // 2, 30 + (h - 30) * (row + 1) // 2
            lines.append(f'tell application "Terminal" to set bounds of front window to {{{x1}, {y1}, {x2}, {y2}}}')
        subprocess.run(["osascript"] + sum([["-e", l] for l in lines], []), check=False)
        time.sleep(0.4)
    subprocess.run(["osascript", "-e", 'tell application "Terminal" to activate'], check=False)
    return True


def linux_terminal_command(title, script):
    """argv that opens `script` in the first supported terminal emulator found, else None."""
    bash = ["bash", str(script)]
    table = [
        ("gnome-terminal", ["gnome-terminal", "--title", title, "--"] + bash),
        ("konsole", ["konsole", "-p", f"tabtitle={title}", "-e"] + bash),
        ("xfce4-terminal", ["xfce4-terminal", "--title", title, "-x"] + bash),
        ("mate-terminal", ["mate-terminal", "--title", title, "-x"] + bash),
        ("tilix", ["tilix", "-t", title, "-e", f"bash {quote(str(script))}"]),
        ("kitty", ["kitty", "--title", title] + bash),
        ("alacritty", ["alacritty", "-t", title, "-e"] + bash),
        ("xterm", ["xterm", "-T", title, "-e"] + bash),
        ("x-terminal-emulator", ["x-terminal-emulator", "-e"] + bash),
    ]
    for exe, argv in table:
        if shutil.which(exe):
            return argv
    return None


def open_linux(titles, scripts):
    if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        return False
    for title, script in zip(titles, scripts):
        argv = linux_terminal_command(title, script)
        if argv is None:
            return False
        subprocess.Popen(argv, env=demo_env(), start_new_session=True)
        time.sleep(0.6)
    return True


def open_windows(titles, scripts):
    for script in scripts:
        subprocess.Popen(["cmd.exe", "/k", str(script)], env=demo_env(),
                         creationflags=subprocess.CREATE_NEW_CONSOLE)
        time.sleep(0.6)
    return True


def open_terminals(titles, scripts) -> bool:
    return {"windows": open_windows, "macos": open_macos, "linux": open_linux}[OS_NAME](titles, scripts)


def check_ports(with_llm: bool):
    ports = [p for n, c, r in NODES for p in (c, r)] + ([LLM_PORT] if with_llm else [])
    busy = [p for p in ports if port_in_use(p)]
    if busy:
        sys.exit(f"Port(s) {', '.join(map(str, busy))} are already in use: an earlier demo is still "
                 "running.\nClose its terminal windows (or press Ctrl+C in them, or run "
                 "'python scripts/demo_launcher.py stop') and try again.")


def start_background(py, args, with_llm):
    """Run every process hidden, logging to .demo/logs, then run the client walkthrough here."""
    logs = RUN_DIR / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    terms = build_terminals(py, not args.keep, with_llm, windows=False)
    procs = []
    extra = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if IS_WIN else {"start_new_session": True}
    say("Starting in the background. Logs: " + str(logs))
    for i, (title, cmds, _) in enumerate(terms[:-1], 1):
        name = f"terminal_{i}"
        f = open(logs / f"{name}.log", "w", encoding="utf-8")
        procs.append(subprocess.Popen(cmds[0], cwd=ROOT, env=demo_env(), stdout=f, stderr=subprocess.STDOUT, **extra))
        say(f"  started {title}  (pid {procs[-1].pid})  log: .demo/logs/{name}.log")
        time.sleep(2 if "LLM" in title else 0.7)
    (RUN_DIR / "pids.json").write_text(json.dumps([p.pid for p in procs]))
    try:
        for cmd in terms[-1][1]:
            subprocess.call(cmd, cwd=ROOT, env=demo_env())
        if args.once:
            return
        say("\nThe cluster keeps running. Try:  python scripts/assistant.py  |  "
            "python scripts/cluster_admin.py status\nPress Ctrl+C here to stop everything.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_pids([p.pid for p in procs] + restarted_pids())
        (RUN_DIR / "pids.json").unlink(missing_ok=True)
        say("Stopped.")


def restarted_pids():
    """Nodes restarted in the background by failover_demo.py (and forget them)."""
    path = RUN_DIR / "restarted.json"
    if not path.exists():
        return []
    try:
        pids = json.loads(path.read_text())
    except ValueError:
        pids = []
    path.unlink(missing_ok=True)
    return pids


def stop_pids(pids):
    for pid in pids:
        try:
            if IS_WIN:
                subprocess.call(["taskkill", "/PID", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
            else:
                import signal
                os.kill(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass


def cmd_start(args):
    with_llm = not args.no_llm
    py = setup(args) if not args.dry_run else Path(args.python or venv_python())
    if args.setup_only:
        return
    if not args.dry_run:
        check_ports(with_llm)
    terms = build_terminals(py, not args.keep, with_llm)
    if args.dry_run:
        paths = write_scripts(terms)
        for (title, cmds, _), p in zip(terms, paths):
            say(f"\n----- {p.name}  [{title}] -----\n{p.read_text()}")
        return
    if args.background:
        return start_background(py, args, with_llm)
    paths = write_scripts(terms)
    step("Opening the demo terminals")
    if not open_terminals([t[0] for t in terms], paths):
        say("Could not open graphical terminals here (no display or no supported terminal program).")
        say("Falling back to --background mode.\n")
        return start_background(py, args, with_llm)
    say(f"\nOpened {len(terms)} terminals: " + ", ".join(t[0] for t in terms))
    say("T5 waits for the cluster, walks through every client function, then crashes the leader "
        "automatically and prints PASS/FAIL checks (a new window opens for the restarted node).")
    say("Stop the demo by pressing Ctrl+C / closing the windows. Re-running this launcher "
        "starts it again.")


def cmd_wait(args):
    """Wait until the Raft cluster has a leader and the LLM server answers (used by T5)."""
    sys.path.insert(0, str(ROOT / "src"))
    import grpc
    from dist_ticket_booking.monitoring.console import setup_console
    from dist_ticket_booking.grpc import ticket_service_pb2 as pb
    from dist_ticket_booking.grpc import ticket_service_pb2_grpc as stubs
    setup_console()

    def leader():
        up, lead = 0, None
        for _, cport, _ in NODES:
            ch = grpc.insecure_channel(f"localhost:{cport}")
            try:
                r = stubs.TicketClientServiceStub(ch).Get(pb.GetRequest(type="cluster_status"), timeout=1.0)
                st = json.loads(r.items[0].data)
                up += 1
                if st["state"] == "leader":
                    lead = st["node_id"]
            except Exception:
                pass
            finally:
                ch.close()
        return lead if up >= 2 else None

    say("Waiting for the Raft cluster to elect a leader ...")
    deadline, found = time.time() + args.timeout, None
    while time.time() < deadline and not found:
        found = leader()
        if not found:
            time.sleep(1)
    if not found:
        sys.exit("No leader after %ds. Check the node terminals for errors." % args.timeout)
    say(f"Raft cluster ready: leader is {found}.")
    if not args.no_llm:
        say("Waiting for the LLM server (it loads the model first, this can take a minute) ...")
        ch = grpc.insecure_channel(f"localhost:{LLM_PORT}")
        try:
            grpc.channel_ready_future(ch).result(timeout=args.llm_timeout)
            say("LLM server ready.")
        except grpc.FutureTimeoutError:
            say("LLM server not ready yet; continuing (FAQ answers use keyword rules until it is).")
        finally:
            ch.close()
    time.sleep(0.5)


def cmd_stop(args):
    pid_file = RUN_DIR / "pids.json"
    extra = restarted_pids()
    stop_pids(extra)
    if pid_file.exists():
        stop_pids(json.loads(pid_file.read_text()))
        pid_file.unlink()
        say("Stopped the background demo.")
    elif extra:
        say("Stopped the node(s) restarted by the failover test.")
    else:
        say("No background demo recorded. Close the terminal windows, or press Ctrl+C in each.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    for name in ("start", "setup"):
        p = sub.add_parser(name, help="install if needed and open the terminals" if name == "start"
                           else "create the environment and install only")
        p.add_argument("--no-llm", action="store_true", help="skip PyTorch/model and the LLM terminal (low-spec machines)")
        p.add_argument("--no-model", action="store_true", help="install packages but do not pre-download the model")
        p.add_argument("--reinstall", action="store_true", help="redo the dependency installation")
        p.add_argument("--python", help="use this interpreter instead of creating .demo_venv")
        p.add_argument("--keep", action="store_true", help="keep saved Raft data (default: start from a clean state)")
        p.add_argument("--background", action="store_true", help="no new windows: run hidden, logs in .demo/logs")
        p.add_argument("--once", action="store_true", help="with --background: run the client walkthrough, then stop")
        p.add_argument("--dry-run", action="store_true", help="only write and print the per-terminal scripts")
    w = sub.add_parser("wait", help="wait for the cluster (used by terminal 5)")
    w.add_argument("--no-llm", action="store_true")
    w.add_argument("--timeout", type=int, default=90)
    w.add_argument("--llm-timeout", type=int, default=180)
    sub.add_parser("stop", help="stop a --background demo")
    argv = sys.argv[1:]
    if not argv or (argv[0] not in ("start", "setup", "wait", "stop") and argv[0] not in ("-h", "--help")):
        argv = ["start"] + argv  # no sub-command: behave like `start`
    args = ap.parse_args(argv)
    if args.cmd == "setup":
        args.setup_only, args.dry_run = True, False
        return cmd_start(args)
    if args.cmd == "start":
        args.setup_only = False
        return cmd_start(args)
    return {"wait": cmd_wait, "stop": cmd_stop}[args.cmd](args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

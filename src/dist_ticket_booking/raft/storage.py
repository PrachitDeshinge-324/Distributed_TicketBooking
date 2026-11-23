"""Crash-safe file persistence for Raft's stable storage.

Raft requires that currentTerm, votedFor and the log reach stable storage
before a node answers an RPC. A plain ``open(path, "w")`` truncates the file
first, so a crash (e.g. SIGKILL) in the middle of a write would leave a
half-written file and the node would come back having forgotten entries it
already acknowledged. Writing to a temporary file, fsync-ing it and renaming
it over the old one makes every update all-or-nothing.
"""
import json
import os
import threading
import time
from typing import Any


class CorruptStateError(RuntimeError):
    """Stable storage exists but cannot be read.

    A node must not silently start from empty state in this case: it could
    vote twice in a term or elect a leader that is missing committed entries.
    """


def atomic_write_text(path: str, text: str) -> None:
    """Replace the contents of `path` with `text` atomically and durably."""
    # Unique temp name: concurrent writers (gRPC handler threads) must not share one file.
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        # On Windows os.replace raises PermissionError (WinError 5/32) if another
        # handle (reader, antivirus, indexer) has `path` open for a moment: retry.
        for attempt in range(40):
            try:
                os.replace(tmp_path, path)  # atomic on POSIX and Windows
                return
            except PermissionError:
                if attempt == 39:
                    raise
                time.sleep(0.01 * (attempt + 1) if attempt < 10 else 0.1)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def atomic_write_json(path: str, data: Any) -> None:
    """Write `data` as JSON to `path` atomically and durably."""
    atomic_write_text(path, json.dumps(data))


def read_json(path: str) -> Any:
    """Read JSON from `path`; raises CorruptStateError if it is unreadable."""
    for attempt in range(40):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except PermissionError as exc:  # Windows: file briefly locked by a concurrent replace
            if attempt == 39:
                raise CorruptStateError(f"stable storage file {path} is unreadable: {exc}") from exc
            time.sleep(0.01)
        except (OSError, json.JSONDecodeError) as exc:
            raise CorruptStateError(f"stable storage file {path} is unreadable: {exc}") from exc

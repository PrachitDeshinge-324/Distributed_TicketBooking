"""Make a script's terminal output behave the same on Windows, macOS and Linux."""
import os
import sys


def setup_console() -> bool:
    """Prepare stdout/stderr for the symbols and colours our scripts print.

    * Forces UTF-8 output: the default Windows console encoding (cp1252 and
      friends) cannot encode characters such as the leader star or tick mark
      and would raise UnicodeEncodeError mid-demo.
    * Enables ANSI colour codes on Windows 10+ (they are off by default in
      cmd.exe and PowerShell consoles).

    Returns True when coloured output is appropriate (a real terminal that has
    not opted out through the NO_COLOR convention).
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    if os.name == "nt":
        os.system("")  # side effect: switches the console to virtual-terminal mode
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None

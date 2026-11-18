"""Accounts, roles and password handling.

Passwords are never stored or replicated in plain text. The application
server derives a salted PBKDF2 digest and only that digest travels through
the Raft log and lives in the replicated user table.

Accounts are created explicitly with a ``register`` command; logging in as
an unknown user fails. Each account has a role:

* ``citizen``: books and cancels its own appointments.
* ``admin``: may also list every booking, cancel any booking (help desk)
  and run cluster operations (membership changes, leadership transfer).
  Registering as admin requires the cluster's admin sign-up code.
"""
import hashlib
import hmac
import os
import re
from typing import Optional

_ITERATIONS = 20_000

ROLE_CITIZEN = "citizen"
ROLE_ADMIN = "admin"
ROLES = (ROLE_CITIZEN, ROLE_ADMIN)

# Shared secret that authorises creating admin accounts. Set the same value
# on every node through the ADMIN_SIGNUP_CODE environment variable.
DEFAULT_ADMIN_CODE = "aos-admin"


def admin_signup_code() -> str:
    return os.environ.get("ADMIN_SIGNUP_CODE", DEFAULT_ADMIN_CODE)


_USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")


def validate_username(username: str) -> Optional[str]:
    """Error message for an unacceptable username, or None if it is fine."""
    if not _USERNAME_RE.match(username or ""):
        return "username must be 1-32 characters: letters, digits, '_', '.' or '-'"
    return None


def credential_digest(username: str, password: str) -> str:
    """Deterministic salted digest of a password (salt is bound to the username).

    It must be deterministic so that every replica applying the same log
    entry reaches the same decision.
    """
    salt = f"dist-ticket-booking:{username}".encode()
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _ITERATIONS).hex()


def _record(users: dict, username: str) -> Optional[dict]:
    rec = users.get(username)
    if isinstance(rec, str):  # snapshot from before roles existed
        rec = {"digest": rec, "role": ROLE_CITIZEN}
        users[username] = rec
    return rec


def register_user(users: dict, username: str, digest: str, role: str = ROLE_CITIZEN) -> dict:
    """Create an account. Returns a result dict (status success / error)."""
    if role not in ROLES:
        return {"status": "error", "message": f"unknown role {role!r}"}
    if _record(users, username) is not None:
        return {"status": "error", "message": f"username {username!r} is already taken"}
    users[username] = {"digest": digest, "role": role}
    return {"status": "success", "message": f"registered {username!r} as {role}"}


def verify_credential(users: dict, username: str, digest: str) -> Optional[bool]:
    """True if the password matches, False if it does not, None if the user is unknown."""
    rec = _record(users, username)
    if rec is None:
        return None
    return hmac.compare_digest(rec["digest"], digest)


def role_of(users: dict, username: str) -> Optional[str]:
    rec = _record(users, username)
    return rec["role"] if rec else None

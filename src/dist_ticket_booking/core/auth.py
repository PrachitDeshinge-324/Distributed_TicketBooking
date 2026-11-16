"""Password handling for user authentication.

Passwords are never stored or replicated in plain text. The application
server derives a salted PBKDF2 digest and only that digest travels through
the Raft log and lives in the replicated user table.
"""
import hashlib
import hmac

_ITERATIONS = 20_000


def credential_digest(username: str, password: str) -> str:
    """Deterministic salted digest of a password (salt is bound to the username).

    It must be deterministic so that every replica applying the same log
    entry reaches the same decision.
    """
    salt = f"dist-ticket-booking:{username}".encode()
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _ITERATIONS).hex()


def check_credential(users: dict, username: str, digest: str) -> bool:
    """Verify a login against the user table, registering first-time users.

    The first successful login of a username creates the account; from then
    on the same password is required. Returns False for a wrong password.
    """
    stored = users.get(username)
    if stored is None:
        users[username] = digest
        return True
    return hmac.compare_digest(stored, digest)

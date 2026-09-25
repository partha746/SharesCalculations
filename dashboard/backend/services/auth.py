"""Single-user authentication: password hash + opaque server-side sessions.

The account lives in SQLite, which is git-ignored, so no credential ever reaches the
repository (it is public). Sessions are random tokens; only their SHA-256 is stored, so
a leaked database still cannot be replayed as a login. The token goes to the browser in
an HttpOnly cookie rather than localStorage, so injected script cannot read it.
"""
import hashlib
import ipaddress
import secrets
import time
from datetime import datetime, timedelta, timezone

from werkzeug.security import check_password_hash, generate_password_hash

from db import get_db

# Sliding window: a session used within its window is pushed out this far again.
SESSION_DAYS = 30
MIN_PASSWORD_LEN = 8

# Login throttle. In-process is enough: server.py runs a single Flask process, and an
# attacker cannot restart it to clear the counter.
_MAX_FAILURES = 8
_LOCKOUT_SEC = 15 * 60
_failures = {}  # client ip -> [count, first_failure_monotonic]


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.replace(microsecond=0).isoformat()


def _parse(text):
    try:
        dt = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    # Rows written before this ran through _iso may lack an offset; treat them as UTC.
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def ensure_tables():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_user (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_session (
                token_hash TEXT PRIMARY KEY,
                username   TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                user_agent TEXT
            )
            """
        )


# --- Account ---

def account():
    """The single account row, or None before the password has been set."""
    ensure_tables()
    with get_db() as conn:
        row = conn.execute("SELECT username, created_at, updated_at FROM auth_user LIMIT 1").fetchone()
    return dict(row) if row else None


def password_is_set():
    return account() is not None


def set_password(username, password):
    """Create or replace the account. Every existing session is dropped, so a password
    change also signs out any other browser that was already in."""
    username = (username or "").strip()
    if not username:
        raise ValueError("Username is required")
    if not password or len(password) < MIN_PASSWORD_LEN:
        raise ValueError("Password must be at least {} characters".format(MIN_PASSWORD_LEN))

    ensure_tables()
    now = _iso(_now())
    pw_hash = generate_password_hash(password)
    with get_db() as conn:
        existing = conn.execute("SELECT created_at FROM auth_user LIMIT 1").fetchone()
        created = existing["created_at"] if existing else now
        conn.execute("DELETE FROM auth_user")
        conn.execute(
            "INSERT INTO auth_user (username, password_hash, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (username, pw_hash, created, now),
        )
        conn.execute("DELETE FROM auth_session")


def verify_password(username, password):
    ensure_tables()
    with get_db() as conn:
        row = conn.execute("SELECT username, password_hash FROM auth_user LIMIT 1").fetchone()
    if not row or not password:
        return False
    if (username or "").strip().lower() != row["username"].lower():
        return False
    return check_password_hash(row["password_hash"], password)


# --- Sessions ---

def _hash_token(token):
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


def create_session(username, user_agent=None):
    ensure_tables()
    token = secrets.token_urlsafe(32)
    now = _now()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO auth_session (token_hash, username, created_at, expires_at, user_agent) VALUES (?, ?, ?, ?, ?)",
            (_hash_token(token), username, _iso(now), _iso(now + timedelta(days=SESSION_DAYS)), (user_agent or "")[:300]),
        )
    return token


def resolve_session(token):
    """Username for a live session token, or None. Extends the window on each use."""
    if not token:
        return None
    ensure_tables()
    token_hash = _hash_token(token)
    now = _now()
    with get_db() as conn:
        conn.execute("DELETE FROM auth_session WHERE expires_at < ?", (_iso(now),))
        row = conn.execute(
            "SELECT username, expires_at FROM auth_session WHERE token_hash = ?", (token_hash,)
        ).fetchone()
        if not row:
            return None
        expires = _parse(row["expires_at"])
        if expires is None or expires < now:
            conn.execute("DELETE FROM auth_session WHERE token_hash = ?", (token_hash,))
            return None
        # Only rewrite when it actually moves, to avoid a write on every single request.
        fresh = now + timedelta(days=SESSION_DAYS)
        if (fresh - expires).total_seconds() > 3600:
            conn.execute("UPDATE auth_session SET expires_at = ? WHERE token_hash = ?", (_iso(fresh), token_hash))
        return row["username"]


def destroy_session(token):
    if not token:
        return
    ensure_tables()
    with get_db() as conn:
        conn.execute("DELETE FROM auth_session WHERE token_hash = ?", (_hash_token(token),))


# --- Login throttle ---

def lockout_seconds(client_ip):
    """Seconds this IP must wait before another login attempt, 0 when it may try now."""
    entry = _failures.get(client_ip)
    if not entry:
        return 0
    count, first = entry
    elapsed = time.monotonic() - first
    if elapsed > _LOCKOUT_SEC:
        _failures.pop(client_ip, None)
        return 0
    if count < _MAX_FAILURES:
        return 0
    return int(_LOCKOUT_SEC - elapsed) + 1


def record_failure(client_ip):
    entry = _failures.get(client_ip)
    if not entry or time.monotonic() - entry[1] > _LOCKOUT_SEC:
        _failures[client_ip] = [1, time.monotonic()]
    else:
        entry[0] += 1


def clear_failures(client_ip):
    _failures.pop(client_ip, None)


# --- Network origin ---

def is_private_ip(ip):
    """True for loopback / RFC1918 / link-local, used to gate first-run password setup."""
    if not ip:
        return False
    # X-Forwarded-For can carry a list; the first entry is the original client.
    ip = str(ip).split(",")[0].strip()
    if ip.startswith("::ffff:"):
        ip = ip[len("::ffff:"):]
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return addr.is_loopback or addr.is_private or addr.is_link_local

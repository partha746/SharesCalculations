"""
ICICI Direct Breeze API (breeze-connect) — optional integration with multi-account support.

Setup:
  pip install breeze-connect

Environment (never commit secrets):
  Account 1: BREEZE_API_KEY / BREEZE_API_SECRET
  Account 2: BREEZE_API_KEY_2 / BREEZE_API_SECRET_2

Session (per account):
  1. Register redirect URL in ICICI (must match /api/breeze/callback/1 or /2 — see /api/breeze/status/<acct>).
  2. Open the login URL, log in; ICICI redirects with apisession in the query string.
  3. The callback auto-connects and redirects back to the dashboard.

SDK: https://pypi.org/project/breeze-connect/
"""
from __future__ import annotations

import os
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from db import get_db

_PORTFOLIO_HOLDINGS_DEFAULT_FROM = datetime(2015, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_PORTFOLIO_HOLDINGS_EXCHANGES = frozenset({"NSE", "NFO"})

# Accounts 1 & 2 come from env (.env); additional accounts are stored in the DB.
_ENV_ACCOUNT_IDS = ("1", "2")

_lock = threading.Lock()
_clients: Dict[str, Any] = {}
_account_names: Dict[str, str] = {}  # acct -> holder name (from customer details)
_session_tokens: Dict[str, str] = {}  # acct -> original session token (needed for get_customer_details)


def _ensure_accounts_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS breeze_accounts (
           id TEXT PRIMARY KEY, label TEXT, api_key TEXT NOT NULL, api_secret TEXT NOT NULL
        )"""
    )


def _db_accounts() -> Dict[str, dict]:
    try:
        with get_db() as conn:
            _ensure_accounts_table(conn)
            rows = conn.execute("SELECT id, label, api_key, api_secret FROM breeze_accounts ORDER BY id").fetchall()
        return {r["id"]: {"label": r["label"] or "", "api_key": r["api_key"], "api_secret": r["api_secret"]} for r in rows}
    except Exception:
        return {}


def _ensure_names_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS breeze_account_names (
           id TEXT PRIMARY KEY, name TEXT NOT NULL, updated_at TEXT NOT NULL
        )"""
    )


def stored_account_name(acct: str) -> Optional[str]:
    """Last holder name seen for this account. Persisted so cards stay named across restarts
    and while disconnected (the live session cache is cleared on both)."""
    try:
        with get_db() as conn:
            _ensure_names_table(conn)
            row = conn.execute("SELECT name FROM breeze_account_names WHERE id = ?", (acct,)).fetchone()
        return (row["name"] or "").strip() or None if row else None
    except Exception:
        return None


def _save_account_name(acct: str, name: str) -> None:
    try:
        with get_db() as conn:
            _ensure_names_table(conn)
            conn.execute(
                "INSERT INTO breeze_account_names (id, name, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET name = excluded.name, updated_at = excluded.updated_at",
                (acct, name, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")),
            )
    except Exception:
        pass  # naming is cosmetic; never fail a request over it


def valid_account_ids() -> tuple:
    ids = list(_ENV_ACCOUNT_IDS)
    for k in _db_accounts():
        if k not in ids:
            ids.append(k)
    return tuple(ids)


# Backwards-compat alias (env accounts only); prefer valid_account_ids().
VALID_ACCOUNT_IDS = _ENV_ACCOUNT_IDS


def is_env_account(acct: str) -> bool:
    return acct in _ENV_ACCOUNT_IDS


def account_label(acct: str) -> str:
    db = _db_accounts().get(acct)
    return (db["label"] if db else "") or ""


def add_db_account(label: str, api_key: str, api_secret: str) -> str:
    key = (api_key or "").strip()
    sec = (api_secret or "").strip()
    if not key or not sec:
        raise ValueError("API key and secret are required")
    with get_db() as conn:
        _ensure_accounts_table(conn)
        existing = [r[0] for r in conn.execute("SELECT id FROM breeze_accounts").fetchall()]
        nums = [int(x) for x in (list(_ENV_ACCOUNT_IDS) + existing) if str(x).isdigit()]
        new_id = str(max(nums + [2]) + 1)
        conn.execute(
            "INSERT INTO breeze_accounts (id, label, api_key, api_secret) VALUES (?, ?, ?, ?)",
            (new_id, (label or "").strip(), key, sec),
        )
    return new_id


def delete_db_account(acct: str) -> None:
    if acct in _ENV_ACCOUNT_IDS:
        raise ValueError("Built-in accounts cannot be removed")
    disconnect(acct)
    with get_db() as conn:
        _ensure_accounts_table(conn)
        _ensure_names_table(conn)
        conn.execute("DELETE FROM breeze_accounts WHERE id = ?", (acct,))
        conn.execute("DELETE FROM breeze_account_names WHERE id = ?", (acct,))


def sdk_installed() -> bool:
    # NB: importing breeze_connect performs a network download (SecurityMaster) at import time,
    # so a transient DNS/network failure raises here too — treat any failure as "not available"
    # rather than crashing the caller (e.g. /api/breeze/status).
    try:
        import breeze_connect  # noqa: F401
        return True
    except Exception:
        return False


def get_env_config(acct: str = "1") -> dict:
    if acct in _ENV_ACCOUNT_IDS:
        suffix = "" if acct == "1" else f"_{acct}"
        return {
            "api_key": (os.environ.get(f"BREEZE_API_KEY{suffix}") or "").strip(),
            "api_secret": (os.environ.get(f"BREEZE_API_SECRET{suffix}") or "").strip(),
        }
    db = _db_accounts().get(acct)
    if db:
        return {"api_key": db["api_key"], "api_secret": db["api_secret"]}
    return {"api_key": "", "api_secret": ""}


def is_configured(acct: str = "1") -> bool:
    c = get_env_config(acct)
    return bool(c["api_key"] and c["api_secret"])


def login_url(acct: str = "1") -> Optional[str]:
    key = get_env_config(acct)["api_key"]
    if not key:
        return None
    return "https://api.icicidirect.com/apiuser/login?api_key=" + urllib.parse.quote_plus(key)


def get_client(acct: str = "1"):
    with _lock:
        return _clients.get(acct)


def connect_session(session_token: str, acct: str = "1") -> Any:
    if not sdk_installed():
        raise RuntimeError("breeze-connect is not installed (pip install breeze-connect)")
    cfg = get_env_config(acct)
    if not cfg["api_key"] or not cfg["api_secret"]:
        raise ValueError(f"Set BREEZE_API_KEY{'_' + acct if acct != '1' else ''} and BREEZE_API_SECRET in the environment")
    t = (session_token or "").strip()
    if not t:
        raise ValueError("session_token is required")

    from breeze_connect import BreezeConnect

    b = BreezeConnect(api_key=cfg["api_key"])
    b.generate_session(api_secret=cfg["api_secret"], session_token=t)
    with _lock:
        _clients[acct] = b
        _account_names.pop(acct, None)
        # generate_session overwrites b.session_key with a decoded value, so keep the
        # original token around: get_customer_details needs the raw SessionToken.
        _session_tokens[acct] = t
    # Capture the account holder name for display (best-effort).
    try:
        get_account_name(acct)
    except Exception:
        pass
    return b


# Candidate keys (across Breeze/ICICI response variants) that hold the holder name.
_NAME_KEYS = (
    "idirect_user_name",
    "idirectUserName",
    "user_name",
    "userName",
    "ClientName",
    "clientName",
    "name",
)


def get_account_name(acct: str = "1") -> Optional[str]:
    """Holder name from ICICI customer details, cached per account."""
    cached = _account_names.get(acct)
    if cached:
        return cached
    b = get_client(acct)
    if b is None:
        return None
    token = _session_tokens.get(acct) or getattr(b, "session_key", "") or ""
    try:
        res = b.get_customer_details(api_session=token) or {}
        success = res.get("Success") if isinstance(res, dict) else None
        if isinstance(success, dict):
            for key in _NAME_KEYS:
                val = success.get(key)
                if val and str(val).strip():
                    name = str(val).strip().title()
                    _account_names[acct] = name
                    _save_account_name(acct, name)
                    return name
            print(f"[breeze] account {acct} customer-details keys: {sorted(success.keys())}", flush=True)
        else:
            print(f"[breeze] account {acct} customer-details unexpected response: {str(res)[:300]}", flush=True)
    except Exception as e:  # pragma: no cover - network/SDK variability
        print(f"[breeze] account {acct} get_account_name error: {e!r}", flush=True)
    return None


def disconnect(acct: str = "1") -> None:
    with _lock:
        _clients.pop(acct, None)
        _account_names.pop(acct, None)
        _session_tokens.pop(acct, None)


def _require_client(acct: str = "1"):
    b = get_client(acct)
    if b is None:
        raise RuntimeError(f"Account {acct} not connected")
    return b


def api_get_funds(acct: str = "1") -> Any:
    return _require_client(acct).get_funds()


def api_get_demat_holdings(acct: str = "1") -> Any:
    return _require_client(acct).get_demat_holdings()


def api_get_customer_details(acct: str = "1") -> Any:
    return _require_client(acct).get_customer_details()


def api_get_portfolio_positions(acct: str = "1") -> Any:
    return _require_client(acct).get_portfolio_positions()


def _iso8601_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


_MF_UNIT_HOLDING_URL = "https://api.icicidirect.com/mf/mfUnitHolding"
# ICICI servers run on IST; the legacy /mf API rejects timestamps off by > 60s.
_IST = timezone(timedelta(hours=5, minutes=30))


def api_get_mf_holdings(acct: str = "1", portfolio_type: str = "A") -> Any:
    """Fetch mutual-fund unit holdings via the legacy ICICI /mf/mfUnitHolding endpoint.

    The breeze-connect SDK does not wrap MF endpoints, so we sign the request manually using the
    same apiuser credentials/session: Checksum = sha256(time_stamp + JSONPostData + secret_key),
    time_stamp = 'DD-Mon-YYYY HH:MM:SS' (IST). Returns the raw parsed JSON from ICICI.
    """
    import hashlib
    import json as _json

    import requests as _requests

    b = _require_client(acct)
    api_key = getattr(b, "api_key", "")
    secret = getattr(b, "secret_key", "")
    # generate_session() replaces b.session_key with the decoded value used for the breezeapi
    # X-SessionToken header; the legacy /mf endpoint wants the original API session token.
    session = _session_tokens.get(acct) or getattr(b, "session_key", "")
    userid = getattr(b, "user_id", "")
    if not (api_key and secret and session and userid):
        raise RuntimeError("Account session is missing credentials; reconnect the account")

    pt = (portfolio_type or "A").strip().upper()
    if pt not in ("A", "Z"):
        pt = "A"
    time_stamp = datetime.now(_IST).strftime("%d-%b-%Y %H:%M:%S")
    post_data = _json.dumps(
        {"SessionToken": session, "Idirect_Userid": userid, "portfolio_type": pt},
        separators=(",", ":"),
    )
    checksum = hashlib.sha256((time_stamp + post_data + secret).encode("utf-8")).hexdigest()
    body = {
        "AppKey": api_key,
        "time_stamp": time_stamp,
        "JSONPostData": post_data,
        "Checksum": checksum,
    }
    resp = _requests.post(
        _MF_UNIT_HOLDING_URL,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=20,
    )
    try:
        return resp.json()
    except ValueError:
        return {"Status": resp.status_code, "Error": resp.text[:500], "Success": None}


def api_get_portfolio_holdings(
    exchange_code: str,
    from_date: str = "",
    to_date: str = "",
    stock_code: str = "",
    portfolio_type: str = "",
    acct: str = "1",
) -> Any:
    ec = (exchange_code or "").strip().upper()
    if not ec:
        raise ValueError("exchange_code is required")
    if ec not in _PORTFOLIO_HOLDINGS_EXCHANGES:
        raise ValueError(
            f'exchange_code must be "NSE" or "NFO" for portfolio holdings (ICICI Breeze docs)'
        )
    fd = (from_date or "").strip()
    td = (to_date or "").strip()
    if not fd or not td:
        now = datetime.now(timezone.utc)
        if not td:
            td = _iso8601_utc_z(now)
        if not fd:
            fd = _iso8601_utc_z(_PORTFOLIO_HOLDINGS_DEFAULT_FROM)
    return _require_client(acct).get_portfolio_holdings(
        ec, fd, td,
        (stock_code or "").strip(),
        (portfolio_type or "").strip(),
    )

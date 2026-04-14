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
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_PORTFOLIO_HOLDINGS_DEFAULT_FROM = datetime(2015, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_PORTFOLIO_HOLDINGS_EXCHANGES = frozenset({"NSE", "NFO"})

VALID_ACCOUNT_IDS = ("1", "2")

_lock = threading.Lock()
_clients: Dict[str, Any] = {}


def sdk_installed() -> bool:
    try:
        import breeze_connect  # noqa: F401
        return True
    except ImportError:
        return False


def get_env_config(acct: str = "1") -> dict:
    suffix = "" if acct == "1" else f"_{acct}"
    return {
        "api_key": (os.environ.get(f"BREEZE_API_KEY{suffix}") or "").strip(),
        "api_secret": (os.environ.get(f"BREEZE_API_SECRET{suffix}") or "").strip(),
    }


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
    return b


def disconnect(acct: str = "1") -> None:
    with _lock:
        _clients.pop(acct, None)


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

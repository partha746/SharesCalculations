"""
ICICI Direct Breeze API (breeze-connect) — optional integration.

Setup:
  pip install breeze-connect

Environment (never commit secrets):
  BREEZE_API_KEY     — from ICICI Breeze API registration
  BREEZE_API_SECRET  — secret key paired with API key

Session:
  1. Register redirect URL in ICICI Breeze app (must match /api/breeze/callback — see /api/breeze/status callbackUrl).
  2. Open the login URL from /api/breeze/status, log in; ICICI redirects with apisession in the query string.
  3. POST the token to /api/breeze/session as JSON { "session_token": "..." }.

Regulatory: orders must originate from your registered static IP; see ICICI docs.

SDK: https://pypi.org/project/breeze-connect/
"""
from __future__ import annotations

import os
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone

_PORTFOLIO_HOLDINGS_DEFAULT_FROM = datetime(2015, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
from typing import Any, Optional

# GetPortfolioHoldings: https://api.icicidirect.com/breezeapi/documents/index.html?python#portfolioholdings
_PORTFOLIO_HOLDINGS_EXCHANGES = frozenset({"NSE", "NFO"})

_lock = threading.Lock()
_client: Any = None


def sdk_installed() -> bool:
    try:
        import breeze_connect  # noqa: F401

        return True
    except ImportError:
        return False


def get_env_config() -> dict:
    return {
        "api_key": (os.environ.get("BREEZE_API_KEY") or "").strip(),
        "api_secret": (os.environ.get("BREEZE_API_SECRET") or "").strip(),
    }


def is_configured() -> bool:
    c = get_env_config()
    return bool(c["api_key"] and c["api_secret"])


def login_url() -> Optional[str]:
    """ICICI OAuth login URL; user completes login and receives session token in redirect."""
    key = get_env_config()["api_key"]
    if not key:
        return None
    return "https://api.icicidirect.com/apiuser/login?api_key=" + urllib.parse.quote_plus(key)


def get_client():
    """Active BreezeConnect instance after successful generate_session, or None."""
    with _lock:
        return _client


def connect_session(session_token: str) -> Any:
    """Exchange one-time session token for an authenticated SDK session."""
    global _client
    if not sdk_installed():
        raise RuntimeError("breeze-connect is not installed (pip install breeze-connect)")
    cfg = get_env_config()
    if not cfg["api_key"] or not cfg["api_secret"]:
        raise ValueError("Set BREEZE_API_KEY and BREEZE_API_SECRET in the environment")
    t = (session_token or "").strip()
    if not t:
        raise ValueError("session_token is required")

    from breeze_connect import BreezeConnect

    b = BreezeConnect(api_key=cfg["api_key"])
    b.generate_session(api_secret=cfg["api_secret"], session_token=t)
    with _lock:
        _client = b
    return b


def disconnect() -> None:
    global _client
    with _lock:
        _client = None


def _require_client():
    b = get_client()
    if b is None:
        raise RuntimeError("Not connected — POST /api/breeze/session with session_token first")
    return b


def api_get_funds() -> Any:
    return _require_client().get_funds()


def api_get_demat_holdings() -> Any:
    return _require_client().get_demat_holdings()


def api_get_customer_details() -> Any:
    return _require_client().get_customer_details()


def api_get_portfolio_positions() -> Any:
    return _require_client().get_portfolio_positions()


def _iso8601_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def api_get_portfolio_holdings(
    exchange_code: str,
    from_date: str = "",
    to_date: str = "",
    stock_code: str = "",
    portfolio_type: str = "",
) -> Any:
    """GET …/breezeapi/api/v1/portfolioholdings — ICICI requires ISO 8601 from_date/to_date; exchange NSE or NFO."""
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
    return _require_client().get_portfolio_holdings(
        ec,
        fd,
        td,
        (stock_code or "").strip(),
        (portfolio_type or "").strip(),
    )

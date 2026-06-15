"""ICICI Direct Breeze (optional) helpers."""
import json
import os

from flask import Response, request

from helpers import config

_DASHBOARD_DIR = os.path.join(config.REPO_ROOT, "dashboard")

try:
    import breeze_icici
except ImportError:
    breeze_icici = None  # type: ignore


def _reload_dashboard_env():
    """Re-read dashboard/.env so Breeze keys apply without restarting Flask."""
    try:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(_DASHBOARD_DIR, ".env"), override=True)
    except ImportError:
        pass

def _extract_breeze_session_token():
    """ICICI may return the session via GET query string or POST body (form / JSON)."""
    keys = (
        "apisession", "API_Session", "api_session",
        "session_token", "Session_Token", "APISession",
    )
    for key in keys:
        v = request.values.get(key)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    if request.is_json:
        data = request.get_json(silent=True) or {}
        for key in keys:
            if key in data and data[key] is not None and str(data[key]).strip() != "":
                return str(data[key]).strip()
    return ""


def _breeze_public_base():
    base = (os.environ.get("BREEZE_PUBLIC_BASE_URL") or "").strip().rstrip("/")
    return base or request.url_root.rstrip("/")


def _breeze_callback_url_for_acct(acct):
    return f"{_breeze_public_base()}/api/breeze/callback/{acct}"


def _breeze_dashboard_url():
    base = (os.environ.get("BREEZE_DASHBOARD_URL") or "").strip().rstrip("/")
    if not base:
        base = _breeze_public_base()
    return base + "/?tab=icici"


def _breeze_js_redirect(url):
    return Response(
        f'<!DOCTYPE html><html><head><meta charset="utf-8"/>'
        f'<title>Redirecting…</title></head><body>'
        f'<p>Connecting… redirecting to dashboard.</p>'
        f'<script>window.location.replace({json.dumps(url)});</script>'
        f'<noscript><p><a href="{url}">Click here to continue</a></p></noscript>'
        f'</body></html>',
        status=200,
        mimetype="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


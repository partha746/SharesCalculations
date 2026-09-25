"""/api/auth blueprint: first-run password setup, login, logout, session probe.

Everything else under /api is gated by the guard installed in app.py; these routes are
the only ones reachable while signed out.
"""
from flask import Blueprint, jsonify, request

from services import auth as svc

bp = Blueprint("auth", __name__)

COOKIE_NAME = "nvsession"


def client_ip():
    """Caller's address. serve-prod.js overwrites X-Forwarded-For with the real socket
    address before proxying, and ProxyFix resolves it, so this cannot be spoofed through
    the normal path. Reaching the backend port directly bypasses the proxy, which is why
    setup is additionally one-shot: it stops working the moment a password exists."""
    return request.remote_addr or ""


def _is_secure_request():
    return request.headers.get("X-Forwarded-Proto", request.scheme) == "https"


def _set_session_cookie(response, token):
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=svc.SESSION_DAYS * 24 * 3600,
        httponly=True,       # script on the page cannot read it
        samesite="Lax",      # not sent on cross-site POSTs
        secure=_is_secure_request(),
        path="/",
    )
    return response


def _clear_session_cookie(response):
    response.set_cookie(COOKIE_NAME, "", max_age=0, httponly=True, samesite="Lax", secure=_is_secure_request(), path="/")
    return response


@bp.route("/api/auth/session", methods=["GET"])
def session_info():
    """Unauthenticated probe the frontend uses to decide between login, setup, dashboard."""
    try:
        acct = svc.account()
        username = svc.resolve_session(request.cookies.get(COOKIE_NAME))
        return jsonify(
            {
                "authenticated": bool(username),
                "username": username,
                "passwordSet": acct is not None,
                # Drives whether the UI offers the first-run form or tells you to use the CLI.
                "canSetUpHere": acct is None and svc.is_private_ip(client_ip()),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/auth/setup", methods=["POST"])
def setup():
    """One-shot: create the account. Refused once a password exists, and refused from
    outside the local network so an exposed instance cannot be claimed by a stranger."""
    body = request.get_json(silent=True) or {}
    try:
        if svc.password_is_set():
            return jsonify({"error": "A password is already set. Use login, or reset it with set_password.py."}), 409
        if not svc.is_private_ip(client_ip()):
            return jsonify({"error": "First-time setup is only allowed from the local network."}), 403

        svc.set_password(body.get("username"), body.get("password"))
        username = (body.get("username") or "").strip()
        token = svc.create_session(username, request.headers.get("User-Agent"))
        return _set_session_cookie(jsonify({"ok": True, "username": username}), token)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/auth/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    ip = client_ip()
    try:
        wait = svc.lockout_seconds(ip)
        if wait:
            return jsonify({"error": "Too many failed attempts. Try again in {} minutes.".format(max(1, wait // 60))}), 429
        if not svc.password_is_set():
            return jsonify({"error": "No password has been set yet."}), 409

        if not svc.verify_password(body.get("username"), body.get("password")):
            svc.record_failure(ip)
            svc.log_failed_attempt(body.get("username"), body.get("password"))
            # Deliberately vague: naming which half was wrong confirms the username.
            return jsonify({"error": "Incorrect username or password."}), 401

        svc.clear_failures(ip)
        username = (body.get("username") or "").strip()
        token = svc.create_session(username, request.headers.get("User-Agent"))
        return _set_session_cookie(jsonify({"ok": True, "username": username}), token)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/auth/logout", methods=["POST"])
def logout():
    try:
        svc.destroy_session(request.cookies.get(COOKIE_NAME))
        return _clear_session_cookie(jsonify({"ok": True}))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/auth/password", methods=["POST"])
def change_password():
    """Requires the current password, so a browser someone left open cannot be used to
    lock the owner out. Every session is dropped, including this one."""
    body = request.get_json(silent=True) or {}
    try:
        username = svc.resolve_session(request.cookies.get(COOKIE_NAME))
        if not username:
            return jsonify({"error": "Not signed in."}), 401
        if not svc.verify_password(username, body.get("currentPassword")):
            return jsonify({"error": "Current password is incorrect."}), 401

        svc.set_password(username, body.get("newPassword"))
        return _clear_session_cookie(jsonify({"ok": True}))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

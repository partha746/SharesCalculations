"""/api/breeze/* blueprint."""
from flask import Blueprint, jsonify, redirect, request

from services.breeze import (
    breeze_icici, _reload_dashboard_env, _extract_breeze_session_token,
    _breeze_public_base, _breeze_callback_url_for_acct, _breeze_dashboard_url,
    _breeze_js_redirect,
)

bp = Blueprint("breeze", __name__)

@bp.route("/api/breeze/callback/<acct>", methods=["GET", "POST"])
def breeze_oauth_callback(acct):
    _reload_dashboard_env()
    if breeze_icici is None or acct not in breeze_icici.valid_account_ids():
        return _breeze_js_redirect(_breeze_dashboard_url() + "&breeze_error=invalid_account")
    token = _extract_breeze_session_token()
    print(f"[breeze-callback/{acct}] token={'YES' if token else 'EMPTY'}, query={request.query_string.decode()}", flush=True)
    if token:
        try:
            breeze_icici.connect_session(token, acct)
            dest = _breeze_dashboard_url() + f"&breeze_connected={acct}"
            print(f"[breeze-callback/{acct}] connect_session OK, redirecting to: {dest}", flush=True)
            return _breeze_js_redirect(dest)
        except Exception as e:
            print(f"[breeze-callback/{acct}] connect_session FAILED: {e}", flush=True)
            import urllib.parse
            return _breeze_js_redirect(_breeze_dashboard_url() + "&breeze_error=" + urllib.parse.quote(str(e)))
    return _breeze_js_redirect(_breeze_dashboard_url() + "&breeze_error=no_token")


# Keep old path as alias for account 1


@bp.route("/api/breeze/callback", methods=["GET", "POST"])
def breeze_oauth_callback_default():
    return breeze_oauth_callback("1")


@bp.route("/api/breeze/status/<acct>", methods=["GET"])
def breeze_status(acct):
    _reload_dashboard_env()
    callback_url = _breeze_callback_url_for_acct(acct)
    if breeze_icici is None:
        return jsonify({
            "sdkInstalled": False, "configured": False, "connected": False,
            "loginUrl": None, "callbackUrl": callback_url,
            "message": "breeze_icici module not found",
        })
    if acct not in breeze_icici.valid_account_ids():
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    connected = breeze_icici.get_client(acct) is not None
    return jsonify({
        "sdkInstalled": breeze_icici.sdk_installed(),
        "configured": breeze_icici.is_configured(acct),
        "connected": connected,
        "name": (breeze_icici.get_account_name(acct) if connected else None) or breeze_icici.stored_account_name(acct),
        "label": breeze_icici.account_label(acct),
        "custom": not breeze_icici.is_env_account(acct),
        "loginUrl": breeze_icici.login_url(acct),
        "callbackUrl": callback_url,
    })


@bp.route("/api/breeze/status", methods=["GET"])
def breeze_status_all():
    """Combined status for all accounts."""
    _reload_dashboard_env()
    if breeze_icici is None:
        return jsonify({"accounts": {}})
    result = {}
    for acct in breeze_icici.valid_account_ids():
        connected = breeze_icici.get_client(acct) is not None
        result[acct] = {
            "sdkInstalled": breeze_icici.sdk_installed(),
            "configured": breeze_icici.is_configured(acct),
            "connected": connected,
            "name": (breeze_icici.get_account_name(acct) if connected else None) or breeze_icici.stored_account_name(acct),
            "label": breeze_icici.account_label(acct),
            "custom": not breeze_icici.is_env_account(acct),
            "loginUrl": breeze_icici.login_url(acct),
            "callbackUrl": _breeze_callback_url_for_acct(acct),
        }
    return jsonify({"accounts": result})


@bp.route("/api/breeze/accounts", methods=["POST"])
def breeze_add_account():
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "").strip()
    api_key = (data.get("apiKey") or data.get("api_key") or "").strip()
    api_secret = (data.get("apiSecret") or data.get("api_secret") or "").strip()
    try:
        new_id = breeze_icici.add_db_account(label, api_key, api_secret)
        return jsonify({"success": True, "id": new_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/breeze/accounts/<acct>", methods=["DELETE"])
def breeze_delete_account(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    try:
        breeze_icici.delete_db_account(acct)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/breeze/disconnect/<acct>", methods=["POST"])
def breeze_disconnect(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.valid_account_ids():
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    breeze_icici.disconnect(acct)
    return jsonify({"success": True})


@bp.route("/api/breeze/portfolio-holdings/<acct>", methods=["GET"])
def breeze_portfolio_holdings(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.valid_account_ids():
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    exchange_code = (request.args.get("exchange_code") or request.args.get("exchangeCode") or "").strip()
    if not exchange_code:
        return jsonify({"error": "exchange_code is required"}), 400
    from_date = (request.args.get("from_date") or request.args.get("fromDate") or "").strip()
    to_date = (request.args.get("to_date") or request.args.get("toDate") or "").strip()
    stock_code = (request.args.get("stock_code") or request.args.get("stockCode") or "").strip()
    portfolio_type = (request.args.get("portfolio_type") or request.args.get("portfolioType") or "").strip()
    try:
        return jsonify(
            breeze_icici.api_get_portfolio_holdings(exchange_code, from_date, to_date, stock_code, portfolio_type, acct=acct)
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/breeze/portfolio-positions/<acct>", methods=["GET"])
def breeze_portfolio_positions(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.valid_account_ids():
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    try:
        return jsonify(breeze_icici.api_get_portfolio_positions(acct))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/breeze/demat-holdings/<acct>", methods=["GET"])
def breeze_demat_holdings(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.valid_account_ids():
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    try:
        return jsonify(breeze_icici.api_get_demat_holdings(acct))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/breeze/mf-holdings/<acct>", methods=["GET"])
def breeze_mf_holdings(acct):
    if breeze_icici is None:
        return jsonify({"error": "Breeze module unavailable"}), 500
    if acct not in breeze_icici.valid_account_ids():
        return jsonify({"error": f"Invalid account id: {acct}"}), 400
    portfolio_type = (request.args.get("portfolio_type") or request.args.get("portfolioType") or "A").strip()
    try:
        return jsonify(breeze_icici.api_get_mf_holdings(acct, portfolio_type))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


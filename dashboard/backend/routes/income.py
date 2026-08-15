"""/api/income blueprint: expected dividend / REIT & InvIT distribution income."""
from flask import Blueprint, jsonify, request

from services import income as svc

bp = Blueprint("income", __name__)


@bp.route("/api/income/resolve", methods=["POST"])
def resolve():
    """Body: { items: [{ key, symbolHint }] } -> annual payout per unit for each key."""
    body = request.get_json(silent=True) or {}
    try:
        return jsonify({"payouts": svc.resolve(body.get("items"))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/income/overrides", methods=["GET"])
def list_overrides():
    try:
        return jsonify({"overrides": svc.list_overrides()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/income/overrides/<key>", methods=["PUT"])
def set_override(key):
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(svc.set_override(
            key,
            yahoo_symbol=body.get("yahooSymbol"),
            annual_payout=body.get("annualPayout"),
        ))
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/income/overrides/<key>", methods=["DELETE"])
def delete_override(key):
    """Clear an override so the payout falls back to the auto-fetched value."""
    try:
        svc.delete_override(key)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

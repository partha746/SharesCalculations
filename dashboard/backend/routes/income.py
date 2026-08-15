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
    """Partial update: only the keys present in the body are written."""
    body = request.get_json(silent=True) or {}
    fields = {}
    if "yahooSymbol" in body:
        fields["yahoo_symbol"] = body["yahooSymbol"]
    if "annualPayout" in body:
        fields["annual_payout"] = body["annualPayout"]
    try:
        return jsonify(svc.set_override(key, **fields))
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

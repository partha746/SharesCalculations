"""/api/mf blueprint: manual mutual-fund holdings + live NAV (AMFI via mfapi.in)."""
from flask import Blueprint, jsonify, request

from services import mf as mf_service

bp = Blueprint("mf", __name__)


@bp.route("/api/mf/search", methods=["GET"])
def mf_search():
    q = request.args.get("q", default="", type=str)
    try:
        return jsonify(mf_service.search_schemes(q))
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@bp.route("/api/mf/holdings", methods=["GET"])
def mf_holdings():
    account = request.args.get("account", default=None, type=str)
    try:
        return jsonify(mf_service.list_holdings(account=account))
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@bp.route("/api/mf/holdings", methods=["POST"])
def mf_add_holding():
    body = request.get_json(silent=True) or {}
    try:
        new_id = mf_service.add_holding(
            scheme_code=body.get("schemeCode"),
            scheme_name=body.get("schemeName"),
            units=body.get("units"),
            invested=body.get("invested"),
            folio=body.get("folio"),
            account=body.get("account") or "1",
        )
        return jsonify({"id": new_id}), 201
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/mf/import", methods=["POST"])
def mf_import():
    body = request.get_json(silent=True) or {}
    csv_text = body.get("csv") or ""
    account = str(body.get("account") or "1")
    if not csv_text.strip():
        return jsonify({"error": "csv is required"}), 400
    try:
        return jsonify(mf_service.import_csv(csv_text, account=account))
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/mf/holdings/<int:holding_id>", methods=["DELETE"])
def mf_delete_holding(holding_id):
    try:
        deleted = mf_service.delete_holding(holding_id)
        if deleted == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

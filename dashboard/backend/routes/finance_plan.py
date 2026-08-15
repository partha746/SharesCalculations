"""/api/finance-plan blueprint: save/load the financial-planning snapshot."""
from flask import Blueprint, jsonify, request

from services import finance_plan as svc

bp = Blueprint("finance_plan", __name__)


@bp.route("/api/finance-plan", methods=["GET"])
def get_plan():
    try:
        return jsonify(svc.get_plan())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/finance-plan", methods=["POST"])
def save_plan():
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(svc.save_plan(body.get("data")))
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

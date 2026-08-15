"""/api/earmarks blueprint: shares reserved (planned) for sale at a target price."""
from flask import Blueprint, jsonify, request

from services import earmarks as earmarks_service

bp = Blueprint("earmarks", __name__)


@bp.route("/api/earmarks", methods=["GET"])
def list_earmarks():
    try:
        return jsonify({"earmarks": earmarks_service.list_earmarks()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/earmarks", methods=["POST"])
def add_earmarks():
    body = request.get_json(silent=True) or {}
    try:
        result = earmarks_service.add_earmarks(
            price_usd=body.get("priceUsd"),
            allocations=body.get("allocations"),
            label=body.get("label") or "",
        )
        return jsonify(result), 201
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/earmarks/batch/<batch_id>", methods=["DELETE"])
def delete_batch(batch_id):
    try:
        deleted = earmarks_service.delete_batch(batch_id)
        if deleted == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/earmarks/<int:earmark_id>", methods=["DELETE"])
def delete_earmark(earmark_id):
    try:
        deleted = earmarks_service.delete_earmark(earmark_id)
        if deleted == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

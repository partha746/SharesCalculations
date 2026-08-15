"""/api/networth blueprint: manual net-worth line items (assets & liabilities)."""
from flask import Blueprint, jsonify, request

from services import networth as svc

bp = Blueprint("networth", __name__)


@bp.route("/api/networth/items", methods=["GET"])
def list_items():
    try:
        return jsonify({"items": svc.list_items()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/networth/items", methods=["POST"])
def add_item():
    body = request.get_json(silent=True) or {}
    try:
        new_id = svc.add_item(
            label=body.get("label"),
            category=body.get("category"),
            liquidity=body.get("liquidity"),
            kind=body.get("kind"),
            value_inr=body.get("valueInr"),
            note=body.get("note"),
        )
        return jsonify({"id": new_id}), 201
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/networth/items/<int:item_id>", methods=["PUT"])
def update_item(item_id):
    body = request.get_json(silent=True) or {}
    try:
        updated = svc.update_item(
            item_id,
            label=body.get("label"),
            category=body.get("category"),
            liquidity=body.get("liquidity"),
            kind=body.get("kind"),
            value_inr=body.get("valueInr"),
            note=body.get("note"),
        )
        if updated == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/networth/items/<int:item_id>", methods=["DELETE"])
def delete_item(item_id):
    try:
        if svc.delete_item(item_id) == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

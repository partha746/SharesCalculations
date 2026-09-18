"""/api/advance-tax/payments blueprint: advance-tax amounts actually paid, per FY."""
from flask import Blueprint, jsonify, request

from services import advance_tax as advance_tax_service

bp = Blueprint("advance_tax", __name__)


@bp.route("/api/advance-tax/payments", methods=["GET"])
def list_payments():
    fy = request.args.get("fy", default=None, type=int)
    try:
        return jsonify({"payments": advance_tax_service.list_payments(fy)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/advance-tax/payments", methods=["POST"])
def add_payment():
    body = request.get_json(silent=True) or {}
    try:
        new_id = advance_tax_service.add_payment(
            paid_on=body.get("paidOn"),
            amount_inr=body.get("amountInr"),
            fy_start_year=body.get("fyStartYear"),
            note=body.get("note") or "",
        )
        return jsonify({"id": new_id}), 201
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/advance-tax/payments/<int:payment_id>", methods=["PUT"])
def update_payment(payment_id):
    body = request.get_json(silent=True) or {}
    try:
        updated = advance_tax_service.update_payment(
            payment_id,
            paid_on=body.get("paidOn"),
            amount_inr=body.get("amountInr"),
            note=body.get("note"),
            fy_start_year=body.get("fyStartYear"),
        )
        if updated == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/advance-tax/payments/<int:payment_id>", methods=["DELETE"])
def delete_payment(payment_id):
    try:
        deleted = advance_tax_service.delete_payment(payment_id)
        if deleted == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

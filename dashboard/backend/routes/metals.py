"""/api/metals blueprint: Pune gold/silver rates, recorded history and holdings lots."""
from flask import Blueprint, jsonify, request

from services import metals as metals_service

bp = Blueprint("metals", __name__)


@bp.route("/api/metals", methods=["GET"])
def get_metals():
    """Current Pune rates with each series' day range and change."""
    try:
        return jsonify(metals_service.current())
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@bp.route("/api/metals/history", methods=["GET"])
def get_metals_history():
    """Recorded points for one series. Params: metal, from, to (epoch ms), maxPoints."""
    metal = request.args.get("metal", default="gold24k", type=str)

    def _ms(name):
        raw = request.args.get(name)
        try:
            return int(float(raw)) if raw not in (None, "") else None
        except (TypeError, ValueError):
            return None

    try:
        return jsonify(metals_service.history(
            metal, from_ms=_ms("from"), to_ms=_ms("to"),
            max_points=min(5000, max(50, request.args.get("maxPoints", default=1500, type=int))),
        ))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/metals/holdings", methods=["GET"])
def list_metal_holdings():
    try:
        return jsonify(metals_service.list_holdings())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/metals/holdings", methods=["POST"])
def add_metal_holding():
    body = request.get_json(silent=True) or {}
    try:
        new_id = metals_service.add_holding(
            metal=body.get("metal"),
            grams=body.get("grams"),
            price_paid_per_gram=body.get("pricePaidPerGram"),
            buy_date=body.get("buyDate"),
            note=body.get("note") or "",
        )
        return jsonify({"id": new_id}), 201
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/metals/holdings/<int:holding_id>", methods=["PUT"])
def update_metal_holding(holding_id):
    body = request.get_json(silent=True) or {}
    try:
        updated = metals_service.update_holding(
            holding_id,
            metal=body.get("metal"),
            grams=body.get("grams"),
            price_paid_per_gram=body.get("pricePaidPerGram"),
            buy_date=body.get("buyDate"),
            note=body.get("note"),
        )
        if updated == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except (ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/metals/holdings/<int:holding_id>", methods=["DELETE"])
def delete_metal_holding(holding_id):
    try:
        if metals_service.delete_holding(holding_id) == 0:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

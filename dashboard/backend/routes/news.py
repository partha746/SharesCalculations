"""/api/news blueprint: latest NVIDIA news + sentiment."""
from flask import Blueprint, jsonify, request

from services.news import build_news_response

bp = Blueprint("news", __name__)


@bp.route("/api/news", methods=["GET"])
def get_news():
    days = request.args.get("days", default=7, type=int)
    try:
        return jsonify(build_news_response(days=days))
    except Exception as e:
        return jsonify({"error": str(e)}), 503

"""/api/news blueprint: latest NVIDIA news + sentiment."""
from flask import Blueprint, jsonify, request

from services.news import build_news_response

bp = Blueprint("news", __name__)


@bp.route("/api/news", methods=["GET"])
def get_news():
    days = request.args.get("days", default=7, type=int)
    # 0 shows the raw feed, which is useful for judging what the curation removed.
    min_relevance = request.args.get("minRelevance", default=None, type=float)
    try:
        return jsonify(build_news_response(days=days, min_relevance=min_relevance))
    except Exception as e:
        return jsonify({"error": str(e)}), 503
